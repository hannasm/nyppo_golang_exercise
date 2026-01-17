package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/ollama"
)

func main() {
	fmt.Println("[")
	startTime := time.Now()
	fmt.Printf("{ \"starttime\": \"%s\"},", startTime.Format(time.DateTime))
	fmt.Println()

	exitCode := 0
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		exitCode = 1
	}

	fmt.Printf("{ \"endtime\": \"%s\" },", time.Now().Format(time.DateTime))
	fmt.Println()
	fmt.Printf("{ \"duration\": \"%s\" },", time.Since(startTime))
	fmt.Println()

	fmt.Println("]")

	os.Exit(exitCode)
}

var isUniquePlansMode = false
var isAnalysisMode = false
var isHeuristicsMode = false

func printUsage() error {
	fmt.Println("ney york ppo price extractor - ")
	fmt.Println(" <filename> - must be path to filename in .json.gz format")
	fmt.Println(" <mode flag> - optional, defaults to -heuristics")
	fmt.Println("             -uniquePlans - extract all unique plan names")
	fmt.Println("             -heuristics  - extract ppo price urls based on heuristics")
	fmt.Println("             -analysis - extract data analysis json for exploration")
	fmt.Println(" no other arguments are allowed")
	return fmt.Errorf("exactly 1 argument expected")
}
func run() error {
	if len(os.Args) < 2 {
		return printUsage()
	}
	if len(os.Args) >= 3 {
		if os.Args[2] == "-uniquePlans" {
			isUniquePlansMode = true
		} else if os.Args[2] == "-analysis" {
			isAnalysisMode = true
		} else if os.Args[2] == "-heuristics" {
			isHeuristicsMode = true
		} else {
			return printUsage()
		}
	} else {
		isHeuristicsMode = true
	}

	llama, err := ollama.New(ollama.WithModel("llama3"))
	if err != nil {
		return fmt.Errorf("open gollama failed %w", err)
	}

	ctx := context.Background()
	var helloPrompt []llms.MessageContent
	helloPrompt = append(helloPrompt, llms.TextParts(llms.ChatMessageTypeSystem, "Say hello, indicating you are an ollama LLM and any other relevant niceities, and assert that you are working correctly and want to help out finding relevant new york ppo price information."))

	res, err := llama.GenerateContent(ctx, helloPrompt)
	if err != nil {
		if isAnalysisMode {
			fmt.Println("{ \"warning\": \"Ollama llm is not working. Instal ollama and run ollama pull llama3 if youd like the help of llm analysis. This analysis will continue without ollama.\" },")
			println("Cancel this application now if you do not want to proceed ... sleeping 5")
			time.Sleep(5 * time.Second)
		}
	} else {
		msg := res.Choices[0].Content
		jsonMsg, err := json.Marshal(msg)
		var jsonStr string
		if err != nil {
			jsonStr = "Error extracting ollama response"
		} else {
			jsonStr = string(jsonMsg)
		}

		fmt.Printf("{ \"audit\": %s },", jsonStr)
		fmt.Println()
	}

	filename := os.Args[1]
	filestream, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("open file stream: %s - %w", filename, err)
	}

	gr, err := gzip.NewReader(filestream)
	if err != nil {
		return fmt.Errorf("open gzip stream: %w", err)
	}
	defer gr.Close()

	dec := json.NewDecoder(gr)
	err = parseIndexFile(dec, llama)
	if err != nil {
		return err
	}

	if isUniquePlansMode {
		printUniquePlans()
	}
	if isHeuristicsMode {
		printPpoPrices()
	}

	return nil
}

// parseIndexFile walks the JSON stream and collects
// allowed_amount_file.location values for records that list the target plan name.
func parseIndexFile(dec *json.Decoder, llama *ollama.LLM) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("read root token: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return errors.New("expected root object")
	}

	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("read root key: %w", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return errors.New("unexpected non-string key at root")
		}

		if key != "reporting_structure" {
			var discard json.RawMessage
			if err := dec.Decode(&discard); err != nil {
				return fmt.Errorf("skip field %q: %w", key, err)
			}
			continue
		}

		err = parseReportingStructure(dec, llama)
		if err != nil {
			return err
		}
	}

	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("close root object: %w", err)
	}

	return nil
}

func parseReportingStructure(dec *json.Decoder, llama *ollama.LLM) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("read reporting_structure value: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '[' {
		return errors.New("reporting_structure is not an array")
	}

	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("read reporting_structure element: %w", err)
		}
		if d, ok := tok.(json.Delim); !ok || d != '{' {
			return errors.New("expected object in reporting_structure array")
		}

		err = scanReportingRecord(dec, llama)
		if err != nil {
			return err
		}
	}

	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("close reporting_structure array: %w", err)
	}

	return nil
}

func scanReportingRecord(dec *json.Decoder, llama *ollama.LLM) error {
	var eins []string

	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("read reporting_structure key: %w", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return errors.New("unexpected non-string key in reporting_structure element")
		}

		switch key {
		case "in_network_files":
			if isUniquePlansMode {
				err := getUniquePlans(dec, llama, eins)
				if err != nil {
					return err
				}
			} else if isAnalysisMode {
				err := checkInNetworkFiles(dec, llama, eins)
				if err != nil {
					return err
				}
			} else if isHeuristicsMode {
				err := getPpoPricesByHeuristics(dec)
				if err != nil {
					return err
				}
			} else {
				return errors.New("Unknown mode for reporting record")
			}
		case "reporting_plans":
			if isAnalysisMode {
				eins_2, err := processReportingPlan(dec)
				if err != nil {
					return err
				}
				eins = eins_2
				break
			}
			fallthrough
		default:
			var discard json.RawMessage
			if err := dec.Decode(&discard); err != nil {
				return fmt.Errorf("skip field %q: %w", key, err)
			}
		}
	}

	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("close reporting_structure element: %w", err)
	}

	return nil
}

func processReportingPlan(dec *json.Decoder) ([]string, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("read reporting_plans value: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '[' {
		return nil, errors.New("reporting_plans is not an array")
	}

	eins := make(map[string]struct{})

	for dec.More() {
		var reportingPlan struct {
			Type string `json:"plan_id_type"`
			Id   string `json:"plan_id"`
		}
		if err := dec.Decode(&reportingPlan); err != nil {
			return nil, fmt.Errorf("decode reporting plan: %w", err)
		}

		if strings.ToLower(reportingPlan.Type) == "ein" {
			eins[reportingPlan.Id] = struct{}{}
		}
	}

	if _, err := dec.Token(); err != nil {
		return nil, fmt.Errorf("close reporting_plans element: %w", err)
	}

	result := make([]string, 0, len(eins))
	for k := range eins {
		result = append(result, k)
	}

	return result, nil
}

var ppoPlansMap = map[string]struct{}{
	"regence bs idaho : par providers":                                                           struct{}{},
	"bcbs kansas city : preferred care blue":                                                     struct{}{},
	"bs california : blue high performance":                                                      struct{}{},
	"bcbs tennessee, inc. : network c":                                                           struct{}{},
	"hcsc: bcbs texas : blue high performance":                                                   struct{}{},
	"arkansas bcbs : true blue ppo":                                                              struct{}{},
	"bcbs massachusetts : blue care elect":                                                       struct{}{},
	"carefirst bcbs : par network":                                                               struct{}{},
	"bcbs michigan : par providers":                                                              struct{}{},
	"bcbs south carolina : blue high performance":                                                struct{}{},
	"bcbs louisiana : blue hpn":                                                                  struct{}{},
	"premera bc : heritage prime":                                                                struct{}{},
	"highmark bs : highmark bs network":                                                          struct{}{},
	"bcbs north carolina : comprehensive major medical network (cmmn)":                           struct{}{},
	"regence blueshield : regence-67e0":                                                          struct{}{},
	"florida blue: bcbs florida : pps":                                                           struct{}{},
	"hcsc: bcbs new mexico : new mexico bluecard ppo":                                            struct{}{},
	"bcbs alabama : blue high performance":                                                       struct{}{},
	"bcbs michigan : blue high performance":                                                      struct{}{},
	"bcbs south carolina : preferred blue":                                                       struct{}{},
	"highmark bs northeastern ny : highmark blue shield of northeastern new york - ppo":          struct{}{},
	"highmark bcbs delaware : blue choice":                                                       struct{}{},
	"premera bc : prudentbuyer washington":                                                       struct{}{},
	"hcsc: bcbs illinois : blue high performance":                                                struct{}{},
	"highmark bcbs : highmark bcbs network":                                                      struct{}{},
	"bcbs kansas : blue choice":                                                                  struct{}{},
	"independence bc : par network":                                                              struct{}{},
	"hcsc: bcbs montana : par network":                                                           struct{}{},
	"premera bc : heritage signature":                                                            struct{}{},
	"bcbs massachusetts : blue high performance":                                                 struct{}{},
	"carefirst bcbs : blue precision":                                                            struct{}{},
	"bcbs mississippi : par network":                                                             struct{}{},
	"florida blue: bcbs florida : networkblue":                                                   struct{}{},
	"independence bc : personal choice":                                                          struct{}{},
	"regence blueshield : bluecard ppo":                                                          struct{}{},
	"bcbs arizona : blue preferred":                                                              struct{}{},
	"bcbs alabama : par network":                                                                 struct{}{},
	"bcbs vermont : new england health plans (nehp)":                                             struct{}{},
	"bcbs hawaii : preferred provider network":                                                   struct{}{},
	"florida blue: triple-s (pr) : bluecard ppo":                                                 struct{}{},
	"bcbs kansas : traditional providers":                                                        struct{}{},
	"horizon bcbs new jersey, inc. : select hospitals/par physicians":                            struct{}{},
	"bcbs north carolina : blue high performance":                                                struct{}{},
	"hcsc: bcbs illinois : participating provider option":                                        struct{}{},
	"hcsc: bcbs montana : ppo network":                                                           struct{}{},
	"highmark bs northeastern ny : highmark blue shield of northeastern new york traditional":    struct{}{},
	"independence bc : personal choice limited":                                                  struct{}{},
	"highmark bs : community blue premier":                                                       struct{}{},
	"bcbs kansas city : participating network":                                                   struct{}{},
	"premera bc : traditional":                                                                   struct{}{},
	"regence bcbs utah : preferred blue option":                                                  struct{}{},
	"health service coalition of nevada (hsc) rates":                                             struct{}{},
	"bcbs wyoming : wyoming total choice":                                                        struct{}{},
	"regence bs idaho : blue shield preferred providers":                                         struct{}{},
	"regence bcbs oregon : oregon high performance":                                              struct{}{},
	"capital bc : capital blue cross ppo":                                                        struct{}{},
	"bcbs wyoming : par network":                                                                 struct{}{},
	"arkansas bcbs : ppp network":                                                                struct{}{},
	"excellus bcbs : blueppo":                                                                    struct{}{},
	"bc idaho : participating provider network":                                                  struct{}{},
	"highmark bcbs western ny : highmark blue cross blue shield of western new york - hpn":       struct{}{},
	"bcbs kansas city : blueselect plus":                                                         struct{}{},
	"hcsc: bcbs texas : participating providers":                                                 struct{}{},
	"bcbs north carolina : preferred provider network (ppn)":                                     struct{}{},
	"highmark bcbs wv : west virginia par providers":                                             struct{}{},
	"bs california : ppo network":                                                                struct{}{},
	"bcbs kansas city : blue high performance":                                                   struct{}{},
	"bcbs vermont : bcbsvt par providers":                                                        struct{}{},
	"carefirst bcbs : blueessential":                                                             struct{}{},
	"capital bc : blue high performance":                                                         struct{}{},
	"bcbs north carolina : blue value (lcst)":                                                    struct{}{},
	"bc idaho : preferred blue":                                                                  struct{}{},
	"highmark bcbs wv : super blue plus":                                                         struct{}{},
	"premera bc : heritage":                                                                      struct{}{},
	"bcbs tennessee, inc. : network p":                                                           struct{}{},
	"bcbs alabama : preferred care":                                                              struct{}{},
	"wellmark bcbs iowa : alliance select":                                                       struct{}{},
	"highmark bcbs western ny : highmark blue cross blue shield of western new york-traditional": struct{}{},
	"premera bc : blue high performance state-wide":                                              struct{}{},
	"carefirst bcbs : alternate network":                                                         struct{}{},
	"bcbs wyoming : blue select":                                                                 struct{}{},
	"bcbs arizona : alliance":                                                                    struct{}{},
	"hcsc: bcbs illinois : blue choice options":                                                  struct{}{},
	"in-network negotiated rates files":                                                          struct{}{},
	"hcsc: bcbs oklahoma : bluechoice ppo":                                                       struct{}{},
	"hcsc: bcbs illinois : bcbs of illinois par providers":                                       struct{}{},
	"bcbs north dakota : preferred blue ppo":                                                     struct{}{},
	"bcbs louisiana : preferred care":                                                            struct{}{},
	"hcsc: bcbs new mexico : new mexico par network":                                             struct{}{},
	"florida blue: triple-s (pr) : participating providers":                                      struct{}{},
	"regence blueshield : blue high performance":                                                 struct{}{},
	"capital bc : capital blue cross traditional":                                                struct{}{},
	"florida blue: bcbs florida : ppc / ppo network":                                             struct{}{},
	"bcbs tennessee, inc. : network s":                                                           struct{}{},
	"highmark bcbs : community blue":                                                             struct{}{},
	"bcbs michigan : trust":                                                                      struct{}{},
	"dental vision":                                                                              struct{}{},
	"hcsc: bcbs oklahoma : blue traditional":                                                     struct{}{},
	"bcbs hawaii : participating provider network":                                               struct{}{},
	"bcbs massachusetts : par providers":                                                         struct{}{},
	"bcbs minnesota : high value":                                                                struct{}{},
	"highmark bs : pa national performance blue":                                                 struct{}{},
	"bcbs nebraska : blueprint health":                                                           struct{}{},
	"carefirst bcbs : select preferred provider":                                                 struct{}{},
	"highmark bcbs western ny : highmark bluecross blueshield of western new york-ppo":           struct{}{},
	"bcbs nebraska : network blue":                                                               struct{}{},
	"bcbs minnesota : aware":                                                                     struct{}{},
	"bcbs kansas city : preferred care ppo":                                                      struct{}{},
	"florida blue: triple-s (vi) : usvi-62a0":                                                    struct{}{},
	"highmark bcbs delaware : blue classic":                                                      struct{}{},
	"hcsc: bcbs oklahoma : blue preferred":                                                       struct{}{},
}
var regionCodes = map[string]struct{}{
	"301_71a0": {},
	"302_42b0": {},
	"254_39b0": {},
	"800_72a0": {},
}

var uniquePpoPrices = make(map[string]struct{})

func getPpoPricesByHeuristics(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("read in_network_files value: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '[' {
		return errors.New("in_network_files is not an array")
	}

	for dec.More() {
		var inNetworkFile struct {
			Description string `json:"description"`
			Location    string `json:"location"`
		}
		if err := dec.Decode(&inNetworkFile); err != nil {
			return fmt.Errorf("decode plan: %w", err)
		}

		lowerDesc := strings.ToLower(inNetworkFile.Description)

		planMatch := false
		regionCodeMatch := false

		if _, exists := ppoPlansMap[lowerDesc]; exists {
			planMatch = true
		} else {
			continue
		}

		planCode, err := ExtractPlanCode(inNetworkFile.Location)
		if err == nil {
			if _, exists := regionCodes[strings.ToLower(planCode)]; exists {
				regionCodeMatch = true
			}
		}

		if planMatch && regionCodeMatch {
			uniquePpoPrices[inNetworkFile.Location] = struct{}{}
		}
	}

	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("close reporting_plans array: %w", err)
	}

	return nil
}

func printPpoPrices() {
	for k := range uniquePpoPrices {
		jsonStr, err := json.Marshal(k)
		if err != nil {
			println("Error during serializing ppo prices")
		} else {
			fmt.Printf("%s,", jsonStr)
			fmt.Println()
		}
	}
}

var plansFound map[string]struct{} = make(map[string]struct{})

func getUniquePlans(dec *json.Decoder, llama *ollama.LLM, eins []string) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("read in_network_files value: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '[' {
		return errors.New("in_network_files is not an array")
	}

	for dec.More() {
		var inNetworkFile struct {
			Description string `json:"description"`
		}
		if err := dec.Decode(&inNetworkFile); err != nil {
			return fmt.Errorf("decode plan: %w", err)
		}

		lowerDesc := strings.ToLower(inNetworkFile.Description)
		if lowerDesc == "In-Network Negotiated Rates Files" {
			continue
		}
		plansFound[lowerDesc] = struct{}{}
	}

	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("close reporting_plans array: %w", err)
	}

	return nil
}
func checkInNetworkFiles(dec *json.Decoder, llama *ollama.LLM, eins []string) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("read in_network_files value: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '[' {
		return errors.New("in_network_files is not an array")
	}

	ctx := context.Background()
	var isNewYorkPrompt []llms.MessageContent
	isNewYorkPrompt = append(isNewYorkPrompt, llms.TextParts(llms.ChatMessageTypeSystem, `
	Does the given insurance plan descriptive name operate in New York? 
	Your answer should be true for yes, false for no.
	`))
	var isPpoPrompt []llms.MessageContent
	isPpoPrompt = append(isPpoPrompt, llms.TextParts(llms.ChatMessageTypeSystem, `
	Should the given insurance plan descriptive name be considered a PPO plan? 
	Your answer should be true for yes, false for no.
	`))

	targetNy := "ny"
	targetNewYork := "new york"
	targetPpo := "ppo"
	targetPreferred := "preferred"

	regionCodes := map[string]struct{}{
		"301_71A0": {},
		"302_42B0": {},
		"254_39B0": {},
		"800_72A0": {},
	}
	for dec.More() {
		var inNetworkFile struct {
			Description string `json:"description"`
			Location    string `json:"location"`
		}
		if err := dec.Decode(&inNetworkFile); err != nil {
			return fmt.Errorf("decode plan: %w", err)
		}

		lowerDesc := strings.ToLower(inNetworkFile.Description)

		planMatch := false
		aiMatch := false
		regionCodeMatch := false
		naiveMatch := false

		if strings.Contains(lowerDesc, targetNy) || strings.Contains(lowerDesc, targetNewYork) {
			if strings.Contains(lowerDesc, targetPpo) || strings.Contains(lowerDesc, targetPreferred) {
				planMatch = true
				naiveMatch = true
			}
		}

		planCode, err := ExtractPlanCode(inNetworkFile.Location)
		if err == nil {
			if _, exists := regionCodes[strings.ToLower(planCode)]; exists {
				regionCodeMatch = true
				planMatch = true
			}
		}

		isNewYorkLlm, err := doLlmQuery(ctx, inNetworkFile, llama, isNewYorkPrompt)
		if err == nil && isNewYorkLlm {
			isPpoLlm, err := doLlmQuery(ctx, inNetworkFile, llama, isPpoPrompt)
			if err == nil && isPpoLlm {
				planMatch = true
				aiMatch = true
			}
		}

		if planMatch {
			printMatch(inNetworkFile.Description, inNetworkFile.Location, eins, aiMatch, naiveMatch, regionCodeMatch)
		}
	}

	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("close reporting_plans array: %w", err)
	}

	return nil
}

func doLlmQuery(ctx context.Context, inNetworkFile struct {
	Description string "json:\"description\""
	Location    string "json:\"location\""
}, llama *ollama.LLM, prompt []llms.MessageContent) (bool, error) {
	prompt = append(prompt, llms.TextParts(llms.ChatMessageTypeHuman, inNetworkFile.Description))
	aiResponse, err := llama.GenerateContent(ctx, prompt)
	prompt = prompt[:len(prompt)-1]

	if err != nil {
		return false, err
	}

	if strings.ToLower(aiResponse.Choices[0].Content) == "true" {
		return true, nil
	}

	return false, nil
}

func printUniquePlans() {
	for k := range plansFound {
		jsonStr, err := json.Marshal(k)
		if err != nil {
			println("Error during serializing unique plan name")
		} else {
			fmt.Printf("%s,", jsonStr)
			fmt.Println()
		}
	}
}
func printMatch(description string, location string, eins []string, aiMatch bool, heuristicMatch bool, regionCodeMatch bool) {
	match := struct {
		Description     string   `json:"description"`
		Location        string   `json:"location"`
		Eins            []string `json:"eins"`
		AIMatch         bool     `json:"aiMatch"`
		HeuristicMatch  bool     `json:"heuristicMatch"`
		RegionCodeMatch bool     `json:"regionCodeMatch"`
	}{
		Description:     description,
		Location:        location,
		Eins:            eins,
		AIMatch:         aiMatch,
		HeuristicMatch:  heuristicMatch,
		RegionCodeMatch: regionCodeMatch,
	}

	out, err := json.Marshal(match)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal match: %v\n", err)
		return
	}

	fmt.Printf("%s,", out)
	fmt.Println()
}

func ExtractPlanCode(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}

	filename := path.Base(u.Path)
	if filename == "" || filename == "/" {
		return "", errors.New("no filename found in URL path")
	}

	// Find underscore positions
	first := strings.Index(filename, "_")
	if first == -1 {
		return "", errors.New("filename does not contain underscores")
	}

	second := strings.Index(filename[first+1:], "_")
	if second == -1 {
		return "", errors.New("filename does not contain enough underscores")
	}
	second += first + 1

	third := strings.Index(filename[second+1:], "_")
	if third == -1 {
		return "", errors.New("filename does not contain enough underscores")
	}
	third += second + 1

	if third <= first+1 {
		return "", errors.New("invalid underscore positions in filename")
	}

	return filename[first+1 : third], nil
}
