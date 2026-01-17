# New York PPO Price Extractor
The basic idea behind my solution was to read the file using streaming parsers and extract the appropriate data using some basic heuristics. 

The complexity comes from figuring out those basic heuristics, which followed a few steps:

    * Basic data analysis, read the file, print out some data in a format that was focused on the information i thought was relevant (ein, plan name, filename) while filtering out the fluff
        * Test out whether the AI / llm based approach was useful at all
        * Figure out the region codes on the pricing filenames
        * Figure out the PPO plans to match against
        * I never allowed this phase to run to completion, just used it to pull some data samples
    * GEnerate unique plan names
        * Getting unique plan names is the first step
        * then run an LLM pass on those names to identify the ones that are PPO (i did this from the commandline  but it would make sense to add this to the golang program if it was going to be used more often)
        * extract unique PPO plan names into a map for use as a basic heuristic for PPO plans
    * Associate region codes in the pricing filenames
        * Thanks to hint in take-home notes, the anthem website shows state codes even though the filenames use some sort of coding system for the states
        * Pull a variety of eins from the analysis file, 
        * look them up on the anthem website, 
        * pull the filenames of NY state variants and extract the region codes
        * throw the ones identified into a golang map as a basic heuristic for matching NY state pricing files
    * Using the same JSON parser, run heuristic matching against each pricing file entry and print out the ones that are both NY and PPO

Some other things i had to implement include:

    * Basic command line argument parser (theres likely something better in golang but i just threw it together)
    * Performance tracker to measure runtime (requested in take-home assignment)
    * Emitting the data as a json event stream allows easier parsing by other json tools (powershell / jq.exe)
    * Combining the analysis phase code, the unique plan name generator, and the final heuristic matchign into a single program vs. making multiple applications

## Heuristic Matching
Heuristic matching uses basic string comparisons, matching pre-determined plan names to identify PPO plans, and matching the predetermined region codes to identify regional pricing files. The data is stored in maps for efficient retrieval and are stored lowercase, more because that's a habit of how i would normally do things than because it's practically necesarry in this exercise.

## LLM Analysis
I setup some prompts using `langchaingo` and `ollama` running locally to assess whether the plan names can be detected correctly with this technique. The first and most noteworthy answer is that yes, the ollama LLM seems to encode some interesting details such as considering "high performance", "super blue plus", etc... (that are not specifically branded with a "ppo" monniker), as ppo plans (at least thats what my quick research in google indicated). 

It's also very clear that the LLM analysis is finnicky, slow, full of false positives and false negatives, etc... I chose to trust the output for this exercise but clearly in a business setting verification would be warranted for both false positives and false negatives...

It does seem interesting that this would be a useful tool for identifying potentially meaningful targets for enhancing the heuristic based detection system, espsecially when facing large amounts of incosistent data having many tools can help.

## Production pipeline
My assumptions for production would be:

    * New files need processed monthly
    * Database for storing plans and their status (such as new york and PPO) would be source of heuristics
    * Database for storing region codes mappings
    * ... Extracting the entire file into meaningful database storage for faster access... This depends on cost vs time constraints etc...
    * Pipeline
        * Ingest new file:
            * Matching Step - Read in-network plans from file and cross reference them with database, 
                * flag any not found for analysis, 
                * flag any that have dropped out of the file
                * flag updates to existing matches ... date changing vs. actual differences in employer plans and data filename structure might be significant... january is probably the hardest time of year for these ones... congrats its january right now!
            * depending on space and time requirements, this is a good time to ingest everything, specifically just pull all the urls / employers / etc... into some sort of system that doesn't require further json parsing and can get you to a place where you are in control of the the data vs. some document the insurance company is giving you
                * event sourcing?
                * customized tables per business needs?
                * something in between?
                * both?
        * Move data to Data Engineer sandbox
        * Analyze plans:
            * Research
                * using AI / LLM sources can be one avenue of attack
                * fuzzy name matching to address minor typos or other variance with existing records that are essentially false negatives from the matching step above
                * Use EIN lookup techniques as mentioned in assignment hints
                * Other techniques as they are discovered...
                * Other manual research and verification on anything that cannot be automated
            * Updates to database with results of research
            * Push to production with new heuristics
        * Generate new datasets
            * Ingest detailed pricing data from location urls
            * Create / update lookup tables and optimized mappings between plans and costs, using heuristics and analysis to create fast lookups
        * Queries happening in realtime based from API would rely on database schema that uses efficient table designs to support high performing queries against New York PPO plans as well as other variations of regions and plan types etc...
