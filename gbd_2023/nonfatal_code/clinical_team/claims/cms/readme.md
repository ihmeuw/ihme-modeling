## CMS related database and processing code

This directory will contain the code to handle any CMS specific tasks. The
general structure is as follows

- eda
    - exploratory data analysis scripts. These are not for production
- helpers
    - tools to process CMS related data or to make it easier to access
    - /intermediate_tables should contain nearly all the code to create the full CMS schema
    designed by the clinical team. The major exception is the claims_metadata table which was
    created within the CMS database by the db-dev team
- lookups/inputs
    - raw lookup tables from resdac.org. Usually stored as dicts or lists
- lookups/for_upload
    - contains the CSVs for upload of our 4-5 CMS ERD lookup tables [state, race,
    file source, cms facility and cms bundle maps]
- pre_db_processing
    - py code to modify SaS scripts which are used to extract the giant, headerless
    raw fixed-width files that are provided by CMS
    - also contains a test class to validate that the CSV extractions match the
    metadata defined in the .fts files provided
- src/pipeline
    - /api contains the internals module with the main CMS class that is called to process
    CMS data
    - /lib contains all of the helper code for api
- utils
    - logger, readers, and db connection class used in the pipeline but stored here. Otherwise
    this dir will contain additional helper utilities for creating the "intermediate" CMS tables
- validations
    - tools to validate CMS processing at various points. Intermediate plotting code is here
