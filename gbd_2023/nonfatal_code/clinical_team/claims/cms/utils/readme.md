## Utils packages

A package of modules to help with reading / transforming raw CMS data

### Modules
- config.ini
    - filepath configuration file that the Parser class uses. If you need a new "FILEPATH", add it here
- constants.py
    - Single source of truth for certain CMS data attributes. Things like years present for each system. States available.
    Diagnosis codes per file. Also contains mapping of how the data should be processed by the pipeline
    - This is a key file across nearly every CMS preparation step and would need to be updated
    if new data is added or for most methods changes
- logger.py
    - Used for logging in the CMS pipeline. Wraps Python's logging package
- parser.py
    - Class to call to use the config.ini file in code
- readers.py
    - Helper functions to wrap around pyarrow parquet readers.
- get_csv_paths.py
    - Used to collect file paths to .csv versions of the raw data and validates against the associated .fts file.

### Directories
- database
    - an ORM package to read from the CMS database. Note that a user *MUST* have privileges to access the CMS DB
- filters
    - looks like 1 script to filter by beneficiary eligibility
- transformations
    - helper modules used for transforming the raw data into different shapes/structures. 
