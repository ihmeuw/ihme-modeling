GET_AGE_STARTS: str = "SELECT age_group_id, age_group_days_start FROM shared.age_group"

# NOTE: there is no need to reset autocommit, unique_checks etc. since infiles are the only
# commands executed in this session.
PREP_INFILE: str = (
    """
    SET autocommit=0;
    SET unique_checks=0;
    SET foreign_key_checks=0;
    COMMIT;
    """
)

INFILE: str = (
    """
    LOAD DATA
        INFILE "{path}"
        REPLACE
        INTO TABLE {table}
        FIELDS
            TERMINATED BY ","
            OPTIONALLY ENCLOSED BY '"'
        LINES
            TERMINATED BY "\\n"
        IGNORE 1 LINES;
    COMMIT;
    """
)
