GET_COMO_VERSION_ID: str = (
    'SELECT output_version_id '
    'FROM epi.output_version '
    'WHERE gbd_round_id = :gbd_round_id '
    'AND decomp_step_id = :decomp_step_id '
    'AND is_best = 1'
)

GET_RUN_ID: str = (
    'SELECT run_id '
    'FROM mortality.vw_decomp_process_version '
    'WHERE process_id = :process_id '
    'AND gbd_round_id = :gbd_round_id '
    'AND decomp_step_id = :decomp_step_id '
    'AND is_best = 1'
)

INFILE: str = (
    """
    SET autocommit=0;
    SET unique_checks=0;
    SET foreign_key_checks=0;
        LOAD DATA
            INFILE "{path}"
            REPLACE
            INTO TABLE gbd.output_hale_{single}_year_v{version}
            FIELDS
                TERMINATED BY ","
                OPTIONALLY ENCLOSED BY '"'
            LINES
                TERMINATED BY "\\n"
            IGNORE 1 LINES;
    SET foreign_key_checks=1;
    SET unique_checks=1;
    SET autocommit=1;
    COMMIT;"""
)
