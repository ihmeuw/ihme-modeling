class Cause:
    """Cause-related queries."""

    GET_CORRECTION_EXCLUSION_MAP = """
        SELECT
            cause_id, location_id
        FROM
            cod.correction_exclusion_history ceh
        JOIN
            cod.correction_exclusion_version cev USING (correction_exclusion_version_id)
        WHERE
            cev.release_id = :release_id AND
            cev.is_best = 1
    """

    GET_METADATA = """
        SELECT
            cause_id,
            cause_metadata_type,
            cause_metadata_value
        FROM
            shared.cause_metadata_history
        JOIN
            shared.cause_metadata_type USING (cause_metadata_type_id)
        WHERE
            cause_metadata_version_id = :cause_metadata_version_id AND
            cause_id IN :valid_cause_ids
    """

    GET_METADATA_VERSION_ID = """
        SELECT
            cause_metadata_version_id
        FROM
            shared.cause_set_version csv
        WHERE
            cause_set_version_id = :cause_set_version_id
    """


class CoDCorrectVersion:
    """CodCorrect version-related queries for CoD output versions and GBD process versions."""

    GET_NEW_CODCORRECT_VERSION_ID = """
        SELECT
            MAX(CAST(val as INT)) + 1 as codcorrect_version_id
        FROM
            gbd.gbd_process_version_metadata
        JOIN
            gbd.gbd_process_version USING (gbd_process_version_id)
        WHERE
            gbd_process_id = 3 AND -- CodCorrect
            metadata_type_id = 1 -- CodCorrect version
    """

    GET_NEW_COD_OUTPUT_VERSION_ID = """
        SELECT MAX(LAST_INSERT_ID(output_version_id)) + 1 AS
        output_version_id FROM cod.output_version
    """

    INSERT_NEW_OUTPUT_VERSION_METADATA = """
        INSERT INTO cod.output_version_metadata (output_version_id, metadata_type_id, val)
        VALUES (:output_version_id, :metadata_type_id, :val)
    """

    INSERT_NEW_VERSION_ROW = """
        INSERT INTO cod.output_version (output_version_id, output_process_id, tool_type_id,
        release_id, description, code_version, env_version, status, is_best)
        VALUES (:output_version_id, :output_process_id, :tool_type_id,
        :release_id, :description, :code_version, :env_version, :status, :is_best)
    """

    UPDATE_STATUS = """
        UPDATE cod.output_version
        SET status = :status
        WHERE output_version_id = :output_version_id
    """


# TODO: consider combining this with spacetime queries to put all codcorrect dbs together
class Diagnostics:
    """Queries related to CodCorrect diagnostics."""

    DELETE_DIAGNOSTICS = """
        DELETE FROM
            codcorrect.diagnostic
        WHERE
            codcorrect_version_id = :codcorrect_version_id
    """

    INSERT_DIAGNOSTIC_ROW = """
        INSERT INTO codcorrect.diagnostic_version
            (codcorrect_version_id, is_best, inserted_by, last_updated_by)
        VALUES
            (:codcorrect_version_id, :is_best, :inserted_by, :last_updated_by)
    """

    ROW_COUNT = """
        SELECT
            COUNT(*)
        FROM
            codcorrect.diagnostic
        WHERE
            codcorrect_version_id = :codcorrect_version_id
    """


class GbdDatabase:
    """Queries related to the GBD database."""

    GET_COMPARE_VERSION_AND_DESCRIPTION = """
        SELECT
            cv.compare_version_id, cv.compare_version_description
        FROM
            gbd.compare_version_output as cvo
        JOIN
            gbd.compare_version as cv USING (compare_version_id)
        WHERE
            cvo.gbd_process_version_id = :gbd_process_version_id
        ORDER BY
            cvo.date_inserted DESC
    """

    GET_GBD_PROCESS_VERSION_ID = """
        SELECT
            gbd_process_version_id
        FROM
            gbd.gbd_process_version
        JOIN
            gbd.gbd_process_version_metadata USING (gbd_process_version_id)
        WHERE
            gbd_process_id = :gbd_process_id AND
            metadata_type_id = :metadata_type_id AND
            val = :internal_version_id AND
            gbd_process_version_status_id != 3
        ORDER BY
            gbd_process_version_id DESC
    """


class SpacetimeRestrictions:
    """Queries related to spacetime restrictions."""

    ADD_SPACETIME_RESTRICTION_VERSION = """
        INSERT INTO codcorrect.spacetime_restriction_version
            (cause_id, release_id, inserted_by, date_inserted)
        VALUES
            (:cause_id, :release_id, :inserted_by, :date_inserted)
    """

    CHECK_RESTRICTION_VERSION_ID = """
        SELECT
            restriction_version_id
        FROM
            codcorrect.spacetime_restriction_version
        WHERE
            restriction_version_id IN :restriction_version_id
    """

    GET_ALL = """
        SELECT
            rv.cause_id,
            r.location_id,
            r.year_id
        FROM
            codcorrect.spacetime_restriction r
        JOIN
            codcorrect.spacetime_restriction_version rv
                USING (restriction_version_id)
        WHERE
            rv.is_best = 1 AND
            rv.release_id = :release_id
    """

    GET_CURRENT_BEST_VERSION_ID = """
        SELECT
            restriction_version_id
        FROM
            codcorrect.spacetime_restriction_version
        WHERE
            cause_id = :cause_id AND
            release_id = :release_id AND
            is_best = 1
    """

    GET_LATEST_RESTRICTION_VERSION_ID = """
        SELECT
            restriction_version_id
        FROM
            codcorrect.spacetime_restriction_version rv
        ORDER BY restriction_version_id DESC
        LIMIT 1
    """

    GET_RESTRICTION_VERSION_METADATA = """
        SELECT
            cause_id, release_id
        FROM
            codcorrect.spacetime_restriction_version
        WHERE
            restriction_version_id = :restriction_version_id
    """

    MARK_BEST = """
        UPDATE
            codcorrect.spacetime_restriction_version
        SET
            is_best = 1,
            best_start = :best_start,
            best_end = NULL
        WHERE
            restriction_version_id = :restriction_version_id
    """

    UNMARK_BEST = """
        UPDATE
            codcorrect.spacetime_restriction_version
        SET
            is_best = 0,
            best_end = :best_end
        WHERE
            restriction_version_id = :restriction_version_id AND
            is_best = 1
    """


class PredEx:
    """Queries related to predicted life expectancy."""

    GET_BEST_MORTALITY_PROCESS_ID = """
        SELECT
            run_id
        FROM
            mortality.vw_release_process_version
        WHERE
            process_id = :process_id AND
            release_id = :release_id AND
            is_best = 1
    """

    GET_LIFE_TABLE_OVER_95 = """
        SELECT
            year_id, location_id, sex_id, 235 as age_group_id,
            age_group_years_start, mean,
            CAST(ROUND(age_group_years_start + mean, 2) as DECIMAL(5,2)) as age_at_death
        FROM
            mortality.upload_with_shock_life_table_estimate
        LEFT JOIN shared.age_group using(age_group_id)
        WHERE
            run_id = :run_id
              AND life_table_parameter_id = 5
              AND sex_id != 3
              AND age_group_years_start = 95
    """

    GET_LIFE_TABLE_UNDER_90 = """
        SELECT
            year_id, location_id, sex_id, age_group_id,
            age_group_years_start, mean,
            CAST(ROUND(age_group_years_start + mean, 2) as DECIMAL(5,2)) as age_at_death
        FROM
            mortality.upload_with_shock_life_table_estimate
        LEFT JOIN shared.age_group using(age_group_id)
        WHERE
            run_id = :run_id
            AND life_table_parameter_id = 2
            AND sex_id != 3
            AND age_group_years_start <= 90
    """

    GET_TMRLT = """
        SELECT
            precise_age,
            mean as pred_ex
        FROM
            mortality.upload_theoretical_minimum_risk_life_table_estimate
        WHERE
            run_id = :run_id
            AND life_table_parameter_id = 5
            AND estimate_stage_id = 6
    """
