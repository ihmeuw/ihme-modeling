class Cause:
    GET_VERSION_ID = """
        SELECT shared.active_cause_set_decomp_step_version(
            :cause_set_id, :gbd_round_id, :decomp_step_id
        ) AS cause_set_version_id
    """

    GET_METADATA_VERSION_ID = """
        SELECT
            cause_metadata_version_id
        FROM
            shared.cause_set_version csv
        WHERE
            cause_set_version_id = :cause_set_version_id
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


class CoDCorrectVersion:
    INSERT_NEW_VERSION_ROW = """
        INSERT INTO cod.output_version (output_version_id, tool_type_id,
        decomp_step_id, description, code_version, env_version,
        status, is_best)
        VALUES (:output_version_id, :tool_type_id,
        :decomp_step_id, :description, :code_version, :env_version,
        :status, :is_best)
    """

    CREATE_OUTPUT_PARTITION = """
        CALL cod.add_partition_by_list(:schema, :table, :output_version_id)"""

    GET_CODCORRECT_VERSION_METADATA = """
    SELECT
        gbd_process_version_id, gbd_round_id, decomp_step_id
    FROM
        gbd.gbd_process_version_metadata
    JOIN
        gbd.gbd_process_version USING (gbd_process_version_id)
    WHERE
        gbd_process_id = 3 AND -- CodCorrect
        metadata_type_id = 1 AND -- CodCorrect version
        val = :version_id
    """

    GET_CURRENT_BEST_VERSION = """
        SELECT output_version_id
        FROM cod.output_version
        WHERE tool_type_id = :tool_type_id
        AND decomp_step_id = :decomp_step_id
        AND is_best = 1
    """

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

    UPDATE_STATUS = """
        UPDATE cod.output_version
        SET status = :status
        WHERE output_version_id = :output_version_id
    """

    UPDATE_BEST = """
        UPDATE cod.output_version
        SET is_best = :is_best
        WHERE output_version_id = :output_version_id
    """


class Diagnostics:
    INSERT_DIAGNOSTIC_ROW = """
        INSERT INTO codcorrect.diagnostic_version
            (codcorrect_version_id, is_best, inserted_by, last_updated_by)
        VALUES
            (:codcorrect_version_id, :is_best, :inserted_by, :last_updated_by)
        """


class FauxCorrectVersion:
    INSERT_NEW_VERSION_ROW = (
        'INSERT INTO cod.fauxcorrect_version (fauxcorrect_version_id, '
        'description, code_version, gbd_round_id, scalar_version_id, '
        'decomp_step_id, status, is_best) '
        'VALUES ("{fauxcorrect_version_id}", "{description}", '
        '"{code_version}", "{gbd_round_id}", "{scalar_version_id}", '
        '"{decomp_step_id}", "{status}", "{is_best}");'
    )

    GET_CURRENT_BEST_VERSION = (
        'SELECT fauxcorrect_version_id from cod.fauxcorrect_version WHERE '
        'gbd_round_id = :gbd_round_id AND decomp_step_id = :decomp_step_id '
        'AND status = 1'
    )

    GET_LAST_INSERT_ID = (
        'SELECT MAX(LAST_INSERT_ID(fauxcorrect_version_id)) AS '
        'fauxcorrect_version_id FROM cod.fauxcorrect_version'
    )

    GET_NEW_VERSION_ID = (
        'SELECT MAX(LAST_INSERT_ID(fauxcorrect_version_id)) + 1 AS '
        'fauxcorrect_version_id FROM cod.fauxcorrect_version;'
    )

    # Always based on last year's best CoDCorrect.
    GET_SCALAR_VERSION_ID = (
        'SELECT sc.output_version_id AS scalar_version_id FROM cod.scalar sc '
        'JOIN cod.output_version AS ov WHERE ov.code_version = :gbd_round_id '
        'AND is_best = 1 LIMIT 1'
    )

    UPDATE_STATUS = (
        'UPDATE cod.fauxcorrect_version SET status = :status WHERE '
        'fauxcorrect_version_id = :fauxcorrect_version_id'
    )

    UPDATE_BEST = (
        'UPDATE cod.fauxcorrect_version SET is_best = :is_best WHERE '
        'fauxcorrect_version_id = :fauxcorrect_version_id'
    )


class GbdDatabase:
    ACTIVATE_COMPARE_VERSION = """
    UPDATE
        gbd.compare_version
    SET
        compare_version_status_id = 2
    WHERE
        compare_version_id = :compare_version_id AND
        compare_version_status_id = -1
    """
    ACTIVATE_PROCESS_VERSION = """
    UPDATE
        gbd.gbd_process_version
    SET
        gbd_process_version_status_id = 2
    WHERE
        gbd_process_version_id = :process_version_id AND
        gbd_process_version_status_id = -1
    """
    DELETE_PROCESS_VERSION = """
    CALL gbd.delete_gbd_process_version(
        :gbd_process_version_id
    )
    """
    GET_BEST_INTERNAL_VERSION_ID = """
    SELECT
        val
    FROM
        gbd.gbd_process_version
    JOIN gbd_process_version_metadata USING (gbd_process_version_id)
    WHERE
        gbd_process_id = :gbd_process_id
        AND metadata_type_id = :metadata_type_id
        AND gbd_round_id = :gbd_round_id
        AND decomp_step_id = :decomp_step_id
        AND gbd_process_version_status_id = 1  # best
    ORDER BY
        gbd_process_version_id DESC
    """
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
    UPDATE_COMPARE_VERSION_DESCRIPTION = """
    UPDATE
        gbd.compare_version
    SET
        compare_version_description = :description
    WHERE
        compare_version_id = :compare_version_id
    """

class HIV:
    GET_HIV_CAUSES = """
        SELECT
            DISTINCT(c.cause_id)
        FROM
            shared.cause_hierarchy_history c
        JOIN
            shared.cause_set_version_active csv USING (cause_set_version_id)
        WHERE
            c.cause_id = 298 OR
            c.parent_id = 298 AND
            csv.cause_set_id = :cause_set_id AND
            csv.gbd_round_id = :gbd_round_id AND
            c.level > 0
    """


class Location:
    GET_VERSION_ID = """
        SELECT shared.active_location_set_version(
            :location_set_id, :gbd_round_id
        ) AS location_set_version_id
    """


class ModelVersion:
    GET_BEST_METADATA = """
        SELECT
            cause_id,
            sex_id,
            model_version_id,
            model_version_type_id,
            is_best,
            age_start,
            age_end,
            decomp_step_id
        FROM
            cod.model_version
        WHERE
            is_best = 1 AND
            status != 3 AND
            gbd_round_id = :gbd AND
            decomp_step_id = :decomp AND
            model_version_type_id IN :mvtids
    """
    GET_BEST_METADATA_BY_CAUSE = """
        SELECT
            cause_id,
            sex_id,
            model_version_id,
            model_version_type_id,
            is_best,
            age_start,
            age_end,
            decomp_step_id
        FROM
            cod.model_version
        WHERE
            is_best = 1 AND
            status != 3 AND
            gbd_round_id = :gbd AND
            decomp_step_id = :decomp AND
            model_version_type_id IN :mvtids AND
            cause_id IN :causes
    """
    GET_MVID_ROUND_AND_STEP_ID = """
    SELECT
        gbd_round_id, decomp_step_id
    FROM
        cod.model_version
    WHERE
        model_version_id = :model_version_id
    """


class RegionalScalars:
    GET_ALL = """
        SELECT
            year_id,
            location_id,
            mean
        FROM
            mortality.upload_population_scalar_estimate
        WHERE
            run_id = (
                SELECT run_id
                FROM mortality.process_version
                WHERE process_id = 23
                AND gbd_round_id = :gbd_round_id
                AND status_id = 5
            )
    """


class Scalars:
    GET_ALL = """
        SELECT
            output_version_id,
            location_id,
            year_id,
            sex_id,
            age_group_id,
            cause_id,
            scalar
        FROM
            cod.scalar
    """


class Shared:
    GET_ALL_AGE_METADATA = """
        SELECT
            age_group_id, age_group_name, age_group_years_start,
            age_group_years_end
        FROM shared.age_group
    """

class SpacetimeRestrictions:
    ADD_SPACETIME_RESTRICTION_VERSION = """
        INSERT INTO codcorrect.spacetime_restriction_version
            (cause_id, gbd_round, gbd_round_id, decomp_step_id, inserted_by,
            date_inserted)
        VALUES
            (:cause_id, :gbd_round, :gbd_round_id, :decomp_step_id, :inserted_by,
            :date_inserted)
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
            rv.gbd_round_id = :gbd_round_id AND
            rv.decomp_step_id = :decomp_step_id
    """
    GET_CURRENT_BEST_VERSION_ID = """
        SELECT
            restriction_version_id
        FROM
            codcorrect.spacetime_restriction_version
        WHERE
            cause_id = :cause_id AND
            gbd_round_id = :gbd_round_id AND
            decomp_step_id = :decomp_step_id AND
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
            cause_id, gbd_round_id, decomp_step_id
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
    GET_BEST_MORTALITY_PROCESS_ID = """
        SELECT
            run_id
        FROM
            mortality.vw_decomp_process_version
        WHERE
            process_id = :process_id AND
            decomp_step_id = :decomp_step_id AND
            is_best = 1
    """
    GET_PRED_EX = "CALL mortality.get_pred_ex(:gbd_round_id)"
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
