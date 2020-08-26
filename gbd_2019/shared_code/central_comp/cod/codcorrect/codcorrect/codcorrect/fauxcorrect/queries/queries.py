class Cause:
    GET_VERSION_ID = """
        SELECT shared.active_cause_set_version(
            :cause_set_id, :gbd_round_id
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

    GET_CURRENT_BEST_VERSION = """
        SELECT output_version_id
        FROM cod.output_version
        WHERE tool_type_id = :tool_type_id
        AND decomp_step_id = :decomp_step_id
        AND is_best = 1
    """

    GET_LAST_INSERT_ID = """
        SELECT MAX(LAST_INSERT_ID(output_version_id)) AS
        output_version_id FROM cod.output_version
    """

    GET_NEW_VERSION_ID = """
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
            gbd_round_id = :gbd AND
            decomp_step_id = :decomp AND
            model_version_type_id IN :mvtids AND
            cause_id IN :causes
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


class SpacetimeRestrictions:
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
            rv.gbd_round = :gbd_round
    """


class PredEx:
    GET_PRED_EX = 'CALL mortality.get_pred_ex(:gbd_round_id)'
