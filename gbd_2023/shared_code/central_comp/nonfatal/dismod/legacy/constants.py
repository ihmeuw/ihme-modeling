"""cascade_ode constants."""

from dataclasses import dataclass


class ConnectionDefinitions:
    COD = "cod"
    EPI = "epi"
    GBD = "gbd"
    JOBMON = "privileged-jobmon-3"
    LOCAL_CONN_DEFS = [
        "cod",
        "cod-test",
        "mortality",
        "shared",
        "shared-vip",
        "covariates",
        "epi",
        "gbd",
        "bundle-uploader",
        "epi-save-results",
        "epi-save-results-dev",
        "cascade-prod",
        "cascade-dev",
    ]
    VALID_CONN_DEFS = [COD, EPI, GBD]
    READ_ONLY = [COD, GBD]


class EnvironmentVariables:
    DISMOD_CONDA_ENV = "DISMOD_CONDA_ENV"
    DISMOD_CONDA_PATH = "DISMOD_CONDA_PATH"
    ENVIRONMENT_NAME = "ENVIRONMENT_NAME"
    SGE_CLUSTER_NAME = "SGE_CLUSTER_NAME"
    SGE_ENV = "SGE_ENV"


class ClusterNames:
    SLURM = "slurm"
    BUSTER = "buster"
    UNKNOWN = "unknown"


class Methods:
    """
    Descriptions of each of these methods are detailed in release module
    """

    ENABLE_HYBRID_CV = "enable_hybrid_cv"
    ENABLE_EMR_CSMR_RE = "enable_emr_csmr_re"
    DISABLE_NON_STANDARD_LOCATIONS = "disable_nonstandard_locations"
    FIX_CSMR_RE = "fix_csmr_re"
    ENABLE_CSMR_AVERAGE = "enable_csmr_average"


class JobmonTool:
    TOOL_NAME = "cascade_ode"
    TOOL_VERSION_ID = {ClusterNames.SLURM: 4, ClusterNames.BUSTER: 23}


class CSMRQueries:
    PROCESS_VERSION_OF_CODCORRECT = """
        SELECT
            val AS codcorrect_version,
            gbd_process_version_id,
            gbd_process_version_status_id,
            gbd_round_id
        FROM gbd_process_version_metadata
        JOIN gbd_process_version USING (gbd_process_version_id)
        JOIN metadata_type USING (metadata_type_id)
        WHERE
            metadata_type = 'CodCorrect Version'
            and gbd_process_id = 3
            and gbd_process_version_status_id = 1
            and val = :codcorrect_version
        ORDER BY gbd_process_version_id DESC;
        """
    CSMR_INFO_OF_MODEL = """
        SELECT csmr_cod_output_version_id
        FROM epi.model_version_mr
        WHERE model_version_id = :model_version_id
        """
    LNASDR_VERSION_FROM_CSMR_OUTPUT_VERSION = """
        SELECT output_version_id FROM cod.output_version_metadata
        WHERE metadata_type_id = :csmr_output_type_id
        AND val = :cod_output_version_id
        """
    CODCORRECT_VERSION_FROM_OUTPUT_VERSION = """
        SELECT val FROM cod.output_version_metadata
        WHERE output_version_id = :cod_output_version_id
        AND metadata_type_id = 2
        """
    GET_IMPUTED_CSMR = """
        SELECT
            output_version_id,
            cause_id,
            year_id,
            location_id,
            sex_id,
            age_group_id,
            mean_cf AS val,
            upper_cf AS upper,
            lower_cf AS lower
        FROM cod.output_imputed_csmr
        WHERE output_version_id = :vid AND cause_id = :cause_id
        """
    GET_IMPUTED_LNASDR = """
        SELECT
            output_version_id,
            cause_id,
            year_id,
            location_id,
            sex_id,
            age_group_id,
            mean
        FROM cod.output_imputed_lnasdr
        WHERE output_version_id = :vid AND cause_id = :cause_id
        """


@dataclass
class CLIArg:
    """Constants for command-line arguments."""

    FLAG: str
    TYPE: type
    DESCRIPTION: str


class CLIArgs:
    """Constants for command-line arguments."""

    MVID = CLIArg(FLAG="mvid", TYPE=int, DESCRIPTION="model version ID")
    WORKFLOW = CLIArg(
        FLAG="--workflow",
        TYPE=str,
        DESCRIPTION=(
            "Unique identifier for workflow. Lets you rerun a model "
            "by giving it a new identifier for Jobmon. "
            "Jobmon won't let you run the same workflow with different parameters "
            "(e.g., different env) or rerun a completed workfow, "
            "so this makes it a different workflow."
        ),
    )
