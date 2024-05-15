"""FHS Pipeline SEVs Local Constants."""

from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec, VersionMetadata


class SEVConstants:
    """Constants used in SEVs forecasting."""

    FLOOR = 1e-3

    # This value is not a standard REI name but is used in place of one to label the "rei" of
    # an intrinsic SEV value.
    INTRINSIC_SPECIAL_REI = "intrinsic"

    INTRINSIC_SEV_FILENAME_SUFFIX = "intrinsic"


class PastSEVConstants:
    """Constants used in compute Past SEVs."""

    TOL = 1e-4
    RTOL = 1e-2
    MAXITER = 50
    DRAW_CHUNK_SIZE = 100  # compute k in chunks of 100 draws


class FutureSEVConstants:
    """Constants used in computing Future SEVs."""

    FLOOR = 1e-6
    DALY_WEIGHTS_FILE_SPEC = FHSFileSpec(
        version_metadata=VersionMetadata.make(
            root_dir="data",
            data_source="6",
            epoch="past",
            stage="mediation",
            version="20210802_DALY_weights",
        ),
        filename="DALY_weights.csv",
    )
