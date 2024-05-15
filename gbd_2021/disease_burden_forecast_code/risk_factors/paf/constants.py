"""FHS Pipeline Scalars Local Constants."""


class PAFConstants:
    """PAF-calculation-related constants."""

    # these must correspond to file name in vaccine_sev input folder
    VACCINE_RISKS = (
        "vacc_dtp3",
        "vacc_mcv1",
        "vacc_hib3",
        "vacc_pcv3",
        "vacc_rotac",
        "vacc_mcv2",
    )

    PAF_LOWER_BOUND = -0.999
    PAF_UPPER_BOUND = 0.999

    # GBD has RRMax values for TB child causes that we don't model, but not for TB.
    # As of GBD2019, they are all the same. Therefore we can pull RRMax for TB from
    # an arbitrary TB child cause.
    TB_OTHER_CAUSE_ID = 934  # arbitrary TB child cause (tb_other)

    DEBUG_CR_COUNT = 3  # number of cause-risk pairs to pull for debug


class ClusterJobConstants:
    """Constants related to submitting jobs on the cluster."""

    COMPUTE_PAFS_RUNTIME = "8h"
    COMPUTE_SCALARS_RUNTIME = "16h"
