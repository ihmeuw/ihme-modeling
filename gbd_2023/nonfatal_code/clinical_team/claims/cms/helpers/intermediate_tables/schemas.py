"""Schema definitions for intermediate table data"""
import pandera as pa
from crosscutting_functions.clinical_constants.pipeline.cms import max_years, mdcr_years

from cms.lookups.inputs import location_type, outcome

BASE_CLAIMS_METADATA = pa.DataFrameSchema(
    {
        "bene_id": pa.Column(pa.String, nullable=True),
        "claim_id": pa.Column(pa.String, nullable=False),
        "cms_facility_id": pa.Column(pa.Float, nullable=True),
        "file_source_id": pa.Column(
            pa.Int, nullable=False, checks=pa.Check.isin(range(1, 1001))
        ),
        "month_id": pa.Column(pa.Float, nullable=True, checks=pa.Check.isin(range(1, 13, 1))),
        "service_end": pa.Column(pa.String, nullable=True),
        "service_start": pa.Column(pa.String, nullable=True),
    },
    strict=True,
)

BASE_DEMO = pa.DataFrameSchema(
    {
        "bene_id": pa.Column(pa.String, nullable=True),
        "dob": pa.Column(pa.String, nullable=False),
        "dod": pa.Column(pa.String, nullable=True),
        "sex_id": pa.Column(pa.Int32, nullable=False, checks=pa.Check.isin(range(1, 4))),
        "location_id_cnty": pa.Column(pa.String, nullable=True),
        "location_id_gbd": pa.Column(
            pa.Float,
            nullable=True,
            coerce=True,
            checks=pa.Check.isin(list(location_type.loc_lookup.keys())),
        ),
    },
    strict=True,
)

BASE_ELIG = pa.DataFrameSchema(
    {
        "bene_id": pa.Column(pa.String, nullable=True),
        "month_id": pa.Column(
            pa.Int32, nullable=False, coerce=True, checks=pa.Check.isin(range(1, 13))
        ),
        "eligibility_reason": pa.Column(pa.String, nullable=True),
    },
    strict=True,
)

BASE_SAMPLE = pa.DataFrameSchema(
    {
        "bene_id": pa.Column(pa.String, nullable=True),
        "five_percent_sample": pa.Column(pa.Bool, nullable=False, coerce=True),
        "eh_five_percent_sample": pa.Column(pa.Bool, nullable=False, coerce=True),
        "year_id": pa.Column(
            pa.Int32,
            nullable=False,
            coerce=True,
            checks=pa.Check.isin(mdcr_years),
        ),
    },
    strict=True,
)

BASE_DENOM = pa.DataFrameSchema(
    {
        "age": pa.Column(pa.Float, nullable=False, coerce=True),
        "sex_id": pa.Column(
            pa.Int, nullable=False, coerce=True, checks=pa.Check.isin(range(1, 4))
        ),
        "sample_size": pa.Column(
            pa.Float,
            nullable=False,
            coerce=True,
        ),
    },
    strict=True,
)

BASE_RACE = pa.DataFrameSchema(
    {
        "bene_id": pa.Column(pa.String, nullable=True),
        "rti_race_cd": pa.Column(pa.Int, nullable=False, coerce=True),
    },
    strict=True,
)

# Stores intermediate table schemas in a dict using table name as the key or, if the source
# schemas diverge then {source}_{table name}
SCHEMAS = {
    "mdcr_claims_metadata": BASE_CLAIMS_METADATA.add_columns(
        {
            "clm_srvc_fac_zip_cd": pa.Column(pa.String, nullable=True),
            "year_id": pa.Column(
                pa.Int,
                nullable=False,
                checks=pa.Check.isin(mdcr_years),
            ),
            "ptnt_dschrg_stus_cd": pa.Column(
                pa.Float,
                nullable=True,
                checks=pa.Check.isin(outcome.PTNT_DSCHRG_STUS_CD.ptnt_dschrg_stus_cd.tolist()),
            ),
        }
    ),
    "max_claims_metadata": BASE_CLAIMS_METADATA.add_columns(
        {
            "year_id": pa.Column(
                pa.Int,
                nullable=False,
                checks=pa.Check.isin(max_years),
            ),
            "patient_status_cd": pa.Column(
                pa.Float,
                nullable=True,
                checks=pa.Check.isin(outcome.PATIENT_STATUS_CD.patient_status_cd.tolist()),
            ),
        }
    ),
    "mdcr_demo": BASE_DEMO.add_columns(
        {
            "year_id": pa.Column(
                pa.Int32,
                nullable=False,
                checks=pa.Check.isin(mdcr_years),
            ),
        }
    ),
    "max_demo": BASE_DEMO.add_columns(
        {
            "year_id": pa.Column(
                pa.Int32,
                nullable=False,
                checks=pa.Check.isin(max_years),
            ),
        }
    ),
    "mdcr_elig": BASE_ELIG.add_columns(
        {
            "year_id": pa.Column(
                pa.Int32,
                nullable=False,
                coerce=True,
                checks=pa.Check.isin(mdcr_years),
            ),
            "entitlement": pa.Column(
                pa.Int, nullable=False, coerce=True, checks=pa.Check.isin(range(1, 4))
            ),
            "part_c": pa.Column(pa.Bool, nullable=False, coerce=True),
        }
    ),
    "max_elig": BASE_ELIG.add_columns(
        {
            "year_id": pa.Column(
                pa.Int32,
                nullable=False,
                coerce=True,
                checks=pa.Check.isin(max_years),
            ),
            "restricted_benefit": pa.Column(
                pa.String,
                nullable=False,
            ),
            "managed_care": pa.Column(
                pa.String,
                nullable=False,
            ),
        }
    ),
    "mdcr_five_percent": BASE_SAMPLE,
    "mdcr_race": BASE_RACE.add_columns(
        {
            "year_id": pa.Column(
                pa.Int,
                nullable=False,
                coerce=True,
                checks=pa.Check.isin(mdcr_years),
            ),
        }
    ),
    "max_race": BASE_RACE.add_columns(
        {
            "year_id": pa.Column(
                pa.Int,
                nullable=False,
                coerce=True,
                checks=pa.Check.isin(max_years),
            ),
        }
    ),
    "mdcr_denom": BASE_DENOM.add_columns(
        {
            "year_id": pa.Column(
                pa.Int,
                nullable=False,
                coerce=True,
                checks=pa.Check.isin(mdcr_years),
            ),
            "rti_race_cd": pa.Column(pa.Int, nullable=False, coerce=True),
            "entitlement": pa.Column(
                pa.Int, nullable=False, coerce=True, checks=pa.Check.isin(range(1, 4))
            ),
            "location_id_cnty": pa.Column(pa.String, nullable=True),
            "location_id_gbd": pa.Column(
                pa.Float,
                nullable=True,
                coerce=True,
                checks=pa.Check.isin(list(location_type.loc_lookup.keys())),
            ),
            "five_percent_sample": pa.Column(pa.Bool, nullable=False, coerce=True),
            "eh_five_percent_sample": pa.Column(pa.Bool, nullable=False, coerce=True),
        }
    ),
    "max_denom": BASE_DENOM.add_columns(
        {
            "year_id": pa.Column(
                pa.Int,
                nullable=False,
                coerce=True,
                checks=pa.Check.isin(max_years),
            ),
            "rti_race_cd": pa.Column(pa.Int, nullable=False, coerce=True),
            "location_id_cnty": pa.Column(pa.String, nullable=True),
            "location_id_gbd": pa.Column(
                pa.Float,
                nullable=True,
                coerce=True,
                checks=pa.Check.isin(list(location_type.loc_lookup.keys())),
            ),
            "restricted_benefit": pa.Column(
                pa.String,
                nullable=False,
                coerce=True,
            ),
        }
    ),
    "mdcr_denom_cnty": BASE_DENOM.add_columns(
        {
            "year_id": pa.Column(
                pa.Int,
                nullable=False,
                coerce=True,
                checks=pa.Check.isin(mdcr_years),
            ),
            "rti_race_cd": pa.Column(pa.Int, nullable=False, coerce=True),
            "entitlement": pa.Column(
                pa.Int, nullable=False, coerce=True, checks=pa.Check.isin(range(1, 4))
            ),
            "location_id_cnty": pa.Column(pa.String, nullable=True),
            "five_percent_sample": pa.Column(pa.Bool, nullable=False, coerce=True),
            "eh_five_percent_sample": pa.Column(pa.Bool, nullable=False, coerce=True),
        }
    ),
    "mdcr_denom_gbd": BASE_DENOM.add_columns(
        {
            "year_id": pa.Column(
                pa.Int,
                nullable=False,
                coerce=True,
                checks=pa.Check.isin(mdcr_years),
            ),
            "entitlement": pa.Column(
                pa.Int, nullable=False, coerce=True, checks=pa.Check.isin(range(1, 4))
            ),
            "location_id_gbd": pa.Column(
                pa.Float,
                nullable=True,
                coerce=True,
                checks=pa.Check.isin(list(location_type.loc_lookup.keys())),
            ),
            "five_percent_sample": pa.Column(pa.Bool, nullable=False, coerce=True),
            "eh_five_percent_sample": pa.Column(pa.Bool, nullable=False, coerce=True),
        }
    ),
    "max_denom_cnty": BASE_DENOM.add_columns(
        {
            "year_id": pa.Column(
                pa.Int,
                nullable=False,
                coerce=True,
                checks=pa.Check.isin(max_years),
            ),
            "rti_race_cd": pa.Column(pa.Int, nullable=False, coerce=True),
            "location_id_cnty": pa.Column(pa.String, nullable=True),
            "restricted_benefit": pa.Column(
                pa.String,
                nullable=False,
                coerce=True,
            ),
        }
    ),
    "max_denom_gbd": BASE_DENOM.add_columns(
        {
            "year_id": pa.Column(
                pa.Int,
                nullable=False,
                coerce=True,
                checks=pa.Check.isin(max_years),
            ),
            "location_id_gbd": pa.Column(
                pa.Float,
                nullable=True,
                coerce=True,
                checks=pa.Check.isin(list(location_type.loc_lookup.keys())),
            ),
            "restricted_benefit": pa.Column(
                pa.String,
                nullable=False,
                coerce=True,
            ),
        }
    ),
}
