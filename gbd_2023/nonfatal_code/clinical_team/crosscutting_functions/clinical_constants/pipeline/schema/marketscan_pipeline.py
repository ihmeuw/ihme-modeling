"""Schema validation via pandera. Schema's defined here are applied within the
CreateMarketscanEstimates main method.

Notes:
- new cols will pass validtion unless strict=True is set
- required is a schema arg and the default is true, eg all cols listed that don't explicitly
  assign required=False must be present
"""

import pandera as pa
from claims.marketscan.schema import manage_year_tracker
from pandas.api.types import is_number

from crosscutting_functions.clinical_constants.pipeline.marketscan import YEAR_NID_DICT

NIDS = list(YEAR_NID_DICT.values())

# Validate the ICD-mart schema
input_schema = pa.DataFrameSchema(
    {
        "bene_id": pa.Column(pa.Int, nullable=False),
        "service_start": pa.Column(pa.DateTime, nullable=True),
        "facility_id": pa.Column(pa.Float32, nullable=True),
        "diagnosis_id": pa.Column(pa.Int8, nullable=False),
        "cause_code": pa.Column(pa.String, nullable=False),
        "age": pa.Column(pa.Int8, nullable=False),
        "sex_id": pa.Column(pa.Int8, nullable=False),
        "location_id": pa.Column(pa.Int16, nullable=False),
        "year_id": pa.Column(pa.Int16, nullable=False),
        "is_otp": pa.Column(pa.Int8, nullable=False),
        "code_system_id": pa.Column(pa.Int8, nullable=False),
    },
    strict=False,  # allows for additional columns not defined above
)

# Validate prepped numerator-only data ready to be written for correction_factors or
# further processed in the pipeline to add denominators and noise reduce
intermediate_schema = pa.DataFrameSchema(
    {
        "bundle_id": pa.Column(pa.Int, nullable=False),
        "estimate_id": pa.Column(pa.Int, nullable=False),
        "facility_id": pa.Column(pa.Float, nullable=True, required=False),
        "age": pa.Column(
            pa.Int,
            nullable=False,
            checks=[
                pa.Check(lambda a: a >= 0, element_wise=True),
                (pa.Check(lambda a: a <= 125, element_wise=True)),
            ],
        ),
        "sex_id": pa.Column(pa.Int, nullable=False, checks=pa.Check.isin([1, 2])),
        "location_id": pa.Column(
            pa.Int,
            nullable=False,
            checks=[(pa.Check(lambda loc: loc <= 600, element_wise=True))],
        ),
        "year_id": pa.Column(
            pa.Int, nullable=False, checks=pa.Check.isin(manage_year_tracker.get_all_years())
        ),
        "val": pa.Column(pa.Int, nullable=False),
    },
    strict=True,  # columns in data must exactly match this schema
)

denominator_schema = pa.DataFrameSchema(
    {
        "bundle_id": pa.Column(pa.Int, nullable=False),
        "estimate_id": pa.Column(pa.Int, nullable=False),
        "age_start": pa.Column(
            pa.Float,
            nullable=False,
            checks=[
                pa.Check(lambda age: age >= 0, element_wise=True),
                (pa.Check(lambda age: age <= 125, element_wise=True)),
            ],
        ),
        "age_end": pa.Column(
            pa.Float,
            nullable=False,
            checks=[
                pa.Check(lambda age: age >= 0, element_wise=True),
                (pa.Check(lambda age: age <= 125, element_wise=True)),
            ],
        ),
        "sex_id": pa.Column(pa.Int, nullable=False, checks=pa.Check.isin([1, 2])),
        "location_id": pa.Column(
            pa.Int,
            nullable=False,
            checks=[(pa.Check(lambda loc: loc <= 600, element_wise=True))],
        ),
        "year_id": pa.Column(
            pa.Int, nullable=False, checks=pa.Check.isin(manage_year_tracker.get_all_years())
        ),
        "nid": pa.Column(pa.Int, nullable=False),
        "val": pa.Column(pa.Float, nullable=False),
        "sample_size": pa.Column(pa.Int, nullable=False),
    },
    strict=True,
)

pre_noise_reduction_schema = pa.DataFrameSchema(
    {
        "bundle_id": pa.Column(pa.Int, nullable=False),
        "estimate_id": pa.Column(pa.Int, nullable=False),
        "age_start": pa.Column(
            pa.Float,
            nullable=False,
            checks=[
                pa.Check(lambda age: age >= 0, element_wise=True),
                (pa.Check(lambda age: age <= 125, element_wise=True)),
            ],
        ),
        "age_end": pa.Column(
            pa.Float,
            nullable=False,
            checks=[
                pa.Check(lambda age: age >= 0, element_wise=True),
                (pa.Check(lambda age: age <= 125, element_wise=True)),
            ],
        ),
        "sex_id": pa.Column(pa.Int, nullable=False, checks=pa.Check.isin([1, 2])),
        "location_id": pa.Column(
            pa.Int,
            nullable=False,
            checks=[(pa.Check(lambda loc: loc <= 600, element_wise=True))],
        ),
        "year_id": pa.Column(
            pa.Int, nullable=False, checks=pa.Check.isin(manage_year_tracker.get_all_years())
        ),
        "nid": pa.Column(pa.Int, nullable=False, checks=pa.Check.isin(NIDS)),
        "sample_size": pa.Column(pa.Int, nullable=False),
        "mean": pa.Column(pa.Float, nullable=False),
    },
    strict=True,
)

final_schema = pa.DataFrameSchema(
    {
        "bundle_id": pa.Column(pa.Int, nullable=False),
        "estimate_id": pa.Column(pa.Int, nullable=False),
        "age_group_id": pa.Column(pa.Int, nullable=False),
        "sex_id": pa.Column(pa.Int, nullable=False, checks=pa.Check.isin([1, 2])),
        "location_id": pa.Column(
            pa.Int16,
            nullable=False,
            checks=[(pa.Check(lambda loc: loc <= 600, element_wise=True))],
        ),
        "year_id": pa.Column(
            pa.Int16, nullable=False, checks=pa.Check.isin(manage_year_tracker.get_all_years())
        ),
        "nid": pa.Column(pa.Int, nullable=False, checks=pa.Check.isin(NIDS)),
        "sample_size": pa.Column(nullable=False, checks=pa.Check(lambda s: s.map(is_number))),
        "mean": pa.Column(pa.Float, nullable=False),
    },
    strict=True,
)