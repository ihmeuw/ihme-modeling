from typing import Optional

import pandera as pa
from crosscutting_functions.demographic import get_child_locations
from pandera.typing import Series

from crosscutting_functions.pipeline_constants import poland as constants

location_ids = get_child_locations(
    location_parent_id=constants.POL_GBD_LOC_ID, release_id=16, return_dict=False
)
location_ids.append(constants.POL_GBD_LOC_ID)

# Partitioned cols are saved outside of the actual data, and are
# read-in as categorical datatypes. We need to cast these cols
# and the rest of the schema to the correct type
icd_mart_schema = pa.DataFrameSchema(
    {
        "bene_id": pa.Column(pa.Int, nullable=False),
        "age": pa.Column(pa.Int, nullable=False),
        "sex_id": pa.Column(pa.Int, nullable=False),
        "year_id": pa.Column(pa.Int, nullable=False, checks=pa.Check.isin(constants.YEARS)),
        "location_id": pa.Column(pa.Int, nullable=False, checks=pa.Check.isin(location_ids)),
        "facility_id": pa.Column(pa.Int, nullable=True, checks=pa.Check.isin(range(0, 21))),
        "admission_date": pa.Column(pa.DateTime, nullable=False),
        "discharge_date": pa.Column(pa.DateTime, nullable=True),
        "diagnosis_id": pa.Column(pa.Int, nullable=False),
        "code_system_id": pa.Column(pa.Int, nullable=False),
        "cause_code": pa.Column(pa.String, nullable=False),
        "is_otp": pa.Column(pa.Int, nullable=False),
        "primary_dx_only": pa.Column(pa.Int, nullable=False),
    },
    coerce=True,
    strict=False,
)

SCHEMA_COLUMNS = list(icd_mart_schema.columns.keys())


class DemoBaseSchema(pa.SchemaModel):
    sex_id: Series[int] = pa.Field(nullable=False, isin=[1, 2])
    year_id: Series[int] = pa.Field(nullable=False, isin=constants.YEARS)
    location_id: Series[int] = pa.Field(nullable=False, isin=location_ids)

    class Config:
        strict = True


class MappedBaseSchema(pa.SchemaModel):
    bundle_id: Series[int] = pa.Field(nullable=False)
    estimate_id: Series[int] = pa.Field(nullable=False)

    class Config:
        strict = True


class MappedDemoBaseSchema(DemoBaseSchema, MappedBaseSchema):  # type: ignore
    pass


class DenominatorSchema(DemoBaseSchema):
    location_id: Series[int] = pa.Field(nullable=False, isin=location_ids)
    age_group_id: Series[int] = pa.Field(nullable=False)
    population: Series[float] = pa.Field(nullable=False)
    pop_run_id: Series[int] = pa.Field(nullable=False)


class DedupedSchema(MappedDemoBaseSchema):
    age: Series[int] = pa.Field(nullable=False, in_range={"min_value": 0, "max_value": 125})
    primary_dx_only: Optional[Series[int]] = pa.Field(nullable=False)


class CorrectionFactorFinalSchema(DedupedSchema):
    estimate_type: Series[str] = pa.Field(nullable=False)
    val: Series[int] = pa.Field(nullable=False, ge=1)
    primary_dx_only: Optional[Series[int]] = pa.Field(nullable=False)


class GBDFinalSchema(MappedDemoBaseSchema):
    age_group_id: Series[int] = pa.Field(nullable=False)
    location_id: Series[int] = pa.Field(nullable=False, isin=location_ids)
    mean: Series[float] = pa.Field(nullable=False)
    sample_size: Series[float] = pa.Field(nullable=False, gt=1e-8)
    nid: Series[int] = pa.Field(nullable=False)