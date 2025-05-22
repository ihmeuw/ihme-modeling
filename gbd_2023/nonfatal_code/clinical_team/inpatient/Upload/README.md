# Upload

The uploader process is designed to move final estimates produced by a clinical run into the final estimates bundle table. The class is also responsible for transforming pipeline results into DISMOD shape.

## Overview

There are three classes available to perform the upload: `InpatientUploader`, `ClaimsUploader` and `OutpatientUploader`. Each class performs a different series of validations for a given pipeline as well enforcing a uniform set of columns.

### DISMOD shape

Since estimates from `clinical.final_estimates_bundle` are sent directly to Central Comp tables during the refresh process we must make our data ready for DISMOD. In practice, this means appending columns. These columns are:

* source_type_id (inpatient)
* measures_id (claims)
* cases
* uncertainty_type_id
* uncertainty_type_value
* standard_error
* upper (claims and outpatient)
* lower (claims and outpatient)

Some of these columns are static hardcoded values (such as `uncertainty_type_id`) while others are computed within the class. For example, uncertainty for claims is computed using Wilson approximation and SE is created using code provided by Central Comp.

### Filters

Data points are often dropped based on broad criteria approved by team leads. For example, in prevelance bundles we remove any estimates where the mean is greater than 1. Each pipeline applies their own set filters. Filters are purposely broad and are independent of any source / year nuances. 

### Testing

We apply a very broad set of tests which varies by pipeline. In general, these tests are looking for impossible estimates and the data meets DISMOD requirements. They should never be relied on to replace pipeline specific tests. Of course, the team should always work towards expanding this suite by including validations that are independent of source specific idiosyncrasies. For example, the team validate that sample size for maternal bundles is lower than sample size for non-maternal bundles.  

## Run Instructions

Below is an example for uploading final inpatient to the database. Note that the method calls are the same for all three classes.
`year_end_cap` is only relevant for inpatient estimates currently. It can be omitted if no cap is needed and will not affect the other two classes.

```python
run_id = 900
# year_end_cap works here if the last bin is 2018-2022
uploader = InpatientUploader(run_id=run_id, name="inpatient_run58_test", year_end_cap=2019)
uploader.load_csv("inpatient.csv")

# process() applies filters and appends columns
uploader.process()
uploader.test()

try:
    # add a new run_id partition or pass if it exists
    add_final_bundle_estimates_partition(run_id=run_id, odbc_profile=odbc_profile)
except Exception as e:
    if e == f"1517 (HY000): Duplicate partition name P{run_id}":
        pass

uploader.backup_and_upload(path="InpatientUploader", odbc_profile=odbc_profile)
```
Each file loaded from `uploader.load_csv()` must originate from FILEPATH. The backups are saved at FILEPATH

<br>

In the `ClaimsUploader` and `OutpatientUploader` a year limiter is available by instantiating the class with the `max_upload_year` argument.  This optional argument will remove any years greater than the value provided. If no value is provided, all years will be processed.

```python
run_id = 900
odbc_profile = "clinical"

# max_upload_year will keep all years less than or equal to max_upload_year.
# clinical_data_type_id will default to the standard claims clinical datatype.
uploader = ClaimsUploader(run_id=run_id, name="claims_run_test", max_upload_year=2019, clinical_data_type_id=3)
uploader.load_csv("claims.csv")

# process() applies filters and appends columns
uploader.process()
uploader.test()

try:
    # add a new run_id partition or pass if it exists
    add_final_bundle_estimates_partition(run_id=run_id, odbc_profile=odbc_profile)
except Exception as e:
    if e == f"1517 (HY000): Duplicate partition name P{run_id}":
        pass

uploader.backup_and_upload(path="ClaimsUploader", odbc_profile=odbc_profile)
```
