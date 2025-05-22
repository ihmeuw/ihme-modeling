# Purpose
The Clinical De-duplication package was created to create a unified set of Classes
and Methods which the Clinical Informatics team uses to de-duplicate claims and/or
admission level records to meet the requirements of an estimate_id. This includes both removing
duplicate claims (hospital admissions not yet supported eg estimate_ids 1 and 2) and also
filtering data using the is_otp and diagnosis_id columns to identify inpatient/outpatient only
data and primary diagnosis data.

## Example
A concrete example of the simplest form of this is the de-duplication performed when we reduce
fee-for-service claims level records to the individual level for a prevalence bundle with
estimate_id 17.

A patient or enrollee may visit a healthcare provider for treatment of a chronic
condition many times in the same calendar year. This would generate many claims
records in our data, but we would not be interested in creating counts of the
claims themselves for most analyses. The data stored in the ICD-mart or the bundle-mart may
also contain outpatient claims, which should not be included in estimate 17.
First the data is filtered to remove any outpatient data. Then the classes main method will
drop any duplicated records along a set of relevant columns. These usually consist of
a year, a bundle ID and a patient/enrollee identifier.

```
Example:
# instantiate with your column names and desired estimate_id as variables
dedup = ClinicalDedup(my_enrollee_col, my_date_col, my_year_col, my_estimate_id, map_version)
df = dedup.main(df=df, create_backup=False)  # deduplicates the df object and returns it
```
