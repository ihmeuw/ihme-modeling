# Clincal Mapping


| Name     | Description |
| :------- | :---------- |
| gbd_icd_acause_map      |  Modules to make icd to cause table for GBD.      |
| new_map_creation   |    Code for updating a clinical map to a new version.     |
| tables   |    SQL code to create DB tables for a new mapping schema and python module to populate them.     |
| tests   |   Tests for functions within clinical mapping      |
| bundle_swaps.py   | Swap out old bundle ids for new bundles ids, if the mapping between has not changed between the two mapping versions provided        |
| clinical_mapping.py   | This module allows a user to: <ol> <li>Pull a given table from the dB given a map version and then run validations  <li> Map clinical data from cause codes to ICG to bundle.<li> ADD a duration limit column to the data <li>remove rows outside of age-sex restrictions </ol>    |
| compare_bundle_mapping.ipynb   |         |
| extract_maps.py   | Exract the nonfatal cause name, icd code, and Bundle id from Mohsens.        |
| icg_bundle_cause_map.py   | Use bundle_id to map from cause_id to icg_id        |
| mapping_constants.py   |   Constants defined for mapping schema      |
| submit_icd_mapping.py   | Submit worker jobs to map from ICD codes to ICGs        |
| worker_icd_mapping.py   | This is the worker script to map inpatient hospital data from ICD code to Intermediate Cause Group (ICG)        |