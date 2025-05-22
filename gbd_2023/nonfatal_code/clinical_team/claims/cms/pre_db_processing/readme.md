The CMS data we received came in fixed width text files, without headers. Difficult to use in its raw form.
Along with these we received SAS scripts to extract them, and .fts files to check the extractions.

- modify_sas_scripts
    - contains the code to add the input data path, output SAS path and output CSV path for all 150 of the SAS scripts that need to be modified

- test_sas_outputs
    - A class to run 3 checks on the data outputs. 1) Identify missing files, if any. 2) Rough sanity check on file size. Most files should be larger than 15Mb. 3) use the .fts files to get the expected number of rows and columns