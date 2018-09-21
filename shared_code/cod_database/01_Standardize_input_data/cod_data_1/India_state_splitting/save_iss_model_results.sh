# Purpose: Append model results into a csv and save to appropriate locations

# get timestamp for usage in just about everything
timestamp=$(date +"%Y-%m-%d")

# make time-stamped directories
mkdir -p /clustertmp/strUser/iss/icd10/$timestamp/raw
#mkdir -p /clustertmp/strUser/iss/icd9/$timestamp/raw

# move all the raw results to storage
#cp /clustertmp/strUser/iss/*.csv /clustertmp/strUser/iss/icd9/$timestamp/raw
cp /clustertmp/strUser/iss/icd10/*.csv /clustertmp/strUser/iss/icd10/$timestamp/raw

# writes the first line of one of the csvs to the deaths_split file, then the writes all but the first line of all the files 
# this makes them all well structured csvs as long as the columns are the same
# icd9
#head -1 /clustertmp/strUser/iss/deaths-for-cause_12.01.01-sex_1.csv > /clustertmp/strUser/iss/icd9/$timestamp/deaths_split_icd9.csv
#tail -n +2 -q /clustertmp/strUser/iss/deaths* >> /clustertmp/strUser/iss/icd9/$timestamp/deaths_split_icd9.csv
# icd10
head -1 /clustertmp/strUser/iss/icd10/deaths-for-cause_12.01.01-sex_1.csv > /clustertmp/strUser/iss/icd10/$timestamp/deaths_split_icd10.csv
tail -n +2 -q /clustertmp/strUser/iss/icd10/deaths* >> /clustertmp/strUser/iss/icd10/$timestamp/deaths_split_icd10.csv

# this will overwrite the existing file
#cp /clustertmp/strUser/iss/icd9/$timestamp/deaths_split_icd9.csv /home/j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD9/explore/states_splitting/outputs/deaths_split_icd9.csv
cp /clustertmp/strUser/iss/icd10/$timestamp/deaths_split_icd10.csv /home/j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD10/explore/states_splitting/outputs/deaths_split_icd10.csv
# make archive versions
#cp /clustertmp/strUser/iss/icd9/$timestamp/deaths_split_icd9.csv /home/j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD9/explore/states_splitting/outputs/_archive/deaths_split_icd9_$timestamp.csv
cp /clustertmp/strUser/iss/icd10/$timestamp/deaths_split_icd10.csv /home/j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD10/explore/states_splitting/outputs/_archive/deaths_split_icd10_$timestamp.csv
