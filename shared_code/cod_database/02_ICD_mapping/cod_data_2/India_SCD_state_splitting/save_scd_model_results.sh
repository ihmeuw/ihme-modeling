# Purpose: Append model results into a csv and save to appropriate locations

# remove old file
rm /home/j/WORK/03_cod/01_database/03_datasets/India_SCD_states_rural/explore/deaths_split_scd.csv

# get timestamp for usage in just about everything
timestamp=$(date +"%Y-%m-%d")

# make time-stamped directories
mkdir -p /ihme/cod/prep/strUser/iss/scd/$timestamp/

# move all the raw results to storage
cp /ihme/cod/prep/strUser/iss/scd/*.csv /ihme/cod/prep/strUser/iss/scd/$timestamp

# writes the first line of one of the csvs to the deaths_split file, then the writes all but the first line of all the files 
# this makes them all well structured csvs as long as the columns are the same
head -1 /ihme/cod/prep/strUser/iss/scd/deaths-for-cause_1000.csv > /ihme/cod/prep/strUser/iss/scd/$timestamp/deaths_split_scd.csv
tail -n +2 -q /ihme/cod/prep/strUser/iss/scd/deaths-for-cause* >> /ihme/cod/prep/strUser/iss/scd/$timestamp/deaths_split_scd.csv

# this will overwrite the existing file
cp /ihme/cod/prep/strUser/iss/scd/$timestamp/deaths_split_scd.csv /home/j/WORK/03_cod/01_database/03_datasets/India_SCD_states_rural/explore/deaths_split_scd.csv
# make archive versions
cp /ihme/cod/prep/strUser/iss/scd/$timestamp/deaths_split_scd.csv /home/j/WORK/03_cod/01_database/03_datasets/India_SCD_states_rural/explore/_archive/deaths_split_scd_$timestamp.csv
