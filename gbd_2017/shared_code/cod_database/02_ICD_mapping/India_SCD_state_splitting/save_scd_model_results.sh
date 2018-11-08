# remove old file
rm "FILEPATH"

# get timestamp for usage in just about everything
timestamp=$(date +"%Y-%m-%d")

# make time-stamped directories
mkdir -p "FILEPATH"

# move all the raw results to storage
cp "FILEPATH" "FILEPATH"

# writes the first line of one of the csvs to the deaths_split file, then the writes all but the first line of all the files 
# this makes them all well structured csvs as long as the columns are the same
head "FILEPATH" > "FILEPATH"
tail "FILEPATH" >> "FILEPATH"

# this will overwrite the existing file
cp "FILEPATH" "FILEPATH"
# make archive versions
cp "FILEPATH" "FILEPATH"