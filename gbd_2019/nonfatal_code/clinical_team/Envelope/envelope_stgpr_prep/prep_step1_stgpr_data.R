## Prep step 1 inpatient hospital data on this script

## Multiple steps:
# 1) Just Norway without new age groups
# 2) Norway split and new hospital data age groups

# Download GBD 2019 envelope
df <- fread(FILEPATH)

# Download and format norway
nor <- fread(FILEPATH)
setnames(nor, 'standard_error', 'variance')
nor[, data := mean]
nor[, me_name := 'ip_envelope']
nor[, index := NULL]
nor[, V1 := NULL]
nor[, run_id := NULL]
nor[, val := NULL]
nor[, mean := NULL]


# Replace by NID
df <- df[!nid %in% nor$nid]
df <- rbind(df, nor, fill = TRUE)
write_csv(df, FILEPATH)


## Now new age groups
files <- Sys.glob(FILEPATH)
files <- files[-2]
new_df <- rbindlist(lapply(files, fread), fill = TRUE)

new_df[, data := mean]
new_df[, me_name := 'ip_envelope']
new_df[, index := NULL]
new_df[, V1 := NULL]
new_df[, run_id := NULL]
new_df[, val := NULL]
new_df[, mean := NULL]

new_df <- new_df[age_group_id != 22] # Kind of weird artefact from master_data? Guess we have some that isn't age-sex split

# And there's some weird outliers
new_df <- new_df[data < 5] # 5 is the previous max so going to set it at that. Especially given the time crunch
new_df[data > 1 & age_group_id == 2, data := 1] # Set neonata age group to 1. This is basically what we do with IFD. This will prevent craziness, and wiill need to be re-assessed when we figure out the live birth thing

df <- df[!(nid %in% unique(new_df$nid))]
setnames(new_df, 'standard_error', 'variance')
df <- rbind(df, new_df, fill = TRUE)
write_csv(df, FILEPATH)

