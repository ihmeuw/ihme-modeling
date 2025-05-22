# ST-GPR outliering

# Outliering sourced from spreadsheet
outl_path <- paste0(home_dir, "FILEPATH")
outlier_sheet_xlsx <- openxlsx::read.xlsx(outl_path)
outldt <- as.data.table(outlier_sheet_xlsx)
str(outldt)
outldt <- outldt %>%
  select(location_name, year_id, nid, age_group_id, sex_id, confirmed) %>%
  mutate(confirmed = tolower(confirmed)) %>%
  filter(confirmed != "no") %>%
  select(-confirmed)

outldt <- merge(outldt, location_metadata[, .(location_id, location_name)], by = "location_name", all.x = T)
outldt <- outldt %>% select(-location_name)


split_and_melt <- function(dt) {
  # Melt the data to long format
  dt_long <- melt(dt,
    measure.vars = c("age_group_id", "sex_id"),
    variable.name = "type", value.name = "group"
  )

  # Create a row for each element in the 'group' column after splitting by ','
  dt_long <- dt_long[, .(group = unlist(strsplit(as.character(group), ","))), by = .(location_id, year_id, nid, type)]

  dt_long <- unique(dt_long)

  # Separate 'dt_long' into two tables: one for age_group_id and one for sex_id
  dt_age <- dt_long[type == "age_group_id", .(location_id, year_id, nid, age_group_id = as.numeric(group))]
  dt_sex <- dt_long[type == "sex_id", .(location_id, year_id, nid, sex_id = as.numeric(group))]

  # Get distinct records from dt_sex since each (location_id, year_id, nid) combination should have unique sex_id(s)
  dt_sex_unique <- unique(dt_sex)

  # Merge age and sex tables. Since 'age_group_id' is more granular and we want to ensure every age group has sex_id, use an 'all.x = TRUE' merge
  result <- merge(dt_age, dt_sex_unique, by = c("location_id", "year_id", "nid"), all.x = TRUE, allow.cartesian = TRUE)

  return(result)
}

# Final outliering sheet processing
outliers <- split_and_melt(outldt)
outliers <- unique(outliers)
outliers[, is_outlier := 1]
outliers[sex_id == 1, sex := "Male"][sex_id == 2, sex := "Female"]
outliers[, sex_id := NULL]

# Read in pre_outliering data
pre_outliering_dt <- fread(pre_outliering_dt_path) # from apply_split.R

# Apply outliering
setkey(pre_outliering_dt, nid, location_id, year_id, age_group_id, sex) # for proper merging
setkey(outliers, nid, location_id, year_id, age_group_id, sex) # for proper merging

post_outliering_dt <- pre_outliering_dt[outliers, on = .(location_id, year_id, nid, age_group_id, sex), is_outlier := i.is_outlier]

# Legible indicators of adjustments and outliers
post_outliering_dt$split_indicator <- ifelse(post_outliering_dt$needs_split == 1, "Age/sex split", "Originally age/sex specific")
post_outliering_dt$is_outlier_str <- ifelse(post_outliering_dt$is_outlier == 1, "Outlier", "Inlier")
