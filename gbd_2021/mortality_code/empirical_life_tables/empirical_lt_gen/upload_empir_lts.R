## Upload empirical lifetable database to the database
if (Sys.info()[1]=="Windows") {
  root <- "FILEPATH"
} else {
  root <- "FILEPATH"
}

library(assertable)
library(haven)
library(readr)
library(data.table)


library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")
library(ltcore, lib = "FILEPATH")

empir_run_comment <- "COMMENT"

## Import location, age, and source type maps
source_type_map <- setDT(get_mort_ids("source_type"))[, .(source_type_id, type_short)]
source_type_map[, type_short := tolower(type_short)]
loc_map <- setDT(get_locations(level = "all"))[, .(location_id, ihme_loc_id)]
age_map <- setDT(get_age_map("lifetable"))[, .(age_group_years_start, age_group_id)]

## Import lifetables
input_dir <- "FILEPATH"
lt_categories <- list("universal" = 1, "location-specific" = 2, "zaf" = 3, "usa" = 4)
lt_filenames <- list("universal" = "ltbase", "location-specific" = "ltbase_small", "zaf" = "ZAF_life_tables", "usa" = "usa_life_tables")

import_lt <- function(lt_category) {
  print(lt_category)
  print(lt_filenames[[lt_category]])
  dt <- setDT(read_dta(paste0(input_dir, "/", lt_filenames[[lt_category]], ".dta")))
  dt[, life_table_category_id := lt_categories[[lt_category]]]
  if(lt_category == "usa") dt[, source_type := "VR"] # Source type variable is NA for USA -- fill in here.
  return(dt)
}

empir_lts <- rbindlist(lapply(names(lt_categories), import_lt), use.names = T, fill = T)

## Run initial assertions and formatting
lx_colnames <- paste0("lx", c(0,1,seq(5,95,5)))
lx_na_colnames <- paste0("lx", seq(100, 110, 5)) # These colnames do not exist for ZAF and USA-specific LTs
lt_colnames <- c("ihme_loc_id", "year", "sex", "source_type", "life_table_category_id", lx_colnames, 
                "v5q0", "v45q15", "logit5q0", "logit45q15")

empir_lts <- empir_lts[, .SD, .SDcols= c(lt_colnames, lx_na_colnames)]

# ## Extract 5q0 and 45q15
summary_lt_metrics <- empir_lts[, .SD, .SDcols = lt_colnames[!(grepl("lx", lt_colnames) | grepl("logit", lt_colnames))]]
empir_lts[, c("v5q0", "v45q15", "logit5q0", "logit45q15") := NULL]

## Calculate qx from lx
lx_to_qx_wide(dt = empir_lts, keep_lx = T, assert_na = F)

## Reshape from wide to long, and then append everything together
empir_lts <- melt(empir_lts, id.vars = c("ihme_loc_id", "year", "sex", "source_type", "life_table_category_id"))
empir_lts[grepl("lx", variable), life_table_parameter_id := 4]
empir_lts[grepl("qx", variable), life_table_parameter_id := 3]
empir_lts[, age_group_years_start := as.integer(substr(variable, 3, 6))]
empir_lts <- merge(empir_lts, age_map, by = "age_group_years_start", all.x = T)
assert_values(empir_lts, "age_group_id", "not_na")
empir_lts[, c("variable", "age_group_years_start") := NULL]

summary_lt_metrics <- melt(summary_lt_metrics, id.vars = c("ihme_loc_id", "year", "sex", "source_type", "life_table_category_id"))
summary_lt_metrics[, life_table_parameter_id := 3]
summary_lt_metrics[variable == "v5q0", age_group_id := 1]
summary_lt_metrics[variable == "v45q15", age_group_id := 199]
summary_lt_metrics[, variable := NULL]

empir_lts <- rbindlist(list(empir_lts, summary_lt_metrics), use.names = T)

## Convert string and other variables to vartypes
## Sex
empir_lts[sex == "male", sex_id := 1]
empir_lts[sex == "female", sex_id := 2]

## Source type
empir_lts[, source_type := tolower(source_type)]
# empir_lts[source_type == "hmd", source_type := "vr"]
empir_lts <- merge(empir_lts, source_type_map, by.x = "source_type", by.y = "type_short", all.x = T)
assert_values(empir_lts, "source_type_id", "not_na")

## Location
empir_lts <- merge(empir_lts, loc_map, by = "ihme_loc_id", all.x = T)

## Drop extraneous variables and renames
empir_lts[, c("sex", "source_type", "ihme_loc_id") := NULL]
setnames(empir_lts, c("value", "year"), c("mean", "year_id"))

## Final assertions
non_na_cols <- colnames(empir_lts)[!colnames(empir_lts) %in% c("mean")]
assert_values(empir_lts, non_na_cols, "not_na")

lx_not_na_ages <- unique(age_map[age_group_years_start <= 85, age_group_id])
assert_values(empir_lts[age_group_id %in% lx_not_na_ages & life_table_parameter_id == 4], "mean", "not_na")
qx_not_na_ages <- unique(age_map[age_group_years_start <= 80, age_group_id])
assert_values(empir_lts[age_group_id %in% qx_not_na_ages & life_table_parameter_id == 3], "mean", "not_na")

## Output file
empir_version <- gen_new_version("life table empirical", "data")
upload_filepath <- "FILEPATH"
write_csv(empir_lts, upload_filepath)

## Upload new file
upload_results(file = upload_filepath, model_name = "life table empirical", model_type = "data", run_id = empir_version)
update_status("life table empirical", "data", run_id = empir_version, new_status = "best", new_comment = empir_run_comment)

