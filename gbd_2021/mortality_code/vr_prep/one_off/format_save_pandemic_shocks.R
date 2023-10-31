#
# Format and Save Pandemics shocks
#
# Input:
#   Shocks aggregation at the draw level
#
# Output:
#   File formatted to be combined with handoff 1 files including allcause deaths
#   in 2020 for all locations and shocks age groups

library(assertable)
library(data.table)

shocks_version = 
shocks_folder <-paste0()


# Helper function --------------------------------------------------------------
# originally written for FLT shocks handling

aggregate_deaths <- function(dataset, target_age_group_id, child_age_group_ids,
                             id_vars) {

  if(is.data.table(dataset) == F) dataset <- setDT(dataset)

  # subset dataset to only contain the age group ids that are used to aggregate
  # up to the target age group id
  assertable::assert_colnames(dataset, colnames = c("age_group_id"),
                              only_colnames = F)
  child_ages <- dataset[age_group_id %in% child_age_group_ids,]

  # select any age groups that are missing
  child_age_groups_missing <- child_age_group_ids[
      !(child_age_group_ids %in% unique(child_ages[, age_group_id]))
  ]
  if (length(child_age_groups_missing) > 0) {
    stop(paste0("The following age groups id(s) required for aggregation up to
                age group id ", target_age_group_id," are missing: ",
                paste0(child_age_groups_missing, collapse = ", ")))
  }

  target_age <- child_ages[, lapply(.SD, sum), .SDcols = "deaths", by = id_vars]
  target_age[, age_group_id := target_age_group_id]
  return(target_age)
}


# Format and Resave ------------------------------------------------------------

filenames <- list.files(shocks_folder, full.names = T)

# rewrite as mean level in team folder - too large to use at draw
i = 1
for(ff in filenames) {
  loc_id <- gsub(,ff)
  print(loc_id)
  print(i)
  i <- i +1

  if(file.exists(ff)) {

    # IMPORT shocks
    shock_numbers <- fread(ff)

    # keep cause_id 294: all-cause shocks in shocks file
    shock_numbers <- shock_numbers[cause_id == 294 & year_id == 2020]
    shock_numbers <- shock_numbers[, cause_id := NULL]

    # melt long on draw
    shock_numbers <- melt(
      shock_numbers,
      value.name = "deaths",
      id.vars = c("location_id", "sex_id", "age_group_id", "year_id"),
      measure.vars = grep("draw", names(shock_numbers), value = T),
      variable.name = "draw",
      variable.factor = F
    )
    shock_numbers[, draw := as.integer(substr(draw, 6, nchar(draw)))]

    # aggregate over draw
    shock_numbers <- shock_numbers[, .(deaths = mean(deaths)),
                                   by = c("location_id", "sex_id", "age_group_id", "year_id")]

    readr::write_csv(shock_numbers,  paste0())

  } else {

    print("Shocks do not exist in 2020 for that location")

  }
}


# Combine ----------------------------------------------------------------------

# combine and save as one dataset
pandemic_shocks <- assertable::import_files(
  filenames = list.files(),
  folder = ,
  FUN = fread
)
setnames(pandemic_shocks, c("deaths","year_id","sex_id"), c("numkilled","year","sex"))

readr::write_csv(pandemic_shocks,
                 paste0())
