# VR scatters

library(mortdb)
library(data.table)
library(ggplot2)


if (interactive()) {
  version <-
  prev_version <-
  gbd_year <-
  release_id <-
  age_specific_plots <-
  pop_baseline_vers <-
  prev_pop_baseline_vers <-
} else {
  args = commandArgs(trailingOnly = T)
  version <- args[1]
  prev_version <- args[2]
  gbd_year <- args[3]
  release_id <- args[4]
  age_specific_plots <- args[5]
  pop_baseline_vers <- args[6]
  prev_pop_baseline_vers <- args[7]
}

message("Load Inputs")
gbd_year <- as.numeric(gbd_year)
input_folder <- paste0("FILEPATH")

emp_dir <- "FILEPATH"
locs <- demInternal::get_locations(gbd_year = gbd_year)

# get population
pop_baseline <- get_mort_outputs('population', 'estimate', run_id=pop_baseline_vers, gbd_year = gbd_year)
prev_pop_baseline <- get_mort_outputs('population', 'estimate', run_id=prev_pop_baseline_vers, gbd_year = gbd_year)
setnames(pop_baseline, c("mean"), "pop")
setnames(prev_pop_baseline, c("mean"), "pop_old")

message("Finished pulling baseline pop")

# get locations for age specific plots
age_specific_locs <- fread("FILEPATH")

# get gbd year of previous round id (for when previous year not -1)
gbd_round_id_prev <- get_gbd_round(gbd_year = gbd_year-2)
gbd_year_prev <- get_gbd_year(gbd_round_id_prev)

deaths_prev_round <- mortdb::get_mort_outputs(
  model_name = "death number empirical",
  model_type = "data",
  run_id = "best",
  gbd_year = gbd_year_prev
)

deaths_curr <- fread("FILEPATH")
deaths_prev_raw <- fread("FILEPATH")

# Keep both sex, all age, non-outlier only
deaths_prev_round <- deaths_prev_round[outlier==0]
deaths_curr_split <- deaths_curr[outlier == 0]
deaths_curr <- deaths_curr[sex_id==3 & outlier==0]
deaths_prev <- deaths_prev_raw[sex_id==3 & outlier==0]

# Merge
cols <- c("sex_id", "age_group_id", "detailed_source", "location_id", "year_id", "estimate_stage_id", "mean")
all_deaths <- merge(deaths_curr, deaths_prev, by=setdiff(cols, 'mean'), suffixes = c("_new", "_old"), all=T)

# Merge on demographic information
# age name
ages <- mortdb::get_age_map(gbd_year = gbd_year, type = 'all')
all_deaths <- merge(all_deaths, ages[, .(age_group_id, age_group_name, age_group_years_start, age_group_years_end)], by='age_group_id')

# location name and ihme_loc_id
all_deaths <- mortdb::ids_to_names(all_deaths,
                                exclude = c("age_group_id","sex_id",
                                            "year_id","source_type_id"),
                                keep_ids = TRUE
                                )
all_age <- all_deaths[age_group_id == 22]

# Add pop
all_deaths <- merge(
  all_deaths,
  pop_baseline[, c("location_id","year_id","sex_id","age_group_id","pop")],
  by = c("location_id","year_id","sex_id","age_group_id"),
  all.x = TRUE
)
all_deaths <- merge(
  all_deaths,
  prev_pop_baseline[, c("location_id","year_id","sex_id","age_group_id","pop_old")],
  by = c("location_id","year_id","sex_id","age_group_id"),
  all.x = TRUE
)

# estimte rough mx
all_deaths[, `:=` (mx_new = mean_new / pop, mx_old = mean_old / pop_old)]


# Changes to previous run ------------------------------------------------------
message("Calculate changes to previous run")

# identify type of change for each data point
all_age[!is.na(mean_new) & is.na(mean_old), data_change_type := "new"]
all_age[is.na(mean_new) & !is.na(mean_old), data_change_type := "dropped"]
all_age[!is.na(mean_new) & !is.na(mean_old) & mean_new != mean_old, data_change_type := "changed"]
all_age[!is.na(mean_new) & !is.na(mean_old) & mean_new == mean_old, data_change_type := "identical"]

# calculate differences
all_age[data_change_type == "changed", diff := mean_new - mean_old]
all_age[data_change_type == "changed", perc_diff := ((mean_new - mean_old) / mean_old) * 100]

# rearrange location cols
changed_deaths <- merge(
  locs[, c("location_id", "ihme_loc_id", "location_name")],
  all_age[!data_change_type == "identical", -c("ihme_loc_id", "location_name")],
  by = "location_id",
  all.y = TRUE
)

# save
readr::write_csv(changed_deaths, paste0("FILEPATH"))


# Changes to previous round ----------------------------------------------------
message("Calculate changes to previous round")

# combine current deaths with previous round best
# Previous round only estimate stage id 21 so hardcoding for now
round_comp <- merge(deaths_curr_split[estimate_stage_id == 21], deaths_prev_round, by=setdiff(cols, 'mean'), suffixes = c("_new", "_old"), all=T)

# identify type of change for each data point
round_comp[!is.na(mean_new) & is.na(mean_old), data_change_type := "new"]
round_comp[is.na(mean_new) & !is.na(mean_old), data_change_type := "dropped"]
round_comp[!is.na(mean_new) & !is.na(mean_old) & mean_new != mean_old, data_change_type := "changed"]
round_comp[!is.na(mean_new) & !is.na(mean_old) & mean_new == mean_old, data_change_type := "identical"]

# calculate differences
round_comp[data_change_type == "changed", diff := mean_new - mean_old]
round_comp[data_change_type == "changed", perc_diff := ((mean_new - mean_old) / mean_old) * 100]

# get id names
changed_round <- mortdb::ids_to_names(
  round_comp,
  exclude = c("year_id","source_type_id"),
  keep_ids = TRUE
)

# subset to only changes >5%
changed_round <- changed_round[abs(perc_diff) > 5]

# clean up
setnames(changed_round, c("detailed_source", "source_name", "run_id"), c("source_name_new", "source_name_old", "run_id_old"))
changed_round <- changed_round[, c("location_id", "ihme_loc_id", "location_name",
                                   "year_id", "sex_id", "sex","age_group_id", "age_group_name", "nid_new", "underlying_nid_new",
                                   "source_name_new", "run_id_old", "nid_old", "underlying_nid_old",
                                   "source_name_old", "mean_new", "mean_old", "data_change_type",
                                   "diff", "perc_diff")]

# save
readr::write_csv(changed_round, paste0("FILEPATH"))


# Changes between years --------------------------------------------------------
message("Calculate changes between years")

# order so we have ascending years
deaths_curr_year <- deaths_curr_split[order(location_id, age_group_id, sex_id, year_id, estimate_stage_id)]

# calculate yearly difference
deaths_curr_year <- deaths_curr_year[order(location_id, year_id, estimate_stage_id)]
deaths_curr_year[, diff := mean - shift(mean),
                 by = c("location_id", "sex_id", "age_group_id", "estimate_stage_id")]
deaths_curr_year[, perc_diff := ((mean - shift(mean)) / shift(mean)) * 100,
                 by = c("location_id", "sex_id", "age_group_id", "estimate_stage_id")]
deaths_curr_year[, prev_year_value := shift(mean),
                 by = c("location_id", "sex_id", "age_group_id", "estimate_stage_id")]
# only keep where comparison is one year previous
deaths_curr_year[!(shift(year_id) == year_id - 1), ':=' (diff = NA, perc_diff = NA, prev_year_value = NA)]

# get id names
deaths_curr_year <- mortdb::ids_to_names(
  deaths_curr_year,
  exclude = c("year_id","source_type_id"),
  keep_ids = TRUE
)

# subset to abs difference > 5%
deaths_curr_year <- deaths_curr_year[abs(perc_diff) > 5]

# clean up
deaths_curr_year <- deaths_curr_year[, c("location_id", "ihme_loc_id", "location_name",
                                         "year_id", "sex_id", "sex","age_group_id", "age_group_name",
                                         "nid", "underlying_nid", "detailed_source", "source_type_id",
                                         "estimate_stage_id", "prev_year_value", "mean", "diff", "perc_diff"
                                         )]

# save
readr::write_csv(deaths_curr_year, paste0("FILEPATH"))

# NEW COD ----------------------------------------------------------------------

# quickly identify data that has been added to the raw CoD dataset since last run
raw_cod_old <- fread("FILEPATH")
raw_cod_old <- unique(raw_cod_old[, c("location_id", "source", "year_id")])

raw_cod_new <- fread("FILEPATH")
raw_cod_new <- unique(raw_cod_new[, c("location_id", "source", "year_id")])

new_cod_data <- dplyr::anti_join(raw_cod_new, raw_cod_old, by = names(raw_cod_new))

new_cod_data <- merge(
  locs[, c("location_id", "ihme_loc_id")],
  new_cod_data,
  by = "location_id",
  all.y = T
)
new_cod_data[, country_ihme_loc_id := substr(ihme_loc_id, 1, 3)]

# save
readr::write_csv(new_cod_data, paste0("FILEPATH"))

# NEW LOCATION YEARS -----------------------------------------------------------

# quickly identify new location years that have been added since last run
prev_loc_years <- unique(deaths_prev_raw[outlier == 0, c("location_id", "year_id")])
curr_loc_years <- unique(deaths_curr_split[, c("location_id", "year_id")])

new_loc_years <- dplyr::anti_join(curr_loc_years, prev_loc_years, by = c("location_id", "year_id"))

new_loc_years <- merge(
  locs[, c("location_id", "ihme_loc_id", "location_name")],
  new_loc_years,
  by = "location_id",
  all.y = T
)

# save
readr::write_csv(new_loc_years, paste0("FILEPATH"))


# Plots ------------------------------------------------------------------------
message("Plot")

# get sorting order
source("FILEPATH")
sort_order <- get_location_metadata(82, release_id = release_id)

# map sort order
loc_map <- unique(all_age[, .(location_id)])
loc_map <- merge(loc_map, sort_order[, .(location_id, location_name, sort_order)], by='location_id')
loc_map <- loc_map[order(sort_order)]

# Plot all ages and all sexes
title_all_ages <- paste0("FILEPATH")

# Avoid spamming mortality with slack messages, check if the file exists first.
# If we're rerunning graphing, no need to update channel
rerun <- file.exists(title_all_ages)

pdf(title_all_ages)
for (ee in 21:22){

  estimate_type <- ifelse(ee == 21, "without Shocks", "with Shocks")

  for (i in 1:nrow(loc_map)) {

    loc_name = loc_map[i, location_name]
    loc_id = loc_map[i, location_id]
    ihme_loc_id <- unique(all_age[location_id==loc_id, 'ihme_loc_id'])

    p <- ggplot(data=all_age[location_id == loc_id & estimate_stage_id == ee], aes(x=year_id)) +
      geom_point(aes(y=mean_old, colour = 'Previous VR'), shape = 1, size = 2) +
      geom_point(aes(y=mean_new, colour='Current VR'), shape = 23) +
      theme_bw() +
      ggtitle(paste0(loc_id, "; ", loc_name,"; ",ihme_loc_id,"; All Age Both Sexes Deaths ", estimate_type)) +
      scale_colour_manual(values=c("Red", "Black")) +
      labs(colour = "VR Type") +
      xlab("Year") +
      ylab("Deaths") +
      theme(plot.title = element_text(size=10))
    plot(p)


  }
}
dev.off()

if (age_specific_plots) {

  title_age_specific <- paste0("FILEPATH")
  title_age_specific_mx <- paste0("FILEPATH")

  if(nrow(age_specific_locs) > 0){
    age_specific_loc_map <- loc_map[location_id %in% age_specific_locs$location_id]
  } else {
    age_specific_loc_map <- copy(loc_map)
  }

  # age sort order
  age_order <- c(
    "Early Neonatal", "Late Neonatal", "Post Neonatal", "1-5 months",
    "6-11 months", "<1 year", "12 to 23 months", "2 to 4", "1 to 4", "5 to 9",
    "10 to 14", "15 to 19", "20 to 24", "25 to 29", "30 to 34", "35 to 39",
    "40 to 44", "45 plus", "45 to 49", "50 to 54", "55 to 59", "60 plus",
    "60 to 64", "65 plus" , "65 to 69", "70+ years", "70 to 74", "75 plus",
    "75 to 79", "80 plus", "80 to 84", "85 plus", "85 to 89", "90 plus",
    "90 to 94", "95 plus", "All Ages"
  )
  all_deaths$age_group_name <- factor(
    all_deaths$age_group_name,
    levels = age_order
  )

  pdf(title_age_specific, height = 12, width = 24)
  for (ee in 21:22){

    estimate_type <- ifelse(ee == 21, "without Shocks", "with Shocks")

    for (i in 1:nrow(age_specific_loc_map)) {

      loc_name = age_specific_loc_map[i, location_name]
      loc_id = age_specific_loc_map[i, location_id]
      ihme_loc_id <- unique(all_age[location_id==loc_id, 'ihme_loc_id'])

      temp <- all_deaths[location_id == loc_id]

      p <- ggplot(data=temp[estimate_stage_id == ee], aes(x=year_id)) +
        geom_point(aes(y=mean_old, colour = 'Previous VR'), shape = 1, size = 2) +
        geom_point(aes(y=mean_new, colour='Current VR'), shape = 23) +
        facet_wrap(~age_group_name, scales = "free_y") +
        theme_bw() +
        ggtitle(paste0(loc_id, "; ", loc_name,"; ",ihme_loc_id,"; Both Sexes Deaths ", estimate_type)) +
        scale_colour_manual(values=c("Red", "Black")) +
        labs(colour = "VR Type") +
        xlab("Year") +
        ylab("Deaths")
      plot(p)

    }
  }
  dev.off()

  pdf(title_age_specific_mx, height = 12, width = 24)
  for (ee in 21:22){

    estimate_type <- ifelse(ee == 21, "without Shocks", "with Shocks")

    for (i in 1:nrow(age_specific_loc_map)) {

      loc_name = age_specific_loc_map[i, location_name]
      loc_id = age_specific_loc_map[i, location_id]
      ihme_loc_id <- unique(all_age[location_id==loc_id, 'ihme_loc_id'])

      temp <- all_deaths[location_id == loc_id]

      p <- ggplot(data=temp[estimate_stage_id == ee], aes(x=year_id)) +
        geom_point(aes(y=mx_old, colour = 'Previous VR'), shape = 1, size = 2) +
        geom_point(aes(y=mx_new, colour='Current VR'), shape = 23) +
        facet_wrap(~age_group_name, scales = "free_y") +
        theme_bw() +
        ggtitle(paste0(loc_id, "; ", loc_name,"; ",ihme_loc_id,"; Both Sexes Mx estimate ", estimate_type)) +
        scale_colour_manual(values=c("Red", "Black")) +
        labs(colour = "VR Type") +
        xlab("Year") +
        ylab("Deaths")
      plot(p)

    }
  }
  dev.off()

}


if (!rerun) {
  mortdb::send_slack_message(message = paste0(""))
}
