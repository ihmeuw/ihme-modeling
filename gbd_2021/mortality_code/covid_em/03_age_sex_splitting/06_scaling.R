# scale and aggregate the EM results
# format for external teams

library(data.table)
library(mortcore)
library(mortdb)

parser <- argparse::ArgumentParser()

parser$add_argument(
  "--dir_output",
  type = "character",
  required = !interactive(),
  default = "FILEPATH",
  help = "Output directory for em adjustment"
)
args <- parser$parse_args()

list2env(args, .GlobalEnv)


# Setup -------------------------------------------------------------------

loc_map <- fread(fs::path(dir_output, "loc_map", ext = "csv"))

id_vars <- c("location_id", "year_id", "sex_id", "age_group_id", "draw")


# Load EM -----------------------------------------------------------------

files_em <- list.files(
  path = fs::path(
    dir_output, "draws", "04_adjusted_age_sex"
  ),
  full.names = TRUE)

dt_em <- lapply(files_em, readRDS) |> rbindlist(use.names = TRUE)


# Format Data -------------------------------------------------------------

dt_em <- dt_em[, .SD, .SDcols = c(id_vars, "pop", "new_excess_deaths")]

# Scale/Aggregate ---------------------------------------------------------

# aggregate UTLAS to England's 9-region level
# parent locations need to be dropped first if they are present
level_5_gbr <- loc_map[ihme_loc_id %like% "GBR" & level == 5, location_id]
level_5_gbr_ihme <- loc_map[ihme_loc_id %like% "GBR" & level == 5, ihme_loc_id]
dt_em<- dt_em[!location_id %in% level_5_gbr]

dt_em <- mortcore::agg_results(data = dt_em,
                               id_vars = id_vars,
                               value_vars = c("pop",
                                              "new_excess_deaths"),
                               tree_only = "GBR",
                               start_agg_level = 6,
                               end_agg_level = 5)


# Additive scaling --------------------------------------------------------

# distribute the difference in sum and national in accordance with
# the proportion of the absolute value (by age-sex-draw)

dt_em[loc_map, `:=` (level = i.level,
                     parent_id = parent_id),
      on = .(location_id)]

# start scaling from level 4 to level 3, continue until
# level 6 is scaled to level 5
dt_scaled_em <- lapply(4:6, function(level_child) {

  # split parent and child locations
  dt_parent <- dt_em[level == level_child - 1]
  dt_child <- dt_em[level == level_child]

  # sum child deaths
  dt_child[, child_sum_deaths := sum(new_excess_deaths),
           by = .(parent_id, year_id, sex_id, age_group_id, draw)]

  # calculate absolute proportions of deaths
  dt_child[, deaths_absolute := abs(new_excess_deaths)]
  dt_child[, total_deaths_absolute := sum(deaths_absolute),
           by = .(parent_id, year_id, sex_id, age_group_id, draw)]
  dt_child[, prop_absolute := deaths_absolute / total_deaths_absolute]

  # calculate the difference between the parent and child-summed deaths
  dt_child[dt_parent, parent_deaths := i.new_excess_deaths,
           on = .(parent_id = location_id, year_id, sex_id, age_group_id, draw)]
  dt_child[, deaths_diff := parent_deaths - child_sum_deaths]

  # New adjustment is the deaths_diff multiplied by prop absolute
  dt_child[, deaths_adj := deaths_diff * prop_absolute]
  dt_child[, scaled_excess_deaths := deaths_adj + new_excess_deaths]

  return(dt_child)


}) |> rbindlist()

# format and append the national values
# additionally, save the unscaled values of locations we want to aggregate
# to national level (CHN, UKR, GBR)

level_4_keep_locs <- loc_map[level == 4 &
                               (ihme_loc_id %like% "CHN" |
                                  ihme_loc_id %like% "UKR" |
                                ihme_loc_id %like% "GBR"),
                             location_id]

dt_national <- dt_em[level == 3 | location_id %in% level_4_keep_locs]
dt_scaled_em <- dt_scaled_em[!location_id %in% level_4_keep_locs]

setnames(dt_national, "new_excess_deaths", "scaled_excess_deaths")

keep_cols <- c("location_id", "year_id", "sex_id", "age_group_id", "draw",
               "pop", "scaled_excess_deaths")
dt_scaled_em <- rbind(dt_scaled_em[, ..keep_cols],
                      dt_national[, ..keep_cols])

# reaggregate GBR from GBR_433, 434, 4636, 4749
# additionally aggregate UKR and CHN subnats to national
level_3_gbr <- loc_map[ihme_loc_id %like% "GBR" & level == 3, location_id]
dt_scaled_em <- dt_scaled_em[!location_id %in% c(level_3_gbr, "UKR", "CHN")]

for(ihme_loc in c("GBR", "UKR", "CHN")) {
  dt_scaled_em <- mortcore::agg_results(data = dt_scaled_em,
                                        id_vars = id_vars,
                                        value_vars = c("scaled_excess_deaths",
                                                       "pop"),
                                        tree_only = ihme_loc,
                                        start_agg_level = 4,
                                        end_agg_level = 3)
}


# Format Results ----------------------------------------------------------

# calculate excess death rate
dt_scaled_em[, em_rate := scaled_excess_deaths / pop]


# Summarize Results -------------------------------------------------------

dt_scaled_em_summary <- dt_scaled_em[, .(pop_mean = mean(pop),
                                         scaled_excess_deaths_mean = mean(scaled_excess_deaths),
                                         em_rate_mean = mean(em_rate)),
                                     by = .(location_id, year_id, sex_id, age_group_id)]


# Save Results ------------------------------------------------------------

readr::write_csv(
  dt_em,
  fs::path(dir_output, "draws", "06_scaled_em",
           "final_scaled_em", ext = "csv")
)

readr::write_csv(
  dt_scaled_em_summary,
  fs::path(dir_output, "summary", "06_scaled_em",
           "final_scaled_em", ext = "csv")
)
