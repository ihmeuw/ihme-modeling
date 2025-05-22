# Maps of raked mx

library(data.table)
library(pdftools)

parser <- argparse::ArgumentParser()

# Node arguments
parser$add_argument(
  "--dir_output",
  type = "character",
  required = !interactive(),
  default = "FILEPATH"
)
parser$add_argument(
  "--code_dir",
  type = "character",
  required = !interactive(),
  default = here::here()
)

args <- parser$parse_args()

list2env(args, .GlobalEnv)

cfg <- config::get(file = fs::path(args$dir_output, "config.yml"))

source("FILEPATH")

# Get data ----------------------------------------------------------

age_map <- demInternal::get_age_map(
  gbd_year = cfg$gbd_year,
  type = "all",
  all_metadata = TRUE
)

onemod_data <-
  fs::path(args$dir_output, "summaries") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::collect() |>
  setDT()

fs::dir_create(glue::glue("{dir_output}/diagnostics/maps"), mode = "775")

pop_data <-
  fs::path(args$dir_output, "population", ext = "parquet") |>
  arrow::read_parquet() |>
  setDT()
pop_sy_data <-
  fs::path(args$dir_output, "population_sy", ext = "parquet") |>
  arrow::read_parquet() |>
  setDT()

pop_data <- rbind(
  pop_data,
  pop_sy_data[!age_group_id %in% unique(pop_data$age_group_id)],
  fill = TRUE
)

map_loc <- fread(fs::path(args$dir_output, "loc_map.csv"))

# process data

map_loc <- map_loc[is_estimate == 1]
map_loc <- map_loc[!(
  (level == 5 & grepl("IND", ihme_loc_id)) |
    (level == 4 & grepl("UKR", ihme_loc_id))
)]
map_loc[, country := substr(ihme_loc_id, 1, 3)]
map_loc[, max_level := max(level), by = "country"]
map_loc <- map_loc[level == max_level | location_id %in% c(433, 434, 4636, 361, 354)]

onemod_data <- merge(
  onemod_data,
  age_map,
  by = "age_group_id"
)
onemod_data <- onemod_data[
  !is.na(mx_age_scaled_mean)
  ,
  .(location_id, sex_id, age_start, age_end, year_id, mx = mx_age_scaled_mean)
]

onemod_data <- onemod_data[year_id %in% c(seq(1950, 2020, 10), 2023)]

pop_data <- merge(
  pop_data,
  age_map,
  by = "age_group_id"
)

pop_data <- pop_data[, .(location_id, sex_id, age_start, age_end, year_id, pop = mean)]

onemod_data <- merge(
  onemod_data,
  pop_data,
  all.x = TRUE
)

onemod_data[, deaths := mx * pop]
onemod_data[, mx := NULL]

agg_data <- hierarchyUtils::agg(
  onemod_data,
  id_cols = c("location_id", "year_id", "sex_id", "age_start", "age_end"),
  col_stem = "age",
  col_type = "interval",
  value_cols = c("deaths", "pop"),
  mapping = data.table(
    age_start = c(0, 0.07671233, 1, 5, 15, 50, 70),
    age_end = c(0.07671233, 1, 5, 15, 50, 70, Inf)
  ),
  present_agg_severity = "warning",
  overlapping_dt_severity = "warning"
)

agg_5yr_data <- hierarchyUtils::agg(
  onemod_data,
  id_cols = c("location_id", "year_id", "sex_id", "age_start", "age_end"),
  col_stem = "age",
  col_type = "interval",
  value_cols = c("deaths", "pop"),
  mapping = data.table(
    age_start = seq(0, 95, 5),
    age_end = c(seq(5, 95, 5), Inf)
  ),
  present_agg_severity = "warning",
  overlapping_dt_severity = "warning"
)

enn <- onemod_data[age_end == 0.01917808]

agg_data <- rbind(enn, agg_data, agg_5yr_data)

agg_data[, mapvar := deaths / pop]

agg_data <- agg_data[location_id %in% map_loc$location_id]

mapping <- CJ(
  sex_id = unique(agg_data$sex_id),
  year_id = unique(agg_data$year_id)
)

mapping <- tidyr::crossing(mapping, unique(agg_data[, .(age_start, age_end)]))
setDT(mapping)

mapping[, f_name := paste0(dir_output, "/diagnostics/maps/mx_map_", year_id, "_", sex_id, "_", age_start, "_", age_end, ".pdf")]

for (i in 1:nrow(mapping)) {

  s <- mapping[i, sex_id]
  y <- mapping[i, year_id]
  a_s <- mapping[i, age_start]
  a_e <- mapping[i, age_end]
  f_name <- mapping[i, f_name]

  a_name <- age_map[age_start == a_s & age_end ==a_e, age_group_name]
  a_name <- a_name[1]

  temp_data <- agg_data[sex_id == s & year_id == y & age_start == a_s & age_end == a_e]

  temp_limits <- round(
    quantile(temp_data[, mapvar], seq(0, 1, 0.1)),
    5
  )
  temp_limits[1] <- temp_limits[1] - 0.001
  if(temp_limits[1] < 0) temp_limits[1] <- 0
  temp_limits[length(temp_limits)] <- temp_limits[length(temp_limits)] + 0.001

  if (length(unique(temp_limits)) != 11) stop("Limits issue")

  gbd_map(
    data = temp_data,
    limits = temp_limits,
    sub_nat = "all",
    legend = TRUE,
    inset = TRUE,
    labels = NULL,
    pattern = NULL,
    col = "RdYlBu",
    col.reverse = TRUE,
    na.color = "grey20",
    title = glue::glue("Mortality rate from onemod: sex {s}, year {y}, {a_name}"),
    fname = f_name,
    legend.title = "Mortality rate",
    legend.columns = NULL,
    legend.cex = 1,
    legend.shift = c(0,0)
  )

}

# combine and save by age group
fs::dir_create(
  glue::glue("{dir_output}/diagnostics/combined_maps"),
  mode = "775"
)

mapping <- mapping[order(sex_id, age_start, age_end, year_id)]
mapping[, grp := .GRP, by = c("age_start", "age_end")]

for (g in unique(mapping$grp)) {

  a_s <- unique(mapping[grp == g, age_start])
  a_e <- unique(mapping[grp == g, age_end])

  pdftools::pdf_combine(
    input = mapping[grp == g, f_name],
    output = glue::glue("{dir_output}/diagnostics/combined_maps/mx_maps_{a_s}_{a_e}.pdf")
  )

}
