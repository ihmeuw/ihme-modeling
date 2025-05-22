# Make ex plots based on onemod results

library(ggplot2)
library(data.table)
library(furrr)
library(ggrepel)

source("FILEPATH")

plan("multisession")

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

# Load Config and Set up --------------------------------------------------

cfg <- config::get(file = fs::path(args$dir_output, "config.yml"))

map_age <- fread(fs::path(args$dir_output, "age_map_gbd.csv"))
map_loc <- fread(fs::path(args$dir_output, "loc_map.csv"))

map_loc <- map_loc[is_estimate == 1]
map_loc <- map_loc[!(
  (level == 5 & grepl("IND", ihme_loc_id)) |
    (level == 4 & grepl("UKR", ihme_loc_id))
)]
map_loc[, country := substr(ihme_loc_id, 1, 3)]
map_loc[, max_level := max(level), by = "country"]
map_loc <- map_loc[level == max_level | location_id %in% c(433, 434, 4636, 361, 354)]

national_locs <- map_loc[level == 3 | location_id %in% c(44533, 354, 361), location_id]

graph_age_ids <- map_age[age_start %in% cfg$ex_plots$age_starts, age_group_id]

id_vars <- c("location_id", "sex_id", "age_group_id", "year_id")
id_vars_ex <- c("location_id", "sex_id", "age_start", "year_id")
id_vars_lt <- c("location_id", "age_start", "age_end", "sex_id", "year_id")

dir_ex <- fs::path(args$dir_output, "diagnostics", "ex")

if (!fs::dir_exists(dir_ex)) fs::dir_create(dir_ex)


# Load Data ---------------------------------------------------------------

keep_cols <- c(id_vars_ex, "mean")

dt_gbd_2021_ex <-
  fs::path(cfg$dir_canonizer, cfg$run_ids_previous_round$canonizer, "output/final_abridged_lt_summary") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(
    (year_id < 2020 & type == "with_hiv") | (year_id >= 2020 & type == "with_shock"),
    sex_id != 3,
    life_table_parameter_name == "ex"
  ) |>
  dplyr::right_join(map_loc[level > 2], by = "location_id") |>
  dplyr::right_join(map_age, by = "age_group_id") |>
  dplyr::select(dplyr::all_of(id_vars_ex), "mean") |>
  dplyr::rename(ex = mean) |>
  dplyr::collect() |>
  setDT() |>
  list() |>
  setNames("GBD 2021")

dt_wpp_ex <-
  fs::path(cfg$fp_inputs$wpp_comparator, "Ex1", ext = "parquet") |>
  arrow::open_dataset(format = "parquet") |>
  dplyr::filter(sex_id %in% 1:2) |>
  dplyr::select(year_id = date_start, sex_id, ihme_loc_id = country_ihme_loc_id,
                age_start, ex = value) |>
  dplyr::left_join(map_loc[, .(location_id, ihme_loc_id)], by = "ihme_loc_id") |>
  dplyr::select(-ihme_loc_id) |>
  dplyr::collect() |>
  setDT()

dt_onemod_ex <-
  cfg$fp_inputs$onemod_means_msca_unadjusted |>
  arrow::open_dataset(format ="parquet") |>
  dplyr::select(all_of(id_vars), "ex_point_estimate") |>
  dplyr::rename(ex = ex_point_estimate) |>
  dplyr::left_join(map_age[, .(age_group_id, age_start)], by = "age_group_id") |>
  dplyr::collect() |>
  setDT()

dt_onemod <-
  fs::path(args$dir_output, "summaries") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::select(all_of(id_vars), mx = mx_age_scaled_mean) |>
  dplyr::filter(age_group_id %in% map_age$age_group_id) |>
  dplyr::left_join(map_age[, .(age_group_id, age_start, age_end)], by = "age_group_id") |>
  dplyr::mutate(age_length = age_end - age_start) |>
  dplyr::collect() |>
  setDT()

# Calculate life tables ---------------------------------------------------

dt_onemod[, ax := demCore::mx_to_ax(mx, age_length)] |>
  _[, qx := demCore::mx_ax_to_qx(mx, ax, age_length)] |>
  _[qx >= 1 & age_start < 95, qx := 0.99999] |>
  _[, ax := demCore::mx_qx_to_ax(mx, qx, age_length)] |>
  _[, qx := demCore::mx_ax_to_qx(mx, ax, age_length)]

demCore::gen_lx_from_qx(dt_onemod, id_vars_lt)
demCore::gen_dx_from_lx(dt_onemod, id_vars_lt)
demCore::gen_nLx(dt_onemod, id_vars_lt)
demCore::gen_Tx(dt_onemod, id_vars_lt)
demCore::gen_ex(dt_onemod, assert_na = FALSE)

keep_cols_onemod <- c(id_vars_ex, "ex")
dt_onemod <- list(
  `MSCA OneMod` = dt_onemod_ex[, ..keep_cols_onemod],
  `Demo Onemod` = dt_onemod[, ..keep_cols_onemod]
)

# Format Data -------------------------------------------------------------

dt_combined <-
  list(`WPP` = dt_wpp_ex) |>
  c(dt_onemod, dt_gbd_2021_ex) |>
  rbindlist(
    idcol = "estimate_source",
    use.names = TRUE
  ) |>
  _[map_age[, .(age_group_id, age_start, age_group_name)], on = .(age_start), nomatch = NULL] |>
  _[map_loc[, .(location_name, location_id, ihme_loc_id,
                super_region_name, sort_order)], on = .(location_id), nomatch = NULL] |>
  _[, `:=` (
    ihme_loc_id = reorder(ihme_loc_id, sort_order),
    age_group_name = reorder(age_group_name, age_start),
    sex_name = dplyr::case_when(
      sex_id == 1 ~ "Male",
      sex_id == 2 ~ "Female",
      sex_id == 3 ~ "Both"
    )
  )] |>
  _[!is.na(estimate_source)]


# Graph 1: Ex over time ---------------------------------------------------

plot_graph_1 <- function(dt) {

  current_loc_id <- unique(dt$location_id)
  current_ihme_loc_id <- unique(dt$ihme_loc_id)
  current_loc_name <- unique(dt$location_name)

  title_string <- glue::glue("{current_ihme_loc_id} {current_loc_name}: Life Expectancy over time")

  dt |> ggplot(aes(x = year_id, y = ex, color = estimate_source)) +
    geom_line(alpha = 0.6) +
    facet_wrap(
      ~sex_name+age_group_name,
      scales = "free_y",
      ncol = graph_age_ids |> length(),
      nrow = dt$sex_name |> unique() |> length()
    ) +
    theme_bw() +
    scale_color_manual(values = color_pallete) +
    theme(legend.position = "bottom") +
    labs(
      y = "Life Expectancy (ex)",
      x = "Year",
      title = title_string
    )
}

dt_graph_1 <- dt_combined[
  age_group_id %in% graph_age_ids &
    year_id %in% cfg$years
]

unique_sources <- dt_graph_1$estimate_source |>
  unique() |>
  sort()
n_colors <- unique_sources |> length()

color_pallete <- RColorBrewer::brewer.pal(name = "Dark2", n = n_colors) |>
  setNames(unique_sources)

list_plots <- dt_graph_1 |>
  split(by = c("ihme_loc_id"), sorted = TRUE, drop = TRUE) |>
  purrr::map(plot_graph_1)

file_name_out <- "ex_1950_2023"
dir_out_tmp <- tempdir()

furrr::future_iwalk(
  list_plots,
  ~withr::with_pdf(
    new = fs::path(dir_out_tmp, paste(file_name_out, .y, sep = "-"), ext = "pdf"),
    width = 15,
    height = 8,
    code = plot(.x)
  )
)

files_temp <- fs::path(dir_out_tmp, paste(file_name_out, names(list_plots), sep = "-"), ext = "pdf")
stopifnot(all(fs::file_exists(files_temp)))

qpdf::pdf_combine(
  input = files_temp,
  output = fs::path(dir_ex, file_name_out, ext = "pdf")
)

# National locations only
list_plots <-
  dt_graph_1[location_id %in% national_locs] |>
  split(by = c("ihme_loc_id"), sorted = TRUE, drop = TRUE) |>
  purrr::map(plot_graph_1)

file_name_out <- "ex_1950_2023_nationals_only"
dir_out_tmp <- tempdir()

furrr::future_iwalk(
  list_plots,
  ~withr::with_pdf(
    new = fs::path(dir_out_tmp, paste(file_name_out, .y, sep = "-"), ext = "pdf"),
    width = 15,
    height = 8,
    code = plot(.x)
  )
)

files_temp <- fs::path(dir_out_tmp, paste(file_name_out, names(list_plots), sep = "-"), ext = "pdf")
stopifnot(all(fs::file_exists(files_temp)))

qpdf::pdf_combine(
  input = files_temp,
  output = fs::path(dir_ex, file_name_out, ext = "pdf")
)


# Graph 2: e0 males vs. females scatter -----------------------------------

plot_graph_2 <- function(dt) {

  current_year <- unique(dt$year_id)

  title_string <- glue::glue("{current_year}: Male vs Female Life Expectancy at Birth")

  dt |>
    ggplot(aes(color = super_region_name, x = Male, y = Female, label = ihme_loc_id)) +
    geom_point() +
    geom_text_repel(max.overlaps = 15) +
    geom_abline(slope = 1, color = "grey30", lty = "dashed") +
    theme_bw() +
    labs(title = title_string) +
    theme(legend.position = "bottom")
}

# perform this for each onemod model candidate
for (est in unique(dt_combined$estimate_source)) {

  dt_temp <-
    dt_combined[
      estimate_source == est &
        age_start == 0
    ] |>
    dcast(
      ihme_loc_id + year_id + super_region_name ~ sex_name,
      value.var = "ex"
    )

  list_plots <- dt_temp |>
    split(by = c("year_id"), sorted = TRUE) |>
    purrr::map(plot_graph_2)

  file_name_out <- glue::glue("{est}-ex_males_females_scatter")
  dir_out_tmp <- tempdir()

  furrr::future_iwalk(
    list_plots,
    ~withr::with_pdf(
      new = fs::path(dir_out_tmp, paste(file_name_out, .y, sep = "-"), ext = "pdf"),
      width = 15,
      height = 8,
      code = plot(.x)
    )
  )

  files_temp <- fs::path(dir_out_tmp, paste(file_name_out, names(list_plots), sep = "-"), ext = "pdf")
  stopifnot(all(fs::file_exists(files_temp)))

  qpdf::pdf_combine(
    input = files_temp,
    output = fs::path(dir_ex, file_name_out, ext = "pdf")
  )

  # save location years where male > female
  dt_temp[Male > Female] |>
    readr::write_csv(fs::path(dir_ex, glue::glue("{est}_male_e0_greater_than_female_e0.csv")))
}

# also save locs where e80 for males is higher
for (est in unique(dt_combined$estimate_source)) {

  dt_temp <-
    dt_combined[
      estimate_source == est &
        age_start == 80
    ] |>
    dcast(
      ihme_loc_id + year_id + super_region_name ~ sex_name,
      value.var = "ex"
    )

  # save location years where male > female
  dt_temp[Male > Female] |>
    readr::write_csv(fs::path(dir_ex, glue::glue("{est}_male_e80_greater_than_female_e80.csv")))
}


# Scatter Estimates vs other estimates ------------------------------------

plot_graph_3 <- function(dt, value_vars) {

  current_year <- unique(dt$year_id)

  title_string <- glue::glue("{current_year}: {value_vars[1]} vs {value_vars[2]} Life Expectancy at Birth")

  dt |>
    ggplot(aes(color = super_region_name, x = .data[[value_vars[1]]], y = .data[[value_vars[2]]], label = ihme_loc_id)) +
    geom_point() +
    geom_text_repel(max.overlaps = 15) +
    geom_abline(slope = 1, color = "grey30", lty = "dashed") +
    facet_wrap(~sex_name) +
    theme_bw() +
    labs(title = title_string) +
    theme(legend.position = "bottom")


}

combos <-
  dt_combined$estimate_source |>
  unique() |>
  combn(2, simplify = FALSE)

for (value_vars in combos) {

  dt_temp <-
    dt_combined[
      estimate_source %in% value_vars &
        age_start == 0
    ] |>
    dcast(
      ihme_loc_id + year_id + super_region_name + sex_name ~ estimate_source,
      names_from = estimate_source,
      value.var = "ex"
    ) |>
    setDT()

  list_plots <- dt_temp |>
    split(by = c("year_id"), sorted = TRUE) |>
    purrr::map(\(x) plot_graph_3(dt = x, value_vars = value_vars))

  file_name_out <- glue::glue("{value_vars[1]}_vs_{value_vars[2]}_scatter")
  dir_out_tmp <- tempdir()

  furrr::future_iwalk(
    list_plots,
    ~withr::with_pdf(
      new = fs::path(dir_out_tmp, paste(file_name_out, .y, sep = "-"), ext = "pdf"),
      width = 15,
      height = 8,
      code = plot(.x)
    )
  )

  files_temp <- fs::path(dir_out_tmp, paste(file_name_out, names(list_plots), sep = "-"), ext = "pdf")
  stopifnot(all(fs::file_exists(files_temp)))

  qpdf::pdf_combine(
    input = files_temp,
    output = fs::path(dir_ex, file_name_out, ext = "pdf")
  )

}

# Graph 3: e0 maps ------------------------------------------------

mapping <- CJ(
  year_id = c(seq(1950, 2020, 10), 2021:2024),
  sex_id = 1:2
)

mapping[, f_name := paste0(args$dir_output, "/diagnostics/maps/e0_map_", year_id, "_", sex_id, ".pdf")]

for (i in 1:nrow(mapping)) {

  s <- mapping[i, sex_id]
  y <- mapping[i, year_id]
  f_name <- mapping[i, f_name]

  temp_s <- ifelse(s == 1, "male", "female")

  temp_data <- dt_onemod_ex[sex_id == s & year_id == y & age_start == 0]
  setnames(temp_data, "ex", "mapvar")

  temp_limits <- c(seq(20, 60, 10), seq(65, 88, 5), 95)

  gbd_map(
    data = temp_data,
    limits = temp_limits,
    sub_nat = "all",
    legend = TRUE,
    inset = TRUE,
    labels = NULL,
    pattern = NULL,
    col = "RdYlBu",
    col.reverse = FALSE,
    na.color = "grey20",
    title = glue::glue("Life expectancy at birth from onemod processing: {temp_s}, year {y}"),
    fname = f_name,
    legend.title = "Life expectancy",
    legend.columns = NULL,
    legend.cex = 1,
    legend.shift = c(0,0)
  )

}

# combine and save by sex
fs::dir_create(
  glue::glue("{args$dir_output}/diagnostics/combined_maps"),
  mode = "775"
)

for (s in unique(mapping$sex_id)) {

  pdftools::pdf_combine(
    input = mapping[sex_id == s, f_name],
    output = glue::glue("{args$dir_output}/diagnostics/combined_maps/e0_maps_sex_id_{s}.pdf")
  )

}
