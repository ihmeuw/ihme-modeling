# Scatter of enn vs lnn

library(data.table)
library(ggplot2)
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

dir_diagnostic <- fs::path(dir_output, "diagnostics/u5/enn_lnn_scatter")

fs::dir_create(
  dir_diagnostic,
  mode = "775"
)

# Get data ----------------------------------------------------------

loc_map <- fread(fs::path(dir_output, "loc_map.csv"))

onemod_data <-
  fs::path(args$dir_output, "summaries") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::collect() |>
  setDT()

onemod_data <- onemod_data[age_group_id %in% 2:3]

onemod_data <- onemod_data[, .(location_id, sex_id, age_group_id,
                               year_id, mean = mx_age_scaled_mean)]

onemod_data <- dcast(
  onemod_data,
  ...~sex_id,
  value.var = "mean"
)

setnames(onemod_data, c("1", "2"), c("Male", "Female"))

onemod_data[loc_map,
            `:=` (super_region_name = i.super_region_name,
                  ihme_loc_id = i.ihme_loc_id),
            on = .(location_id)
]

# plot ---------------------------------------------------------------

# map of plots
plot_map <- CJ(age_group_id = 2:3, year_id = cfg$years, subs = c(TRUE, FALSE))

plot_map <- plot_map[order(age_group_id, subs, year_id)]

for (i in 1:nrow(plot_map)) {

  if (plot_map[i, subs]) {
    temp_locs <- loc_map$location_id
  } else {
    temp_locs <- loc_map[(level == 3 & location_id != 6) | location_id == 44533, location_id]
  }

  temp_age <- ifelse(plot_map[i, age_group_id] == 2, "Early neonatal", "Late neonatal")
  temp_subs <- ifelse(plot_map[i, subs], "subs", "no_subs")

  if (i > 1) rm(temp_data)

  temp_data <- onemod_data[
    year_id == plot_map[i, year_id] &
      age_group_id == plot_map[i, age_group_id] &
      location_id %in% temp_locs
  ]

  # identify outliers for labeling
  temp_data[, diff := abs((Male - Female) / Female)]

  pdf(
    fs::path(dir_diagnostic, glue::glue("scatter_male_v_female_{temp_age}_{plot_map[i, year_id]}_{temp_subs}.pdf")),
    width = 10,
    height = 7
  )

  p <- ggplot(
    data = temp_data,
    aes(x = Male, y = Female, color = super_region_name)
  ) +
    geom_point(alpha = 0.5) +
    theme_bw() +
    ggrepel::geom_text_repel(data = temp_data[diff >= quantile(diff, 0.95)], aes(label = ihme_loc_id), alpha = 0.5) +
    ggtitle(glue::glue("Log mx for {temp_age}, year {plot_map[i, year_id]}, subs = {plot_map[i, subs]}")) +
    coord_trans(x = "log", y = "log") +
    geom_abline(slope = 1, intercept = 0)

  print(p)

  dev.off()

}

# combine plots

pdftools::pdf_combine(
  list.files(
    path = dir_diagnostic,
    pattern = "Early neonatal_[0-9][0-9][0-9][0-9]_subs",
    full.names = TRUE
  ),
  output = fs::path(dir_output, "diagnostics/u5/scatter_male_v_female_enn_subs.pdf")
)


pdftools::pdf_combine(
  list.files(
    path = dir_diagnostic,
    pattern = "Early neonatal_[0-9][0-9][0-9][0-9]_no_subs",
    full.names = TRUE
  ),
  output = fs::path(dir_output, "diagnostics/u5/scatter_male_v_female_enn_no_subs.pdf")
)


pdftools::pdf_combine(
  list.files(
    path = dir_diagnostic,
    pattern = "Late neonatal_[0-9][0-9][0-9][0-9]_subs",
    full.names = TRUE
  ),
  output = fs::path(dir_output, "diagnostics/u5/scatter_male_v_female_lnn_subs.pdf")
)


pdftools::pdf_combine(
  list.files(
    path = dir_diagnostic,
    pattern = "Late neonatal_[0-9][0-9][0-9][0-9]_no_subs",
    full.names = TRUE
  ),
  output = fs::path(dir_output, "diagnostics/u5/scatter_male_v_female_lnn_no_subs.pdf")
)
