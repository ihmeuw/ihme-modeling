# 5q0 both sex plots

# Set-up -----------------------------------------------------------------------

library(data.table)
library(ggplot2)

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
parser$add_argument(
  "--loc",
  type = "integer",
  required = !interactive(),
  default = 60908
)

args <- parser$parse_args()

list2env(args, .GlobalEnv)

cfg <- config::get(file = fs::path(args$dir_output, "config.yml"))

# make output directory
plot_dir <- glue::glue("{dir_output}/diagnostics/u5/plot_5q0_style")

fs::dir_create(plot_dir)

# get data --------------------------------------------------------------------

loc_map <- fread(glue::glue("{dir_output}/loc_map.csv"))

age_map <- assertable::import_files(
  list.files(
    path = dir_output,
    pattern = "age_map",
    full.names = TRUE
  )
)
age_map <- unique(age_map[, .(age_group_id, age_start, age_end, age_group_name)])

wpp_data <- fread("FILEPATH")
wpp_data <- wpp_data[location == loc_map[location_id == loc, ihme_loc_id]]

onemod_input_data <-
  cfg$fp_inputs$handoff_2 |>
  arrow::read_parquet() |>
  setDT()
onemod_input_data <- onemod_input_data[location_id == loc]


if (!loc %in% c(94364, 60908, 95069)) {

  gbd_2021_data <- arrow::read_feather(glue::glue(
    "{cfg$run_ids_previous_round$dir_best_no_shock}/{loc}-0.arrow"
  ))

} else {

  gbd_2021_data <- data.table()

}

pop_data <-
  fs::path(args$dir_output, "population", ext = "parquet") |>
  arrow::open_dataset(format = "parquet") |>
  dplyr::filter(location_id == loc) |>
  dplyr::collect() |>
  setDT()

onemod_data <- assertable::import_files(
  filenames = paste0(
    dir_output, "/draws/", loc, "-", cfg$years, ".arrow"
  ),
  FUN = arrow::read_feather
)

# prep data for plotting -------------------------------------------------------

onemod_input_data <- onemod_input_data[!is.na(outlier)]

onemod_input_data <- merge(
  onemod_input_data,
  age_map,
  by = "age_group_id"
)

# for surveys, calculate deaths
onemod_input_data[!is.na(deaths_adj), deaths := deaths]
onemod_input_data[!is.na(mx_adj), mx := mx_adj]

onemod_input_data[!is.na(deaths_adj) & mx_adj != 0, sample_size := deaths_adj / mx_adj]
onemod_input_data[is.na(deaths), deaths := mx * sample_size]

# fill deaths with adjusted deaths
onemod_input_data[!is.na(deaths_adj), deaths := deaths_adj]

# merge on survey type
onemod_input_data[, detailed_source_type := source_type_name]
onemod_input_data[
  grepl("Disease Surveillance|DSP", title, ignore.case = TRUE),
  detailed_source_type := "DSP"
]
onemod_input_data[
  grepl("World Fertility Survey|WFS", title, ignore.case = TRUE),
  detailed_source_type := "WFS"
]
onemod_input_data[
  grepl("Demographic and Health|DHS", title, ignore.case = TRUE),
  detailed_source_type := "DHS"
]
onemod_input_data[
  grepl("Special Demographic and Health|SDHS", title, ignore.case = TRUE),
  detailed_source_type := "SDHS"
]
onemod_input_data[
  grepl("Pan Arab Project For Family Health|PAPFAM", title, ignore.case = TRUE),
  detailed_source_type := "PAPFAM"
]
onemod_input_data[
  grepl("Reproductive Health Survey|RHS", title, ignore.case = TRUE),
  detailed_source_type := "RHS"
]
onemod_input_data[
  grepl("Pan Arab Project for Child Development|PAPCHILD", title, ignore.case = TRUE),
  detailed_source_type := "PAPCHILD"
]
onemod_input_data[
  grepl("Multiple Indicator|MICS", title, ignore.case = TRUE),
  detailed_source_type := "MICS"
]
onemod_input_data[
  grepl("World Fertility Survey|WFS", title, ignore.case = TRUE),
  detailed_source_type := "WFS"
]
onemod_input_data[
  grepl("Living Standards Measurement S|LSMS", title, ignore.case = TRUE),
  detailed_source_type := "LSMS"
]
onemod_input_data[
  grepl("Malaria Indicator|MIS", title, ignore.case = TRUE),
  detailed_source_type := "MIS"
]
onemod_input_data[
  grepl("AIDS Indicator|AIS", title, ignore.case = TRUE),
  detailed_source_type := "AIS"
]
onemod_input_data[
  grepl("Maternal and Child Health Survey|MCHS", title, ignore.case = TRUE),
  detailed_source_type := "MCHS"
]
onemod_input_data[
  grepl("Census", title, ignore.case = TRUE),
  detailed_source_type := "Census"
]
onemod_input_data[
  grepl("Sample Survey on Population Changes", title, ignore.case = TRUE),
  detailed_source_type := "SSPC"
]
onemod_input_data[
  detailed_source_type %in% c("Survey", "CBH", "SBH", "SIBS", "HHD"),
  detailed_source_type := "Other"
]

# recategorize source_type
onemod_input_data[, source_type := source_type_name]
onemod_input_data[source_type %in% c("VR", "DSP", "SRS"), source_type := "VR/DSP/SRS"]
onemod_input_data[source_type %in% c("Census", "HHD", "Survey"), source_type := "Census/HHD/Survey"]

onemod_input_data <- onemod_input_data[
  ,
  .(
    age_start, age_end, year_id, location_id,
    sex_id, nid, underlying_nid, detailed_source_type, source_type,
    deaths, sample_size, outlier
  )
]

# subset to u5 ages
onemod_input_data <- onemod_input_data[age_end <= 5]

# aggregate to both sex
onemod_input_data <- onemod_input_data[
  ,
  n_sexes := .N,
  by = setdiff(names(onemod_input_data), c("deaths", "sample_size", "sex_id"))
]

# grab souces with two sexes
onemod_input_data <- onemod_input_data[n_sexes == 2]
onemod_input_data[
  ,
  n_sexes := sum(sex_id),
  by = setdiff(names(onemod_input_data), c("deaths", "sample_size", "sex_id"))
]

# make sure it is male and female (i.e. 1 + 2 = 3, so we exlude duplicates like 2 females where n_sexes = 4)
onemod_input_data <- onemod_input_data[n_sexes == 3]

onemod_input_data[, n_sexes := NULL]

onemod_input_data <- onemod_input_data[
  ,
  .(deaths = sum(deaths), sample_size = sum(sample_size), sex_id = 3),
  by = setdiff(names(onemod_input_data), c("deaths", "sample_size", "sex_id"))
]

# append u5 age group
if (nrow(onemod_input_data) > 0) {

  u5_onemod_input_data <- copy(onemod_input_data)

  u5_onemod_input_data <- hierarchyUtils::agg(
    dt = u5_onemod_input_data,
    id_cols = c("location_id", "year_id", "sex_id", "nid", "underlying_nid",
                "age_start", "age_end",  "detailed_source_type", "source_type",
                "outlier"),
    value_cols = c("deaths", "sample_size"),
    col_stem = "age",
    col_type = "interval",
    mapping = data.table(age_start = 0, age_end = 5),
    missing_dt_severity = "warning"
  )

  u5_onemod_input_data[, mx := deaths / sample_size]
  u5_onemod_input_data[, qx := demCore::mx_to_qx(mx = mx, age_length = age_end - age_start)]

} else {

  u5_onemod_input_data <- data.table(outlier = NA_real_, detailed_source_type = NA_character_, source_type = NA_character_)

}

# prep onemod estimates
pop_data <- merge(
  pop_data,
  age_map,
  by = "age_group_id"
)

pop_data <- pop_data[, .(location_id, year_id, sex_id, age_start, age_end, pop = mean)]

setDT(onemod_data)

onemod_data <- merge(
  onemod_data,
  age_map,
  by = "age_group_id"
)

onemod_data <- onemod_data[age_end <= 5]

onemod_data <- onemod_data[
  ,
  .(location_id, draw, sex_id, year_id, age_start, age_end,
    deaths = deaths_age_scaled, pop = population)
]

u5_onemod_data <- hierarchyUtils::agg(
  dt = onemod_data,
  id_cols = c("location_id", "year_id", "sex_id", "age_start", "age_end", "draw"),
  value_cols = c("deaths", "pop"),
  col_stem = "age",
  col_type = "interval",
  mapping = data.table(age_start = 0, age_end = 5),
  overlapping_dt_severity = "warning"
)

u5_onemod_data <- u5_onemod_data[
  ,
  .(
    deaths = sum(deaths), pop = sum(pop),
    sex_id = 3
  ),
  by = setdiff(names(u5_onemod_data), c("deaths", "pop", "sex_id"))
]

u5_onemod_data[, mx := deaths / pop]
u5_onemod_data[, qx := demCore::mx_to_qx(mx = mx, age_length = age_end - age_start)]

u5_onemod_data <- u5_onemod_data[
  ,
  .(mx = mean(mx),
    lower = quantile(mx, 0.025),
    upper = quantile(mx, 0.975),
    qx = mean(qx),
    qx_lower = quantile(qx, 0.025),
    qx_upper = quantile(qx, 0.975)
    ),
  by = setdiff(names(u5_onemod_data), c("deaths", "pop", "draw", "mx", "qx"))
]

# prep gbd data
if (nrow(gbd_2021_data) > 0) {

  gbd_2021_data <- merge(
    gbd_2021_data,
    age_map[age_end <= 5],
    by = "age_group_id"
  )

  gbd_2021_data <- gbd_2021_data[sex_id == 3]

  gbd_2021_data <- merge(
    gbd_2021_data,
    pop_data,
    by = c("location_id", "sex_id", "year_id", "age_start", "age_end")
  )
  gbd_2021_data <- gbd_2021_data[, .(location_id, sex_id, year_id, draw, age_start, age_end, deaths, pop)]

  u5_gbd_2021_data <- hierarchyUtils::agg(
    dt = gbd_2021_data,
    id_cols = c("location_id", "year_id", "sex_id", "age_start", "age_end", "draw"),
    value_cols = c("deaths", "pop"),
    col_stem = "age",
    col_type = "interval",
    mapping = data.table(age_start = 0, age_end = 5)
  )

  u5_gbd_2021_data[, mx := deaths / pop]
  u5_gbd_2021_data[, qx := demCore::mx_to_qx(mx = mx, age_length = age_end - age_start)]

  u5_gbd_2021_data <- u5_gbd_2021_data[
    ,
    .(mx = mean(mx),
      lower = quantile(mx, 0.025),
      upper = quantile(mx, 0.975),
      qx = mean(qx),
      qx_lower = quantile(qx, 0.025),
      qx_upper = quantile(qx, 0.975)
      ),
    by = setdiff(names(u5_gbd_2021_data), c("deaths", "pop", "draw", "mx", "qx"))
  ]

} else {

  u5_gbd_2021_data <- data.table()

}

# wpp data
if (nrow(wpp_data) > 0) {

  wpp_data <- wpp_data[
    age_end <= 5 & sex_id == 3
    ,
    .(ihme_loc_id = location, age_start, age_end, sex_id, year_id = date_start,
      mx = value)
  ]

  wpp_data <- merge(wpp_data, loc_map[, .(ihme_loc_id, location_id)], by = "ihme_loc_id")
  wpp_data <- merge(
    wpp_data,
    pop_data,
    by = c("location_id", "sex_id", "year_id", "age_start", "age_end")
  )
  wpp_data[, deaths := mx * pop]
  wpp_data[, mx := NULL]

  u5_wpp_data <- hierarchyUtils::agg(
    dt = wpp_data,
    id_cols = c("location_id", "year_id", "sex_id", "age_start", "age_end", "ihme_loc_id"),
    value_cols = c("deaths","pop"),
    col_stem = "age",
    col_type = "interval",
    mapping = data.table(age_start = 0, age_end = 5)
  )

  u5_wpp_data[, mx := deaths / pop]
  u5_wpp_data[, qx := demCore::mx_to_qx(mx = mx, age_length = age_end - age_start)]

} else {

  u5_wpp_data <- data.table()

}

# Plot -------------------------------------------------------------------------

u5_gbd_2021_data[, type := "GBD 2021"]
u5_onemod_data[, type := "Onemod"]
u5_onemod_input_data[, type := "Data"]
u5_wpp_data[, type := "WPP 2024"]

all_data <- rbind(
  u5_gbd_2021_data, u5_onemod_data, u5_onemod_input_data, u5_wpp_data,
  fill = TRUE
)

pdf(
  glue::glue("{plot_dir}/mx_{loc}.pdf"),
  width = 14,
  height = 7
)

p <- ggplot(
  data = all_data[location_id == loc],
  aes(x = year_id, y = mx, color = detailed_source_type, fill = detailed_source_type, shape = source_type)
) +
  theme_bw() +
  ggtitle(paste0(
    "Both sex, 5m0: ", loc_map[location_id == loc, location_name], " (",
    loc_map[location_id == loc, ihme_loc_id], ")"
  )) +
  xlab("Year") +
  ylab("mx") +
  geom_point(
    data = all_data[location_id == loc & type == "Data" & outlier == 0],
    alpha = 0.5
  ) +
  geom_point(
    data = all_data[location_id == loc & type == "Data" & outlier == 1],
    alpha = 0.5,
    fill = NA
  ) +
  geom_line(
    data = all_data[location_id == loc & type == "WPP 2024"],
    color = "black"
  ) +
  geom_line(
    data = all_data[location_id == loc & type == "GBD 2021"],
    color = "green"
  ) +
  geom_ribbon(
    data = all_data[location_id == loc & type == "GBD 2021"],
    aes(ymin = lower, ymax = upper),
    color = NA,
    fill = "green",
    alpha = 0.2
  ) +
  geom_line(
    data = all_data[location_id == loc & type == "Onemod"],
    color = "blue"
  ) +
  geom_ribbon(
    data = all_data[location_id == loc & type == "Onemod"],
    aes(ymin = lower, ymax = upper),
    color = NA,
    fill = "blue",
    alpha = 0.2
  ) +
  scale_color_manual(
    values = c(
      "NA" = "white",
      "VR" = "purple",
      "WFS" = "grey",
      "DHS" = "orange",
      "SRS" = "darkgreen",
      "Census" = "cyan",
      "Other" = "magenta",
      "DSP" = "darkred",
      "SDHS" = "yellowgreen",
      "PAPFAM" = "pink",
      "RHS" = "darkblue",
      "PAPCHILD" = "yellow3",
      "MICS" = "chartreuse",
      "WFS" = "indianred",
      "LSMS" = "slateblue1",
      "MIS" = "mediumorchid1",
      "AIS" = "gold",
      "MCHS" = "springgreen",
      "SSPC" = "maroon4"
    )
  ) +
  scale_fill_manual(
    values = c(
      "NA" = "white",
      "VR" = "purple",
      "WFS" = "grey",
      "DHS" = "orange",
      "SRS" = "darkgreen",
      "Census" = "cyan",
      "Other" = "magenta",
      "DSP" = "darkred",
      "SDHS" = "yellowgreen",
      "PAPFAM" = "pink",
      "RHS" = "darkblue",
      "PAPCHILD" = "yellow3",
      "MICS" = "chartreuse",
      "WFS" = "indianred",
      "LSMS" = "slateblue1",
      "MIS" = "mediumorchid1",
      "AIS" = "gold",
      "MCHS" = "springgreen",
      "SSPC" = "maroon4"
    )
  ) +
  scale_shape_manual(
    values = c(
      "NA" = NA,
      "VR/DSP/SRS" = 21,
      "SBH" = 25,
      "CBH" = 24,
      "SIBS" = 23,
      "Census/HHD/Survey" = 22
    )
  ) +
  labs(caption = "Onemod = blue, GBD 2021 = green, WPP 2024 = black, outlier = X")

print(p)

dev.off()

pdf(
  glue::glue("{plot_dir}/qx_{loc}.pdf"),
  width = 14,
  height = 7
)

p <- ggplot(
  data = all_data[location_id == loc],
  aes(x = year_id, y = qx, color = detailed_source_type, fill = detailed_source_type, shape = source_type)
) +
  theme_bw() +
  ggtitle(paste0(
    "Both sex, 5q0: ", loc_map[location_id == loc, location_name], " (",
    loc_map[location_id == loc, ihme_loc_id], ")"
  )) +
  xlab("Year") +
  ylab("qx") +
  geom_point(
    data = all_data[location_id == loc & type == "Data" & outlier == 0],
    alpha = 0.5
  ) +
  geom_point(
    data = all_data[location_id == loc & type == "Data" & outlier == 1],
    alpha = 0.5,
    fill = NA
  ) +
  geom_line(
    data = all_data[location_id == loc & type == "WPP 2024"],
    color = "black"
  ) +
  geom_line(
    data = all_data[location_id == loc & type == "GBD 2021"],
    color = "green"
  ) +
  geom_ribbon(
    data = all_data[location_id == loc & type == "GBD 2021"],
    aes(ymin = qx_lower, ymax = qx_upper),
    color = NA,
    fill = "green",
    alpha = 0.2
  ) +
  geom_line(
    data = all_data[location_id == loc & type == "Onemod"],
    color = "blue"
  ) +
  geom_ribbon(
    data = all_data[location_id == loc & type == "Onemod"],
    aes(ymin = qx_lower, ymax = qx_upper),
    color = NA,
    fill = "blue",
    alpha = 0.2
  ) +
  scale_color_manual(
    values = c(
      "NA" = "white",
      "VR" = "purple",
      "WFS" = "grey",
      "DHS" = "orange",
      "SRS" = "darkgreen",
      "Census" = "cyan",
      "Other" = "magenta",
      "DSP" = "darkred",
      "SDHS" = "yellowgreen",
      "PAPFAM" = "pink",
      "RHS" = "darkblue",
      "PAPCHILD" = "yellow3",
      "MICS" = "chartreuse",
      "WFS" = "indianred",
      "LSMS" = "slateblue1",
      "MIS" = "mediumorchid1",
      "AIS" = "gold",
      "MCHS" = "springgreen",
      "SSPC" = "maroon4"
    )
  ) +
  scale_color_manual(
    values = c(
      "NA" = "white",
      "VR" = "purple",
      "WFS" = "grey",
      "DHS" = "orange",
      "SRS" = "darkgreen",
      "Census" = "cyan",
      "Other" = "magenta",
      "DSP" = "darkred",
      "SDHS" = "yellowgreen",
      "PAPFAM" = "pink",
      "RHS" = "darkblue",
      "PAPCHILD" = "yellow3",
      "MICS" = "chartreuse",
      "WFS" = "indianred",
      "LSMS" = "slateblue1",
      "MIS" = "mediumorchid1",
      "AIS" = "gold",
      "MCHS" = "springgreen",
      "SSPC" = "maroon4"
    )
  ) +
  scale_shape_manual(
    values = c(
      "NA" = NA,
      "VR/DSP/SRS" = 21,
      "SBH" = 25,
      "CBH" = 24,
      "SIBS" = 23,
      "Census/HHD/Survey" = 22
    )
  ) +
  labs(caption = "Onemod = blue, GBD 2021 = green, WPP 2024 = black, outlier = X")

print(p)

dev.off()
