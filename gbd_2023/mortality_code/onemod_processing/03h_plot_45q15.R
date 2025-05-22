# 45q15 both sex plots

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
plot_dir <- glue::glue("{dir_output}/diagnostics/45q15")

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

onemod_input_data[!is.na(enn_lnn_source) & is.na(extraction_source), extraction_source := enn_lnn_source]
onemod_input_data <- onemod_input_data[
  ,
  .(
    age_start, age_end, year_id, location_id,
    sex_id, nid, underlying_nid, detailed_source_type, source_type, extraction_source,
    deaths, sample_size, outlier, outlier_note
  )
]

# subset to adult ages
onemod_input_data <- onemod_input_data[age_end > 15 & age_end <= 60 & sex_id %in% 1:2]

# append 45q15 age group
if (all(seq(20, 60, by = 5) %in% onemod_input_data[, unique(age_end)])) {

  adult_onemod_input_data <- copy(onemod_input_data)

  dups <- demUtils::identify_non_unique_dt(adult_onemod_input_data, id_cols = c("location_id", "year_id", "sex_id", "nid", "underlying_nid",
                                                                        "age_start", "age_end",  "detailed_source_type", "source_type",
                                                                        "extraction_source", "outlier"))
  if(nrow(dups) > 0) {
    adult_onemod_input_data[, priority := ifelse(outlier == 0, 1, 2)]
    adult_onemod_input_data[outlier == 1 & outlier_note %like% "Duplicate", priority := 3]

    if (loc == 102) {
      adult_onemod_input_data[outlier == 1 & age_start == 15 & outlier_note %like% "handoff 1", priority := 4]
    }
    if (loc == 51) {
      adult_onemod_input_data[, dup :=seq(.N), by = names(adult_onemod_input_data)]
      adult_onemod_input_data[!priority == 1, priority := dup]
      adult_onemod_input_data[, dup := NULL]
    }
    if (loc == 130) {
      adult_onemod_input_data[, dup :=seq(.N), by = setdiff(names(adult_onemod_input_data), c("deaths", "sample_size"))]
      adult_onemod_input_data[!priority == 1, priority := dup]
      adult_onemod_input_data[, dup := NULL]
    }

    adult_onemod_input_data[, outlier_note := NULL]
    adult_onemod_input_data <- hierarchyUtils::agg(
      dt = adult_onemod_input_data,
      id_cols = c("location_id", "year_id", "sex_id", "nid", "underlying_nid",
                  "age_start", "age_end",  "detailed_source_type", "source_type",
                  "extraction_source", "outlier", "priority"),
      value_cols = c("deaths", "sample_size"),
      col_stem = "age",
      col_type = "interval",
      mapping = data.table(age_start = 15, age_end = 60),
      missing_dt_severity = "warning"
    )
    adult_onemod_input_data[, priority := NULL]
  } else {
    adult_onemod_input_data[, outlier_note := NULL]
    adult_onemod_input_data <- hierarchyUtils::agg(
      dt = adult_onemod_input_data,
      id_cols = c("location_id", "year_id", "sex_id", "nid", "underlying_nid",
                  "age_start", "age_end",  "detailed_source_type", "source_type",
                  "extraction_source", "outlier"),
      value_cols = c("deaths", "sample_size"),
      col_stem = "age",
      col_type = "interval",
      mapping = data.table(age_start = 15, age_end = 60),
      missing_dt_severity = "warning"
    )
  }

  adult_onemod_input_data[, mx := deaths / sample_size]
  adult_onemod_input_data[, qx := demCore::mx_to_qx(mx = mx, age_length = age_end - age_start)]

} else {

  adult_onemod_input_data <- data.table(outlier = NA_real_, detailed_source_type = NA_character_, source_type = NA_character_)

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

onemod_data <- onemod_data[age_group_id %in% 8:16]

onemod_data <- onemod_data[
  ,
  .(location_id, draw, sex_id, year_id, age_start, age_end,
    deaths = deaths_age_scaled, pop = population)
]

adult_onemod_data <- hierarchyUtils::agg(
  dt = onemod_data,
  id_cols = c("location_id", "year_id", "sex_id", "age_start", "age_end", "draw"),
  value_cols = c("deaths", "pop"),
  col_stem = "age",
  col_type = "interval",
  mapping = data.table(age_start = 15, age_end = 60),
  overlapping_dt_severity = "warning"
)

adult_onemod_data[, mx := deaths / pop]
adult_onemod_data[, qx := demCore::mx_to_qx(mx = mx, age_length = age_end - age_start)]

adult_onemod_data <- adult_onemod_data[
  ,
  .(mx = mean(mx),
    lower = quantile(mx, 0.025),
    upper = quantile(mx, 0.975),
    qx = mean(qx),
    qx_lower = quantile(qx, 0.025),
    qx_upper = quantile(qx, 0.975)
    ),
  by = setdiff(names(adult_onemod_data), c("deaths", "pop", "draw", "mx", "qx"))
]

# prep gbd data
if (nrow(gbd_2021_data) > 0) {

  gbd_2021_data <- merge(
    gbd_2021_data,
    age_map,
    by = "age_group_id"
  )

  gbd_2021_data <- gbd_2021_data[sex_id %in% 1:2]

  gbd_2021_data <- merge(
    gbd_2021_data,
    pop_data,
    by = c("location_id", "sex_id", "year_id", "age_start", "age_end")
  )
  gbd_2021_data <- gbd_2021_data[, .(location_id, sex_id, year_id, draw, age_start, age_end, deaths, pop)]

  adult_gbd_2021_data <- hierarchyUtils::agg(
    dt = gbd_2021_data,
    id_cols = c("location_id", "year_id", "sex_id", "age_start", "age_end", "draw"),
    value_cols = c("deaths", "pop"),
    col_stem = "age",
    col_type = "interval",
    mapping = data.table(age_start = 15, age_end = 60)
  )

  adult_gbd_2021_data[, mx := deaths / pop]
  adult_gbd_2021_data[, qx := demCore::mx_to_qx(mx = mx, age_length = age_end - age_start)]

  adult_gbd_2021_data <- adult_gbd_2021_data[
    ,
    .(mx = mean(mx),
      lower = quantile(mx, 0.025),
      upper = quantile(mx, 0.975),
      qx = mean(qx),
      qx_lower = quantile(qx, 0.025),
      qx_upper = quantile(qx, 0.975)
      ),
    by = setdiff(names(adult_gbd_2021_data), c("deaths", "pop", "draw", "mx", "qx"))
  ]

} else {

  adult_gbd_2021_data <- data.table()

}

# wpp data
if (nrow(wpp_data) > 0) {

  wpp_data <- wpp_data[
    age_end > 15 & age_end <= 60 & sex_id %in% 1:2
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

  adult_wpp_data <- hierarchyUtils::agg(
    dt = wpp_data,
    id_cols = c("location_id", "year_id", "sex_id", "age_start", "age_end", "ihme_loc_id"),
    value_cols = c("deaths","pop"),
    col_stem = "age",
    col_type = "interval",
    mapping = data.table(age_start = 15, age_end = 60)
  )

  adult_wpp_data[, mx := deaths / pop]
  adult_wpp_data[, qx := demCore::mx_to_qx(mx = mx, age_length = age_end - age_start)]

} else {

  adult_wpp_data <- data.table()

}

# Plot -------------------------------------------------------------------------

adult_gbd_2021_data[, type := "GBD 2021"]
adult_onemod_data[, type := "Onemod"]
adult_onemod_input_data[, type := "Data"]
adult_wpp_data[, type := "WPP 2024"]

all_data <- rbind(
  adult_gbd_2021_data, adult_onemod_data, adult_onemod_input_data, adult_wpp_data,
  fill = TRUE
)

pdf(
  glue::glue("{plot_dir}/mx_{loc}.pdf"),
  width = 14,
  height = 7
)

for (ss in 1:2) {
  sex_name <- ifelse(ss == 1, "Males", "Females")
  p <- ggplot(
    data = all_data[location_id == loc & sex_id == ss],
    aes(x = year_id, y = mx, color = detailed_source_type, fill = detailed_source_type, shape = source_type)
  ) +
    theme_bw() +
    ggtitle(paste0(
      sex_name, ", 45m15: ", loc_map[location_id == loc, location_name], " (",
      loc_map[location_id == loc, ihme_loc_id], ")"
    )) +
    xlab("Year") +
    ylab("mx (log)") +
    geom_point(
      data = all_data[location_id == loc & sex_id == ss & type == "Data" & outlier == 0],
      alpha = 0.5
    ) +
    geom_point(
      data = all_data[location_id == loc & sex_id == ss & type == "Data" & outlier == 1],
      alpha = 0.5,
      fill = NA
    ) +
    geom_line(
      data = all_data[location_id == loc & sex_id == ss & type == "WPP 2024"],
      color = "black"
    ) +
    geom_line(
      data = all_data[location_id == loc & sex_id == ss & type == "GBD 2021"],
      color = "green"
    ) +
    geom_ribbon(
      data = all_data[location_id == loc & sex_id == ss & type == "GBD 2021"],
      aes(ymin = lower, ymax = upper),
      color = NA,
      fill = "green",
      alpha = 0.2
    ) +
    geom_line(
      data = all_data[location_id == loc & sex_id == ss & type == "Onemod"],
      color = "blue"
    ) +
    geom_ribbon(
      data = all_data[location_id == loc & sex_id == ss & type == "Onemod"],
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
    scale_y_log10(labels = scales::label_scientific(), guide = "axis_logticks") +
    labs(caption = "Onemod = blue, GBD 2021 = green, WPP 2024 = black, outlier = X")

  print(p)
}

dev.off()

pdf(
  glue::glue("{plot_dir}/qx_{loc}.pdf"),
  width = 14,
  height = 7
)

for (ss in 1:2) {
  sex_name <- ifelse(ss == 1, "Males", "Females")
  p <- ggplot(
    data = all_data[location_id == loc & sex_id == ss],
    aes(x = year_id, y = qx, color = detailed_source_type, fill = detailed_source_type, shape = source_type)
  ) +
    theme_bw() +
    ggtitle(paste0(
      sex_name, ", 45q15: ", loc_map[location_id == loc, location_name], " (",
      loc_map[location_id == loc, ihme_loc_id], ")"
    )) +
    xlab("Year") +
    ylab("qx") +
    geom_point(
      data = all_data[location_id == loc & sex_id == ss & type == "Data" & outlier == 0],
      alpha = 0.5
    ) +
    geom_point(
      data = all_data[location_id == loc & sex_id == ss & type == "Data" & outlier == 1],
      alpha = 0.5,
      fill = NA
    ) +
    geom_line(
      data = all_data[location_id == loc & sex_id == ss & type == "WPP 2024"],
      color = "black"
    ) +
    geom_line(
      data = all_data[location_id == loc & sex_id == ss & type == "GBD 2021"],
      color = "green"
    ) +
    geom_ribbon(
      data = all_data[location_id == loc & sex_id == ss & type == "GBD 2021"],
      aes(ymin = qx_lower, ymax = qx_upper),
      color = NA,
      fill = "green",
      alpha = 0.2
    ) +
    geom_line(
      data = all_data[location_id == loc & sex_id == ss & type == "Onemod"],
      color = "blue"
    ) +
    geom_ribbon(
      data = all_data[location_id == loc & sex_id == ss & type == "Onemod"],
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
}

dev.off()
