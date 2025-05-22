# Scatter 35q15 and 30q50 for onemod investigations

# Set-up -----------------------------------------------------------------------

library(data.table)
library(ggplot2)
library(ggrepel)
library(patchwork)
library(RColorBrewer)

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

# make output directory
plot_dir <- fs::path(dir_output, "diagnostics/35q15_30q15")

fs::dir_create(plot_dir, mode = "775")

group_vars <- c("location_id", "year_id", "sex_id",  "super_region_name", "age_start", "age_end")

# get data ---------------------------------------------------------------------

all_data <- arrow::read_parquet(cfg$fp_inputs$handoff_2)

dss_data <- fread(cfg$fp_inputs$hdss_pt1)
dss_data2 <- fread(cfg$fp_inputs$hdss_pt2)

loc_map <- fread(fs::path(dir_output, "loc_map.csv"))
age_map_gbd <- fread(fs::path(dir_output, "age_map_gbd.csv"))
age_map_sy <- fread(fs::path(dir_output, "age_map_sy.csv"))

age_map <- rbind(age_map_gbd, age_map_sy)
age_map <- unique(age_map[, .(age_group_id, age_start, age_end)])

onemod_data <-
  fs::path(args$dir_output, "summaries") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(age_group_id %in% age_map_gbd$age_group_id) |>
  dplyr::collect() |>
  setDT()

gbd_2021_data <- mortdb::get_mort_outputs(
  "no shock life table", "estimate",
  run_id = "best",
  gbd_year = 2021,
  life_table_parameter_ids = 3,
  estimate_stage_ids = 5,
  sex_ids = 1:2
)

wpp_data <- fread(fs::path(cfg$fp_inputs$wpp_comparator, "qx5.csv"))

# prep vr data for plotting ----------------------------------------------------

setDT(all_data)
vr_data <- all_data[outlier == 0 & source_type_name %in% c("VR", "DSP", "SRS")]

vr_data[age_map, `:=` (age_start = i.age_start, age_end = i.age_end), on = .(age_group_id)]

vr_data[loc_map, super_region_name := i.super_region_name, on = .(location_id)]

vr_data[, age_group_id := NULL]

# NOTE: filling nas in adjusted with unadjusted mx
vr_data[, input_mx := mx_adj]
vr_data[is.na(input_mx), input_mx := mx]
vr_data[, qx_adj := demCore::mx_to_qx(mx = input_mx, age_length = age_end - age_start)]
vr_data[, px_adj := 1 - qx_adj]

agg50_vr_data <- demCore::agg_lt(
  dt = vr_data[
    age_start >= 15 & age_end <= 50,
    .(location_id, year_id, sex_id, nid, underlying_nid, outlier,
      source_type_name, super_region_name, age_start, age_end,
      onemod_process_type, qx = qx_adj)
  ],
  id_cols = c(group_vars, "nid", "underlying_nid", "outlier", "source_type_name",
              "onemod_process_type"),
  age_mapping = data.table(age_start = c(15), age_end = c(50)),
  missing_dt_severity = "warning"
)
agg80_vr_data <- demCore::agg_lt(
  dt = vr_data[
    age_start >= 50 & age_end <= 80,
    .(location_id, year_id, sex_id, nid, underlying_nid, outlier,
      source_type_name, super_region_name, age_start, age_end,
      onemod_process_type, qx = qx_adj)
  ],
  id_cols = c(group_vars, "nid", "underlying_nid", "outlier", "source_type_name",
              "onemod_process_type"),
  age_mapping = data.table(age_start = c(50), age_end = c(80)),
  missing_dt_severity = "warning"
)

setnames(agg50_vr_data, "qx", "qx_15_35")
setnames(agg80_vr_data, "qx", "qx_50_30")

agg50_vr_data[, c("age_start", "age_end") := NULL]
agg80_vr_data[, c("age_start", "age_end") := NULL]

agg_vr_data <- agg50_vr_data[
  agg80_vr_data,
  on = .(location_id, year_id, sex_id, nid, underlying_nid, outlier, source_type_name,
         super_region_name, onemod_process_type),
  nomatch = NULL
]

# prep survey data -------------------------------------------------------------

survey_data <- all_data[source_type_name %in% c("Census", "Survey", "HHD") & outlier == 0]

survey_data[
  age_map,
  `:=` (age_start = i.age_start, age_end = i.age_end),
  on = .(age_group_id)
]

survey_data[loc_map, super_region_name := i.super_region_name, on = .(location_id)]

survey_data <- survey_data[
  ,
  .(location_id, year_id, sex_id, nid, underlying_nid, super_region_name,
    mx, mx_adj, age_start, age_end, source_type_name)
]

# NOTE: filling nas in adjusted with unadjusted mx
survey_data[is.na(mx_adj), mx_adj := mx]

survey_data <- melt(
  survey_data,
  id.vars = c(group_vars, "nid", "underlying_nid", "source_type_name"),
  value.name = "mx",
  variable.name = "type"
)

survey_data[, qx := demCore::mx_to_qx(mx = mx, age_length = age_end - age_start)]

agg50_survey_data <- demCore::agg_lt(
  dt = survey_data[
    age_start >= 15 & age_end <= 50,
    .(location_id, year_id, sex_id, nid, underlying_nid,
      source_type_name, super_region_name, age_start, age_end,
      type, qx)
  ],
  id_cols = c(group_vars, "nid", "underlying_nid", "source_type_name", "type"),
  age_mapping = data.table(age_start = c(15), age_end = c(50)),
  missing_dt_severity = "warning"
)
agg80_survey_data <- demCore::agg_lt(
  dt = survey_data[
    age_start >= 50 & age_end <= 80,
    .(location_id, year_id, sex_id, nid, underlying_nid,
      source_type_name, super_region_name, age_start, age_end,
      type, qx = qx)
  ],
  id_cols = c(group_vars, "nid", "underlying_nid", "source_type_name", "type"),
  age_mapping = data.table(age_start = c(50), age_end = c(80)),
  missing_dt_severity = "warning"
)

setnames(agg50_survey_data, "qx", "qx_15_35")
setnames(agg80_survey_data, "qx", "qx_50_30")

agg50_survey_data[, c("age_start", "age_end") := NULL]
agg80_survey_data[, c("age_start", "age_end") := NULL]

agg_survey_data <- agg50_survey_data[
  agg80_survey_data,
  on = .(location_id, year_id, sex_id, nid, underlying_nid, source_type_name,
         super_region_name, type),
  nomatch = NULL
]

agg_survey_data <- agg_survey_data[
  !(qx_50_30 < 0.25 | qx_15_35 < 0.02 | qx_15_35 > 0.8)
]

# prep onemod data -------------------------------------------------------------

setDT(onemod_data)

onemod_data[
  age_map,
  `:=` (age_start = i.age_start, age_end = i.age_end),
  on = .(age_group_id)
]

onemod_data[loc_map, super_region_name := i.super_region_name, on = .(location_id)]

onemod_data[, qx := demCore::mx_to_qx(mx = mx_age_scaled_mean, age_length = age_end - age_start)]

onemod_data[
  ,
  c("super_region_id", "region_id", "national_id",
    "age_group_id", "kreg_lwr", "kreg_upr", "spxmod", "kreg") := NULL
]

agg50_onemod_data <- demCore::agg_lt(
  dt = onemod_data[
    age_start >= 15 & age_end <= 50,
    .(location_id, year_id, sex_id, super_region_name, age_start, age_end, qx)
  ],
  id_cols = group_vars,
  age_mapping = data.table(age_start = c(15), age_end = c(50))
)
agg80_onemod_data <- demCore::agg_lt(
  dt = onemod_data[
    age_start >= 50 & age_end <= 80,
    .(location_id, year_id, sex_id, super_region_name, age_start, age_end, qx)
  ],
  id_cols = group_vars,
  age_mapping = data.table(age_start = c(50), age_end = c(80))
)

setnames(agg50_onemod_data, "qx", "qx_15_35")
setnames(agg80_onemod_data, "qx", "qx_50_30")

agg50_onemod_data[, c("age_start", "age_end") := NULL]
agg80_onemod_data[, c("age_start", "age_end") := NULL]

agg_onemod_data <- agg50_onemod_data[
  agg80_onemod_data,
  on = .(location_id, year_id, sex_id, super_region_name),
  nomatch = NULL
]

other_agg_onemod_data <- agg_onemod_data[
  !super_region_name %in% "Sub-Saharan Africa"
]

agg_onemod_data <- agg_onemod_data[
  super_region_name %in% "Sub-Saharan Africa"
]

# prep GBD 2021 data -----------------------------------------------------------------

gbd_2021_data[loc_map, super_region_name := i.super_region_name, on = .(location_id)]

gbd_2021_data[
  age_map,
  `:=` (age_start = i.age_start, age_end = i.age_end),
  on = .(age_group_id)
]

gbd_2021_data <- gbd_2021_data[, .(location_id, year_id, sex_id, age_start, age_end,
                                   super_region_name, age_start, age_end, qx = mean)]

agg50_gbd_2021_data <- demCore::agg_lt(
  dt = gbd_2021_data[
    age_start >= 15 & age_end <= 50,
    .(location_id, year_id, sex_id, super_region_name, age_start, age_end, qx)
  ],
  id_cols = group_vars,
  age_mapping = data.table(age_start = c(15), age_end = c(50))
)
agg80_gbd_2021_data <- demCore::agg_lt(
  dt = gbd_2021_data[
    age_start >= 50 & age_end <= 80,
    .(location_id, year_id, sex_id, super_region_name, age_start, age_end, qx)
  ],
  id_cols = group_vars,
  age_mapping = data.table(age_start = c(50), age_end = c(80))
)

setnames(agg50_gbd_2021_data, "qx", "qx_15_35")
setnames(agg80_gbd_2021_data, "qx", "qx_50_30")

agg50_gbd_2021_data[, c("age_start", "age_end") := NULL]
agg80_gbd_2021_data[, c("age_start", "age_end") := NULL]

agg_gbd_2021_data <- agg50_gbd_2021_data[
  agg80_gbd_2021_data,
  on = .(location_id, year_id, sex_id, super_region_name),
  nomatch = NULL
]

other_agg_gbd_2021_data <- agg_gbd_2021_data[
  !super_region_name %in% "Sub-Saharan Africa"
]

agg_gbd_2021_data <- agg_gbd_2021_data[
  super_region_name %in% "Sub-Saharan Africa"
]

# prep DSS data ----------------------------------------------------------------

# prep DSS part 1

dss_data <- rbind(
  dss_data[, type := "HDSS data, collab 2015-2021"],
  dss_data2[, type := "HDSS data, INDEPTH 1990-2018"]
)

dss_data[
  age_map,
  `:=` (age_start = i.age_start, age_end = i.age_end),
  on = .(age_group_id)
]

dss_data[, qx := demCore::mx_to_qx(mx = mx, age_length = age_end - age_start)]
setnames(dss_data, c("year_start", "CentreId"), c("year_id", "site"))

dss_data[
  loc_map,
  `:=` (location_name = i.location_name, super_region_name = i.super_region_name),
  on = .(location_id)
]

dss_data <- dss_data[, .(location_id, site, year_id, sex_id, age_start, age_end,
                         super_region_name, qx, type)]
assertable::assert_values(dss_data, names(dss_data), "not_na")

agg50_dss_data <- demCore::agg_lt(
  dt = dss_data[
    age_start >= 15 & age_end <= 50,
    .(location_id, site, type, year_id, sex_id, super_region_name, age_start, age_end, qx)
  ],
  id_cols = c(group_vars, "site", "type"),
  age_mapping = data.table(age_start = c(15), age_end = c(50)),
  missing_dt_severity = "warning"
)
agg80_dss_data <- demCore::agg_lt(
  dt = dss_data[
    age_start >= 50 & age_end <= 80,
    .(location_id, site, type, year_id, sex_id, super_region_name, age_start, age_end, qx)
  ],
  id_cols = c(group_vars, "site", "type"),
  age_mapping = data.table(age_start = c(50), age_end = c(80)),
  missing_dt_severity = "warning"
)

setnames(agg50_dss_data, "qx", "qx_15_35")
setnames(agg80_dss_data, "qx", "qx_50_30")

agg50_dss_data[, c("age_start", "age_end") := NULL]
agg80_dss_data[, c("age_start", "age_end") := NULL]

agg_dss_data <- agg50_dss_data[
  agg80_dss_data,
  on = .(location_id, site, type, year_id, sex_id, super_region_name),
  nomatch = NULL
]

# remove HDSS outliers
agg_dss_data <- agg_dss_data[qx_50_30 > 0.2 & qx_50_30 < 0.96]

female_trim_val <- sort(agg_dss_data[sex_id == 2, qx_15_35])[4]
male_trim_val <- sort(agg_dss_data[sex_id == 1, qx_15_35])[4]

agg_dss_data <- agg_dss_data[!(sex_id == 2 &
                                 (qx_15_35 < female_trim_val | qx_15_35 > 0.75))]
agg_dss_data <- agg_dss_data[!(sex_id == 1 &
                                 (qx_15_35 < male_trim_val | qx_15_35 > 0.75))]

# prep WPP data ----------------------------------------------------------------

setnames(wpp_data, "location", "ihme_loc_id")
wpp_data[
  loc_map,
  `:=` (super_region_name = i.super_region_name, location_id = i.location_id),
  on = .(ihme_loc_id)
]

wpp_data <- wpp_data[, .(location_id, super_region_name, year_id = date_start,
                         sex_id,age_start, age_end, qx = value)]

agg50_wpp_data <- demCore::agg_lt(
  dt = wpp_data[
    age_start >= 15 & age_end <= 50,
    .(location_id, year_id, sex_id, super_region_name, age_start, age_end, qx)
  ],
  id_cols = group_vars,
  age_mapping = data.table(age_start = c(15), age_end = c(50))
)
agg80_wpp_data <- demCore::agg_lt(
  dt = wpp_data[
    age_start >= 50 & age_end <= 80,
    .(location_id, year_id, sex_id, super_region_name, age_start, age_end, qx)
  ],
  id_cols = group_vars,
  age_mapping = data.table(age_start = c(50), age_end = c(80))
)

setnames(agg50_wpp_data, "qx", "qx_15_35")
setnames(agg80_wpp_data, "qx", "qx_50_30")

agg50_wpp_data[, c("age_start", "age_end") := NULL]
agg80_wpp_data[, c("age_start", "age_end") := NULL]

agg_wpp_data <- agg50_wpp_data[
  agg80_wpp_data,
  on = .(location_id, year_id, sex_id, super_region_name),
  nomatch = NULL
]

other_agg_wpp_data <- agg_wpp_data[
  !super_region_name %in% "Sub-Saharan Africa"
]

agg_wpp_data <- agg_wpp_data[
  super_region_name %in% "Sub-Saharan Africa"
]


# prep for plotting ------------------------------------------------------------

agg_vr_data[, type := "VR/SRS/DSP data"]
agg_onemod_data[, type := "onemod estimates"]
agg_gbd_2021_data[, type := "GBD 2021 estimates"]
agg_wpp_data[, type := "WPP estimates"]

ssa_w_vr_locs <- unique(agg_vr_data[super_region_name == "Sub-Saharan Africa", location_id])
ssa_w_vr <- loc_map[location_id %in% ssa_w_vr_locs, paste0(location_name, "(", ihme_loc_id, ")")]

ssa_w_vr[5] <- paste0(ssa_w_vr[5], " \n")
ssa_w_vr[10] <- paste0(ssa_w_vr[10], " \n")

agg_data <- rbind(
  agg_onemod_data,
  agg_vr_data,
  agg_gbd_2021_data,
  agg_dss_data,
  agg_wpp_data,
  fill = TRUE
)

# plot -------------------------------------------------------------------------

# SSA overall summary ---

pdf(
  fs::path(plot_dir, "scatter_35q15_30q50_highlight_SSA_onemod_sex_specific.pdf"),
  width = 14,
  height = 7
)

for (s in 1:2) {

  sex_name <- ifelse(s == 1, "male", "female")

  p1 <- ggplot(
    data = agg_data[sex_id == s],
    aes(x = qx_15_35, y = qx_50_30, color = super_region_name, shape = type)
  ) +
    theme_bw() +
    ggtitle(paste0("Compare 35q15 vs 30q50 (", sex_name, ", all years)")) +
    xlim(0, 1) +
    ylim(0, 1) +
    xlab("35q15") +
    ylab("30q50") +
    scale_shape_manual(values = c("VR/SRS/DSP data" = 16, "onemod estimates" = 4, "GBD 2021 estimates" = 4)) +
    geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == s], alpha = 0.3) +
    geom_point(data = agg_data[type == "onemod estimates" & sex_id == s], color = "black", alpha = 0.3) +
    geom_abline(slope = 1, intercept = 0) +
    theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
    guides(fill = guide_legend(override.aes = list(shape = 21))) +
    labs(fill = "Super region", shape = "Point type", caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

  p2 <- ggplot(
    data = agg_data[sex_id == s],
    aes(x = qx_15_35, y = qx_50_30, color = super_region_name, shape = type)
  ) +
    theme_bw() +
    ggtitle(paste0("Compare 35q15 vs 30q50 (", sex_name, ", all years)")) +
    xlim(0, 1) +
    ylim(0, 1) +
    xlab("35q15") +
    ylab("30q50") +
    scale_shape_manual(values = c("VR/SRS/DSP data" = 16, "onemod estimates" = 4, "GBD 2021 estimates" = 4)) +

    geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == s], alpha = 0.3) +
    geom_point(data = agg_data[type == "GBD 2021 estimates" & sex_id == s], color = "red", alpha = 0.3) +
    geom_abline(slope = 1, intercept = 0) +
    theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
    guides(fill = guide_legend(override.aes = list(shape = 21))) +
    labs(fill = "Super region", shape = "Point type",  caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

  p_all <- p1 + p2 + plot_layout(ncol = 2, nrow = 1)

  print(p_all)

}

dev.off()

# SSA loc specific summary

pdf(
  fs::path(plot_dir, "scatter_35q15_30q50_highlight_SSA_onemod_per_SSA_sex_specific.pdf"),
  width = 14,
  height = 7
)

for (l in unique(agg_onemod_data[location_id %in% loc_map[level == 3, location_id], location_id])) {

  loc_name <- loc_map[location_id == l, location_name]
  ihme_loc <- loc_map[location_id == l, ihme_loc_id]

  for (s in 1:2) {

    sex_name <- ifelse(s == 1, "male", "female")

    p <- ggplot(
      data = agg_data[sex_id == s],
      aes(x = qx_15_35, y = qx_50_30, shape = type)
    ) +
      theme_bw() +
      ggtitle(paste0("Compare 35q15 vs 30q50 (", sex_name, ", all years), ", loc_name, ", ", ihme_loc)) +
      xlab("35q15") +
      ylab("30q50") +
      geom_point(
        data = agg_data[type == "VR/SRS/DSP data" & sex_id == s & !location_id %in% ssa_w_vr_locs],
        alpha = 0.3,
        color = "white",
        aes(fill = super_region_name)
      ) +
      geom_point(
        data = agg_data[type == "VR/SRS/DSP data" & sex_id == s & location_id %in% ssa_w_vr_locs],
        alpha = 1,
        color = "black",
        aes(fill = super_region_name)
      ) +
      geom_point(
        data = agg_data[type == "GBD 2021 estimates" & sex_id == s & location_id == l],
        fill = NA,
        aes(color = year_id)
      ) +
      geom_point(
        data = agg_data[type == "onemod estimates" & sex_id == s & location_id == l],
        fill = NA,
        aes(color = year_id)
      ) +
      geom_abline(slope = 1, intercept = 0) +
      scale_shape_manual(values = c("VR/SRS/DSP data" = 21, "onemod estimates" = 22, "GBD 2021 estimates" = 24)) +
      theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
      guides(fill = guide_legend(override.aes = list(shape = 21))) +
      labs(
        fill = "Super region",
        color = "Year",
        shape = "Point type",
        caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", "))
      )

    print(p)

  }
}

dev.off()

# Overall with HDSS comparison

pdf(
  fs::path(plot_dir, "scatter_35q15_30q50_highlight_SSA_HDSS_sex_specific.pdf"),
  width = 14,
  height = 7
)

for (s in 1:2) {

  sex_name <- ifelse(s == 1, "male", "female")

  sex_name <- ifelse(s == 1, "male", "female")

  p1 <- ggplot(
    data = agg_data[sex_id == s],
    aes(x = qx_15_35, y = qx_50_30, shape = type)
  ) +
    theme_bw() +
    ggtitle(paste0("Compare SSA 35q15 vs 30q50 (", sex_name, ", all years)")) +
    xlim(0, 1) +
    ylim(0, 1) +
    xlab("35q15") +
    ylab("30q50") +
    scale_shape_manual(values = c("VR/SRS/DSP data" = 21, "HDSS data, collab 2015-2021" = 22, "HDSS data, INDEPTH 1990-2018" = 24, "onemod estimates" = 4, "GBD 2021 estimates" = 4)) +
    geom_point(data = agg_data[type == "onemod estimates" & sex_id == s], color = "grey", alpha = 0.5) +
    geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == s & location_id %in% ssa_w_vr_locs], color = "grey", fill = "blue", alpha = 0.5) +
    geom_point(data = agg_data[type == "HDSS data, INDEPTH 1990-2018" & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "yellow") +
    geom_point(data = agg_data[type == "HDSS data, collab 2015-2021" & sex_id == s & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "#33CC33") +
    geom_abline(slope = 1, intercept = 0) +
    theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
    guides(fill = guide_legend(override.aes = list(shape = 21))) +
    labs(shape = "Point type", caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

  p2 <- ggplot(
    data = agg_data[sex_id == s],
    aes(x = qx_15_35, y = qx_50_30, shape = type)
  ) +
    theme_bw() +
    ggtitle(paste0("Compare SSA 35q15 vs 30q50 (", sex_name, ", all years)")) +
    xlim(0, 1) +
    ylim(0, 1) +
    xlab("35q15") +
    ylab("30q50") +
    scale_shape_manual(values = c("VR/SRS/DSP data" = 21, "HDSS data, collab 2015-2021" = 22, "HDSS data, INDEPTH 1990-2018" = 24, "onemod estimates" = 4, "GBD 2021 estimates" = 4)) +
    geom_point(data = agg_data[type == "GBD 2021 estimates" & sex_id == s], color = "red", alpha = 0.5) +
    geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == s & location_id %in% ssa_w_vr_locs], color = "grey", alpha = 0.5, fill = "blue") +
    geom_point(data = agg_data[type == "HDSS data, INDEPTH 1990-2018" & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "yellow") +
    geom_point(data = agg_data[type == "HDSS data, collab 2015-2021" & sex_id == s & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "#33CC33") +
    geom_abline(slope = 1, intercept = 0) +
    theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
    guides(fill = guide_legend(override.aes = list(shape = 21))) +
    labs(shape = "Point type",  caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

  p_all <- p1 + p2 + plot_layout(ncol = 2, nrow = 1)

  print(p_all)

}

dev.off()

# Overall region summary ---

# append remaining data
other_agg_gbd_2021_data[, type := "GBD 2021 estimates"]
other_agg_onemod_data[, type := "onemod estimates"]
other_agg_wpp_data[, type := "WPP estimates"]

agg_data <- rbind(
  agg_data,
  other_agg_onemod_data,
  other_agg_gbd_2021_data,
  other_agg_wpp_data,
  fill = TRUE
)

pdf(
  fs::path(plot_dir, "scatter_35q15_30q50_gbd2021_onemod_sex_region_specific.pdf"),
  width = 14,
  height = 7
)


for (reg in unique(agg_data[!is.na(super_region_name)]$super_region_name)) {

  for (s in 1:2) {


    sex_name <- ifelse(s == 1, "male", "female")
    temp_agg_data <- agg_data[super_region_name == reg]

    p1 <- ggplot(
      data = temp_agg_data[sex_id == s],
      aes(x = qx_15_35, y = qx_50_30, shape = type)
    ) +
      theme_bw() +
      ggtitle(paste0("Compare 35q15 vs 30q50 (", reg, ", ", sex_name, ", all years)")) +
      xlim(0, 1) +
      ylim(0, 1) +
      xlab("35q15") +
      ylab("30q50") +
      scale_shape_manual(
        values = c(
          "VR/SRS/DSP data" = 21,
          "onemod estimates" = 4,
          "GBD 2021 estimates" = 4,
          "HDSS data, collab 2015-2021" = 22,
          "HDSS data, INDEPTH 1990-2018" = 24
        )
      ) +
      geom_point(data = temp_agg_data[type == "onemod estimates" & sex_id == s], color = "black", alpha = 0.3) +
      geom_point(data = temp_agg_data[type == "VR/SRS/DSP data" & sex_id == s], alpha = 0.3, color = "grey", fill = "blue") +
      geom_point(data = temp_agg_data[type == "HDSS data, collab 2015-2021" & sex_id == s], color = "black", fill = "yellow") +
      geom_point(data = temp_agg_data[type == "HDSS data, INDEPTH 1990-2018"], color = "black", fill = "orange") +
      geom_abline(slope = 1, intercept = 0) +
      theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5), title = element_text(size = 7)) +
      guides(fill = guide_legend(override.aes = list(shape = 21))) +
      labs(shape = "Point type")

    p2 <- ggplot(
      data = agg_data[sex_id == s],
      aes(x = qx_15_35, y = qx_50_30, shape = type)
    ) +
      theme_bw() +
      ggtitle(paste0("Compare 35q15 vs 30q50 (", reg, ", ", sex_name, ", all years)")) +
      xlim(0, 1) +
      ylim(0, 1) +
      xlab("35q15") +
      ylab("30q50") +
      scale_shape_manual(
        values = c(
          "VR/SRS/DSP data" = 21,
          "onemod estimates" = 4,
          "GBD 2021 estimates" = 4,
          "HDSS data, collab 2015-2021" = 22,
          "HDSS data, INDEPTH 1990-2018" = 24
        )
      ) +
      geom_point(data = temp_agg_data[type == "GBD 2021 estimates" & sex_id == s], color = "red", alpha = 0.3) +
      geom_point(data = temp_agg_data[type == "VR/SRS/DSP data" & sex_id == s], color = "grey", alpha = 0.3, fill = "blue") +
      geom_point(data = temp_agg_data[type == "HDSS data, collab 2015-2021" & sex_id == s], color = "black", fill = "yellow") +
      geom_point(data = temp_agg_data[type == "HDSS data, INDEPTH 1990-2018"], color = "black", fill = "orange") +
      geom_abline(slope = 1, intercept = 0) +
      theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5), title = element_text(size = 7)) +
      guides(fill = guide_legend(override.aes = list(shape = 21))) +
      labs(shape = "Point type",  caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

    p_all <- p1 + p2 + plot_layout(ncol = 2, nrow = 1)

    print(p_all)

  }
}

dev.off()

# all countries loc specific summary
# MOST IMPORTANT !!!

for (reg in unique(loc_map[!is.na(super_region_name)]$super_region_name)) {

  temp_pdf <- fs::path(plot_dir, paste0("scatter_35q15_vs_30q50_", reg, "_gbd2021_onemod_per_loc_sex.pdf"))
  temp_pdf <- gsub(" ", "_", temp_pdf)
  temp_pdf <- gsub(",", "", temp_pdf)

  pdf(
    temp_pdf,
    width = 14,
    height = 7
  )

  for (l in unique(agg_data[location_id %in% loc_map[level == 3 & super_region_name == reg, location_id], location_id])) {

    loc_name <- loc_map[location_id == l, location_name]
    ihme_loc <- loc_map[location_id == l, ihme_loc_id]

    reg_locs <- loc_map[super_region_name == reg, location_id]

    for (s in 1:2) {

      sex_name <- ifelse(s == 1, "male", "female")

      p <- ggplot(
        data = agg_data[sex_id == s],
        aes(x = qx_15_35, y = qx_50_30, shape = type)
      ) +
        theme_bw() +
        ggtitle(paste0("Compare 35q15 vs 30q50 (", sex_name, ", all years), ", loc_name, ", ", ihme_loc)) +
        xlab("35q15") +
        ylab("30q50") +
        geom_point(
          data = agg_data[type == "VR/SRS/DSP data" & sex_id == s & !location_id %in% reg_locs],
          alpha = 0.3,
          color = "white",
          aes(fill = super_region_name)
        ) +
        geom_point(
          data = agg_data[type == "VR/SRS/DSP data" & sex_id == s & location_id %in% reg_locs],
          alpha = 1,
          color = "black",
          aes(fill = super_region_name)
        ) +
        geom_point(
          data = agg_data[type == "GBD 2021 estimates" & sex_id == s & location_id == l],
          fill = NA,
          aes(color = year_id)
        ) +
        geom_point(
          data = agg_data[type == "onemod estimates" & sex_id == s & location_id == l],
          fill = NA,
          aes(color = year_id)
        ) +
        geom_point(
          data = agg_data[type == "WPP estimates" & sex_id == s & location_id == l],
          fill = NA,
          aes(color = year_id)
        ) +
        geom_abline(slope = 1, intercept = 0) +
        scale_shape_manual(values = c("VR/SRS/DSP data" = 21, "onemod estimates" = 22, "GBD 2021 estimates" = 24, "WPP estimates" = 23)) +
        theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
        guides(fill = guide_legend(override.aes = list(shape = 21))) +
        labs(
          fill = "Super region",
          color = "Year",
          shape = "Point type"
        )

      print(p)

    }
  }

  dev.off()

}

# highlight other regions

pdf(
  fs::path(plot_dir, "scatter_35q15_30q50_highlight_all_reg_onemod_sex_specific.pdf"),
  width = 14,
  height = 7
)

for (reg in unique(agg_data[!is.na(super_region_name)]$super_region_name)) {

  for (s in 1:2) {

    sex_name <- ifelse(s == 1, "male", "female")

    p1 <- ggplot(
      data = agg_data[sex_id == s],
      aes(x = qx_15_35, y = qx_50_30, color = super_region_name, shape = type)
    ) +
      theme_bw() +
      ggtitle(paste0("Compare 35q15 vs 30q50 (", reg, ", ", sex_name, ", all years)")) +
      xlim(0, 1) +
      ylim(0, 1) +
      xlab("35q15") +
      ylab("30q50") +
      scale_shape_manual(values = c("VR/SRS/DSP data" = 16, "onemod estimates" = 4, "GBD 2021 estimates" = 4)) +
      geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == s], alpha = 0.3) +
      geom_point(data = agg_data[type == "onemod estimates" & sex_id == s & super_region_name == reg], color = "black", alpha = 0.3) +
      geom_abline(slope = 1, intercept = 0) +
      theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5), title = element_text(size = 7)) +
      guides(fill = guide_legend(override.aes = list(shape = 21))) +
      labs(fill = "Super region", shape = "Point type", caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

    p2 <- ggplot(
      data = agg_data[sex_id == s],
      aes(x = qx_15_35, y = qx_50_30, color = super_region_name, shape = type)
    ) +
      theme_bw() +
      ggtitle(paste0("Compare 35q15 vs 30q50 (", reg, ", ", sex_name, ", all years)")) +
      xlim(0, 1) +
      ylim(0, 1) +
      xlab("35q15") +
      ylab("30q50") +
      scale_shape_manual(values = c("VR/SRS/DSP data" = 16, "onemod estimates" = 4, "GBD 2021 estimates" = 4)) +

      geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == s], alpha = 0.3) +
      geom_point(data = agg_data[type == "GBD 2021 estimates" & sex_id == s & super_region_name == reg], color = "red", alpha = 0.3) +
      geom_abline(slope = 1, intercept = 0) +
      theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5), title = element_text(size = 7)) +
      guides(fill = guide_legend(override.aes = list(shape = 21))) +
      labs(fill = "Super region", shape = "Point type",  caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

    p_all <- p1 + p2 + plot_layout(ncol = 2, nrow = 1)

    print(p_all)

  }
}

dev.off()

year_map <- data.table(
  year_start = c(min(cfg$years), min(cfg$years), 1990),
  year_end = c(max(cfg$years), 1989, max(cfg$years))
)

for(i in 1:nrow(year_map)) {

  y_s <- year_map[i, year_start]
  y_e <- year_map[i, year_end]

  pdf(
    fs::path(plot_dir, glue::glue("scatter_35q15_30q50_highlight_SSA_HDSS_sex_specific_no_subs_{y_s}_{y_e}.pdf")),
    width = 21,
    height = 7
  )

  min_x <- min(agg_data[
    super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id] &
      (year_id %in% y_s:y_e | grepl("data", type)),
    qx_15_35
  ])
  max_x <- max(agg_data[
    super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id] &
      (year_id %in% y_s:y_e | grepl("data", type)),
    qx_15_35
  ])
  min_y <- min(agg_data[
    super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id] &
      (year_id %in% y_s:y_e | grepl("data", type)),
    qx_50_30
  ])
  max_y <- max(agg_data[
    super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id] &
      (year_id %in% y_s:y_e | grepl("data", type)),
    qx_50_30
  ])

  for (s in 1:2) {

    sex_name <- ifelse(s == 1, "male", "female")

    p1 <- ggplot(
      data = agg_data[sex_id == s & year_id %in% y_s:y_e],
      aes(x = qx_15_35, y = qx_50_30, shape = type)
    ) +
      theme_bw() +
      ggtitle(paste0("Compare SSA 35q15 vs 30q50 (", sex_name, ", ", y_s, "-", y_e, ")")) +
      xlim(min_x, max_x) +
      ylim(min_y, max_y) +
      xlab("35q15") +
      ylab("30q50") +
      scale_shape_manual(values = c("VR/SRS/DSP data" = 21, "HDSS data, collab 2015-2021" = 22, "HDSS data, INDEPTH 1990-2018" = 24, "onemod estimates" = 4, "GBD 2021 estimates" = 4, "WPP estimates" = 4)) +
      geom_point(data = agg_data[type == "onemod estimates" & sex_id == s & super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id] & year_id %in% y_s:y_e], color = "#FF33FF", alpha = 0.5) +
      geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == s & location_id %in% ssa_w_vr_locs], color = "grey", fill = "blue", alpha = 0.5) +
      geom_point(data = agg_data[type == "HDSS data, INDEPTH 1990-2018" & sex_id == s & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "yellow") +
      geom_point(data = agg_data[type == "HDSS data, collab 2015-2021" & sex_id == s & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "#33CC33") +
      geom_abline(slope = 1, intercept = 0) +
      theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
      guides(fill = guide_legend(override.aes = list(shape = 21))) +
      labs(shape = "Point type", caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

    p2 <- ggplot(
      data = agg_data[sex_id == s],
      aes(x = qx_15_35, y = qx_50_30, shape = type)
    ) +
      theme_bw() +
      ggtitle(paste0("Compare SSA 35q15 vs 30q50 (", sex_name, ", ", y_s, "-", y_e, ")")) +
      xlim(min_x, max_x) +
      ylim(min_y, max_y) +
      xlab("35q15") +
      ylab("30q50") +
      scale_shape_manual(values = c("VR/SRS/DSP data" = 21, "HDSS data, collab 2015-2021" = 22, "HDSS data, INDEPTH 1990-2018" = 24,  "onemod estimates" = 4, "GBD 2021 estimates" = 4, "WPP estimates" = 4)) +
      geom_point(data = agg_data[type == "GBD 2021 estimates" & sex_id == s & super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id] & year_id %in% y_s:y_e], color = "red", alpha = 0.5) +
      geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == s & location_id %in% ssa_w_vr_locs], color = "grey", alpha = 0.5, fill = "blue") +
      geom_point(data = agg_data[type == "HDSS data, INDEPTH 1990-2018" & sex_id == s & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "yellow") +
      geom_point(data = agg_data[type == "HDSS data, collab 2015-2021" & sex_id == s & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "#33CC33") +
      geom_abline(slope = 1, intercept = 0) +
      theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
      guides(fill = guide_legend(override.aes = list(shape = 21))) +
      labs(shape = "Point type",  caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

    p3 <- ggplot(
      data = agg_data[sex_id == s],
      aes(x = qx_15_35, y = qx_50_30, shape = type)
    ) +
      theme_bw() +
      ggtitle(paste0("Compare SSA 35q15 vs 30q50 (", sex_name, ", ", y_s, "-", y_e, ")")) +
      xlim(min_x, max_x) +
      ylim(min_y, max_y) +
      xlab("35q15") +
      ylab("30q50") +
      scale_shape_manual(values = c("VR/SRS/DSP data" = 21, "HDSS data, collab 2015-2021" = 22, "HDSS data, INDEPTH 1990-2018" = 24, "onemod estimates" = 4, "GBD 2021 estimates" = 4, "WPP estimates" = 4)) +
      geom_point(data = agg_data[type == "WPP estimates" & sex_id == s & super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id] & year_id %in% y_s:y_e], color = "#9933FF", alpha = 0.5) +
      geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == s & location_id %in% ssa_w_vr_locs], color = "grey", alpha = 0.5, fill = "blue") +
      geom_point(data = agg_data[type == "HDSS data, INDEPTH 1990-2018" & sex_id == s & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "yellow") +
      geom_point(data = agg_data[type == "HDSS data, collab 2015-2021" & sex_id == s & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "#33CC33") +
      geom_abline(slope = 1, intercept = 0) +
      theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
      guides(fill = guide_legend(override.aes = list(shape = 21))) +
      labs(shape = "Point type",  caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

    p_all <- p1 + p2 + p3 + plot_layout(ncol = 3, nrow = 1)

    print(p_all)

  }

  dev.off()

}

# per national

for (i in 1:nrow(year_map)) {

  y_s <- year_map[i, year_start]
  y_e <- year_map[i, year_end]

  min_x <- min(agg_data[
    super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id] &
      (year_id %in% y_s:y_e | grepl("data", type)),
    qx_15_35
  ])
  max_x <- max(agg_data[
    super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id] &
      (year_id %in% y_s:y_e | grepl("data", type)),
    qx_15_35
  ])
  min_y <- min(agg_data[
    super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id] &
      (year_id %in% y_s:y_e | grepl("data", type)),
    qx_50_30
  ])
  max_y <- max(agg_data[
    super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id] &
      (year_id %in% y_s:y_e | grepl("data", type)),
    qx_50_30
  ])

  pdf(
    fs::path(plot_dir, glue::glue("scatter_35q15_30q50_highlight_SSA_HDSS_sex_specific_no_subs_per_loc_{y_s}_{y_e}.pdf")),
    width = 14,
    height = 7
  )

  for (loc in loc_map[super_region_name == "Sub-Saharan Africa" & level == 3, location_id]) {

    loc_name <- loc_map[location_id == loc, location_name]
    loc_ihme <- loc_map[location_id == loc, ihme_loc_id]

    p1 <- ggplot(
      data = agg_data[sex_id == 2],
      aes(x = qx_15_35, y = qx_50_30, shape = type, color = year_id)
    ) +
      theme_bw() +
      ggtitle(paste0("Compare SSA 35q15 vs 30q50 ", loc_name, ",  (", loc_ihme, ", female, ", y_s, "-", y_e, ")")) +
      xlim(min_x, max_x) +
      ylim(min_y, max_y) +
      xlab("35q15") +
      ylab("30q50") +
      scale_shape_manual(values = c("VR/SRS/DSP data" = 21, "HDSS data, collab 2015-2021" = 22, "HDSS data, INDEPTH 1990-2018" = 24, "onemod estimates" = 5, "GBD 2021 estimates" = 8, "WPP estimates" = 3)) +
      geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == 2 & location_id %in% ssa_w_vr_locs], color = "grey", fill = "red", alpha = 0.3) +
      geom_point(data = agg_data[type == "HDSS data, INDEPTH 1990-2018" & sex_id == 2 & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "yellow", alpha = 0.3) +
      geom_point(data = agg_data[type == "HDSS data, collab 2015-2021" & sex_id == 2 & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "#33CC33", alpha = 0.3) +
      geom_point(data = agg_data[type == "GBD 2021 estimates" & sex_id == 2 & location_id == loc & year_id %in% y_s:y_e]) +
      geom_point(data = agg_data[type == "onemod estimates" & sex_id == 2 & location_id == loc & year_id %in% y_s:y_e]) +
      geom_point(data = agg_data[type == "WPP estimates" & sex_id == 2 & location_id == loc & year_id %in% y_s:y_e]) +
      geom_abline(slope = 1, intercept = 0) +
      theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
      guides(fill = guide_legend(override.aes = list(shape = 21))) +
      labs(shape = "Point type", caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

    p2 <- ggplot(
      data = agg_data[sex_id == 1],
      aes(x = qx_15_35, y = qx_50_30, shape = type, color = year_id)
    ) +
      theme_bw() +
      ggtitle(paste0("Compare SSA 35q15 vs 30q50 ", loc_name, ",  (", loc_ihme, ", male, ", y_s, "-", y_e, ")")) +
      xlim(min_x, max_x) +
      ylim(min_y, max_y) +
      xlab("35q15") +
      ylab("30q50") +
      scale_shape_manual(values = c("VR/SRS/DSP data" = 21, "HDSS data, collab 2015-2021" = 22, "HDSS data, INDEPTH 1990-2018" = 24, "onemod estimates" = 5, "GBD 2021 estimates" = 8, "WPP estimates" = 3)) +
      geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == 1 & location_id %in% ssa_w_vr_locs], color = "grey", fill = "red", alpha = 0.3) +
      geom_point(data = agg_data[type == "HDSS data, INDEPTH 1990-2018" & sex_id == 1 & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "yellow", alpha = 0.3) +
      geom_point(data = agg_data[type == "HDSS data, collab 2015-2021" & sex_id == 1 & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "#33CC33", alpha = 0.3) +
      geom_point(data = agg_data[type == "GBD 2021 estimates" & sex_id == 1 & location_id == loc & year_id %in% y_s:y_e]) +
      geom_point(data = agg_data[type == "onemod estimates" & sex_id == 1 & location_id == loc & year_id %in% y_s:y_e]) +
      geom_point(data = agg_data[type == "WPP estimates" & sex_id == 1 & location_id == loc & year_id %in% y_s:y_e]) +
      geom_abline(slope = 1, intercept = 0) +
      theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
      guides(fill = guide_legend(override.aes = list(shape = 21))) +
      labs(shape = "Point type",  caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

    p_all <- p1 + p2 + plot_layout(ncol = 2, nrow = 1)

    print(p_all)

  }

  dev.off()

}

# Survey census SSA
agg_survey_data[, type := paste0("HHD data", ifelse(type == "mx", "", ", adj")), by = .I]

agg_data <- rbind(agg_data, agg_survey_data, fill = TRUE)

pdf(
  fs::path(plot_dir, "scatter_35q15_30q50_highlight_SSA_HHD_sex_specific_no_subs.pdf"),
  width = 14,
  height = 7
)

p1 <- ggplot(
  data = agg_data[sex_id == 2],
  aes(x = qx_15_35, y = qx_50_30, shape = type)
) +
  theme_bw() +
  ggtitle(paste0("Compare SSA 35q15 vs 30q50 (female, all years)")) +
  xlim(0, 1) +
  ylim(0, 1) +
  xlab("35q15") +
  ylab("30q50") +
  scale_shape_manual(
    values = c(
      "VR/SRS/DSP data" = 21,
      "HDSS data, collab 2015-2021" = 22,
      "HDSS data, INDEPTH 1990-2018" = 24,
      "HHD data" = 4,
      "HHD data, adj" = 6
    )
  ) +
  geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == 2 & location_id %in% ssa_w_vr_locs], color = "grey", fill = "blue", alpha = 0.3) +
  geom_point(data = agg_data[type == "HDSS data, INDEPTH 1990-2018" & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "yellow", alpha = 0.3) +
  geom_point(data = agg_data[type == "HDSS data, collab 2015-2021" & sex_id == 2 & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "#33CC33", alpha = 0.3) +
  geom_point(data = agg_data[type ==  "HHD data" & sex_id == 2 & super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id]], color = "salmon") +
  geom_abline(slope = 1, intercept = 0) +
  theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
  guides(fill = guide_legend(override.aes = list(shape = 21))) +
  labs(shape = "Point type", caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

p2 <- ggplot(
  data = agg_data[sex_id == 1],
  aes(x = qx_15_35, y = qx_50_30, shape = type)
) +
  theme_bw() +
  ggtitle(paste0("Compare SSA 35q15 vs 30q50 (male, all years)")) +
  xlim(0, 1) +
  ylim(0, 1) +
  xlab("35q15") +
  ylab("30q50") +
  scale_shape_manual(
    values = c(
      "VR/SRS/DSP data" = 21,
      "HDSS data, collab 2015-2021" = 22,
      "HDSS data, INDEPTH 1990-2018" = 24,
      "Census data" = 4,
      "Census data, adj" = 6,
      "Survey data" = 4,
      "Survey data, adj" = 6,
      "HHD data" = 4,
      "HHD data, adj" = 6
    )
  ) +
  geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == 1 & location_id %in% ssa_w_vr_locs], color = "grey", fill = "blue", alpha = 0.3) +
  geom_point(data = agg_data[type == "HDSS data, INDEPTH 1990-2018" & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "yellow", alpha = 0.3) +
  geom_point(data = agg_data[type == "HDSS data, collab 2015-2021" & sex_id == 1 & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "#33CC33", alpha = 0.3) +
  geom_point(data = agg_data[type == "HHD data" & sex_id == 1 & super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id]], color = "salmon") +
  geom_abline(slope = 1, intercept = 0) +
  theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
  guides(fill = guide_legend(override.aes = list(shape = 21))) +
  labs(shape = "Point type",  caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

p_all <- p1 + p2 + plot_layout(ncol = 2, nrow = 1)

print(p_all)

dev.off()

pdf(
  fs::path(plot_dir, "scatter_35q15_30q50_highlight_SSA_HHD_adj_sex_specific_no_subs.pdf"),
  width = 14,
  height = 7
)

p1 <- ggplot(
  data = agg_data[sex_id == 2],
  aes(x = qx_15_35, y = qx_50_30, shape = type)
) +
  theme_bw() +
  ggtitle(paste0("Compare SSA 35q15 vs 30q50 (female, all years)")) +
  xlim(0, 1) +
  ylim(0, 1) +
  xlab("35q15") +
  ylab("30q50") +
  scale_shape_manual(
    values = c(
      "VR/SRS/DSP data" = 21,
      "HDSS data, collab 2015-2021" = 22,
      "HDSS data, INDEPTH 1990-2018" = 24,
      "HHD data" = 4,
      "HHD data, adj" = 6
    )
  ) +
  geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == 2 & location_id %in% ssa_w_vr_locs], color = "grey", fill = "blue", alpha = 0.3) +
  geom_point(data = agg_data[type == "HDSS data, INDEPTH 1990-2018" & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "yellow", alpha = 0.3) +
  geom_point(data = agg_data[type == "HDSS data, collab 2015-2021" & sex_id == 2 & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "#33CC33", alpha = 0.3) +
  geom_point(data = agg_data[type ==  "HHD data, adj" & sex_id == 2 & super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id]], color = "salmon") +
  geom_abline(slope = 1, intercept = 0) +
  theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
  guides(fill = guide_legend(override.aes = list(shape = 21))) +
  labs(shape = "Point type", caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

p2 <- ggplot(
  data = agg_data[sex_id == 1],
  aes(x = qx_15_35, y = qx_50_30, shape = type)
) +
  theme_bw() +
  ggtitle(paste0("Compare SSA 35q15 vs 30q50 (male, all years)")) +
  xlim(0, 1) +
  ylim(0, 1) +
  xlab("35q15") +
  ylab("30q50") +
  scale_shape_manual(
    values = c(
      "VR/SRS/DSP data" = 21,
      "HDSS data, collab 2015-2021" = 22,
      "HDSS data, INDEPTH 1990-2018" = 24,
      "Census data" = 4,
      "Census data, adj" = 6,
      "Survey data" = 4,
      "Survey data, adj" = 6,
      "HHD data" = 4,
      "HHD data, adj" = 6
    )
  ) +
  geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == 1 & location_id %in% ssa_w_vr_locs], color = "grey", fill = "blue", alpha = 0.3) +
  geom_point(data = agg_data[type == "HDSS data, INDEPTH 1990-2018" & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "yellow", alpha = 0.3) +
  geom_point(data = agg_data[type == "HDSS data, collab 2015-2021" & sex_id == 1 & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "#33CC33", alpha = 0.3) +
  geom_point(data = agg_data[type == "HHD data, adj" & sex_id == 1 & super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id]], color = "salmon") +
  geom_abline(slope = 1, intercept = 0) +
  theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
  guides(fill = guide_legend(override.aes = list(shape = 21))) +
  labs(shape = "Point type",  caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

p_all <- p1 + p2 + plot_layout(ncol = 2, nrow = 1)

print(p_all)

dev.off()

# Just SSA census HHD data

pdf(
  fs::path(plot_dir, "scatter_35q15_30q50_highlight_SSA_census_HHD_sex_specific_no_subs.pdf"),
  width = 14,
  height = 7
)

p1 <- ggplot(
  data = agg_data[sex_id == 2],
  aes(x = qx_15_35, y = qx_50_30, shape = type)
) +
  theme_bw() +
  ggtitle(paste0("Compare SSA 35q15 vs 30q50 (female, all years)")) +
  xlim(0, 1) +
  ylim(0, 1) +
  xlab("35q15") +
  ylab("30q50") +
  scale_shape_manual(
    values = c(
      "VR/SRS/DSP data" = 21,
      "HDSS data, collab 2015-2021" = 22,
      "HDSS data, INDEPTH 1990-2018" = 24,
      "HHD data" = 4,
      "HHD data, adj" = 6
    )
  ) +
  geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == 2 & location_id %in% ssa_w_vr_locs], color = "grey", fill = "blue", alpha = 0.3) +
  geom_point(data = agg_data[type == "HDSS data, INDEPTH 1990-2018" & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "yellow", alpha = 0.3) +
  geom_point(data = agg_data[type == "HDSS data, collab 2015-2021" & sex_id == 2 & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "#33CC33", alpha = 0.3) +
  geom_point(data = agg_data[type ==  "HHD data" & source_type_name == "Census" & sex_id == 2 & super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id]], color = "salmon") +
  geom_abline(slope = 1, intercept = 0) +
  theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
  guides(fill = guide_legend(override.aes = list(shape = 21))) +
  labs(shape = "Point type", caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

p2 <- ggplot(
  data = agg_data[sex_id == 1],
  aes(x = qx_15_35, y = qx_50_30, shape = type)
) +
  theme_bw() +
  ggtitle(paste0("Compare SSA 35q15 vs 30q50 (male, all years)")) +
  xlim(0, 1) +
  ylim(0, 1) +
  xlab("35q15") +
  ylab("30q50") +
  scale_shape_manual(
    values = c(
      "VR/SRS/DSP data" = 21,
      "HDSS data, collab 2015-2021" = 22,
      "HDSS data, INDEPTH 1990-2018" = 24,
      "Census data" = 4,
      "Census data, adj" = 6,
      "Survey data" = 4,
      "Survey data, adj" = 6,
      "HHD data" = 4,
      "HHD data, adj" = 6
    )
  ) +
  geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == 1 & location_id %in% ssa_w_vr_locs], color = "grey", fill = "blue", alpha = 0.3) +
  geom_point(data = agg_data[type == "HDSS data, INDEPTH 1990-2018" & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "yellow", alpha = 0.3) +
  geom_point(data = agg_data[type == "HDSS data, collab 2015-2021" & sex_id == 1 & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "#33CC33", alpha = 0.3) +
  geom_point(data = agg_data[type == "HHD data" & source_type_name == "Census" & sex_id == 1 & super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id]], color = "salmon") +
  geom_abline(slope = 1, intercept = 0) +
  theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
  guides(fill = guide_legend(override.aes = list(shape = 21))) +
  labs(shape = "Point type",  caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

p_all <- p1 + p2 + plot_layout(ncol = 2, nrow = 1)

print(p_all)

dev.off()

pdf(
  fs::path(plot_dir, "scatter_35q15_30q50_highlight_SSA_census_HHD_adj_sex_specific_no_subs.pdf"),
  width = 14,
  height = 7
)

p1 <- ggplot(
  data = agg_data[sex_id == 2],
  aes(x = qx_15_35, y = qx_50_30, shape = type)
) +
  theme_bw() +
  ggtitle(paste0("Compare SSA 35q15 vs 30q50 (female, all years)")) +
  xlim(0, 1) +
  ylim(0, 1) +
  xlab("35q15") +
  ylab("30q50") +
  scale_shape_manual(
    values = c(
      "VR/SRS/DSP data" = 21,
      "HDSS data, collab 2015-2021" = 22,
      "HDSS data, INDEPTH 1990-2018" = 24,
      "HHD data" = 4,
      "HHD data, adj" = 6
    )
  ) +
  geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == 2 & location_id %in% ssa_w_vr_locs], color = "grey", fill = "blue", alpha = 0.3) +
  geom_point(data = agg_data[type == "HDSS data, INDEPTH 1990-2018" & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "yellow", alpha = 0.3) +
  geom_point(data = agg_data[type == "HDSS data, collab 2015-2021" & sex_id == 2 & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "#33CC33", alpha = 0.3) +
  geom_point(data = agg_data[type ==  "HHD data, adj" & source_type_name == "Census" & sex_id == 2 & super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id]], color = "salmon") +
  geom_abline(slope = 1, intercept = 0) +
  theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
  guides(fill = guide_legend(override.aes = list(shape = 21))) +
  labs(shape = "Point type", caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

p2 <- ggplot(
  data = agg_data[sex_id == 1],
  aes(x = qx_15_35, y = qx_50_30, shape = type)
) +
  theme_bw() +
  ggtitle(paste0("Compare SSA 35q15 vs 30q50 (male, all years)")) +
  xlim(0, 1) +
  ylim(0, 1) +
  xlab("35q15") +
  ylab("30q50") +
  scale_shape_manual(
    values = c(
      "VR/SRS/DSP data" = 21,
      "HDSS data, collab 2015-2021" = 22,
      "HDSS data, INDEPTH 1990-2018" = 24,
      "Census data" = 4,
      "Census data, adj" = 6,
      "Survey data" = 4,
      "Survey data, adj" = 6,
      "HHD data" = 4,
      "HHD data, adj" = 6
    )
  ) +
  geom_point(data = agg_data[type == "VR/SRS/DSP data" & sex_id == 1 & location_id %in% ssa_w_vr_locs], color = "grey", fill = "blue", alpha = 0.3) +
  geom_point(data = agg_data[type == "HDSS data, INDEPTH 1990-2018" & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "yellow", alpha = 0.3) +
  geom_point(data = agg_data[type == "HDSS data, collab 2015-2021" & sex_id == 1 & super_region_name == "Sub-Saharan Africa"], color = "black", fill = "#33CC33", alpha = 0.3) +
  geom_point(data = agg_data[type == "HHD data, adj" & source_type_name == "Census" & sex_id == 1 & super_region_name == "Sub-Saharan Africa" & location_id %in% loc_map[level == 3, location_id]], color = "salmon") +
  geom_abline(slope = 1, intercept = 0) +
  theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5)) +
  guides(fill = guide_legend(override.aes = list(shape = 21))) +
  labs(shape = "Point type",  caption = paste0("SSA locations with VR: ", paste(ssa_w_vr, collapse = ", ")))

p_all <- p1 + p2 + plot_layout(ncol = 2, nrow = 1)

print(p_all)

dev.off()

