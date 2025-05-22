# Scatter comparing all u5 ages for onemod investigations

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
plot_dir <- fs::path(dir_output, "/diagnostics/u5/")

fs::dir_create(plot_dir, mode = "775")

group_vars <- c("location_id", "year_id", "sex_id", "nid", "underlying_nid", "age_start", "age_end")

# get data --------------------------------------------------------------------

onemod_input_data <-
  arrow::read_parquet(cfg$fp_inputs$handoff_2) |>
  setDT()

loc_map <- fread(fs::path(dir_output, "loc_map.csv"))
age_map_gbd <- fread(fs::path(dir_output, "age_map_gbd.csv"))
age_map_sy <- fread(fs::path(dir_output, "age_map_sy.csv"))

age_map <- rbind(age_map_gbd, age_map_sy)

onemod_data <-
  fs::path(args$dir_output, "summaries") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(age_group_id %in% age_map_gbd$age_group_id) |>
  dplyr::collect() |>
  setDT()

gbd_2021_data <- mortdb::get_mort_outputs(
  "no shock life table",
  "estimate",
  run_id = "best",
  gbd_year = 2021,
  life_table_parameter_ids = 1,
  estimate_stage_ids = 5,
  sex_ids = 1:2,
  age_group_ids = age_map[age_end <= 5, age_group_id],
  location_ids = loc_map[level == 3 | location_id == 44533, location_id]
)

ddm <- mortdb::get_mort_outputs(
  "ddm", "estimate",
  run_id = "best",
  gbd_year = 2021,
  estimate_stage_ids = 14,
  location_ids = loc_map[level == 3 | location_id == 44533, location_id]
)

pop_data <-
  fs::path(dir_output, "population", ext = "parquet") |>
  arrow::read_parquet() |>
  setDT()

# identify high quality VR locations -------------------------------------------

ddm <- ddm[source %in% c("VR", "DSP", "SRS")]

ddm <- ddm[, .(location_id, year_id, sex_id, ddm = mean, source_type_name = source)]

ddm <- ddm[ddm >= 0.95]

# prep vr data for plotting ----------------------------------------------------

vr_data <- onemod_input_data[outlier == 0 & source_type_name %in% c("VR", "DSP", "SRS")]

# national only
vr_data <- vr_data[location_id %in% loc_map[level == 3 | location_id == 44533, location_id]]

vr_data[
  age_map,
  `:=` (age_start = i.age_start, age_end = i.age_end, age_group_name = i.age_group_name),
  on = .(age_group_id)
]

# subset to u5 ages
vr_data <- vr_data[age_end <= 5]

# subset to high quality VR
vr_data <- ddm[vr_data, on = .(location_id, year_id, sex_id, source_type_name), nomatch = NULL]

# append u5 age group
u5_vr_data <- copy(vr_data)

u5_vr_data <- u5_vr_data[!nid %in% c(310443, 310444, 233896)]

u5_vr_data <- u5_vr_data[, .(location_id, year_id, sex_id, nid, underlying_nid,
                             deaths, sample_size, age_start, age_end)]

u5_vr_data <- hierarchyUtils::agg(
  dt = u5_vr_data,
  id_cols = group_vars,
  value_cols = c("deaths", "sample_size"),
  col_stem = "age",
  col_type = "interval",
  mapping = data.table(age_start = 0, age_end = 5),
  missing_dt_severity = "warning"
)

u5_vr_data[, mx := deaths / sample_size]
u5_vr_data[, age_group_name := "Under 5"]

vr_data <- rbind(vr_data, u5_vr_data, fill = TRUE)

vr_data <- vr_data[, .(location_id, year_id, sex_id, nid, underlying_nid, mx = mx,
                       age_group_name, type = "VR/DSP/SRS data")]

# prep survey data for plotting ------------------------------------------------

survey_data <- onemod_input_data[
  outlier == 0 &
    source_type_name %in% c("CBH") &
    onemod_process_type == "standard gbd age group data"
]

# national only
survey_data <- survey_data[location_id %in% loc_map[level == 3 | location_id == 44533, location_id]]

survey_data[
  age_map,
  `:=` (age_start = i.age_start, age_end = i.age_end, age_group_name = i.age_group_name),
  on = .(age_group_id)
  ]

# subset to u5 ages
survey_data <- survey_data[age_end <= 5]

# append u5 age group
u5_survey_data <- copy(survey_data)
u5_survey_data <- u5_survey_data[, .(location_id, year_id, sex_id, nid, underlying_nid,
                                     deaths = mx * sample_size, sample_size, age_start, age_end)]

u5_survey_data <- hierarchyUtils::agg(
  dt = u5_survey_data,
  id_cols = group_vars,
  value_cols = c("deaths", "sample_size"),
  col_stem = "age",
  col_type = "interval",
  mapping = data.table(age_start = 0, age_end = 5),
  missing_dt_severity = "warning"
)

u5_survey_data[, mx := deaths / sample_size]
u5_survey_data[, age_group_name := "Under 5"]

survey_data <- rbind(survey_data, u5_survey_data, fill = TRUE)

survey_data <- survey_data[, .(location_id, year_id, sex_id, nid, underlying_nid, mx = mx,
                               age_group_name, type = "CBH data")]

# prep GBD 2021 estimates ------------------------------------------------------------

gbd_2021_data[
  age_map,
  `:=` (age_start = i.age_start, age_end = i.age_end, age_group_name = i.age_group_name),
  on = .(age_group_id)
  ]

# aggregate to the u5 age group
pop_data[age_map, `:=`(age_start = i.age_start, age_end = i.age_end), on = .(age_group_id)]

pop_data <- pop_data[, .(location_id, year_id, sex_id, age_start, age_end, pop = mean)]

u5_gbd_2021_data <- copy(gbd_2021_data)
u5_gbd_2021_data <- u5_gbd_2021_data[, .(location_id, year_id, sex_id, mean, age_start, age_end)]

u5_gbd_2021_data <- pop_data[
  u5_gbd_2021_data,
  on = .(location_id, year_id, sex_id, age_start, age_end)
]

u5_gbd_2021_data[, deaths := pop * mean]
u5_gbd_2021_data[, mean := NULL]

u5_gbd_2021_data <- hierarchyUtils::agg(
  dt = u5_gbd_2021_data,
  id_cols = c("location_id", "year_id", "sex_id", "age_start", "age_end"),
  value_cols = c("deaths", "pop"),
  col_stem = "age",
  col_type = "interval",
  mapping = data.table(age_start = 0, age_end = 5),
  overlapping_dt_severity = "warning"
)

u5_gbd_2021_data[, mean := deaths / pop]
u5_gbd_2021_data[, age_group_name := "Under 5"]

gbd_2021_data <- rbind(gbd_2021_data, u5_gbd_2021_data, fill = TRUE)

gbd_2021_data <- gbd_2021_data[, .(location_id, year_id, sex_id, mx = mean,
                                   age_group_name, type = "GBD 2021 estimates")]

# prep onemod estimates --------------------------------------------------------

onemod_data[
  age_map,
  `:=` (
    age_start = i.age_start,
    age_end = i.age_end,
    age_group_name = i.age_group_name
    ),
  on = .(age_group_id)
]

onemod_data <- onemod_data[age_end <= 5]

# national only
onemod_data <- onemod_data[location_id %in% loc_map[level == 3 | location_id == 44533, location_id]]

# aggregate u5
u5_onemod_data <- copy(onemod_data)

u5_onemod_data <-
  pop_data[
  u5_onemod_data,
  on = .(location_id, year_id, sex_id, age_start, age_end),
  nomatch = NULL
] |>
  setnames(c("deaths_age_scaled_mean", "population_mean"), c("deaths", "pop"))

u5_onemod_data <- u5_onemod_data[, .(location_id, year_id, sex_id, age_start, age_end, deaths, pop)]

u5_onemod_data <-
  hierarchyUtils::agg(
  dt = u5_onemod_data,
  id_cols = c("location_id", "year_id", "sex_id", "age_start", "age_end"),
  value_cols = c("deaths", "pop"),
  col_stem = "age",
  col_type = "interval",
  mapping = data.table(age_start = 0, age_end = 5)
)

u5_onemod_data[, kreg := deaths / pop]
u5_onemod_data[, age_group_name := "Under 5"]

onemod_data <- rbind(onemod_data, u5_onemod_data, fill = TRUE)

onemod_data <- onemod_data[, .(location_id, year_id, sex_id, mx = kreg,
                               age_group_name, type = "onemod estimates")]

# combine for plotting ---------------------------------------------------------

all_data <- rbind(
  vr_data,
  survey_data,
  gbd_2021_data,
  onemod_data,
  fill = TRUE
)

# create all age groups for plotting
age_crossings <- pairwise_combinations <- combn(unique(onemod_data$age_group_name), 2)
age_crossings <- as.data.table(t(age_crossings))
setnames(age_crossings, names(age_crossings), c("age_group_name", "comp_age_group_name"))

age_crossings <- age_crossings[age_group_name != comp_age_group_name]

comp_all_data <- copy(all_data)
setnames(comp_all_data, c("age_group_name", "mx"), c("comp_age_group_name", "comp_mx"))

comp_all_data <- merge(
  comp_all_data,
  age_crossings,
  by = "comp_age_group_name",
  allow.cartesian = TRUE
)

all_data <- comp_all_data[
  all_data,
  on = .(location_id, year_id, sex_id, nid, underlying_nid, type, age_group_name)
]

all_data[, comp_title := paste0(age_group_name, " vs ", comp_age_group_name)]

all_data <- loc_map[all_data, on = .(location_id)]

all_data <- all_data[!is.na(super_region_name)]

# make plots -------------------------------------------------------------------

pdf(
  fs::path(plot_dir, "scatter_u5_high_qual_VR_vs_estimates.pdf"),
  width = 14,
  height = 7
)

for (a in unique(all_data$comp_title)) {
  for (s in 1:2) {

    temp_x <- unique(all_data[comp_title == a, age_group_name])
    temp_y <- unique(all_data[comp_title == a, comp_age_group_name])

    sex_name <- ifelse(s == 1, "male", "female")

    p1 <- ggplot(
      data = all_data[comp_title == a & sex_id == s],
      aes(x = mx, y = comp_mx, color = type, fill = super_region_name, shape = type)
    ) +
      theme_bw() +
      ggtitle(paste0("U5 ages mx comparison of high quality VR: ", sex_name, ", ", a)) +
      xlab(temp_x) +
      ylab(temp_y) +
      scale_x_continuous(transform = "log") +
      scale_y_continuous(transform = "log") +
      scale_shape_manual(
        values = c(
          "VR/DSP/SRS data" = 21,
          "onemod estimates" = 4,
          "GBD 2021 estimates" = 4
        )
      ) +
      scale_color_manual(
        values = c(
          "VR/DSP/SRS data" = "grey",
          "onemod estimates" = "blue",
          "GBD 2021 estimates" = "red"
        )
      ) +
      geom_point(data = all_data[comp_title == a & type == "onemod estimates" & sex_id == s], alpha = 0.3) +
      geom_point(data = all_data[comp_title == a & type == "VR/DSP/SRS data" & sex_id == s], alpha = 0.5) +
      geom_abline(slope = 1, intercept = 0) +
      guides(fill = guide_legend(override.aes = list(shape = 21))) +
      theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5))

    p2 <- ggplot(
      data = all_data[comp_title == a & sex_id == s],
      aes(x = mx, y = comp_mx, color = type, fill = super_region_name, shape = type)
    ) +
      theme_bw() +
      ggtitle(paste0("U5 ages mx comparison of high quality VR: ", sex_name, ", ", a)) +
      xlab(temp_x) +
      ylab(temp_y) +
      scale_x_continuous(transform = "log") +
      scale_y_continuous(transform = "log") +
      scale_shape_manual(
        values = c(
          "VR/DSP/SRS data" = 21,
          "onemod estimates" = 4,
          "GBD 2021 estimates" = 4
        )
      ) +
      scale_color_manual(
        values = c(
          "VR/DSP/SRS data" = "grey",
          "onemod estimates" = "blue",
          "GBD 2021 estimates" = "red"
        )
      ) +
      geom_point(data = all_data[comp_title == a & type == "GBD 2021 estimates" & sex_id == s], alpha = 0.3) +
      geom_point(data = all_data[comp_title == a & type == "VR/DSP/SRS data" & sex_id == s], alpha = 0.5) +
      geom_abline(slope = 1, intercept = 0) +
      guides(fill = guide_legend(override.aes = list(shape = 21))) +
      theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5))

    p_all <- p1 + p2 + plot_layout(ncol = 2, nrow = 1)

    print(p_all)

  }
}

dev.off()

# ----------------------

pdf(
  fs::path(plot_dir, "scatter_u5_cbh_vs_estimates.pdf"),
  width = 14,
  height = 7
)

for (a in unique(all_data$comp_title)) {
  for (s in 1:2) {

    temp_x <- unique(all_data[comp_title == a, age_group_name])
    temp_y <- unique(all_data[comp_title == a, comp_age_group_name])

    sex_name <- ifelse(s == 1, "male", "female")

    p1 <- ggplot(
      data = all_data[comp_title == a & sex_id == s],
      aes(x = mx, y = comp_mx, color = type, fill = super_region_name, shape = type)
    ) +
      theme_bw() +
      ggtitle(paste0("U5 ages mx comparison of CBH: ", sex_name, ", ", a)) +
      xlab(temp_x) +
      ylab(temp_y) +
      scale_x_continuous(transform = "log") +
      scale_y_continuous(transform = "log") +
      scale_shape_manual(
        values = c(
          "CBH data" = 21,
          "onemod estimates" = 4,
          "GBD 2021 estimates" = 4
        )
      ) +
      scale_color_manual(
        values = c(
          "CBH data" = "grey",
          "onemod estimates" = "blue",
          "GBD 2021 estimates" = "red"
        )
      ) +
      geom_point(data = all_data[comp_title == a & type == "onemod estimates" & sex_id == s], alpha = 0.3) +
      geom_point(data = all_data[comp_title == a & type == "CBH data" & sex_id == s], alpha = 0.5) +
      geom_abline(slope = 1, intercept = 0) +
      guides(fill = guide_legend(override.aes = list(shape = 21))) +
      theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5))

    p2 <- ggplot(
      data = all_data[comp_title == a & sex_id == s],
      aes(x = mx, y = comp_mx, color = type, fill = super_region_name, shape = type)
    ) +
      theme_bw() +
      ggtitle(paste0("U5 ages mx comparison of CBH: ", sex_name, ", ", a)) +
      xlab(temp_x) +
      ylab(temp_y) +
      scale_x_continuous(transform = "log") +
      scale_y_continuous(transform = "log") +
      scale_shape_manual(
        values = c(
          "CBH data" = 21,
          "onemod estimates" = 4,
          "GBD 2021 estimates" = 4
        )
      ) +
      scale_color_manual(
        values = c(
          "CBH data" = "grey",
          "onemod estimates" = "blue",
          "GBD 2021 estimates" = "red"
        )
      ) +
      geom_point(data = all_data[comp_title == a & type == "GBD 2021 estimates" & sex_id == s], alpha = 0.3) +
      geom_point(data = all_data[comp_title == a & type == "CBH data" & sex_id == s], alpha = 0.5) +
      geom_abline(slope = 1, intercept = 0) +
      guides(fill = guide_legend(override.aes = list(shape = 21))) +
      theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5))

    p_all <- p1 + p2 + plot_layout(ncol = 2, nrow = 1)

    print(p_all)

  }
}

dev.off()

# ------------------------------

gbd_2021_data <- gbd_2021_data[, .(location_id, year_id, sex_id, age_group_name, gbd_2021_mx = mx)]
onemod_data <- onemod_data[, .(location_id, year_id, sex_id, age_group_name, onemod_mx = mx)]

compare_data <- gbd_2021_data[
  onemod_data,
  on = .(location_id, year_id, sex_id, age_group_name),
  nomatch = NULL
]

compare_data <- compare_data[
  loc_map,
  super_region_name := i.super_region_name,
  on = .(location_id)
]

pdf(
  fs::path(plot_dir, "scatter_u5_gbd_2021_vs_onemod.pdf"),
  width = 14,
  height = 7
)

for (a in unique(compare_data$age_group_name)) {

  sex_name <- ifelse(s == 1, "male", "female")

  p1 <- ggplot(
    data = compare_data[age_group_name == a & sex_id == 1],
    aes(x = gbd_2021_mx, y = onemod_mx, fill = super_region_name)
  ) +
    theme_bw() +
    ggtitle(paste0("U5 ages mx comparison: male, ", a)) +
    xlab("GBD 2021 estimate (mx)") +
    ylab("Onemod estimate (mx)") +
    scale_x_continuous(transform = "log") +
    scale_y_continuous(transform = "log") +
    geom_point(data = compare_data[sex_id == 1], alpha = 0.3, shape = 21) +
    geom_abline(slope = 1, intercept = 0) +
    guides(fill = guide_legend(override.aes = list(shape = 21))) +
    theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5))

  p2 <- ggplot(
    data = compare_data[age_group_name == a & sex_id == 2],
    aes(x = gbd_2021_mx, y = onemod_mx, fill = super_region_name)
  ) +
    theme_bw() +
    ggtitle(paste0("U5 ages mx comparison: female, ", a)) +
    xlab("GBD 2021 estimate (mx)") +
    ylab("Onemod estimate (mx)") +
    scale_x_continuous(transform = "log") +
    scale_y_continuous(transform = "log") +
    geom_point(data = compare_data[sex_id == 2], alpha = 0.3, shape = 21) +
    geom_abline(slope = 1, intercept = 0) +
    guides(fill = guide_legend(override.aes = list(shape = 21))) +
    theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5))

  p_all <- p1 + p2 + plot_layout(ncol = 2, nrow = 1)

  print(p_all)

}

dev.off()

for (a in unique(compare_data$age_group_name)) {

  pdf(
    fs::path(plot_dir, glue::glue("scatter_{a}_gbd_2021_vs_onemod_per_year.pdf")),
    width = 14,
    height = 7
  )

  for(y in unique(compare_data$year_id)) {

    sex_name <- ifelse(s == 1, "male", "female")

    p1 <- ggplot(
      data = compare_data[age_group_name == a & sex_id == 1 & year_id == y],
      aes(x = gbd_2021_mx, y = onemod_mx, fill = super_region_name)
    ) +
      theme_bw() +
      ggtitle(paste0("U5 ages mx comparison: male, ", a, ", ", y)) +
      xlab("GBD 2021 estimate (mx)") +
      ylab("Onemod estimate (mx)") +
      scale_x_continuous(transform = "log") +
      scale_y_continuous(transform = "log") +
      geom_point(data = compare_data[sex_id == 1 & year_id == y], alpha = 0.3, shape = 21) +
      geom_abline(slope = 1, intercept = 0) +
      guides(fill = guide_legend(override.aes = list(shape = 21))) +
      theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5))

    p2 <- ggplot(
      data = compare_data[age_group_name == a & sex_id == 2 & year_id == y],
      aes(x = gbd_2021_mx, y = onemod_mx, fill = super_region_name)
    ) +
      theme_bw() +
      ggtitle(paste0("U5 ages mx comparison: female, ", a, ", ", y)) +
      xlab("GBD 2021 estimate (mx)") +
      ylab("Onemod estimate (mx)") +
      scale_x_continuous(transform = "log") +
      scale_y_continuous(transform = "log") +
      geom_point(data = compare_data[sex_id == 2 & year_id == y], alpha = 0.3, shape = 21) +
      geom_abline(slope = 1, intercept = 0) +
      guides(fill = guide_legend(override.aes = list(shape = 21))) +
      theme(legend.text = element_text(size = 5), plot.caption = element_text(size = 5))

    p_all <- p1 + p2 + plot_layout(ncol = 2, nrow = 1)

    print(p_all)

  }

  dev.off()

}


# region and all pairwise comparison plots

for(reg in unique(loc_map[!is.na(super_region_name)]$super_region_name)) {

  temp_dir <- fs::path(plot_dir, glue::glue("u5_scatter_{reg}"))
  temp_dir <- gsub(" ", "_", temp_dir)
  temp_dir <- gsub(",", "", temp_dir)
  fs::dir_create(temp_dir, mode = "775")

  for(a in unique(all_data$comp_title)) {

    temp_pdf <- fs::path(temp_dir, glue::glue("/scatter_{reg}_{a}_gbd_2021_onemod_per_loc_sex.pdf"))
    temp_pdf <- gsub(" ", "_", temp_pdf)
    temp_pdf <- gsub(",", "", temp_pdf)

    temp_x <- unique(all_data[comp_title == a, age_group_name])
    temp_y <- unique(all_data[comp_title == a, comp_age_group_name])

    pdf(
      temp_pdf,
      width = 14,
      height = 7
    )

    for (l in unique(all_data[location_id %in% loc_map[level == 3 & super_region_name == reg, location_id], location_id])) {

      loc_name <- loc_map[location_id == l, location_name]
      ihme_loc <- loc_map[location_id == l, ihme_loc_id]

      reg_locs <- loc_map[super_region_name == reg, location_id]

      for (s in 1:2) {

        sex_name <- ifelse(s == 1, "male", "female")

        p <- ggplot(
          data = all_data[sex_id == s & comp_title == a],
          aes(x = mx, y = comp_mx, shape = type)
        ) +
          theme_bw() +
          ggtitle(paste0("Compare mx ", a," (", sex_name, ", all years), ", loc_name, ", ", ihme_loc)) +
          xlab(temp_x) +
          ylab(temp_y) +
          geom_point(
            data = all_data[type == "VR/DSP/SRS data" & sex_id == s & comp_title == a & !location_id %in% reg_locs],
            alpha = 0.3,
            color = "white",
            aes(fill = super_region_name)
          ) +
          geom_point(
            data = all_data[type == "VR/DSP/SRS data" & sex_id == s & comp_title == a & location_id %in% reg_locs],
            alpha = 1,
            color = "darkgrey",
            aes(fill = super_region_name)
          ) +
          geom_point(
            data = all_data[type == "GBD 2021 estimates" & sex_id == s & comp_title == a & location_id == l],
            fill = NA,
            aes(color = year_id)
          ) +
          geom_point(
            data = all_data[type == "onemod estimates" & sex_id == s & comp_title == a & location_id == l],
            fill = NA,
            aes(color = year_id)
          ) +
          geom_abline(slope = 1, intercept = 0) +
          scale_shape_manual(values = c("VR/DSP/SRS data" = 21, "onemod estimates" = 22, "GBD 2021 estimates" = 24)) +
          scale_color_gradient(low = "black", high = "red") +
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
}
