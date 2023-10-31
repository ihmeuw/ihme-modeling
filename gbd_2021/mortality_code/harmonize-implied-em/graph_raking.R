# graph COVID before and after raking

# Read Arguments ----------------------------------------------------------

parser <- argparse::ArgumentParser()

parser$add_argument(
  "--dir_output",
  type = "character",
  required = !interactive(),
  default = "FILEPATH",
  help = "Output directory for em adjustment"
)
parser$add_argument(
  "--run_id_balancing",
  type = "integer",
  required = !interactive(),
  default = 81L,
  help = "No shock Envelope - no pandemic years data"
)
args <- parser$parse_args()

list2env(args, .GlobalEnv)

# Load packages -----------------------------------------------------------

library(mortdb)
library(data.table)
library(ggplot2)
library(furrr)


# Set parameters ----------------------------------------------------------

plan("multisession")

analysis_years <- 2010:2021

dir_diagnostics <- fs::path(dir_output, "diagnostics")
file_name_out <- "compare_post_raking_covid_oprm"


dir_cc_covid <- "FILEPATH"

id_cols <- c("location_id", "year_id","sex_id", "age_group_id")

# Load maps ---------------------------------------------------------------

map_locs <- fread(
  fs::path(
    dir_output, "loc_map", ext = "csv"
  )
)

map_ages <- fread(
  fs::path(
    dir_output, "age_map", ext = "csv"
  )
)

# Load data ---------------------------------------------------------------

get_current_cc_draws <- function(file_results, cause_id) {

  dt <- fread(
    fs::path(
      dir_cc_covid,
      run_id_balancing,
      "pipeline_draws",
      file_results
    )
  )

  # !!! HOTFIX: Use OPRM version without IND 2020 SRS included
  dt_ind <- fread(fs::path(dir_cc_covid, 85, "pipeline_draws", file_results))
  ind_locs <- map_locs[ihme_loc_id %like% "IND", location_id]
  dt <- rbind(
    dt[!location_id %in% ind_locs],
    dt_ind[location_id %in% ind_locs]
  )

  dt[, cause_id := cause_id]

  # also melt to long
  dt <- melt(
    data = dt,
    id.vars = c(id_cols, "cause_id"),
    measure.vars = patterns("draw"),
    variable.name = "draw",
    variable.factor = FALSE,
    value.name = "deaths"
  )

  # summarize
  dt <- demUtils::summarize_dt(dt,
                               id_cols = c(id_cols, "draw"),
                               summarize_cols = "draw",
                               value_cols = "deaths",
                               probs = c())

  # aggregate to all locations
  dt <- mortcore::agg_results(
    data = dt,
    id_vars = id_cols,
    value_vars = "mean",
    end_agg_level = 3
  )

  setnames(dt, "mean", "deaths")
  return(dt)
}


# pre raking harmonizer results
dt_preraked <- fs::path(dir_output, "summary/loc_specific_env") |>
  fs::dir_ls(regexp = "harmonized") |>
  future_map(readRDS) |>
  rbindlist()

# harmonizer (post adjustment covid and OPRM)
dt_harmonizer <- fs::path(dir_output, "summary/scaled_env") |>
  fs::dir_ls(regexp = "scaled") |>
  future_map(readRDS) |>
  rbindlist()

# Pre-adjusted COVID and OPRM
dt_indirect_draws <- get_current_cc_draws("indirect_covid_draws_corrected.csv", 9999)


# Format Data -------------------------------------------------------------

# subset harmonizer to only the most detailed age groups and sexes
dt_harmonizer <- dt_harmonizer[age_group_id %in% dt_indirect_draws$age_group_id &
                                 sex_id != 3]

# melt to long
dt_harmonizer <- melt(dt_harmonizer,
                      id.vars = id_cols,
                      measure.vars = c("no_shock_mean",
                                       "covid_mean",
                                       "pos_oprm_mean"),
                      variable.name = "measure",
                      value.name = "deaths")
dt_harmonizer <- dt_harmonizer[measure != "no_shock_mean"]

dt_harmonizer[measure == "pos_oprm_mean", measure := "oprm_mean"]

# add on indirect and calculate EM
dt_indirect_draws[, measure := "indirect_covid"]

dt_harmonizer <- rbind(dt_harmonizer, dt_indirect_draws)

# calculate total EM
dt_after_em <- dt_harmonizer[, .(deaths = sum(deaths),
                                 measure = "em"),
                             by = id_cols]

dt_harmonizer <- rbind(dt_harmonizer, dt_after_em)


# subset harmonizer to only the most detailed age groups and sexes
dt_preraked <- dt_preraked[age_group_id %in% dt_indirect_draws$age_group_id &
                                 sex_id != 3]

# melt to long
dt_preraked <- melt(dt_preraked,
                      id.vars = id_cols,
                      measure.vars = c("no_shock_mean",
                                       "covid_mean",
                                       "pos_oprm_mean"),
                      variable.name = "measure",
                      value.name = "deaths")
dt_preraked <- dt_preraked[measure != "no_shock_mean"]

dt_preraked[measure == "pos_oprm_mean", measure := "oprm_mean"]

# add on indirect and calculate EM
dt_indirect_draws[, measure := "indirect_covid"]

dt_preraked <- rbind(dt_preraked, dt_indirect_draws)

# calculate total EM
dt_after_em <- dt_preraked[, .(deaths = sum(deaths),
                                 measure = "em"),
                             by = id_cols]

dt_preraked <- rbind(dt_preraked, dt_after_em)


# bind before and after draws together
dt_graph <- rbindlist(
  list(
    before = dt_preraked,
    after = dt_harmonizer
  ),
  idcol = "stage",
  use.names = TRUE
)

new_cols <- c("ihme_loc_id", "sex_name", "age_start",
              "age_end", "age_group_name")

dt_graph <- demInternal::ids_names(
  dt_graph,
  extra_output_cols = new_cols
)

dt_graph[, age_group_name := factor(age_group_name, levels = map_ages$age_group_name, ordered = TRUE)]


# Graph -------------------------------------------------------------------

make_plot <- function(data) {

  loc_code <- unique(data$ihme_loc_id)
  loc_name <- unique(data$location_name)
  yr <- unique(data$year_id)
  sex <- unique(data$sex_name)

  ggplot(data, aes(x = age_start, y = deaths, color = measure, lty = stage)) +
    geom_line(alpha = 0.6) +
    geom_point() +
    facet_wrap(~sex_name + year_id) +
    labs(
      title = glue::glue("{loc_code}; {loc_name} Deaths from COVID and OPRM")
    ) +
    scale_color_brewer(palette = "Dark2") +
    theme_bw()
}

list_plots <- dt_graph |>
  split(by = c("ihme_loc_id")) |>
  lapply(make_plot)

list_plots[[1]]


# Save --------------------------------------------------------------------

dir_out_tmp <- tempdir()

furrr::future_iwalk(
  list_plots,
  ~withr::with_pdf(
    new = fs::path(dir_out_tmp, paste(file_name_out, .y, sep = "-"), ext = "pdf"),
    width = 15,
    height = 10,
    code = plot(.x)
  )
)

files_temp <- fs::path(dir_out_tmp, paste(file_name_out, names(list_plots), sep = "-"), ext = "pdf")
stopifnot(all(fs::file_exists(files_temp)))

qpdf::pdf_combine(
  input = files_temp,
  output = fs::path(dir_diagnostics, file_name_out, ext = "pdf")
)
