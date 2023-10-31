# graph COVID before and after harmonizer

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
  default = 82L,
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
file_name_out <- "compare_post_harmonization_covid_oprm"


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

# harmonizer (post adjustment covid and OPRM)
dt_harmonizer <- fs::path(dir_output, "summary/scaled_env") |>
  fs::dir_ls(regexp = "scaled") |>
  future_map(readRDS) |>
  rbindlist()

# Pre-adjusted COVID and OPRM
dt_covid <- get_current_cc_draws("total_covid_draws_corrected.csv", 1048)
dt_oprm <- get_current_cc_draws("other_pandemic_draws_corrected.csv", 1058)
dt_indirect_draws <- get_current_cc_draws("indirect_covid_draws_corrected.csv", 9999)


# Format Data -------------------------------------------------------------

# subset harmonizer to only the most detailed age groups and sexes
dt_harmonizer <- dt_harmonizer[age_group_id %in% dt_oprm$age_group_id &
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

# bind the CC draws together
dt_cc_draws <- rbindlist(
  list(
    covid_mean = dt_covid,
    oprm_mean = dt_oprm,
    indirect_covid = dt_indirect_draws[, 1:5]
  ),
  idcol = "measure"
)

dt_before_em <- dt_cc_draws[, .(deaths = sum(deaths),
                                  measure = "em"),
                              by = id_cols]

dt_cc_draws <- rbind(dt_cc_draws, dt_before_em)

# bind before and after draws together
dt_graph <- rbindlist(
  list(
    before = dt_cc_draws,
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



# Save Places with Large Changes in EM ------------------------------------

# if em or covid changed by more than 10% and more than 10 deaths, save that
# location age sex

dt_diagnostic <- copy(dt_graph)

dt_diagnostic <- dcast(
  dt_diagnostic,
  location_name + ihme_loc_id + year_id + sex_name + age_group_name + measure ~ stage,
  value.var = "deaths"
)

# reorder columns and subste to just covid and em
dt_diagnostic <- dt_diagnostic[measure %in% c("em", "covid_mean"),
                               .(location_name, ihme_loc_id, year_id, sex_name,
                                 age_group_name, measure, deaths_before = before,
                                 deaths_after = after)]

dt_diagnostic[, `:=` (abs_diff = abs(deaths_before - deaths_after),
                      abs_pct_diff = abs(deaths_before - deaths_after) / deaths_before * 100)]
# save subset
readr::write_csv(
  dt_diagnostic[abs_diff > 10 & abs_pct_diff > 10],
  fs::path(
    dir_diagnostics,
    "large_covid_em_changes.csv"
  )
)


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

