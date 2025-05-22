
# load in cluster args ----------------------------------------------------

if(interactive()) {
  task_id <- 1
  params_path <- file.path(getwd(), 'ensemble/params.rds')
  loc_id_path <- file.path(getwd(), 'ensemble/locs.csv')
  out_dir <- 'FILEPATH'
} else {
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  command_args <- commandArgs(trailingOnly = TRUE)
  params_path <- command_args[1]
  loc_id_path <- command_args[2]
  out_dir <- command_args[3]
}

#future::plan(future::multisession)

params <- readRDS(params_path)

source(file.path(getwd(), 'ensemble/ensemble_functions.R'))

# get sd file names -------------------------------------------------------

loc_param_map <- read.csv(loc_id_path)

sd_file <- file.path(
  out_dir,
  paste0('loc_', loc_param_map$location_id[task_id], '.fst')
)

# load in proportion of population pregnant data --------------------------

preg_pop_dat <- fst::read.fst(
  path = params$pregnancy_population_file, 
  as.data.table = TRUE
)

# helper function for processing pregnancy data ---------------------------

update_mean_hb <- function(dat) {
  PREG_XWALK_FACTOR <- 0.919325
  dat |>
    dplyr::mutate(
      mean_hb_preg = dplyr::case_when(
        !(is.na(prop_pregnant)) ~ meanval * PREG_XWALK_FACTOR,
        .default = NA_real_
      )
    )
}

get_pregnancy_anemia_prevalence <- function(dat) {
  ensemble_weights <- list(
    gamma = 0.4,
    mgumbel = 0.6
  )
  
  MIN_HB <- params$xmin
  MAX_HB <- params$xmax
  hb_vec <- params$x
  SEVERE_PREG_CUTOFF <- 70
  MODERATE_PREG_CUTOFF <- 100
  MILD_PREG_CUTOFF <- 110
  
  anemia_threshold_indices <- list(
    est_prev_sev = which(params$x <= SEVERE_PREG_CUTOFF),
    est_prev_modsev = which(params$x <= MODERATE_PREG_CUTOFF),
    est_prev_total = which(params$x <= MILD_PREG_CUTOFF)
  )
  
  pregnancy_index_vec <- which(!(is.na(dat$prop_pregnant)))
  
  for(n in names(anemia_threshold_indices)) {
    new_preg_col <- paste0(n, '_preg')
    dat[[new_preg_col]] <- 0
    dat[[new_preg_col]][pregnancy_index_vec] <- sapply(pregnancy_index_vec, \(i) {
      get_cdf(
        mean_val = dat$mean_hb_preg[i],
        variance = dat$stdev[i] ^ 2,
        weights = ensemble_weights,
        xmin = MIN_HB,
        xmax = MAX_HB,
        x = hb_vec
      ) |>
        get_prev(prev_name = n, indices = anemia_threshold_indices)
    }, simplify = TRUE)
  }
  
  return(dat)
}

process_pregnancy_data <- function(dat) {
  update_mean_hb(dat) |>
    get_pregnancy_anemia_prevalence() |>
    dplyr::mutate(
      prop_pregnant = dplyr::case_when(
        is.na(prop_pregnant) ~ 0,
        .default = prop_pregnant
      )
    ) |>
    data.table::setDT()
}

incorperate_preg_anemia_prevalence <- function(
    preg_pop_prop, non_preg_anemia_prev, preg_anemia_prev
) {
  non_preg_anemia_prev * (1 - preg_pop_prop) +
    preg_anemia_prev * preg_pop_prop
}

get_prop_values <- function(dat) {
  dat[['mean_hb']] <- dat$meanval
  
  dat[['prop_mild']] <- incorperate_preg_anemia_prevalence(
    preg_pop_prop = dat$prop_pregnant,
    non_preg_anemia_prev = dat$est_prev_total,
    preg_anemia_prev = dat$est_prev_total_preg
  ) - incorperate_preg_anemia_prevalence(
    preg_pop_prop = dat$prop_pregnant,
    non_preg_anemia_prev = dat$est_prev_modsev,
    preg_anemia_prev = dat$est_prev_modsev_preg
  )
  
  dat[['prop_mod']] <- incorperate_preg_anemia_prevalence(
    preg_pop_prop = dat$prop_pregnant,
    non_preg_anemia_prev = dat$est_prev_modsev,
    preg_anemia_prev = dat$est_prev_modsev_preg
  ) - incorperate_preg_anemia_prevalence(
    preg_pop_prop = dat$prop_pregnant,
    non_preg_anemia_prev = dat$est_prev_sev,
    preg_anemia_prev = dat$est_prev_sev_preg
  )
  
  dat[['prop_severe']] <- incorperate_preg_anemia_prevalence(
    preg_pop_prop = dat$prop_pregnant,
    non_preg_anemia_prev = dat$est_prev_sev,
    preg_anemia_prev = dat$est_prev_sev_preg
  )
  
  dat[['prop_total']] <- incorperate_preg_anemia_prevalence(
    preg_pop_prop = dat$prop_pregnant,
    non_preg_anemia_prev = dat$est_prev_total,
    preg_anemia_prev = dat$est_prev_total_preg
  )
  
  dat[['prop_mod_sev']] <- incorperate_preg_anemia_prevalence(
    preg_pop_prop = dat$prop_pregnant,
    non_preg_anemia_prev = dat$est_prev_modsev,
    preg_anemia_prev = dat$est_prev_modsev_preg
  )
  
  return(dat)
}

# apply pregnancy shifts --------------------------------------------------

dat <- fst::read.fst(path = sd_file, as.data.table = TRUE) |>
  data.table::merge.data.table(
    y = preg_pop_dat,
    by = c('age_group_id', 'sex_id', 'year_id', 'location_id'),
    all.x = TRUE
  ) |>
  process_pregnancy_data() |>
  get_prop_values()

# save out data -----------------------------------------------------------

fst::write.fst(
  x = dat,
  path = sd_file
)
