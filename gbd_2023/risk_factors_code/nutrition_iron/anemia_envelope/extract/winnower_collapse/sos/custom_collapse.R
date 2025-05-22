
# load in libraries -------------------------------------------------------

packages <- c("data.table", "DBI", "RMySQL", "survey", "binom", "parallel", "dplyr", 'Hmisc')
lapply(packages, function(x) library(x, character.only = TRUE))

# load in command line args -----------------------------------------------

if(interactive()) {
  out_dir <- 'FILEPATH'
  map_file_name <- file.path(getwd(), 'extract/winnower_collapse/sos/param_maps/small_file_list.csv')
  config_file_name <- file.path(getwd(), 'extract/winnower_collapse/config.csv')
  out_dir <- 'FILEPATH'
  topic_name <- 'anemia_winnower_small'
  
  task_id <- 134
} else {
  cli_args <- commandArgs(trailingOnly = TRUE)
  map_file_name <- cli_args[1]
  topic_name <- cli_args[2]
  config_file_name <- cli_args[3]
  out_dir <- cli_args[4]
  
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  message(task_id)
  if(task_id != 1) Sys.sleep(log(task_id) + 5) # allow for directories to be cleaned
}

param_file_map <- read.csv(map_file_name)
config <- read.csv(config_file_name)

out_dir <- file.path(
  out_dir,
  topic_name
)

if(task_id == 1){
  if(dir.exists(out_dir)) unlink(out_dir, recursive = TRUE)
  dir.create(out_dir)
} 

loc_df <- ihme::get_location_metadata(location_set_id = 22, release_id = 16)
ihme_loc_vec <- append(loc_df$ihme_loc_id, 'ETH_44858')

# load core functions -----------------------------------------------------

setup.design <- function(df, var) {
  options(survey.lonely.psu = 'adjust')
  options(survey.adjust.domain.lonely = TRUE)
  ## Check for survey design vars
  
  check_list <- c("strata", "psu", "pweight")
  for (i in check_list) {
    ## Assign to *_formula the variable if it exists and nonmissing, else NULL
    assign(paste0(i, "_formula"),
           ifelse(i %in% names(df) & nrow(df[!is.na(i)]) > 0, paste("~", i), NULL) %>% as.formula
    )
  }
  ## Set svydesign
  return(svydesign(id = psu_formula, weight = pweight_formula, strat = strata_formula, data = df[!is.na(var)], nest = TRUE))
}

##-------------------------------------------------------------------------
reweight_for_subnat_collapse <- function(df, by_vars){
  ## Check for cases where subnational weights present
  ## If present, set pweight equal to those weights for collapse
  weights_below_admin0 <- grep("pweight_admin_.", names(df), value=TRUE)
  cols_na <- sapply(df, function(x)all(is.na(x)))
  na_cols <- names(df)[cols_na]
  non_na_admin_wts <- weights_below_admin0[! weights_below_admin0 %in% na_cols]
  if (length(non_na_admin_wts) > 0){
    ## regex the column whose weights represent that level
    admin_ids <- grep(by_vars, pattern='admin_.*id', value=T)
    admins_w_subnat_info <- substring(admin_ids, 1, 7)
    correct_pweight_for_collapse <- paste0('pweight_', admins_w_subnat_info)
    ## set pweight to that weight
    ## Produce subnationally weighted estimates when data allows
    if (correct_pweight_for_collapse != 'pweight_'){
      df[, pweight := get(correct_pweight_for_collapse)]
    }
  }
  return(df)
}


## Core function to collapse dataframe
collapse.by <- function(df, var, by_vars, calc.sd = FALSE) {
  ## Subset dataframe to where not missing variable or survey design variables
  df.c <- df[!is.na(get(var)) & !is.na(strata) & !is.na(psu) & !is.na(pweight)] %>% copy
  
  ## Setup design for national aggregates and cases where subnational estimates use national weights
  design <- setup.design(df, var)
  ## Setup by the by call as a formula
  by_formula <- as.formula(paste0("~", paste(by_vars, collapse = "+")))
  ## Calculate number of observations, number of clusters, strata
  meta <- df[, list(sample_size = length(which(!is.na(get(var)))),
                    nclust = length(unique(psu)),
                    nstrata= length(unique(strata)),
                    var = var
  ), by = by_vars]
  ## Calculate mean and standard error by by_vars.  Design effect is dependent on the scaling of the sampling weights
  est <- svyby(
    ~get(var), 
    by_formula, 
    svymean, 
    design = design, 
    deff = "replace", 
    na.rm = TRUE, 
    drop.empty.groups = TRUE, 
    keep.names = FALSE, 
    multicore=TRUE
  ) %>% data.table
  old <- c("get(var)", "DEff.get(var)", "se")
  new <- c("mean", "design_effect", "standard_error")
  setnames(est, old, new)
  ## Merge
  out <- merge(meta, est, by=by_vars)
  
  ## Calculate standard deviation by by_vars(s), Design effect is dependent on the scaling of the sampling weights
  if (calc.sd) {
    stdev <- svyby(
      ~get(var), 
      by_formula, 
      svyvar, 
      design = design, 
      deff = "replace", 
      na.rm = TRUE, 
      drop.empty.groups = TRUE, 
      keep.names = FALSE, 
      multicore=TRUE
    ) %>% data.table
    setnames(stdev, "get(var)", "variance")
    stdev <- stdev[, standard_deviation := sqrt(variance)]
    stdev <- stdev[, standard_deviation_se := 1 / (2*standard_deviation) * se]
    stdev <- stdev[, c("se", "variance") := NULL]
    ## Merge
    out <- merge(out, stdev, by=by_vars)
  }
  return(out)
}

# process age group ids ---------------------------------------------------

update_age_group_ids <- function(dat, age_df) {
  
  dat$age_group_id <- NA_integer_
  for(r in seq_len(nrow(age_df))){
    i_vec <- which(dat$age_year >= age_df$age_group_years_start[r] &
                     dat$age_year < age_df$age_group_years_end[r])
    dat$age_group_id[i_vec] <- age_df$age_group_id[r]
  }
  
  return(dat)
}

# add necessary cols ------------------------------------------------------

add_syv_cols <- function(dat) {
  if('hhweight' %in% colnames(dat) && !('pweight' %in% colnames(dat))) {
    setnames(
      x = dat, old = 'hhweight', new = 'pweight'
    )
  } else if (!(any(c('hhweight', 'pweight') %in% colnames(dat)))) {
    dat$pweight <- 1
  }
  if(any(is.na(dat$pweight))) {
    i_vec <- which(is.na(dat$pweight))
    dat$pweight[i_vec] <- median(dat$pweight, na.rm = TRUE)
  }
  
  if(!('psu' %in% colnames(dat))) {
    pweight_df <- data.frame(
      pweight = unique(dat$pweight)
    ) |> 
      dplyr::mutate(psu = seq_len(dplyr::n()))
    dat <- merge.data.frame(
      x = dat,
      y = pweight_df,
      by = 'pweight'
    ) |>
      setDT()
  }
  if(any(is.na(dat$psu))) {
    new_psu <- max(dat$psu, na.rm = TRUE) + 1
    i_vec <- which(is.na(dat$psu))
    dat$psu[i_vec] <- new_psu
  }
  if(!('strata' %in% colnames(dat))) dat$strata <- 1
  if(any(is.na(dat$strata))) {
    new_strata <- max(dat$strata, na.rm = TRUE) + 1
    i_vec <- which(is.na(dat$strata))
    dat$strata[i_vec] <- new_strata
  }
  
  return(dat)
}

clean_ihme_loc_id <- function(vec){
  vec <- stringr::str_remove_all(string = vec, pattern = "\\n")
  vec <- stringr::str_remove_all(string = vec, pattern = "\\r")
  vec <- stringr::str_remove_all(string = vec, pattern = "\\t")
  return(vec)
}

# split up collapse file --------------------------------------------------

collapse_microdata <- function(collapse_topic, config_map, file_name, out_dir) {
  config_map <- config_map |> dplyr::filter(topic == collapse_topic)
  age_df <- fread(file.path(getwd(), 'extract/winnower_collapse/sos/param_maps/age_map.csv'))
  dat <- haven::read_dta(file = file_name) |> 
    data.table::setDT() |>
    update_age_group_ids(age_df = age_df) |> 
    add_syv_cols()
  
  vars_to_aggregate <- config_map |>
    purrr::chuck('vars') |>
    stringr::str_split(pattern = ',', simplify = TRUE)
  
  collapse_vars <- config_map |>
    purrr::chuck('cv.manual') |>
    stringr::str_split(pattern = ',', simplify = TRUE) |>
    append(c('age_group_id', 'sex_id'))
  
  df_list <- list(dat)
  admin_cols <- c('admin_1_id', 'admin_1_urban_id', 'admin_2_id')
  for(c in admin_cols) {
    if(c %in% colnames(dat) && all(clean_ihme_loc_id(unique(dat[[c]])) %in% ihme_loc_vec)) {
      df_list <- append(
        df_list,
        lapply(unique(dat[[c]]), \(a) {
          if(is.na(a)) {
            i_vec <- which(is.na(dat[[c]]))
            dat |>
              dplyr::slice(i_vec)
          } else {
            i_vec <- which(dat[[c]] == a)
            dat |>
              dplyr::slice(i_vec) |>
              dplyr::mutate(ihme_loc_id = a)
          }
        })
      )
    }
  }
  
  if(interactive()) assign('precollapse_list', df_list, envir = .GlobalEnv)
  
  collapse_dat <- lapply(df_list, \(d) {
    lapply(vars_to_aggregate, \(v) {
      if(v %in% colnames(d)) {
        collapse.by(
          df = d,
          var = v,
          by_vars = collapse_vars
        )
      } else {
        data.table()
      }
    }) |> rbindlist(use.names = TRUE, fill = TRUE) |>
      dplyr::mutate(
        nid = unique(d$nid),
        ihme_loc_id = unique(d$ihme_loc_id),
        survey_name = unique(d$survey_name),
        year_start = unique(d$year_start),
        year_end = unique(d$year_end),
        survey_module = unique(d$survey_module),
        file_path = unique(d$file_path)
      ) |>
      merge.data.table(
        y = age_df[,.(age_group_id, age_group_years_start, age_group_years_end)],
        by = 'age_group_id'
      ) |>
      setnames(
        old = c('age_group_years_start', 'age_group_years_end'),
        new = c('age_start', 'age_end')
      )
  }) |> 
    rbindlist(use.names = TRUE, fill = TRUE) |>
    dplyr::filter(
      sample_size >= 5
    )
  
  if(interactive()) {
    assign('temp_dat', collapse_dat, envir = .GlobalEnv)
  } else {
    new_file_name <- file.path(
      out_dir,
      file_name |>
        fs::path_file() |>
        tools::file_path_sans_ext() |>
        paste0('.fst')
    )
    
    fst::write.fst(
      x = collapse_dat,
      path = new_file_name
    )
  }

}

# run function ------------------------------------------------------------

collapse_microdata(
  collapse_topic = topic_name,
  config_map = config,
  file_name = param_file_map$file_name[task_id],
  out_dir = out_dir
)

