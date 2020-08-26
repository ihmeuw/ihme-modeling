library(data.table)
library(magrittr)
library(rhdf5)
library(lme4)

logit <- function(x) {
  return(log(x/(1-x)))
}

inv.logit <- function(x) {
  return(exp(x)/(exp(x)+1))
}

string_split = function(x){
  return(unlist(strsplit(x, split =',')))
}

h5read.py = function(h5File, name) {
  
  listing = h5ls(h5File)
  
  if (!(name %in% listing$name)) stop(paste0(name, " not in HDF5 file"))
  
  # only take requested group (df) name
  listing = listing[listing$group==paste0('/',name),]
  
  # Find all data nodes, values are stored in *_values and corresponding column
  # titles in *_items
  data_nodes = grep("_values", listing$name)
  name_nodes = grep("_items", listing$name)
  
  data_paths = paste(listing$group[data_nodes], listing$name[data_nodes], sep = "/")
  name_paths = paste(listing$group[name_nodes], listing$name[name_nodes], sep = "/")
  
  columns = list()
  for (idx in seq(data_paths)) {
    data <- data.frame(t(h5read(h5File, data_paths[idx])))
    names <- t(h5read(h5File, name_paths[idx]))
    entry <- data.frame(data)
    colnames(entry) <- names
    columns <- append(columns, entry)
  }
  
  data <- data.frame(columns)
  
  return(data)
}

model_save <- function(df, run_id, obj, holdout=1) {
  H5close()
  path <- model_path(run_id, obj, holdout)
  h5write(df, path, obj)
  print(paste("Saved", obj, "to", path, sep=" "))
}

prep.df <- function(run_root, holdout_num, gbd_round_id) {

  #load prepped data
  path <-  sprintf('%s/prepped.csv', run_root)    
  df <-  fread(path)
  
  #wipe data for given ko (ko_0 has all data)
  if (holdout_num > 0){
    ko <- sprintf('ko_%i', holdout_num)
    df[get(ko) == 0, data:=NA]
  }
  
  #add standard locations weights
  poppath = sprintf('%s/square.h5', run_root)
  df = weight_subnat_standard_locations(df, poppath, gbd_round_id)
  
  return(df)
}

weight_subnat_standard_locations <- function(df, population_path, gbd_round_id, national_level = 3){
  if (gbd_round_id < 6) {
    df[ , wt := ifelse(standard_location == 1, 1, 0)]
  } else {
    pops <- h5read.py(population_path, 'populations') %>% as.data.table

    ids <- c('location_id', 'year_id', 'age_group_id', 'sex_id')
    lvl_cols <- grep('location_id|level', names(df), value = 1)
    wts <- merge(pops[, c(ids, 'population'), with = F], 
                 unique(df[, c(lvl_cols, 'standard_location'), with = F]), by = 'location_id', all.x = T)
    
    #identify subnational standard locations, and weight according to percentage of parent population
    sl_subnats <- unique(wts[standard_location == 1 & level > national_level, 
                      .(level_3, location_id, year_id, age_group_id, sex_id, population)])
    sl_nats <- unique(wts[location_id %in% unique(sl_subnats$level_3), 
                  .(level_3, year_id, age_group_id, sex_id, population)])
    setnames(sl_nats, 'population', 'parent_pop')
    
    #create weights for subnational standard locatiosn based on their proportion of the population
    wts <- merge(sl_subnats, sl_nats, by = c('level_3', 'year_id', 'age_group_id', 'sex_id'), all.x =T)
    wts[, wt:=population/parent_pop, by = ids]
    
    sum_to_one <- all(round(wts[, sum(wt), by = c('level_3', 'year_id', 'age_group_id', 'sex_id')][, V1], 2) == 1)
    if (sum_to_one == F){
      stop('Central issue! There is an problem with standard location weighting in your linear model!')
    }else{
      df <- merge(df, wts[, c(ids, 'wt'), with = F], by = ids, all.x = T)
      df[standard_location == 1 & is.na(wt), wt:=1]
      df[standard_location == 0, wt:=0]
    }
  }
  
  return(df)

}

#' Helper function for run.lm: subsets the data to run
#' a sex-specific (if specified) linear regression.
#' Main purpose is to throw more meaningful errors
#' if there is an issue at this point.
#'
#' @param df modeler's ST-GPR model data
#' @param standard_locations_only TRUE to subset the df to
#'     just standard locations. Required (?) from GBD 2019 and on
#' @param sex sex_id, if not 3, the data will be subset to this sex as well
#' 
#' @return subsetted data ready for lm(er)
subset.data.lm <- function(df, standard_locations_only, sex) {
  subset_df <- copy(df)
  if (standard_locations_only) {
    subset_df <- subset_df[standard_location == 1]
    
    if (nrow(subset_df) == 0) {
      stop(paste("Uh oh, your data doesn't include any standard locations!",
                 "This is currently required for stage 1. If this is a problem,",
                 "either upload a custom stage 1 or submit a help ticket."))
    }
  }
  
  if (sex != 3) {
    subset_df <- subset_df[sex_id == sex]
    
    if (nrow(subset_df) == 0) {
      stop(paste("After subsetting for sex_id", sex, "you don't have any data.",
                 "Please submit a ticket!"))
    }
  }
  
  return (subset_df[])
}

#' Returns a list of strings that are the random effects
#' in a random effect model specification
#'
#' @param model a lmer model formula with random effects
#' @example "data ~ sdi + (1|level_1/level_2/level_3)" -> "level_1" "level_2" "level_3"
#' @return list of random effects as strings
get_random_effect_covars <- function(model) {
  # strsplit isn't part of the pipe train bc it doesn't play nice
  covars <- gsub(".+~", "", model)                # eliminate 'data ~' part
  covars <- strsplit(covars, " *\\+ *")[[1]] %>%  # split into individual components
    grep("[|]", ., value = T) %>%                 # only take the random effects ones
    gsub("^\\(|\\)$", "", .)                      # drop leading '(' and trailing ')'
    
  covars <- strsplit(covars, "\\|+") %>% unlist()  # split up random slopes vs random intercepts
  if (is.null(covars)) {                           # avoid strsplit errors
    return(vector(mode = "character"))
  }
  
  covars <- strsplit(covars, "/") %>% unlist()    # split up nested REs
  if (is.null(covars)) {                          # avoid strsplit errors
    return(vector(mode = "character"))
  }
  
  covars <- gsub("[A-z0-9_\\.]*\\(|\\)", "", covars) %>% # drop function calls like log(), as.factor()
    gsub(" ", "", .) %>%                                 # drop spaces
    grep("^[\\d\\.]+$", ., value = T,                    # Drop numbers (inc decimals)
         invert = T, perl = T) %>%                       # like the '1' in (1|level_) or do math
    unique()

  return(covars)
}

#' Returns a list of strings that are the fixed effects/covariates
#' in a model specification
#'
#' @param model a lmer model formula
#' @example "data ~ sdi + (1|level_1/level_2/level_3)" -> "sdi"
#' @return list of fixed effects as strings
get_fixed_effect_covars <- function(model) {
  # strsplit isn't part of the pipe train bc it doesn't play nice
  covars <- gsub(".+~", "", model)                # eliminate 'data ~' part
  covars <- strsplit(covars, " *\\+ *")[[1]] %>%  # split into individual components
    grep("[|]", ., value = T, invert = T) %>%     # drop the random effects
    gsub("[A-z0-9_\\.]*\\(|\\)", "", .)           # drop function calls like log(), as.factor()
    
  covars <- strsplit(covars, " *[-/\\*] *") %>%   # split any math, ie cov/100
    unlist() %>% 
    gsub(" ", "", .) %>%                          # drop spaces
    grep("^[\\d\\.]+$", ., value = T,             # Drop numbers (inc decimals)
         invert = T, perl = T)                    # as sometimes ppl put '1' or do math

  return(covars)
}


#' Linear modeling functions lm and lmer (mostly lmer)
#' have little to no top-level validations, so if there
#' is an issue with data or the model formula, the errors
#' thrown are deep and very cryptic. This function seeks
#' to catch known errors before running the model and give
#' more informative errors
#'
#' @param df the square data frame to be run in the linear model
#' @param model a lmer model formula with random effects
#' @param the function to for linear modeling: lm, lmer
validate_lm <- function(df, model, func) {
  ## Confirm we have, like, at least SOME non-0 weights
  if (length(unique(df$wt)) == 1 && any(unique(df$wt) == 0)) {
    stop(paste0("Before running ", func, ", we've found that all of ",
                "your data has been down-weighted to 0. Something ",
                "has gone wrong! Please submit a ticket."))
  }
  
  ## Check that the covariates, fixed effects, random effects exist in the df
  effects <- c(get_fixed_effect_covars(model), get_random_effect_covars(model))
  missing <- c()
  for (effect in effects) {
    if (!(effect %in% colnames(df))) {
      missing <- c(missing, effect)
    }
  }
  
  if (length(missing) > 0) {
    stop(paste0("Your stage 1 formula contains the following covariates, fixed effects, ",
                "and/or random effects that are NOT in your data: ", paste0(missing, collapse = ", "),
                "\n\nFormula: ", model, "\nDid you misspell something?"))
  }
  
  
  ## Check grouping factors must have > 1 sampled level error
  ## aka a random effect has only 1 level, which is impossible to fit
  if (func == "lmer") {
    covariates <- get_random_effect_covars(model)
    has_data_df <- df[!is.na(data)]
    issues <- c()
    for (cov in covariates) {
      if (length(unique(has_data_df[[cov]])) == 1) {
        issues <- c(issues, paste0(cov, " (", unique(has_data_df[[cov]]), ")"))
      }
    }
    
    if (length(issues) > 0) {
      issues <- paste0(issues, collapse = ", ")
      sex_id <- unique(has_data_df$sex_id)
      if (length(sex_id) > 1) {
        sex_id <- 3
      }
      stop(paste0("An issue in your data has been caught before running your sex id ", 
                  sex_id, " ", func, " model. The following random effects specified ",
                  "in your model formula only have a single value (in parentheses), which is not allowed for ",
                  "grouping factors: ", issues, ". You must either add data in other groups ",
                  "or drop these random effects.\n\nYour model formula: ", model,
                  "\n\nlme4::lmer says -> Error: grouping factors must have > 1 sampled level"))
    }
  }
}

fit.lm <- function(df, model) {
  ## Detect function
  func <- ifelse(grepl("[|]", model), "lmer", "lm")
  
  ## Catch some lm/lmer errors before running the model
  ## and give a better error message
  validate_lm(df, model, func)
  
  ## Run fit data
  if (func == "lmer") mod <- lmer(as.formula(model), weights = wt, data = df, na.action = na.omit)
  if (func == "lm") mod <- lm(as.formula(model), weights = wt, data = df, na.action = na.omit)
  ## Store summary
  Vcov <- vcov(mod, useScale = FALSE) 
  if (func=="lmer") betas <- fixef(mod) 
  if (func=="lm") betas <- coefficients(mod) 
  se <- sqrt(diag(Vcov))
  zval <- betas / se 
  pval <- 2 * pnorm(abs(zval), lower.tail = FALSE) 
  sum <- cbind(model=model, betas, se, zval, pval)
  return(list(mod=mod, sum=sum))
}

pred.lm <- function(df, model, predict_re=0) {
  ## RE form
  if (predict_re==1) re.form=NULL else re.form=NA
  ## Predict
  if (class(model) == "lmerMod") {
    prior <- predict(model, newdata=df, allow.new.levels=T, re.form=re.form)
  } else {
    prior <- predict(model, newdata=df)
  }
  return(prior)
}

run.lm <- function(df, prior_model, predict_re, study_covs = NULL, standard_locations_only = TRUE, holdout_num) {
  ## If sex_id in prior model, run once with sex dummies. Otherwise, run by sex
  sex.run <- if (grepl("sex_id", prior_model)) {
    3 
  } else {
    unique(df$sex_id)
  }

  ## Run
  sum.out <- NULL
  for (sex in sex.run) {
    subset_df <- subset.data.lm(df, standard_locations_only, sex)

    ## Model
    mod.out <- fit.lm(subset_df, prior_model)
    mod <- mod.out$mod
    sum <- mod.out$sum
    names <- rownames(sum)
    sum <- data.table(sum)
    sum <- cbind(covariate=names, sum)
    sum <- sum[, sex_id := sex]
    sum.out <- rbind(sum.out, sum)
    
    ## Predict (subset on sex if necessary)
    if (sex != 3) {
      df <- df[sex_id == sex, stage1 := pred.lm(df[sex_id == sex], mod, predict_re)]
    } else {
      df <- df[, stage1 := pred.lm(df, mod, predict_re)]
    }
    
  }
  ## Save model summary
  if (holdout_num == 0) {
    write.csv(sum.out, paste0(run_root, "/stage1_summary.csv"), na = "", row.names = FALSE)
  }

  return(df)
}

clean.prior <- function(df) {
  cols <- c("location_id", "year_id", "sex_id", "age_group_id", "location_id_count", "stage1")
  return(df[, cols, with=F] %>% unique)
}

count.country.years <- function(df, location, year, age, sex, dt, lvl){
  
  #identify level we're looking at based on location, and restrict inputs to those above that level if a "level_n" count
  if(grepl("level", location)){
    lev <- str_split_fixed(location, "_", 2)[, 2] %>% as.numeric()
    counts <- df[!is.na(get(location)) & !is.na(get(dt)) & get(lvl) <= max(lev, 3), c(location, year, age, sex), with = F] %>% unique
  } else{
    counts <- df[!is.na(get(location)) & !is.na(get(dt)), c(location, year, age, sex), with = F] %>% unique
  }
  
  #save an original copy with all location/year/age/sex combos for later
  cj <- copy(df[!is.na(get(location)), c(location, sex), with = F]) %>% unique 
  
  #get # of data points by country and strat_group
  counts <- counts[, .N, by = c(location, age, sex)]
  col <- paste0(location, "_count")
  
  #keep max for each country/strat - only do if an age or sex specific model
  counts <- counts[, paste0(col):=max(N), by = c(location, sex)] 
  
  #grab missing locs (ie had zero data)
  counts <- counts[, c(location, sex, col), with = F] %>% unique
  counts <- merge(counts, cj, by = c(location, sex), all = T)
  #make sure missing locations aren't left out
  counts[, (col):=ifelse(is.na(get(col)), 0, get(col))]
  
  df <- merge(df, counts, by = c(location, sex), all.x = T)
  
  return(df)
}
