#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Vaccinations - prep model results for save_results (covariates)
#          For all vaccines, age is currently 22, sex is currently 3
#          This code: duplicates estimates for age groups (2, 3, 4, 5), sex(1, 2)
#          For DPT3 and MCV1, ready to upload once duplicated.
#          For Hib3, PCV, ROTA:
#             - Hib straight replacement of DPT3
#             - Set introduction dates
#             - Duplicate out
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### set paths
setwd(paste0(j, "FILEPATH"))
source("init.r")
code.root  <- paste0(unlist(strsplit(getwd(), "hsa"))[1], "FILEPATH")
paths.file <- paste0(code.root, "/paths.csv"); paths <- fread(paths.file)
source(paths[obj=="ubcov_tools", 2, with=FALSE] %>% gsub("J:/", j, .) %>% unlist)
path_loader(paths.file)
run_log    <- paste0(code_root, "FILEPATH/vaccination_run_log.csv")
source(paste0(j, "FILEPATH/cluster_tools.r"))

### load functions
source(db_tools)
source(paste0(j, "FILEPATH/get_population.R"))
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------
### get the best run_id from vaccination run log
best.run_id <- function(me) {
  
  df <- fread(run_log)
  run_id <- df[me_name==me & is_best==1]$run_id
  if (length(run_id) > 1) stop("More than 1 run_id")
  return(run_id)
  
}

### read in draws
read.draws <- function(run_id) {
  
  path <- paste0("FILEPATH")
  files <- list.files(path, full.names=TRUE)
  if (length(files)==0) stop("No draws")
  df <- mclapply(files, fread, mc.cores=10) %>% rbindlist(., use.names=TRUE)
  key <- c("location_id", "year_id", "age_group_id", "sex_id")
  setkeyv(df, cols=key)
  df <- unique(df)
  return(df)
  
}

### duplicate draws
duplicate.draws <- function(df, age_group_ids=c(2:20, 30:32, 235), sex_ids=c(1, 2)) { 
  
  demo <- expand.grid(age_group_id=age_group_ids, sex_id=sex_ids) %>% data.table
  df.i <- df %>% copy
  ## Duplicate
  df <- lapply(1:nrow(demo), function(x) {
    age <- demo[x]$age_group_id
    sex <- demo[x]$sex_id
    cf <- df.i[, `:=` (age_group_id=age, sex_id=sex)] %>% copy
    return(cf)
  }) %>% rbindlist
  return(df)
  
}

### apply introduction dates
set.intro <- function(df, me) {
  
  ## Vaccine Introduction
  vacc.intro <- data.table(readRDS(paste0(data_root, "FILEPATH/vaccine_intro.rds")))[me_name==me]
  vacc.intro <- vacc.intro[, .(location_id, year_id, cv_intro_years)]
  df <- merge(df, vacc.intro, by=c('location_id', 'year_id'), all.x=TRUE)
  cols <- grep("draw_", names(df), value=TRUE)
  df <- df[cv_intro_years < 1, (cols) := 0]
  df$cv_intro_years <- NULL
  return(df)
  
}

## cap estimates to 99% coverage
cap.est <- function(df) {
  cols <- grep("draw_", names(df), value=TRUE)
  df <- df[, (cols) := lapply(.SD, function(x) ifelse(x > 1, 0.99, x)), .SDcols=cols]
  return(df)
}

### collapse draws into mean, lower, and upper
collapse.draws <- function(df) {
  
  cols <- grep("draw_", names(df), value=TRUE)
  df <- df[, gpr_mean := rowMeans(.SD), .SDcols=cols]
  df$gpr_lower <- apply(df[, (cols), with=F], 1, quantile, probs=0.025)
  df$gpr_upper <- apply(df[, (cols), with=F], 1, quantile, probs=0.975)
  df <- df[, -(cols), with=F]
  return(df)
  
}

### save draws to /share/ folder
save.draws <- function(df, me, root) {
  
  path <- paste0(root, "/", me)
  unlink(path, recursive=TRUE)
  dir.create(path, showWarnings=FALSE)
  location_ids <- unique(df$location_id)
  mclapply(location_ids, function(x) {
    write.csv(df[location_id==x], paste0(path, "/", x, ".csv"), row.names=FALSE)
  }, mc.cores=10)
  
}

### save collapsed draws to best folder
save.collapsed <- function(df, me) {
  
  path         <- paste0(results.root, "/", me, ".rds")
  saveRDS(df, path)
  
}

### prep draws from the model output folder
prep.draws <- function(me, draws=FALSE, path=save.root, dupe=FALSE, quantiles=TRUE) {
  
  if (me %in% me_db$me_name) cov_id <- me_db[me_name==me, covariate_id]
  id <- best.run_id(me)
  if (me == "vacc_hib3") {
    df <- read.draws(best.run_id("vacc_dpt3"))
  } else if (me == "vacc_rotac") {
    df <- read.draws(best.run_id("vacc_rota1"))
  } else {
    df <- read.draws(id)
  }
  df <- cap.est(df)
  df <- df[, run_id := id]
  if (me %in% c("vacc_hib3", "vacc_pcv3", "vacc_rotac")) df <- set.intro(df, me)
  if (grepl("ratio", me)) df <- set.intro(df, me)
  if (draws) {
    if (dupe) df2 <- duplicate.draws(df) else df2 <- copy(df)
    if (me %in% me_db$me_name) df2[, covariate_id := cov_id]
    save.draws(df2, me, path)
    print(paste0("Saved draws of ", me, " under run_id ", id))
  } 
  if (quantiles){
    df <- collapse.draws(df)
    df <- df[, me_name := me]
    if (me %in% me_db$me_name) df[, covariate_id := cov_id]
    save.collapsed(df, me)
    print(paste0("Saved collapsed ", me, " under run_id ", id))
  }
  
}

### prep draws/summary for mes calculated as ratios
prep.ratio <- function(me, head, draws=FALSE, path=save.root, dupe=FALSE, flip=FALSE, quantiles=TRUE) {
  
  # setup and load
  key <- c("location_id", "year_id", "age_group_id", "sex_id")
  mes <- unlist(strsplit(me, "_"))[2:3]
  num <- paste0(head, mes[1])
  denom <- paste0(head, mes[2])
  # flip if denom / num instead of num / denom (i.e. DTP1)
  if (flip) {
    num <- paste0(head, mes[2])
    denom <- paste0(head, mes[1])
  } 
  id.ratio <- best.run_id(me)
  id.denom <- best.run_id(denom)
  df.ratio <- read.draws(id.ratio)
  df.ratio <- set.intro(df.ratio, me)
  df.denom <- read.draws(id.denom)
  df.ratio <- cap.est(df.ratio)
  df.denom <- cap.est(df.denom)
  # save draws for future use
  if (draws) { 
    save.draws(df.ratio, me, path)
    print(paste0("Saved ratio draws of ", me, " under run_id ", id.ratio))
  }
  # reshape long, merge
  df.ratio <- melt(df.ratio, id.vars=key, measure=patterns("^draw"), variable.name="draw",  value.name="ratio")
  df.denom <- melt(df.denom, id.vars=key, measure=patterns("^draw"), variable.name="draw", value.name="denom")
  df <- merge(df.ratio, df.denom, by=c(key, "draw"))
  # multiply out ratio
  if (flip) df <- df[, ratio := 1 / ratio]
  df <- df[, est := ratio * denom]
  df <- df[, c("ratio", "denom") := NULL]
  df <- df[est >= 1, est := 0.999]
  # first get covariate id
  if (num %in% me_db$me_name) cov_id <- me_db[me_name==num, covariate_id]
  # if by draws, reshape
  if (draws) {
    df2 <- dcast.data.table(df, location_id + year_id + age_group_id + sex_id ~ draw, value.var="est")
    df2 <- df2[, measure_id := 18]
    if (num %in% me_db$me_name) df2[, covariate_id := cov_id]
    if (dupe) {
      df2 <- duplicate.draws(df2)
    }
    save.draws(df2, num, path)
    print(paste0("Saved draws of ", num, " under run_id ", id.ratio))
  } 
  if (quantiles) {
    # calculate and save summaries
    df <- df[, gpr_mean := mean(est), by=key]
    df <- df[, gpr_lower := quantile(est, 0.025), by=key]
    df <- df[, gpr_upper := quantile(est, 0.975), by=key]
    df <- df[, c(key, "gpr_mean", "gpr_lower", "gpr_upper"), with=FALSE] %>% unique
    ## Save Collapsed
    df <- df[, me_name := num]
    df <- df[, measure_id := 18]
    if (num %in% me_db$me_name) df[, covariate_id := cov_id]
    save.collapsed(df, num)
    print(paste0("Saved collapsed ", num, " under run_id ", id.ratio))
  }
  
}

### aggregate estimates by super region
aggregate_estimates <- function(df, vars, by="super_region_name") {
  
  key <- c("year_id", by)
  agg <- df %>% copy
  # aggregate estimates to the parent_id [ sum(var * pop) /sum_pop ]
  agg <- agg[, sum_pop := sum(population), by=key]
  agg <- agg[, (vars) := lapply(.SD, function(x) x * agg[['population']]), .SDcols=vars]
  agg <- agg[, (vars) := lapply(.SD, sum), .SDcols=vars, by=key]
  # de-duplicate so get one set of estimates
  agg <- unique(agg[, c(by, "year_id", "age_group_id", "sex_id", "sum_pop", vars), with=F])
  # divide by sum_pop
  agg <- agg[, (vars) := lapply(.SD, function(x) x / agg[['sum_pop']]), .SDcols=vars]
  agg <- agg[, sum_pop := NULL]
  # rename parent_id -> location_id
  return(agg)
  
}

### lagged vaccine coverage
make_lags <- function(me, lag_years) {
  
  new <- paste0(me, "_lag_", lag_years)
  clean_data <- readRDS(file.path(results.root, paste0(me, ".rds")))
  square <- CJ(location_id=unique(clean_data$location_id), year_id=year_start:year_end, age_group_id=22, sex_id=3)
  data_lag <- merge(square, copy(clean_data)[, year_id := year_id + lag_years], by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE)
  data_lag[is.na(gpr_mean), c("gpr_mean", "gpr_lower", "gpr_upper") := 0]
  data_lag[, me_name := new]
  data_lag[, measure_id := clean_data$measure_id %>% unique]
  save.collapsed(data_lag, new)
  print(paste0("Summaries saved for ", new))
  
}

### cohort effect
make_cohort <- function(me) {
  
  # prep
  ages <- c(2:20, 30:32, 235)
  clean_data <- readRDS(file.path(results.root, paste0(me, ".rds")))
  square <- CJ(location_id=unique(clean_data$location_id), year_id=year_start:year_end, age_group_id=ages, sex_id=3)
  data <- merge(square, clean_data[, age_group_id := 5], by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE)
  
  # cohort lag
  for (i in 2:length(ages[ages >= 5])) {
    for (yr in (as.numeric(year_start) + 5):year_end) {
      data[age_group_id==ages[ages >= 5][i] & year_id==yr, gpr_mean := data[age_group_id==ages[ages >= 5][i - 1] & year_id==(yr - 5), gpr_mean] ]
      data[age_group_id==ages[ages >= 5][i] & year_id==yr, gpr_lower := data[age_group_id==ages[ages >= 5][i - 1] & year_id==(yr - 5), gpr_lower] ]
      data[age_group_id==ages[ages >= 5][i] & year_id==yr, gpr_upper := data[age_group_id==ages[ages >= 5][i - 1] & year_id==(yr - 5), gpr_upper] ]
    }
  }
  data[is.na(gpr_mean), gpr_mean := 0]
  data[is.na(gpr_lower), gpr_lower := 0]
  data[is.na(gpr_upper), gpr_upper := 0]
  
  # save
  data[, me_name := paste0(me, "_cohort")]
  data[, measure_id := clean_data$measure_id %>% unique]
  save.collapsed(data, paste0(me, "_cohort"))
  print(paste0("Summaries saved for ", paste0(me, "_cohort")))
  
}
#***********************************************************************************************************************