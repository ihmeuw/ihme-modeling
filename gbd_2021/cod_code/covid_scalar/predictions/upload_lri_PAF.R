# prep LRI ratio for PAF upload

# submit script
source("FILEPATH")
# submit_job("FILEPATH")

rm(list=ls())
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}
pacman::p_load(data.table, openxlsx, pbapply, parallel)

username <- Sys.info()[["user"]]
#date <- gsub("-", "_", Sys.Date())
date <- "2022_08_22"
gbd_round_id <- 7
year_fill <- NULL # NULL or 2021; i.e., to forecast or not to forecast? 
subnational <- T
seas_date <- "2021_06_08"
cause_compare <- "flu"
# This generates both YLLs and YLDs

# etio <- c("flu", "rsv", "pneumo", "hib")
# Only adjust PAFs for Flu & RSV
etio <- c("flu", "rsv")

# Get FLU annual ratios
# Input model versions and filepaths
ratio_model_version <- "2021-07-08"
#ratio_path <- paste0("FILEPATH")
seas_path <- paste0(j, "FILEPATH")
n_ratio_path <- paste0("FILEPATH")
n_ratio <- fread(n_ratio_path)

# SOURCE SHARED FUNCTIONS ------------------------------
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# Function to duplicate selected national rows subnationally
# Duplicate annual ratios for the subnationals
expand_subnational <- function(dt, hierarchy){
  loc_estimate <- hierarchy[most_detailed == 1]
  l <- strsplit(as.character(loc_estimate$path_to_top_parent), ',')
  df1new <- data.frame(country_id = as.integer(unlist(l)), 
                       path_to_top_parent = rep(loc_estimate$path_to_top_parent, lengths(l)),
                       location_id = rep(loc_estimate$location_id, lengths(l)))
  dt <- merge(dt, df1new, by.x = "location_id", by.y = "country_id", all.x = TRUE, allow.cartesian = T)
  dt[, location_id := NULL]
  setnames(dt, "location_id.y", "location_id")
  return(dt)
}

get_monthly <- function(data, pred_seas, drawnames){
  # initialize empty list with length == nrow(dataframe)
  list_date_dfs <- vector(mode = "list", length = nrow(data))
  
  # for-loop generates new dates and adds as dataframe to list
  list_date_dfs <- pblapply(1:length(list_date_dfs), function(i){
    
    # transfer dataframe row to variable `row`
    row <- data[i,]
    
    # add tibble to list
    date_df <- data.table(row, month = 1:12)
    return(date_df)
  }
  , cl = 8)
  
  # bind dataframe list elements into single dataframe
  df_monthly <- rbindlist(list_date_dfs)
  
  # now merge in the seasonality, which was generated for all locs in predict_annual_cases 
  df_monthly <- merge(df_monthly, pred_seas, by=c("month", "location_id"))
  
  # multiply the mean by the seasonality and divide by 12 to get monthly
  df_monthly[, (drawnames) := (.SD*lat_ratio)/12, .SDcols = drawnames]
  return(df_monthly)
}

fill_monthly_ratio <- function(ratio_path, partial_year_fill = 2021){
  # Read in predicted monthly ratios (month will not be NA for rows with that month's official covid team mob estimate)
  pred_month_disrup_ratio <- fread(ratio_path)[!is.na(month)]
  pred_month_disrup_ratio[is.na(year), year := year(date)]
  setnames(pred_month_disrup_ratio, "year", "year_id")
  if(is.null(partial_year_fill)){
    # Keep only complete years
    count_check <- length(unique(pred_month_disrup_ratio$location_id))*12
    count_check <- pred_month_disrup_ratio[, .N, by = "year_id"][N == count_check]
    pred_month_disrup_ratio <- pred_month_disrup_ratio[year_id == count_check$year_id]
  } else {
    # Fill missing months with predicted zeros
    add_grid <- lapply(partial_year_fill, function(check_year){
      months_add <- setdiff(unique(pred_month_disrup_ratio$month), unique(pred_month_disrup_ratio[year_id==check_year]$month))
      if(length(months_add)!=0)add_grid <- expand.grid(year_id = check_year, month = months_add, location_id = unique(pred_month_disrup_ratio$location_id)) else add_grid <- data.table()
      return(add_grid)
    }
    )
    fill_list <- rbindlist(add_grid)
    fill_list[, paste0("ratio_draw_", 0:999) := 1]
    pred_month_disrup_ratio <- rbind(pred_month_disrup_ratio, fill_list, fill = T)
  }
  return(pred_month_disrup_ratio)
}

# Function to get the annual ratio by location, from seasonality and the monthly ratios
get_annual_ratio <- function(seas_dt, pred_month_disrup_ratio, hierarchy = hierarchy){
  # Merge seasonality with pred ratio and use wts to calc annual ratio, draw-wise
  drawnames <- names(pred_month_disrup_ratio)[names(pred_month_disrup_ratio) %like% "draw"]
  seas_dt <- merge(seas_dt, pred_month_disrup_ratio[, c(drawnames, "location_id", "month", "mask_avg_month", "mob_avg_month", "year_id"), with = F], 
                   by = c("location_id", "month"))
  annual_avg_ratio <- seas_dt[, lapply(.SD, weighted.mean, w = wt), .(location_id, year_id), .SDcols = drawnames]
  return(annual_avg_ratio)
}


# get location data
hierarchy <- get_location_metadata(35, gbd_round_id = 7, decomp_step = "iterative")
countries <- hierarchy[level == 3]
age_meta <- get_age_metadata(age_group_set_id = 19, gbd_round_id = 7)
locs <- hierarchy[most_detailed == 1]$location_id

# get annual ratios from monthly ratios
#monthly_ratio <- fill_monthly_ratio(ratio_path, partial_year_fill = year_fill)
seas_dt <- as.data.table(read.xlsx(seas_path))
#annual_ratio <- get_annual_ratio(seas_dt, monthly_ratio, hierarchy = hierarchy)
# Duplicate annual ratios for the subnationals
# if(subnational){
#   annual_ratio <- expand_subnational(annual_ratio, hierarchy)
#   ids <- c("location_id", "year_id", "path_to_top_parent")
# } else ids <- c("location_id", "year_id")
#drawnames <- names(annual_ratio)[names(annual_ratio) %like% "draw"]
# Reshape to long, get into correct format for draw manipulation
#annual_long <- melt(annual_ratio, id.vars = ids, variable.name = "draw",
#                    value.name = "ratio")
#annual_long$draw <- substr(annual_long$draw, 7, nchar(as.character(annual_long$draw)))

################################ New Ratio ##############################
n_ids <- c("cause_id", "location_id", "year_id", "sex_id", "age_group_id")
# Reshape to long, get into correct format for draw manipulation
n_annual_long <- melt(n_ratio, id.vars = n_ids, variable.name = "draw",
                    value.name = "ratio")
n_annual_long$draw <- substr(n_annual_long$draw, 7, nchar(as.character(n_annual_long$draw)))

#########################################################################

# Get the original PAF % draws
# Read from flat files to get all 1000 draws
paf_draws_list <- lapply(etio, function(e){
  directory <- paste0("FILEPATH")
  paf_draws_sublist <- pblapply(list.files(directory)[list.files(directory) %like% ".csv"], function(file){
    dt <- fread(paste0(directory, "/", file))
    # print(unique(dt$location_id))
    return(dt)
  }, cl = 2)
  return(paf_draws_sublist)
})
names(paf_draws_list) <- etio

# Reshape to long
id_vars <- c("location_id", "year_id", "sex_id", "age_group_id", "pathogen", "cause_id", "measure_id")
paf_draws_list_copy <- copy(paf_draws_list)
for(e in etio){
  dt_tmp <- rbindlist(paf_draws_list[[e]])
  dt_tmp <- dt_tmp [year_id %in% c(2020, 2021, 2022)]
  # Remove unneeded columns
  dt_tmp[, (setdiff(names(dt_tmp), c(id_vars, paste0("draw_", 0:999)))):= NULL]
  paf_draws_list[[e]] <- melt(dt_tmp , id.vars = id_vars, variable.name = "draw", 
                    value.name = "paf")
}

# Multiply these ratio draws by all measures in get_model_results
gbd_id_type <- "modelable_entity_id"
gbd_id <- 1258
source <- "epi"
measure_id <- 6
version_id <- 600542
  
lri_draws <- get_draws(gbd_id_type = gbd_id_type,
                       gbd_id = gbd_id,
                       source = source,
                       version_id = version_id,
                       release_id = 9,
                       measure_id = measure_id, # incidence
                       location_id = locs,
                       age_group_id = age_meta$age_group_id,
                       sex_id = c(1,2),
                       year_id = c(2020:2022), 
                       num_workers = 10)

# Reshape to long
lri_draws[, (c("metric_id", "model_version_id")):= NULL]
if("modelable_entity_id" %in% names(lri_draws)){
  lri_draws$cause_id <- 322
  lri_draws$modelable_entity_id <- NULL
}
lri_draws <- melt(lri_draws, id.vars = id_vars[id_vars !="pathogen"], variable.name = "draw", 
                   value.name = "lri_rate")

# Get the model results for adjusted LRI
gbd_id_type <- "modelable_entity_id"
gbd_id <- 26959
source <- "epi"
measure_id <- 6
version_id <- 736220 

adj_lri_draws <- get_draws(gbd_id_type = gbd_id_type,
                           gbd_id = gbd_id,
                           source = source,
                           version_id = version_id,
                           measure_id = measure_id, # incidence
                           release_id = 9,
                           location_id = locs,
                           age_group_id = age_meta$age_group_id,
                           sex_id = c(1,2),
                           year_id = c(2020:2022), 
                           num_workers = 10)

# Reshape to long
adj_lri_draws[, (c("metric_id", "model_version_id")):= NULL]
if("modelable_entity_id" %in% names(adj_lri_draws)){
  adj_lri_draws$cause_id <- 322
  adj_lri_draws$modelable_entity_id <- NULL
}
adj_lri_draws <- melt(adj_lri_draws, id.vars = id_vars[id_vars !="pathogen"], variable.name = "draw", 
                  value.name = "lri_rate")

setnames(adj_lri_draws, "lri_rate", "adj_lri_rate")

# Multiply paf by LRI draws to get [etiology] rate
merge_cols <- c("location_id", "year_id", "age_group_id", "sex_id", "draw", "cause_id") 
total_prop_dt_list <- list()
total_prop_dt_list <- lapply(etio, function(e){
  # get the unadjusted PAF
  # use only the YLDs for this part
  total_prop_dt <- merge(paf_draws_list[[e]][measure_id == 3], lri_draws, by = merge_cols)
  total_prop_dt[, etio_rate := paf*lri_rate]
  # get the adjusted PAF (only relevant for flu, RSV)
  # Multiply by FLU ratio
  total_prop_dt <- merge(total_prop_dt, n_annual_long, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw", "cause_id")) # new ratio
  if (e == "flu" | e == "rsv"){
    total_prop_dt[, adjusted_etio_rate := ratio*etio_rate]
  } else total_prop_dt[, adjusted_etio_rate := etio_rate]
  total_prop_dt <- merge(total_prop_dt, adj_lri_draws, by = merge_cols)
  # Final calculation to get PAF: For each etiology, take the New etio rate / New total
  total_prop_dt[, adjusted_paf := adjusted_etio_rate/adj_lri_rate]
  # Some draws are Inf when the adjusted LRI draw is 0
  # Set these Inf to NA for later
  total_prop_dt[is.infinite(adjusted_paf), adjusted_paf := NA]
  # Coerce draws > 1 to 1 
  total_prop_dt[adjusted_paf > 1, adjusted_paf := 1]
  total_prop_dt$measure_id <- NULL; total_prop_dt$measure_id.y <- NULL
  setnames(total_prop_dt, "measure_id.x", "measure_id")
  # get YLL pafs from these YLD pafs
  # if we assume the YLL/YLD ratio is constant:
  # new YLL% = new YLD% *(old YLL%/old YLD%)
  # gotta get the old YLL from paf_draws_list
  total_prop_fatal <- copy(total_prop_dt)
  total_prop_fatal <- merge(total_prop_fatal, paf_draws_list[[e]][measure_id == 4], by = c(merge_cols, "pathogen"))
  # do the calculation
  total_prop_fatal[, adjusted_paf := adjusted_paf * (paf.y/paf.x)]
  # delete nonfatal column
  total_prop_fatal$measure_id.x <- NULL; total_prop_fatal$paf.x <- NULL
  setnames(total_prop_fatal, c("measure_id.y", "paf.y"), c("measure_id", "paf"))
  #rbind
  total_prop_dt <- rbind(total_prop_dt, total_prop_fatal)
  return(total_prop_dt)
}) 
names(total_prop_dt_list) <- etio

# Write out 
# Get non-2020 years from paf_draws_list
for (e in etio){
  lri_draws_edited <- total_prop_dt_list[[e]]
  lri_draws_edited <- lri_draws_edited[,.(location_id, year_id, age_group_id, sex_id, cause_id, measure_id, draw, adjusted_paf)]
  # reshape to wide
  lri_draws_edited <- dcast(lri_draws_edited, ...~draw, value.var = "adjusted_paf")
  # Deal with infinite values - set them to the mean PAF value
  drawnames <- names(lri_draws_edited)[names(lri_draws_edited)%like%"draw"]
  means <- rowMeans(lri_draws_edited[,drawnames, with = F], na.rm = T)
  lri_draws_edited <- cbind(mean = means, lri_draws_edited)
  for (j in drawnames){
    lri_draws_edited[is.na(get(j)),(j):= mean]
  }
  lri_draws_edited$mean <- NULL
  # If draws are STILL NA because of 0 in adj_LRI, set PAF to 0
  for (j in drawnames){
    lri_draws_edited[is.na(get(j)),(j):= 0]
  }
  out_dir <- paste0("FILEPATH")
  dir.create(out_dir, recursive = T, showWarnings = F)
  list_loop <- paf_draws_list_copy[[e]]
  pblapply(1:length(list_loop), function(i){
    dt_other_years <- list_loop[[i]]
    location <- unique(dt_other_years$location_id)
    dt_2020 <- lri_draws_edited[location_id == location]
    dt_write <- rbind(dt_other_years[year_id < 2020, c("location_id", "year_id", "age_group_id", "sex_id", "cause_id", "measure_id", paste0("draw_", 0:999)), with = F], 
                      dt_2020, dt_other_years[year_id > 2022, c("location_id", "year_id", "age_group_id", "sex_id", "cause_id", "measure_id", paste0("draw_", 0:999)), with = F])
    write.csv(dt_write, paste0(out_dir, location, ".csv"), row.names = F)
  })
}
