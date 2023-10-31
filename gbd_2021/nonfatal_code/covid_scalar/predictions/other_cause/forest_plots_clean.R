##################################################
## Project: CVPDs
## Script purpose: Comparing Prelim JRF reports in 2020 (w/ covid) vs monthly reports in 2019 (w/o covid)
## Date: March 2021
## Author: username
##################################################
rm(list=ls())

pacman::p_load(data.table, openxlsx, ggplot2, grid, gridExtra, lubridate, metafor, pbapply, plyr, dplyr, rlist)
# set default theme to be bw and legend to go at bottom
theme_set(theme_bw() + theme(legend.position = 'bottom'))
username <- Sys.info()[["user"]]
date <- gsub("-", "_", Sys.Date())
gbd_round_id <- 7
decomp_step <- "step3"
use_offset <- F
method <- "REML"
agg_etiologies <- T # aggregate diarrhea etiologies
nationality <- "national"
drop_bad_locs <- T

# SOURCE SHARED FUNCTIONS ------------------------------
invisible(sapply(list.files("/filepath", full.names = T), source))
source("/filepath/prep_allcause_extraction.R")

# ARGUMENTS -------------------------------------
years <- c(2017, 2018, 2019, 2020)
out_dir <- paste0("/filepath/", username, "/filepath/", date, "/")
if(!dir.exists(out_dir)) dir.create(out_dir, recursive = T)

# Goal - plot the patterns of different infectious causes against one another
# Using flu and measles as the reference.

# Load extraction which now includes flu and measles
extraction <- prep_allcause_extraction("/filepath/2021_06_01_all_extractions.xlsx")
my_data_extraction <- copy(extraction)
# TEMP - outlier POL for 05/21
my_data_extraction <- my_data_extraction[!ihme_loc_id %like% "POL"]
if(drop_bad_locs == T){
  keep_locs <- read.xlsx("/filepath/loc_include_final.xlsx")
  # drop bad flu locs
  my_data_extraction <- my_data_extraction[(cause_name == "influenza" & ihme_loc_id %in% keep_locs$loc_list) | cause_name != "influenza"]
}
if(agg_etiologies == T) my_data_extraction[!parent_cause %in% c("lri", "meningitis"), cause_name := parent_cause]
# keep only 2017-2020
my_data_extraction <- my_data_extraction[year_id >= 2017 & year_id <= 2020]
# FIRST - sum cases across ETIOLOGIES or CASE STATUS within the same age and sex - sum CASES but keep SAMPLE SIZE the same.
my_data_extraction <- my_data_extraction[, lapply(.SD, sum),
                                         by=.(ihme_loc_id, parent_cause, cause_name, year_id, start_date, end_date, surveillance_name, age_start, age_end, sex, sample_size), .SDcols=c("cases")]
# THEN take the sum of all cases within a given date range - aggregating BOTH CASES AND SAMPLE SIZE by age and sex
my_data_extraction <- my_data_extraction[,`:=`(age_start = min(age_start, na.rm = T),
                                                 age_end = max(age_end, na.rm = T),
                                                 cases = sum(cases, na.rm = F), # TOGGLE HERE FOR T OR F FOR NA IMPUTE WITH 0 OR NA KEEP
                                                 sample_size = sum(sample_size, na.rm = T),
                                                 sex = "both"), .(ihme_loc_id,parent_cause,cause_name,year_id,start_date,end_date,surveillance_name)]
my_data_extraction <- unique(my_data_extraction)
# drop age and sex
my_data_extraction[, `:=` (age_start = NULL, age_end = NULL, sex = NULL)]

# check data ranges - are they all weekly, monthly, quarterly, or yearly?
my_data_extraction[,date_range := as.numeric(end_date - start_date)+1]
if(!all(my_data_extraction$date_range %in% c(7, (7*4):(7*5), 90,91,92, (7*52):(7*53)))) stop("invalid date ranges!")
# set as a factor to class
my_data_extraction$time <- ifelse(my_data_extraction$date_range==7, "weekly",
                                  ifelse(my_data_extraction$date_range<=35, "monthly",
                                         ifelse (my_data_extraction$date_range<=92, "quarterly", "yearly")))
# set month for weekly or monthly data
my_data_extraction[time %in% c("weekly","monthly"), month := month(start_date + (end_date - start_date)/2)]
# Convert all data to "monthly"
# For weekly data - calculate weekly average by month
week_extraction <- my_data_extraction[time == "weekly"][, .(cases = mean(cases, na.rm = TRUE)),.(ihme_loc_id, parent_cause, cause_name, year_id, surveillance_name, time, month, sample_size)]
# then multiply by number of WEEKS in month to get # of cases in the month
week_extraction$start_date <- as.Date(paste0(week_extraction$year_id, "-", week_extraction$month, "-01"))
week_extraction$end_date <- ceiling_date(week_extraction$start_date, "month")-1
week_extraction[,date_range := as.numeric((end_date - start_date)+1)]
week_extraction[, cases := cases*(date_range/7)]
# leave sample size unchanged
# duplicate quarterly and annual into monthly
my_data_split <- my_data_extraction[time %in% c("quarterly", "yearly")]
my_data_extraction <- my_data_extraction[time == "monthly"]
# split quarters into months ----------------------------------------------

# initialize empty list with length == nrow(dataframe)
list_date_dfs <- vector(mode = "list", length = nrow(my_data_split))

# for-loop generates new dates and adds as dataframe to list
list_date_dfs <- pblapply(1:length(list_date_dfs), function(i){

  # transfer dataframe row to variable `row`
  row <- my_data_split[i,]

  # use lubridate to compute first and last days of relevant months
  m_start <- seq(row$start_date, row$end_date, by = "month") %>%
    round_date(unit = "month") # ROUND Not floor
  m_start <- m_start[year(m_start) == row$year_id] # keep only year of interest
  m_end <- m_start + days_in_month(m_start) - 1
  m_start <- m_end - days_in_month(m_end) + 1

  # check that the correct number of days are being created
  if(row$time == "yearly" & length(m_start)!=12) stop(paste("annual row", i, "not split correctly"))
  if(row$time == "quarterly" & length(m_start)!=3) stop(paste("quarterly row", i, "not split correctly"))

  # # replace first and last elements with original dates
  # m_start[1] <- row$start
  # m_end[length(m_end)] <- row$end
  # do not replace with original dates - this has the effect of coercing odd date ranges ex. 12/28/18-12/29/19 to 1/1/19 to 12/31/19

  # compute the number of days per month as well as CASES per month
  # correct difference by adding 1
  m_days <- as.integer(m_end - m_start) + 1
  if(!all(m_days %in% 28:31)) print(i)
  m_cases <- (row$cases / sum(m_days)) * m_days

  # add tibble to list
  date_df <- data.table(ihme_loc_id = row$ihme_loc_id,
                                   parent_cause = row$parent_cause,
                                   cause_name = row$cause_name,
                                   year_id = row$year_id,
                                   start_date = m_start,
                                   end_date = m_end,
                                   surveillance_name = row$surveillance_name,
                                   cases = m_cases,
                                   sample_size = row$sample_size,
                                   date_range = m_days,
                                   time = row$time,
                                   month = month(m_start)
  )
  return(date_df)
}
, cl = 8)

# bind dataframe list elements into single dataframe
df_monthly <- rbindlist(list_date_dfs)
# now the "time" variable is the ORIGINAL time, and the date_range reflects adjusted date range.

my_data_wide <- rbind(my_data_extraction, week_extraction, df_monthly)
# now my_data_extraction is all monthly

# create parent location
my_data_wide[,ihme_parent_loc := substr(ihme_loc_id, 1, 3)]

# reshape to wide, daily average cases for a given year
my_data_wide <- data.table::dcast(my_data_wide, ihme_parent_loc+parent_cause+cause_name+ihme_loc_id+surveillance_name+month+time~year_id, value.var = c("cases", "sample_size"))
# drop any rows NA in 2020
my_data_wide <- my_data_wide[!is.na(`cases_2020`)]
# rename columns
setnames(my_data_wide, c("cases_2017", "cases_2018", "cases_2019", "cases_2020"), c("2017", "2018", "2019", "2020"))
# average sample size across the 4 years to be sample size
my_data_wide$sample_size <- rowMeans(my_data_wide[,.(sample_size_2017, sample_size_2018, sample_size_2019, sample_size_2020)], na.rm = T)

data <- copy(my_data_wide)
data[, `:=` (`2017` = as.numeric(`2017`), `2018` = as.numeric(`2018`), `2019` = as.numeric(`2019`), `2020` = as.numeric(`2020`))]
data[, avg_cases := rowMeans(.SD, na.rm=T), .SDcols=c("2017", "2018", "2019")]
data <- data[!is.na(avg_cases)]

# get source counts
data[, length(unique(ihme_parent_loc)), by = "cause_name"]

# National or subnational?
# Goal to deal with subnationals - aggregate subnationals to national
# then only include them IF national not already present in orig. data
data[, location_cause := paste0(cause_name, ihme_loc_id)]
if (nationality == "national"){
  # collapse to national data by summing subnationals
  data_subnat <- data[nchar(ihme_loc_id) > 3]
  # check for missingness in subnationals
  # loop through each location which we model subnationally
  for(l in c("IND", "JPN", "BRA", "MEX", "NGA", "USA", "GBR")){
    for (c in unique(data_subnat$cause_name)){
      dt_tmp <- data_subnat[ihme_loc_id %like% l & cause_name == c]
      if(nrow(dt_tmp) == 0) next
      counts <- dt_tmp[, .N, by = c("ihme_parent_loc", "cause_name", "parent_cause", "surveillance_name", "ihme_loc_id")]
      print(paste(c, l, unique(counts$N)))
    }
  }
  data_subnat_agg <- data_subnat[, lapply(.SD, sum, na.rm = T), by=.(ihme_parent_loc, parent_cause, cause_name, surveillance_name, month, time),
                                 .SDcols=c("2020", "2019", "2018", "2017", "avg_cases", "sample_size")]
  data_subnat_agg[, location_cause := paste0(cause_name, ihme_parent_loc)]
  data_subnat_agg[,ihme_loc_id := ihme_parent_loc]
  # add data for aggregated subnationals to the overall - only for those location-causes that are missing a national row
  data_nosubnat <- rbind(data[nchar(ihme_loc_id) == 3], data_subnat_agg[!location_cause %in% data$location_cause], fill = T)
  data <- copy(data_nosubnat) # keep only national locations
} else if (nationality == "subnational") {
  # remove parent ihme locs where subnationals are present for that cause-location
  all_subnats <- copy(data[nchar(ihme_loc_id) > 3])
  all_subnats[, parent_location_cause := paste0(cause_name, ihme_parent_loc)]
  #NOTE the below line is dropping some countries (for meningitis NGA and JPN)
  data_subnat <- rbind(all_subnats, data[!ihme_loc_id %like% "_" & !location_cause %in% all_subnats$parent_location_cause], fill = T)
  data_subnat$parent_location_cause <- NULL
  data <- copy(data_subnat) # keep only national locations
}

data[, month := month.name[month]]

# IF we are standardizing to Jan/Feb
standardize_data <- function(data){
  if (!"ihme_parent_loc" %in% names(data)) data[,ihme_parent_loc := substr(ihme_loc_id, 1, 3)]
  if ("time" %in% names(data)) data <- data[time %in% c("weekly", "monthly")]
  ## calculate jan/feb combo ratio to standardize to
  janfeb <- data[month %in% c(1,2,"January","February")]
  janfeb_combo <- janfeb[,.(janfeb_2020 = sum(`2020`, na.rm = T), prev_yrs_janfeb_avg = sum(avg_cases, na.rm=T)),
                         by = c("ihme_loc_id", "ihme_parent_loc", "parent_cause", "cause_name", "surveillance_name", "time")]
  janfeb_combo[, janfeb_ratio_avg := `janfeb_2020`/prev_yrs_janfeb_avg]

  # Calculate cumulative ratios for sources with ratio of ratio ---------------------------------------------
  standardized_data <- copy(data)
  standardized_data <- merge(standardized_data,
                             janfeb_combo[,.(ihme_loc_id, janfeb_combo_2020 = janfeb_2020, prev_yrs_janfeb_avg, janfeb_ratio_avg, cause_name, parent_cause, surveillance_name)],
                             by=c("ihme_loc_id", "cause_name", "parent_cause", "surveillance_name"))
  return(standardized_data)
}
standardized_data <- standardize_data(data)

calc_se <- function(data, se_var){
  # Leave population as annual!
  for (i in 1:length(se_var)){
    # copy the original columns into count
    data[, paste0(se_var[i], "_count")] <- copy(data)[, paste0(se_var[i]), with = F]
    # now create columns for rate
    data[, paste0(se_var[i])] <- copy(data)[, paste0(se_var[i], "_count"), with = F]/copy(data)[, "sample_size", with = F]
    # Use Poisson SE for rate parameters with more than 5 cases
    data[get(paste0(se_var[i], "_count")) > 5, paste0(se_var[i], "_se") := sqrt(get(paste0(se_var[i]))/sample_size)]
    # Use linear interpolation for rate parameters with fewer than 5 cases
    data[get(paste0(se_var[i], "_count")) <= 5, paste0(se_var[i], "_se") := ((5 - get(paste0(se_var[i])) * sample_size) / sample_size + get(paste0(se_var[i])) * sample_size * sqrt(5 / sample_size ** 2)) / 5]
  }
  return(data)
}

# get ratios and percent changes of selected variables
get_ratios <- function(data, cause_compare, standardizality){
  if (!is.null(cause_compare)){
    data[, ratio_avg.x := `2020.x`/avg_cases.x]
    data[, ratio_avg.y := `2020.y`/avg_cases.y]
    data[, pct_change.x := (`2020.x`-avg_cases.x)/avg_cases.x]
    data[, pct_change.y := (`2020.y`-avg_cases.y)/avg_cases.y]
    if (standardizality == "Standardized") {
      data[, janfeb_ratio_avg.x := `janfeb_combo_2020.x`/prev_yrs_janfeb_avg.x]
      data[, Standardizedratio_avg.x := ratio_avg.x/janfeb_ratio_avg.x]
      data[, janfeb_ratio_avg.y := `janfeb_combo_2020.y`/prev_yrs_janfeb_avg.y]
      data[, Standardizedratio_avg.y := ratio_avg.y/janfeb_ratio_avg.y]
      data[, janfeb_pct_change.x := (`janfeb_combo_2020.x`-prev_yrs_janfeb_avg.x)/prev_yrs_janfeb_avg.x]
      data[, Standardizedpct_change.x := pct_change.x-janfeb_pct_change.x]
      data[, janfeb_pct_change.y := (`janfeb_combo_2020.y`-prev_yrs_janfeb_avg.y)/prev_yrs_janfeb_avg.y]
      data[, Standardizedpct_change.y := pct_change.y-janfeb_pct_change.y]
    }
  } else {
    data[, ratio_avg := `2020`/avg_cases]
    data[, pct_change := (`2020`-avg_cases)/avg_cases]
    if (standardizality == "Standardized"){
      data[, janfeb_pct_change := (`janfeb_combo_2020`-prev_yrs_janfeb_avg)/prev_yrs_janfeb_avg]
      data[, Standardizedpct_change := pct_change-janfeb_pct_change]
      data[, janfeb_ratio_avg := `janfeb_combo_2020`/prev_yrs_janfeb_avg]
      data[, Standardizedratio_avg := ratio_avg/janfeb_ratio_avg]
    }
  }
  return(data)
}

# Time to compare causes to each other
forest_plot <- function(all_dt, # all cause data table
                        cause_compare = NULL, # measles or flu
                        transform, # Log or ""
                        nationality, # national or subnational?
                        annuality, # monthly or annual?
                        standardizality, #Standardized or ""
                        use_offset = F, # use offset?  default = F, drop zeros
                        var, # what is the variable of interest?
                        se_var, # what variables to incorporate into SE?
                        out_dir,
                        date,
                        method,
                        small = F){
  if(!is.null(cause_compare)){
    out_dir <- file.path(out_dir, var, cause_compare)
    if(!exists(out_dir)) dir.create(out_dir, recursive = T)
    xy <- c("x", "y")
    se_var <- apply(expand.grid(se_var, xy), 1, paste, collapse=".")
    # create separate flu or measles data table of ONLY FluNet / WHO Measles
    compare_dt <- copy(all_dt)[cause_name == cause_compare & (surveillance_name %like% "WHO" | ihme_loc_id %like% "_")]
    compare_dt$sample_size <- NULL
    all_dt <- all_dt[! cause_name %in% c("measles", "influenza")]
    data_compare <- merge(compare_dt, all_dt, by = c("ihme_loc_id", "month"))
    # Make the time variable a numeric factor, where 1 = weekly, 2 = monthly, 3 = quarterly, 4 = yearly
    data_compare <- mutate(data_compare, month = factor(month, levels = month.name),
                           time = as.numeric(factor(time.y, levels = c("weekly", "monthly", "quarterly", "yearly"))))
    group_cols <- c("ihme_loc_id", "parent_cause.y", "cause_name.y", "surveillance_name.y")
  } else {
    out_dir <- file.path(out_dir, var, "single_cause")
    if(!exists(out_dir)) dir.create(out_dir, recursive = T)
    data_compare <- copy(all_dt)
    # Make the time variable a numeric factor, where 1 = weekly, 2 = monthly, 3 = quarterly, 4 = yearly
    data_compare <- mutate(data_compare, month = factor(month, levels = month.name),
                           time = as.numeric(factor(time, levels = c("weekly", "monthly", "quarterly", "yearly"))))
    group_cols <- c("ihme_loc_id", "parent_cause", "cause_name", "surveillance_name")
  }
  # Omit January and February *WHERE POSSIBLE* regardless of if we are standardizing.
  # i.e., drop January and February only for rows with weekly or monthly data
  # Deleting Jan/Feb everywhere would be a bias as annual-resolution data is compared to monthly-resolution data within the same fraction
  # data_compare <- data_compare[time.y %in% c("weekly", "monthly") & !month %in% c("January", "February") | time.y %in% c("quarterly", "yearly")]
  setDT(data_compare)
  data_compare <- data_compare[!month %in% c("January", "February")]
  if(annuality == "annual"){
    # use sum and max
    # Sum for avg_cases and 2020
    # Max for any of the Jan/Feb variables (because they're the same for all rows) and for the time - to get time granularity
    data_compare <- data_compare %>%
      group_by_at(group_cols) %>%
      summarise_at(c(se_var, "sample_size", "time"), list(sum = sum, max = max), na.rm = FALSE)
    cols_keep <- paste0(c(se_var[se_var %like% "jan"], "sample_size", "time"), "_max")
    cols_keep <- c(group_cols, cols_keep, paste0(c(se_var[!se_var %like% "jan"]), "_sum"))
    data_compare <- data_compare[, names(data_compare) %in% c(cols_keep)] # this is robust to not all columns being present
    data_compare <- as.data.table(data_compare)
    # fix names back to original names
    new_names <- names(data_compare)[names(data_compare) %like% "_sum" | names(data_compare) %like% "_max"]
    old_names <- substr(new_names, 1, nchar(new_names)-4)
    setnames(data_compare, new_names, old_names, skip_absent = T)
    data_compare$month <- "annual"
  }
  # calculate values of error
  data_compare <- calc_se(data_compare, se_var)
  # recalculate ratios, pct changes, standardized ratios, standardized pct changes.
  data_compare <- get_ratios(data_compare, cause_compare, standardizality)
  if(!is.null(cause_compare)){
    # drop values that had an increase in the index cause
    if(nrow(data_compare[ratio_avg.x > 1])>0){
      message(paste("Outliering",nrow(data_compare[ratio_avg.x > 1]),"rows with an increase in", cause_compare, "for", paste(unique(data_compare[ratio_avg.x > 1]$ihme_loc_id), collapse =", ")))
      data_compare <- data_compare[ratio_avg.x <= 1]
    }
    # drop zeros at this point
    if(!use_offset){
      data_compare <- data_compare[`2020.x` != 0 & avg_cases.x != 0 & `2020.y` != 0 & avg_cases.y != 0]
      if ("janfeb_combo_2020.y" %in% names(data_compare)){
        data_compare <- data_compare[`janfeb_combo_2020.x` != 0 & prev_yrs_janfeb_avg.x != 0 & `janfeb_combo_2020.y` != 0 & prev_yrs_janfeb_avg.y != 0]
      }
    } else stop("offset parameter needed!")
    # plotting variables
    data_compare[, plot_ratio := get(paste0(standardizality, var, ".y"))/get(paste0(standardizality, var, ".x"))]
  } else {
    # drop zeros at this point
    if(!use_offset){
      data_compare <- data_compare[`2020` != 0 & avg_cases != 0]
      if ("janfeb_combo_2020" %in% names(data_compare)){
        data_compare <- data_compare[`janfeb_combo_2020` != 0 & prev_yrs_janfeb_avg != 0]
      }
    } else stop("offset parameter needed!")
    # plotting variables
    data_compare[, plot_ratio := get(var)]
  }

  # get SE - different for log-transformed vs non-transformed
  # get can only pull 1 SE variable at a time.
  se_tmp <- list()
  if(transform == "Log"){
    for (i in 1:length(se_var)){
      se_tmp[[i]] <- data_compare[,get(paste0(se_var[i], "_count"))]
    }
    se_tmp <- list.cbind(se_tmp)
    data_compare[, ratio_seLog := sqrt(rowSums(1/se_tmp))]
    data_compare[, plot_ratioLog := log(plot_ratio)]
    # drop NA or Inf values
    data_compare <- data_compare[!is.na(plot_ratioLog) & !is.infinite(plot_ratioLog)]
  } else if (transform == ""){
    # get standard errors
    se_tmp <- list()
    for (i in 1:length(se_var)){
      # calculate the standard error of the ratios from https://www.geol.lsu.edu/jlorenzo/geophysics/uncertainties/Uncertaintiespart2.html#muldiv
      se_tmp[[i]] <- data_compare[,(get(paste0(se_var[i], "_se"))/get(paste0(se_var[i])))^2]
    }
    # divide again
    se_tmp <- sqrt(rowSums(list.cbind(se_tmp)))
    data_compare[, ratio_se := se_tmp]
  }
  # get names right for plotting
  setnames(data_compare, "cause_name.y", "cause_name", skip_absent = T)
  # bring the time back to text format
  data_compare$time <- ifelse(data_compare$time==1, "weekly", ifelse(data_compare$time==2, "monthly", ifelse(data_compare$time == 3, "quarterly", "yearly")))
  save_results <- list()
  if (nationality == "national") ht <- 5 else ht <- 10 # set pdf height
  if (!small) pdf(file = paste0(out_dir, "/", standardizality, "_", transform, "_",nationality, "_", annuality, "_forest.pdf"), height = ht, width = 7.5)
  if (small) pdf(file = paste0(out_dir, "/", standardizality, "_", transform, "_", nationality, "_", annuality, "_small_forest.pdf"), height = ht, width = 6)
  for (cause in sort(unique(data_compare$cause_name))){
    if (annuality == "monthly") months <- sort(unique(data_compare[cause_name == cause]$month)) else months <- "annual"
    for (m in months){
      data_tmp <- copy(data_compare[cause_name == cause & month == m])
      # if(nrow(data_tmp)<=1) next
      meta <- rma(yi = get(paste0("plot_ratio", transform)), sei = get(paste0("ratio_se", transform)),
                  data=data_tmp, slab = paste(data_tmp$ihme_loc_id, data_tmp$time), method = method, control=list(maxiter=1000))
      if(!is.null(cause_compare)){
        p <- forest(meta, xlab = paste(standardizality, transform, "Ratio of", cause, var, "2020 to 2017-2019 over", cause_compare, var, "2020 to 2017-2019", m),
                    showweights = T, cex = 0.9, refline = ifelse(transform == "Log", 0 ,1))
      } else {
        p <- forest(meta, xlab = paste(standardizality, var, "of", cause, "2020 to 2017-2019", m),
                    showweights = T, cex = 0.9, refline = ifelse(var == "pct_change", 0 ,1))
      }
      print(p)
      # write outputs
      meta_b_df <- data.frame(cause = cause, mean = as.numeric(meta$b), lower = as.numeric(meta$ci.lb), upper = as.numeric(meta$ci.ub), n_locs = as.numeric(meta$k),
                              mean.lin = exp(as.numeric(meta$b)), lower.lin = exp(as.numeric(meta$ci.lb)), upper.lin = exp(as.numeric(meta$ci.ub)))
      save_results[[paste0(cause,m)]] <- meta_b_df
    }
  }
  dev.off()
  save_results <- rbindlist(save_results)
  write.csv(save_results, paste0(out_dir, "/",standardizality, "_", transform, "_", nationality, "_", annuality, "_forest.csv"), row.names = F)
}

# cause_compare <- "influenza" # measles or influenza
# annuality <- "annual" # monthly or annual
# var <- "pct_change" # pct_change or ratio_avg
# standardizality <- "Standardized" # "Standardized" or ""

# for(standardizality in c("Standardized", "")){
for(standardizality in c("Standardized", "")){
  if (standardizality == "") {
    # if not standardizing, include all data - not just monthly/weekly
    dt_plot <- copy(data)
    se_var <- c("2020", "avg_cases")
  } else {
    dt_plot <- copy(standardized_data)
    se_var <- c("janfeb_combo_2020", "prev_yrs_janfeb_avg", "2020", "avg_cases")
  }
  for(annuality in c("annual")){
    for(var in c("ratio_avg", "pct_change")){
      # these are the single cause plots
      forest_plot(all_dt = dt_plot,
                  transform = "",
                  cause_compare = NULL,
                  nationality = nationality,
                  annuality = annuality,
                  standardizality = standardizality,
                  var = var, # what is the variable of interest?
                  se_var = se_var, # what variables to incorporate into SE?
                  out_dir = out_dir,
                  date = date,
                  method = method)
      for (cause_compare in c("measles", "influenza")){
        for (transform in c("Log", "")){
          # these are the cause v cause plots
          forest_plot(all_dt = dt_plot,
                      transform = transform,
                      cause_compare = cause_compare,
                      nationality = nationality,
                      annuality = annuality,
                      standardizality = standardizality,
                      var = var, # what is the variable of interest?
                      se_var = se_var, # what variables to incorporate into SE?
                      out_dir = out_dir,
                      date = date,
                      method = method,
                      small = T)
        }
      }
    }
  }
}
