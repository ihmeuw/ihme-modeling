## EMPTY THE ENVIRONMENT
rm(list = ls())

## SET UP FOCAL DRIVES
os <- .Platform$OS.type
if (os=="windows") {
  ADDRESS <-"ADDRESS"
  ADDRESS <-"ADDRESS"
  ADDRESS <-"ADDRESS"
} else {
  ADDRESS <-"ADDRESS"
  ADDRESS <-paste0("ADDRESS/", Sys.info()[7], "/")
  ADDRESS <-"ADDRESS"
}

## LOAD FUNCTIONS AND PACKAGES
source(paste0(ADDRESS, "FILEPATH/get_location_metadata.R"))
source(paste0(ADDRESS, "FILEPATH/get_age_metadata.R"))
source(paste0(ADDRESS, "FILEPATH/get_population.R"))

#############################################################################################
###                               HELPER OBJECTS AND FUNCTIONS                            ###
#############################################################################################

## HELPER OBJECTS
compute_inflation <- F
date              <- "2020_01_22"
decomp_step       <- "iterative"
ep_mvid           <- 56726

## DIRECTORIES
ep_stgpr_dir <- paste0("FILEPATH")
adj_data_dir <- paste0(ADDRESS, "FILEPATH")
save_dir     <- paste0(ADDRESS, "FILEPATH")

## CREATE DIRECTORIES
ifelse(!dir.exists(save_dir), dir.create(save_dir), FALSE)

## OTHER OBJECTS
draw_csvs    <- list.files(ep_stgpr_dir)
draws        <- paste0("draw_", 0:999)

#############################################################################################
###                                   PROCESS ST-GPR DRAWS                                ###
#############################################################################################
if (compute_inflation == T) {
  ## AGE METADATA
  ages <- get_age_metadata(age_group_set_id = 12)
  ages <- ages[, .(age_group_id, age_group_years_start, age_group_years_end)]
  setnames(ages, old = c("age_group_years_start", "age_group_years_end"), new = c("age_start", "age_end"))

  ## MATCH TO WHO AGE GROUPS
  ages[age_group_id <= 5, who_age := 5][age_group_id %in% c(6:7), who_age := 6]
  ages[age_group_id %in% c(8:9), who_age := 8][age_group_id %in% c(10:11), who_age := 10]
  ages[age_group_id %in% c(12:13), who_age := 12][age_group_id %in% c(14:15), who_age := 14]
  ages[age_group_id %in% c(16:17), who_age := 16][is.na(who_age), who_age := 18]

  ## CRETE A SKELTON DATA TABLE
  parent_table <- as.data.table(expand.grid(year_id = 1980:2019, age_group_id = ages$age_group_id, sex_id = 1:2))
  parent_table <- merge(parent_table, ages, by = "age_group_id")

  ## LOOP THROUGH DRAWS TO EXTACT MEAN, LOWER, AND UPPER
  for (file in draw_csvs){
    # get data
    print(file)
    temp <- fread(paste0(ep_stgpr_dir, file))
    # compute summaries
    temp[, mean  := rowMeans(.SD), .SDcols = draws]
    temp[, lower := apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draws]
    temp[, upper := apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draws]
    temp[mean > 1, mean := 1][upper > 1, upper := 1]
    # expand to GBD age groups
    setnames(temp, old = "age_group_id", new = "who_age")
    temp <- merge(parent_table, temp, by = c("who_age", "sex_id", "year_id"))
    temp[, who_age := NULL]
    # clean
    temp <- temp[, .SD, .SDcols = -draws]
    if(file == draw_csvs[1]) dt <- copy(temp) else dt <- rbind(dt, temp)
  }

  ## SAVE ESTIMATES
  dt <- dt[, .(location_id, year_id, sex_id, age_group_id, age_start, age_end, mean, lower, upper)]
  dt <- dt[order(location_id, year_id, sex_id, age_group_id)]
  write.csv(dt, paste0(ADDRESS, "FILEPATH/ep_tb_mean_", ep_mvid, "_decomp2.csv"), row.names = F)

#############################################################################################
###                         CREATE CUSTOM EP INFLATION FOR PREV DATA                      ###
#############################################################################################

  ## READ IN DATA
  data <- fread(paste0(adj_data_dir, decomp_step, "_", date, "_download.csv"))
  data <- data[measure == "prevalence"]
  data[sex == "Male", sex_id := 1][sex == "Female", sex_id := 2][sex == "Both", sex_id := 3]

  ## GET POPULATION
  pops <- get_population(location_id  = unique(data$location_id),
                         age_group_id = ages$age_group_id,
                         year_id      = min(data$year_start):max(data$year_end),
                         sex_id       = 1:2,
                         decomp_step  = decomp_step)

  ## TEST
  for (i in 1:nrow(data)){
    # identify age groups to aggregate over
    print(i)
    tmp         <- data[i]
    loc_id      <- tmp$location_id
    round_start <- 5*round(tmp$age_start/5)
    round_end   <- 5*round(tmp$age_end/5)
    if (round_end == 100) round_end <- 125
    # identfiy if sex aggreagtion is neccesary
    sexid  <- tmp[, sex_id]
    if (sexid == 3) sexid <- 1:2
    # read in ST-GPR draws
    ep_tmp <- fread(paste0(ep_stgpr_dir, loc_id, ".csv"))
    setnames(ep_tmp, old = "age_group_id", new = "who_age")
    # merge in GBD age groups
    ep_tmp <- merge(parent_table, ep_tmp, by = c("who_age", "sex_id", "year_id"))
    ep_tmp[, who_age := NULL]
    # subset ST-GPR draws to what is necessary
    ep_tmp <- ep_tmp[(year_id >= tmp$year_start & year_id <= tmp$year_end)]
    ep_tmp <- ep_tmp[(sex_id %in% sexid) & (age_start >= round_start & age_end <= round_end)]
    # merge in population and turn into case space
    ep_tmp <- merge(ep_tmp, pops, by = c("location_id", "year_id", "age_group_id", "sex_id"))
    ep_tmp[, paste0("draw_", 0:999) := lapply(0:999, function(x) { get(paste0("draw_", x)) * population})]
    # collapse age and sex groups
    ep_tmp <- ep_tmp[, lapply(.SD, sum), by = .(location_id), .SDcols = append(draws, "population")]
    ep_tmp[, paste0("draw_", 0:999) := lapply(0:999, function(x) { get(paste0("draw_", x)) / population})]
    # compute summaries
    ep_tmp[, mean  := rowMeans(.SD), .SDcols = draws]
    ep_tmp[, lower := apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draws]
    ep_tmp[, upper := apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draws]
    # clean
    ep_tmp <- ep_tmp[, .SD, .SDcols = -draws]
    ep_tmp[, `:=` (year_start = tmp$year_start, year_end = tmp$year_end, sex_id = tmp$sex_id,
                    age_start = tmp$age_start, age_end = tmp$age_end, population = NULL)]
    if(i == 1) ep_adj <- copy(ep_tmp) else ep_adj <- rbind(ep_adj, ep_tmp)
  }

  ## SAVE
  write.csv(ep_adj, paste0(ADDRESS, "FILEPATH/ep_inflation.csv"), row.names = F)
}
#############################################################################################
###                                  INFLATE FOR EP                                       ###
#############################################################################################

## READ IN UNADJUSTED DATA
data <- fread(paste0(adj_data_dir, decomp_step, "_", date, "_download.csv"))
data <- data[measure == "prevalence"]
data <- data[!(nid==220942 & site_memo %like% "geos")]

## READ IN EP INFLATION DATA
ep   <- fread(paste0(ADDRESS, "FILEPATH/ep_inflation.csv"))
ep   <- ep[, .(location_id, year_start, year_end, sex_id, age_start, age_end, mean, lower, upper)]
ep   <- unique(ep)
setnames(ep, old = c("mean", "lower", "upper"), new = c("inflat_mean", "inflat_lower", "inflat_upper"))

## MERGE
data[sex == "Male", sex_id := 1][sex == "Female", sex_id := 2][sex == "Both", sex_id := 3]
data <- merge(data, ep, by = c("location_id", "year_start", "year_end", "sex_id", "age_start", "age_end"))

## COMPUTE SE
data[(!is.na(upper)) & (!is.na(lower)), standard_error := ((upper-lower)/3.92)]
data[(!is.na(cases)) & (!is.na(sample_size)) & (mean != 0), standard_error := sqrt((mean*(1-mean))/sample_size)]

## INFLATE DATA FOR EP
data[, inflat_factor := (1+(inflat_mean/(1-inflat_mean)))]
data[, mean_inflated := mean * inflat_factor]
data[, se_inflated   := mean_inflated*sqrt((standard_error/mean)^2 + (((inflat_upper-inflat_lower)/3.92)/inflat_factor)^2)]

## OVER WRITE MEAN AND SE WITH INFLATED VERSIONS
data[, `:=` (mean = mean_inflated, standard_error = se_inflated)]
data[, cases := mean*sample_size]
data[(!is.na(cases)) & (!is.na(sample_size)), standard_error := sqrt((mean*(1-mean))/sample_size)]
data <- data[, .SD, .SDcols = -names(data)[names(data) %like% "inflat"]]

## CREATE CI
data[, `:=` (upper = mean + 1.96*standard_error, lower = mean - 1.96*standard_error)]
data[lower <0, lower := 0]
data[, `:=` (uncertainty_type = "Standard error", uncertainty_type_value = 95)]

## SAVE
write.csv(data, paste0(save_dir, decomp_step, "_all_ep_inflated_", date, ".csv"), row.names = F, na = "")
writexl::write_xlsx(list(extraction = data), path = paste0(save_dir, decomp_step, "_all_ep_inflated_", date, ".xlsx"))

