## LOAD FUNCTIONS AND PACKAGES
source(paste0(k, "/current/r/get_population.R"))
source(paste0(k, "/current/r/get_ids.R"))
source(paste0(k, "/current/r/get_age_metadata.R"))
library(openxlsx)

## SET OBJECTS
gbd_round <- 7
decomp_step <- "iterative"
data <- as.data.table(read.xlsx("filepath"))
cols_numeric <- c("nid", "location_id", "year_start", "year_end", "age_start", "age_end", "age_demographer", "mean", "lower", "upper")
data <- data[,(cols_numeric) := lapply(.SD, as.numeric), .SDcols = cols_numeric]

## HELPER FUNCTION
# find the correct age group(s)
find_closest <- function(number, vector){
  index <- which.min(abs(number-vector))
  closest <- vector[index]
  return(closest)
}

## FUNCTION
add_population_cols <- function(data, gbd_round, decomp_step){
  if (!("sex_id" %in% colnames(data))) { data[, sex_id := ifelse(sex == "Male", 1, ifelse(sex == "Female", 2, 3))]}
  
  # split those that need a sample size and those that don't
  data_ss <- copy(data[is.na(sample_size)])
  data_ok <- copy(data[!is.na(sample_size)])
  
  sexes <- unique(data_ss[, sex_id])
  locs  <- unique(data_ss[, location_id])
  years <- unique(c(data_ss$year_start, data_ss$year_end))
  age_dt <- fread("/ihme/code/vpds/vpd_functions/age_id_dt.csv") 
  ages_over_1 <- age_dt[age_start >=1 , age_group_id]
  under_1     <- age_dt[age_start <1, age_group_id]

  # edit age_dt to be fully non-demographer notation
  age_dt[nchar(age_group_name)<=2, age_end := age_end+1]

  # Pull population all but under 1 and over 95
  pop <- get_population(age_group_id = ages_over_1,
                        single_year_age = TRUE,
                        location_id = locs,
                        year_id = years,
                        sex_id = sexes,
                        gbd_round_id = gbd_round,
                        decomp_step = decomp_step,
                        status = "best")
  pop[,run_id:=NULL]
  # Pull pop over 95
  pop_over_95 <- get_population(age_group_id = 235,
                                location_id = locs,
                                year_id = years,
                                sex_id = sexes,
                                gbd_round_id = gbd_round,
                                decomp_step = decomp_step,
                                status = "best")
  pop_over_95[, run_id:=NULL]
  # divide by five to create single year age groups from 95-99
  pop_over_95[, population:=population/5]
  pop_over_95_template<-as.data.table(expand.grid(age_group_id=age_dt[age_start>=95,age_group_id],sex_id=sexes,location_id=locs,year_id=years))
  cols <- c("age_group_id", "location_id", "year_id")
  pop_over_95_template <- pop_over_95_template[,(cols) := lapply(.SD, as.numeric), .SDcols = cols]
  pop_over_95[, age_group_id:=NULL]
  pop_over_95 <- merge(pop_over_95, pop_over_95_template,by=c("sex_id","location_id","year_id"))
  # Pull pop under 1
  pop_under_1 <- get_population(age_group_id = under_1,
                                location_id = locs,
                                year_id = years,
                                sex_id = sexes,
                                gbd_round_id = gbd_round,
                                decomp_step = decomp_step,
                                status = "best")
  pop_under_1[, run_id := NULL]
  pops <- rbindlist(list(pop, pop_under_1, pop_over_95), use.names = T)

  # get age group names, age_group_years_start and age_group_years_end
  ages <- get_ids("age_group")
  pops <- merge(pops ,ages, by = "age_group_id", all.x = T)
  age_metadata <- get_age_metadata(19)
  pops <- merge(pops, age_metadata, by = c("age_group_name","age_group_id"), all.x = T)
  pops[is.na(age_group_years_start), age_group_years_start := as.numeric(age_group_name)]
  pops[is.na(age_group_years_end), age_group_years_end := as.numeric(age_group_name)+1]
  
  # merge population and data

  # set of all possible age start and end
  gbd_age_start <- as.numeric(unique(pops$age_group_years_start))
  gbd_age_end <- as.numeric(unique(pops$age_group_years_end))
  
  # round the age start and end to the closest available age start and end in pops
  data_ss[, age_start := sapply(age_start, find_closest, vector = gbd_age_start)]
  data_ss[, age_end := sapply(age_end, find_closest, vector = gbd_age_end)]
  
  # sum the populations across all the years & ages in the row, by the correct sex
  # go by row
  for (i in 1:nrow(data_ss)){
    row <- data_ss[i,]
    row_years <- c(row$year_start:row$year_end)
    row_sex <- row$sex_id
    row_location <- row$location_id
    # deal with age_demographer
    row_age_start <- row$age_start
    if (!is.na(row$age_demographer) & row$age_demographer == 1 | row$age_end-row$age_start == 0){
      row_age_end <- row$age_end + 1
    } else {row_age_end <- row$age_end}
    # pull age groups with any age start BETWEEN age start (incl) and age end (excl)
    # AND an age end between age start (excl) and age end (incl)
    # these should be the same - sanity check!
    row_ages <- copy(age_dt[age_start >= row_age_start & age_start < row_age_end]$age_group_id)
    row_ages_2 <- copy(age_dt[age_end > row_age_start & age_end <= row_age_end]$age_group_id)
    if(length(row_ages) != length(row_ages_2)){print("double check method to pull row age groups")}
    # sum population across ages, sexes, and years for the row
    pop_sub <- pops[sex_id == row_sex & location_id == row_location & year_id %in% row_years & age_group_id %in% row_ages]
    pop_total <- pop_sub[, total_pop := sum(population)]
    pop_total <- unique(pop_total$total_pop)
    if(length(pop_total)!=1){stop("in iteration ", i, " total population did not aggregate")}
    # put that value into sample_size
    data_ss[i, sample_size := pop_total]
  }
  
  if(any(is.na(data_ss$sample_size))){
    print("error - some sample sizes still missing")
  }
  
  data <- rbind(data_ss, data_ok)
  return(data)
  # done
}
