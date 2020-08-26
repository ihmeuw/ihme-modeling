#***********************************************************************************************************************
# Purpose: Calculate probability of SHS exposure from HH composition and primary smoking prevalence
#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
### qsub args
args <- commandArgs()[-(1:3)]
print(args)
hh <- args[1]
print(hh)
slots <- as.numeric(args[2])
print(slots)

### runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}

### set scientific notation
options(scipen=15)

### load packages
pacman::p_load(dplyr, tidyr, plyr, magrittr, data.table)

### load functions
file.path("FILEPATH") %>% source
locations <- get_location_metadata(gbd_round_id=5, location_set_id=22)
file.path("FILEPATH") %>% source

### set directory
home <- file.path("FILEPATH")
#***********************************************************************************************************************


#----PREP---------------------------------------------------------------------------------------------------------------
### by hh microdata
  # pull in hh extraction
  data <- fread(file.path("FILEPATH"))
  # keep only first 10,000 observations
  num_rows <- 50000
  if (length(data$ihme_loc_id) > num_rows) {
    set.seed(031194)
    rows <- sample(1:length(data$ihme_loc_id), num_rows, replace=FALSE)
    data <- data[rows, ]
  }
  # add location_id
  data <- merge(data, subset(locations, select=c("ihme_loc_id", "location_id")), by="ihme_loc_id")
  # set year_id as floor of mean year_start and year_end
  data[, year_id := floor((as.numeric(year_start) + as.numeric(year_end))/2)]
  
  # define age_sex dummy variable columns
  age_sex_cols <- c("age_sex_10_1", "age_sex_10_2", "age_sex_11_1", "age_sex_11_2", "age_sex_12_1", "age_sex_12_2", 
                    "age_sex_13_1", "age_sex_13_2", "age_sex_14_1", "age_sex_14_2", "age_sex_15_1", "age_sex_15_2", 
                    "age_sex_16_1", "age_sex_16_2", "age_sex_17_1", "age_sex_17_2", "age_sex_18_1", "age_sex_18_2", 
                    "age_sex_19_1", "age_sex_19_2", "age_sex_20_1", "age_sex_20_2", "age_sex_235_1","age_sex_235_2", 
                    "age_sex_30_1", "age_sex_30_2", "age_sex_31_1", "age_sex_31_2", "age_sex_32_1", "age_sex_32_2", 
                    "age_sex_1_1",  "age_sex_1_2",  "age_sex_5_1",  "age_sex_5_2",  "age_sex_6_1",  "age_sex_6_2", 
                    "age_sex_7_1",  "age_sex_7_2",  "age_sex_8_1",  "age_sex_8_2",  "age_sex_9_1",  "age_sex_9_2")
  
  # define identification vars
  id_cols <- c("ihme_loc_id", "nid", "survey_name", "file_path", "year_start", "year_end", "survey_module", 
               "strata", "psu", "hh_id", "new_id", "pweight", "location_id", "year_id", "age_group_id", "sex_id")
  
  # individual counts as a zero in that house
  for (age in unique(data$age_group_id)) {
    for (sex in unique(data$sex_id)) {
      data[age_group_id==age & sex_id==sex,  paste0("age_sex_", age, "_", sex) := (get(paste0("age_sex_", age, "_", sex)) - 1), with=FALSE]
    }
  }
  
  ### add up number of other members of household
  data[, hh_size := rowSums(data[, age_sex_cols, with=FALSE])]
  # call out survey if strange household sizes
  if (quantile(data$hh_size, 0.95) <= 1) { fwrite(data, file.path("FILEPATH"), row.names=FALSE) }
  if (max(data$hh_size) > 100) { fwrite(data, file.path("FILEPATH"), row.names=FALSE) }
  # save hh_size for analysis
  hh_size <- data[!duplicated(new_id), c("ihme_loc_id", "nid", "survey_name", "file_path", "year_start", "year_end", "survey_module", 
                                             "strata", "psu", "hh_id", "new_id", "location_id", "year_id", "hh_size"), with=FALSE]
  hh_size[, hh_size := hh_size + 1]
  fwrite(hh_size, file.path("FILEPATH"), row.names=FALSE)
  
  # only keep HHs less than or equal to 16 people (15 "others") for SHS calculation
  max_hh_size <- max(data$hh_size)
  if (max_hh_size > 15) max_hh_size <- 15
  # ignore HHs with more than 15 other people
  data <- data[hh_size <= 15, ]
  
  ### read in primary smoking prevalence
  prev_mean <- fread(file.path("FILEPATH"))
  prev_draws <- fread(file.path("FILEPATH"))
  occ_mean <- fread(file.path("FILEPATH"))
  occ_draws <- fread(file.path("FILEPATH"))
  
  # merge together mean and draws
  prev <- merge(prev_mean, prev_draws, by=c("location_id", "year_id", "age_group_id", "sex_id"))
  occ <- merge(occ_mean, occ_draws, by = c("location_id", "year_id", "age_group_id", "sex_id"))
  
  # randomly select 100 draws
  set.seed(031194)
  draws <- sample(1:1000, 100, replace=FALSE)
  draw_cols <- paste0("draw_", draws)
  prev <- prev[, c("location_id", "year_id", "age_group_id", "sex_id", "mean", draw_cols), with=FALSE]
  colnames(prev) <- c("location_id", "year_id", "age_group_id", "sex_id", "mean", paste0("draw_", seq(0, 99)))
  occ <- occ[, c("location_id", "year_id", "age_group_id", "sex_id", "mean", draw_cols), with=FALSE]
  colnames(occ) <- c("location_id", "year_id", "age_group_id", "sex_id", "mean", paste0("draw_", seq(0, 99)))
  
  # save copy of draws and mean for ind smoking prev of respondent, for multiplying out probability that person is a smoker themselves
  ind_prev <- melt(prev, id.vars=c("location_id", "year_id", "age_group_id", "sex_id"))
  setnames(ind_prev, "value", "ind_prev")
  setnames(ind_prev, "variable", "draw")
  occ_prev <- melt(occ, id.vars=c("location_id", "year_id", "age_group_id", "sex_id"))
  setnames(occ_prev, "value", "occ_prev")
  setnames(occ_prev, "variable", "draw")
  
  # save age_sex_ var for merging with HH members
  prev[, age_sex := paste("age_sex", age_group_id, sex_id, sep="_") %>% as.factor]
  prev[, c("age_group_id", "sex_id") := NULL]

  ### format data to enable SHS calculations from dummy variable to columns with age and sex of all hh members
  # return age_sex_ var of all hh members
  h <- copy(data)
  data[, c("extra_2", "extra_3", "extra_4", "extra_5") := NA_character_]
  for (i in age_sex_cols){
    data[, (i) := as.character(get(i))]
    data[get(i) == "1", (i) := i]
    # if multiple people of the same age-sex group in the same houshold, repeat column name that number of times
    data[get(i) == "2", extra_2 := i]
    data[get(i) == "2", (i) := i]
    data[get(i) == "3", extra_2 := i]
    data[get(i) == "3", extra_3 := i]
    data[get(i) == "3", (i) := i]
    data[get(i) == "4", extra_2 := i]
    data[get(i) == "4", extra_3 := i]
    data[get(i) == "4", extra_4 := i]
    data[get(i) == "4", (i) := i]
    data[get(i) == "5", extra_2 := i]
    data[get(i) == "5", extra_3 := i]
    data[get(i) == "5", extra_4 := i]
    data[get(i) == "5", extra_5 := i]
    data[get(i) == "5", (i) := i]
    # make binary 0s missings for reshape
    data[get(i) == "0", (i) := NA_character_]
  }
  # keep only required columns for reshape
  data <- data[, c(id_cols, age_sex_cols, "extra_2", "extra_3", "extra_4", "extra_5"), with=FALSE]
  # make people in hh into vector, separate into individual person columns
  formatted_hh <- t(apply(data, 1, function(x) { return(c(x[!is.na(x)], x[is.na(x)])) } )) %>% as.data.table
  formatted_hh <- formatted_hh[, 1:31]
  colnames(formatted_hh) <- c(id_cols, paste0("person_", seq(1, 15))) 
  person_cols <- paste0("person_", seq(1, 15))
  # make NAs zeros for smoking prevalence calculation
  formatted_hh[, (person_cols) := lapply(.SD, function(x) { x[is.na(x)]<-0; x } ), .SDcols = person_cols]
  
  ### multiply primary smoking prev by household roster to get probability that hh member is a smoker
  # prep data for draws by melting long by person 1-15
  formatted_hh[, person_id := paste0(new_id, "_", seq_along(new_id))]
  formatted_hh[, c("location_id", "year_id", "age_group_id", "sex_id") := lapply(.SD, as.integer), 
           .SDcols=c("location_id", "year_id", "age_group_id", "sex_id")]
  temp1 <- melt(formatted_hh, id.vars=c(id_cols, "person_id"), measure.vars=person_cols)
  setnames(temp1, "value", "age_sex")
  setnames(temp1, "variable", "people")
  # merge data tables and propagate smoking prevalence
  temp2 <- join(temp1, prev, by=c("age_sex", "location_id", "year_id"), type="left")
  temp <- melt(temp2, id.vars=c(id_cols, "person_id", "people", "age_sex"))
  setnames(temp, "value", "prev")
  setnames(temp, "variable", "draw")
  # if smoking prevalence is missing, it's from age_group under 7, where smoking prev is considered zero
  temp[is.na(prev), prev := 0]
  temp[, probability_smoker := prev]
  # make people columns back wide
  exposure <- dcast.data.table(temp, value.var="probability_smoker", new_id + person_id + ihme_loc_id + location_id + nid + survey_name + file_path +
                                 year_id + survey_module + strata + psu + pweight + age_group_id + sex_id + draw ~ people)

  ### use probability of union of sets (maximum 15 for a total HH size of 16 including the respondent) to estimate SHS exposure probability, 
  ### accounting for the potential that a person could be living with multiple smokers
  name <- "person"
  number_sets <- max_hh_size
  people_cols <- c(paste0(name, "_", seq(1, number_sets)))
  # 1 - prob that no one is a smoker
  exposure[, shs_exp := apply(.SD, 1, function(x) { 1 - prod(1 - x) }), .SDcols=people_cols]

  ### multiply out probability that exposed is smoker themself (1 minus primary smoking prevalence)
  # make sure columns are numeric
  exposure[, c("location_id", "year_id", "age_group_id", "sex_id"):= lapply(.SD, as.integer), 
                .SDcols=c("location_id", "year_id", "age_group_id", "sex_id")]
  # merge on primary prevalence
  exposure <- merge(exposure, ind_prev, by=c("location_id", "year_id", "age_group_id", "sex_id", "draw"), all.x=TRUE)
  exposure[is.na(ind_prev), ind_prev := 0]
  occ_prev<-unique(occ_prev)
  exposure <- merge(exposure, occ_prev, by=c("location_id", "year_id", "age_group_id", "sex_id", "draw"), all.x=TRUE)
  exposure[is.na(occ_prev), occ_prev := 0]
  # Combine occupational and non-occupational estimates
  exposure[, shs_exp := shs_exp + occ_prev - (shs_exp * occ_prev)]
  
  # multiply combined shs exposure probability by the probability that that person is a smoker
  exposure[, shs := shs_exp * (1 - ind_prev)]
  
  ### set all older age groups to same age_group_id
  exposure[age_group_id %in% c(30:32, 235), age_group_id := 999]
  
  ### save for collapse
  exposure <- exposure[, list(ihme_loc_id, nid, survey_name, file_path, year_id, 
                                        survey_module, strata, psu, pweight, age_group_id, sex_id, draw, shs)]
  write.csv(exposure, file.path("FILEPATH"), row.names=FALSE)
#***********************************************************************************************************************