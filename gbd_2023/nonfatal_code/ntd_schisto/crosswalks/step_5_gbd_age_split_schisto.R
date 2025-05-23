##########################################################################
#Schisto age crosswalk 
##########################################################################

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2, magrittr)
date <- gsub("-", "_", Sys.Date())
date <- Sys.Date()

# GET OBJECTS -------------------------------------------------------------
setwd("FILEPATH")


repo_dir <- "FILEPATH"
functions_dir <- paste0(j_root, "FILEPATH")
date <- gsub("-", "_", date)
draws <- paste0("draw_", 0:999)

# GET FUNCTIONS -----------------------------------------------------------

functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids")


source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/save_results_epi.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_bundle_version.R")

## CORRECT COLUMN ORDER
e_order <- fread("FILEPATH", header = F)

col_order <- function(dt){
  e_order <- e_order
  e_order <- tolower(as.character(e_order[!V1 == "", V1]))
  names_dt <- tolower(names(dt))
  setnames(dt, names(dt), names_dt)
  for (name in e_order){
    if (name %in% names(dt) == F){
      dt[, c(name) := ""]
    }
  }
  extra_cols <- setdiff(names(dt), tolower(e_order))
  new_epiorder <- c(e_order, extra_cols)
  setcolorder(dt, new_epiorder)
  return(dt)
}

## FILL OUT MEAN/CASES/SAMPLE SIZE
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS

get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size)]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

## GET CASES IF THEY ARE MISSING

calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

## MAKE SURE DATA IS FORMATTED CORRECTLY
format_data <- function(unformatted_dt, sex_dt){
  dt <- copy(unformatted_dt)
  dt[, `:=` (mean = as.numeric(mean), sample_size = as.numeric(sample_size), cases = as.numeric(cases),
             age_start = as.numeric(age_start), age_end = as.numeric(age_end), year_start = as.numeric(year_start))]
  dt <- dt[measure %in% c("prevalence", "incidence"),]
  dt <- dt[!group_review==0 | is.na(group_review),] 
  dt <- dt[is_outlier==0,] 
  dt <- dt[(age_end-age_start)>25,]
  dt <- dt[!mean == 0 & !cases == 0, ] 
  dt <- merge(dt, sex_dt, by = "sex")
  dt[measure == "prevalence", measure_id := 5]
  dt[measure == "incidence", measure_id := 6]
  dt[, year_id := round((year_start + year_end)/2, 0)]
  return(dt)
}

## CREATE NEW AGE ROWS
expand_age <- function(small_dt, age_dt = ages){
  dt <- copy(small_dt)
  
  ## ROUND AGE GROUPS
  dt[, age_start := age_start - age_start %%5]
  dt[, age_end := age_end - age_end %%5 + 4]
  dt <- dt[age_end > 99, age_end := 99]
  
  ## EXPAND FOR AGE  - 
  dt[, n.age:=(age_end+1 - age_start)/5]
  dt[, age_start_floor:=age_start]
  dt[, drop := cases/n.age] 
  #THE NEXT LINE HERE WILL DROP RECORDS FOR WHICH <20 CASES ARE REPORTED
  dt <- dt[!drop<1,]
  expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .)
  split <- merge(expanded, dt, by="id", all=T)
  split[, age.rep := 1:.N - 1, by =.(id)]
  split[, age_start:= age_start+age.rep*5]
  split[, age_end :=  age_start + 4]
  split <- merge(split, age_dt, by = c("age_start", "age_end"), all.x = T)
  split[age_start == 0 & age_end == 1, age_group_id := 1]
  split <- split[age_group_id %in% age | age_group_id == 1] 
  return(split)
}

## GET DISMOD AGE PATTERN
get_age_pattern <- function(locs, id, age_groups){
  age_pattern <- get_draws(gbd_id_type = "ADDRESS", gbd_id = id, 
                           measure_id = c(5, 6), location_id = locs, source = "ADDRESS",
                           status = "ADDRESS", sex_id = c(1,2), release_id = ADDRESS,
                           age_group_id = age_groups, year_id = 2010) 
  us_population <- get_population(location_id = locs, year_id = 2010, sex_id = c(1, 2), 
                                  age_group_id = age_groups)
  us_population <- us_population[, .(age_group_id, sex_id, population, location_id)]
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]
  
  ## AGE GROUP 1 (SUM POPULATION WEIGHTED RATES)
  age_1 <- copy(age_pattern)
  age_1 <- age_1[age_group_id %in% c(2, 3, 388, 389), ]
  se <- copy(age_1)
  se <- se[age_group_id==238, .(measure_id, sex_id, se_dismod, location_id)] 
  age_1 <- merge(age_1, us_population, by = c("age_group_id", "sex_id", "location_id"))
  age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
  age_1[, frac_pop := population / total_pop]
  age_1[, weight_rate := rate_dis * frac_pop]
  age_1[, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
  age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id"))
  age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
  age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))
  age_1[, age_group_id := 1]
  age_pattern <- age_pattern[!age_group_id %in% c(2, 3, 388, 389)]
  age_pattern <- rbind(age_pattern, age_1)
  
  ## CASES AND SAMPLE SIZE
  age_pattern[measure_id == 5, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
  age_pattern[measure_id == 6, sample_size_us := rate_dis/se_dismod^2]
  age_pattern[, cases_us := sample_size_us * rate_dis]
  age_pattern[is.nan(sample_size_us), sample_size_us := 0] 
  age_pattern[is.nan(cases_us), cases_us := 0]
  
  ## GET SEX ID 3
  sex_3 <- copy(age_pattern)
  sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
  sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
  sex_3[, rate_dis := cases_us/sample_size_us]
  sex_3[measure_id == 5, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] 
  sex_3[measure_id == 6, se_dismod := sqrt(cases_us)/sample_size_us]
  sex_3[is.nan(rate_dis), rate_dis := 0] 
  sex_3[is.nan(se_dismod), se_dismod := 0]
  sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
  sex_3[, sex_id := 3]
  age_pattern <- rbind(age_pattern, sex_3)
  
  age_pattern[, super_region_id := location_id]
  age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
  return(age_pattern)
}

## GET POPULATION STRUCTURE
get_pop_structure <- function(locs, years, age_groups){
  populations <- get_population(location_id = locs, year_id = years,
                                sex_id = c(1, 2, 3), age_group_id = age_groups, release_id=ADDRESS)
  age_1 <- copy(populations) 
  age_1 <- age_1[age_group_id %in% c(2, 3, 388, 389)]
  age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
  age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
  age_1[, age_group_id := 1]
  populations <- populations[!age_group_id %in% c(2, 3, 388, 389)]
  populations <- rbind(populations, age_1)  
  return(populations)
}

## ACTUALLY SPLIT THE DATA
split_data <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[, total_pop := sum(population), by = "id"]
  dt[, sample_size := (population / total_pop) * sample_size]
  dt[, cases_dis := sample_size * rate_dis]
  dt[, total_cases_dis := sum(cases_dis), by = "id"]
  dt[, total_sample_size := sum(sample_size), by = "id"]
  dt[, all_age_rate := total_cases_dis/total_sample_size]
  dt[, ratio := mean / all_age_rate]
  dt[, mean := ratio * rate_dis]
  dt[, cases := mean * sample_size]
  return(dt)
}

## FORMAT DATA TO FINISH
format_data_forfinal <- function(unformatted_dt, location_split_id, region, original_dt){
  dt <- copy(unformatted_dt)
  dt[, group := 1]
  dt[, specificity := "age,sex"]
  dt[, group_review := 1]
  dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
  blank_vars <- c("lower", "upper", "effective_sample_size", "standard_error", "uncertainty_type", "uncertainty_type_value", "seq")
  dt[, (blank_vars) := NA]
  dt <- get_se(dt)
  dt <- col_order(dt)
  if (region ==T) {
    dt[, note_modeler := paste0(note_modeler, "| age split using the super region age pattern", date)]
  } else {
    dt[, note_modeler := paste0(note_modeler, "| age split using the age pattern from location id ", location_split_id, " ", date)]
  }
  split_ids <- dt[, unique(id)]
  dt <- rbind(original_dt[!id %in% split_ids], dt, fill = T)
  dt <- dt[, c(names(df)), with = F]
  return(dt)
}

#-------------------------------------------YOU ENTER YOUR DATA HERE BELOW--------------------------------
## 
#put your input file here:

dt <- as.data.table(read.xlsx("FILEPATH"))



dt$sample_size<-ceiling(dt$sample_size)
dt$cases<-ceiling(dt$mean*dt$sample_size)

dt <- subset(dt, cases <= sample_size)


dt$effective_sample_size<-ceiling(dt$effective_sample_size)

#change any age = 0 to age = 1 
dt[age_start == 0, age_start := 1]
dt[age_end>99, age_end := 99]

#create variable age_diff to enable filtering of output dataset to check splits
dt$age_diff<-dt$age_end - dt$age_start

ages <- get_age_metadata(19)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
#age_groups - 
age_groups <- ages[age_start >= 1, age_group_id]


# from dismod model
id <- ADDRESS
df <- copy(dt)
age <- age_groups
gbd_id <- id

############DO NOT MODIFY THIS SECTION #################################################
age_split <- function(gbd_id, df, age, region_pattern, location_pattern_id){
  
  ## GET TABLES
  sex_names <- get_ids(table = "sex")
  ages <- get_age_metadata(ADDRESS)
  setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
  ages[, age_group_weight_value := NULL]
  ages[age_start >= 1, age_end := age_end - 1]
  ages[age_end == 124, age_end := 99]
  super_region_dt <- get_location_metadata(location_set_id = 22,release_id = "ADDRESS")
  super_region_dt <- super_region_dt[, .(location_id, super_region_id)]
  
  
  ## SAVE ORIGINAL DATA
  original <- copy(df)
  original[, id := 1:.N]
  
  ## FORMAT DATA
  dt <- format_data(original, sex_dt = sex_names)
  dt <- get_cases_sample_size(dt)
  dt <- get_se(dt)
  dt <- calculate_cases_fromse(dt)
  
  ## EXPAND AGE
  split_dt <- expand_age(dt, age_dt = ages)
  
  ## GET PULL LOCATIONS
  if (region_pattern == T){
    split_dt <- merge(split_dt, super_region_dt, by = "location_id")
    super_regions <- unique(split_dt$super_region_id) ##get super regions for dismod results
    locations <- super_regions
  } else {
    locations <- location_pattern_id
  }
  
  ##GET LOCS AND POPS
  pop_locs <- unique(split_dt$location_id)
  pop_years <- unique(split_dt$year_id)
  
  ## GET AGE PATTERN
  print("getting age pattern")
  age_pattern <- get_age_pattern(locs = locations, id = gbd_id, age_groups = age)
  
  if (region_pattern == T) {
    age_pattern1 <- copy(age_pattern)
    split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
  } else {
    age_pattern1 <- copy(age_pattern)
    split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))
  }
  
  ## GET POPULATION INFO
  print("getting pop structure")
  pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = age)
  split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))
  
  #####CALCULATE AGE SPLIT POINTS#######################################################################
  ## CREATE NEW POINTS
  print("splitting data")
  split_dt <- split_data(split_dt)
  ######################################################################################################
 
  final_dt <- format_data_forfinal(split_dt, location_split_id = location_pattern_id, region = region_pattern,
                                   original_dt = original)
  
  ## BREAK IF NO ROWS
  if (nrow(final_dt) == 0){
    print("nothing in bundle to age-sex split")
    break
  }
  return(final_dt)
}

#THIS CODE EXECUTES THE SPLIT:
#using global pattern
final_split <- age_split(gbd_id = id, df = dt, age = age_groups, region_pattern = F, location_pattern_id = 1)


#COPY DATASET 
final_split_2<- final_split



##################################################
##############################################


#calculating lower and upper for those rows that were age split

#fixing these as we had earlier changed all age start from 0 to 1. Some of these had age end of 1 which is why diff -1
final_split_2$age_start[final_split_2$age_diff==-1] <- 0

final_split_2[is.na(lower), lower := (mean - (1.96*standard_error))]
final_split_2[is.na(upper), upper := (mean + (1.96*standard_error))]
summary(final_split_2$lower)


##plot to visualize split
x <- copy(final_split)
x[, age := (age_start + age_end) / 2]
gg <- ggplot(x, aes(x = age, y = mean)) +
  #fix plot with geom_point
  geom_smooth(se = F) +
  labs(x = "Age", y = "Prevalence") +
  theme_classic()

#Plot the figure
plot(gg)



###
############  SAVE CROSSWALK VERSION

#applying celings to higher and lower CIs - this wont drop any of the rows where lowers were originally less than 0

final_split_2$uncertainty_type_value<- NULL
final_split_2$uncertainty_type_value<- 95

#group review, group, and specificity can be blanked
final_split_2[, c("group", "group_review", "specificity") := NA]

#deleting rows where mean > 1   
final_split_2 <- subset(final_split_2, mean<1)

final_split_2[is.na(effective_sample_size), effective_sample_size := sample_size]

#replace upper and lower

final_split_2$upper[final_split_2$upper>1] <- 1

final_split_2$lower[final_split_2$lower<0] <- 0

#limit  SE to 1
final_split_2$standard_error[final_split_2$standard_error>1] <- 1

#outlier subnational Nigeria, China, ethiopia, kenya- higher than 0.4 after 2014
loc_metadata <- get_location_metadata(location_set_id = 9,release_id = "ADDRESS")
loc_metadata  <- subset(loc_metadata , select = c(location_id, parent_id))

final_split_3 <- merge(x=final_split_2, y= loc_metadata, by="location_id", all.x=T)
final_split_3[(parent_id == 6| parent_id== 214 | parent_id == 179 | parent_id == 180) & mean > 0.4 & year_start >=2014, is_outlier := 1]

table(final_split_3$is_outlier)


#outlier age range higher than 25
final_split_3$age_diff2 <- final_split_3$age_end - final_split_3$age_start
final_split_3[age_diff2 >25, is_outlier := 1]

table(final_split_3$is_outlier)


#outliering all data from Osun (Nigeria subnational), and the data prev >60% for KwaZulu Natal in South Africa
final_split_3[location_id == 25347, is_outlier := 1]
table(final_split_3$is_outlier)


final_split_3[location_id == 485 & mean> 0.6, is_outlier := 1]


#outlier data >80% adjusted prevalence from all Kenya subnationals 
final_split_3[parent_id == 180 & mean > 0.8, is_outlier := 1]
table(final_split_3$is_outlier)


# outliering Ogun, Uganda and Cote d'Ivoire

final_split_3[(location_id == 25345 | location_id == 190 | location_id == 205 ) & mean> 0.7, is_outlier := 1]
table(final_split_3$is_outlier)


#saving it as a flat file
openxlsx::write.xlsx(final_split_3, sheetName = "extraction", file = paste0("FILEPATH"))

final_outfile <- paste0("FILEPATH")


#save crosswalk version - 
b_cw_md <- save_crosswalk_version(bundle_version_id = ADDRESS , 
                             data_filepath = final_outfile , 
                             description = "DESCRIPTION") 


############################################################################################################################################
############
#######         END
####
##############################################################################################################################################