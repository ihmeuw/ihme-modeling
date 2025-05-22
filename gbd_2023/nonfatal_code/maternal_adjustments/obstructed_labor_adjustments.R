################################################################################################################################################
# Purpose: data processing for obstructed labor input data
# age-split lit data into GBD age bins
################################################################################################################################################
#setup

remove(list = ls())
library(dplyr)
library(data.table)
library(readxl)
library('openxlsx')
source("FILEPATH/get_draws.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/cw_mrbrt_helper_functions.R")

gbd_id <- ID 
version_id <- MVID 
draws <- paste0("draw_", 0:999)

## GET TABLES
ages <- get_age_metadata(release_id=16)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age <- ages[age_start >= 10 & age_end <=55, age_group_id]
ages[, age_group_weight_value := NULL]
ages[age_start >= 1, age_end := age_end - 1]
ages[age_end == 124, age_end := 102]
super_region_dt <- get_location_metadata(location_set_id = 22, release_id=16)
super_region_dt <- super_region_dt[, .(location_id, super_region_id)]

################################################################################################################################################
# pull in bv 
obstruction <- get_bundle_version(bundle_version_id=bvid)

# calc mean and SE where mean and SE are NA
obstruction <- get_cases_sample_size(obstruction)
obstruction <- get_se(obstruction)
obstruction <- calculate_cases_fromse(obstruction)
obstruction <- calc_year(obstruction)

#subset to rows with age grange >5 that need split. Set aside age-specific data. Leave zeroes (cases or mean) alone
obstruct <- copy(obstruction)
obstruct <- obstruct[, age_range:=age_end-age_start]
obstruct_already_split <- obstruct[age_range<=5 | mean==0 | cases==0]
#obstruct_already_split <- obstruct_already_split[, age_end:=age_end-1]
obstruct_to_split <- obstruct[age_range>5 & mean!=0 & cases!=0]
obstruct_to_split[, id := 1:.N]
obstruct_to_split[, age_group_id:=NULL]
obstruct_to_split[age_end==55, age_end:=54]

expand_age <- function(small_dt, age_dt = ages){
  dt <- copy(small_dt)
  
  ## ROUND AGE GROUPS
  dt[, age_start := age_start - age_start %%5]
  dt[, age_end := age_end - age_end %%5 + 4]
  dt <- dt[age_end > 102, age_end := 102] 
  
  ## EXPAND FOR AGE
  dt[, n.age:=(age_end+1 - age_start)/5]
  dt[, age_start_floor:=age_start]
  dt[, drop := cases/n.age] 
  expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .)
  split <- merge(expanded, dt, by="id", all=T)
  split[, age.rep := 1:.N - 1, by =.(id)]
  split[, age_start:= age_start+age.rep*5]
  split[, age_end :=  age_start + 4]
  split <- merge(split, ages[, c("age_start", "age_end", "age_group_id")], by = c("age_start", "age_end"), all.x = T)
  split[age_start == 0 & age_end == 4, age_group_id := 1]
  split <- split[age_group_id>6 & age_group_id<16] ##don't keep where age group id isn't estimated for cause
  return(split)
}

split_df <- expand_age(obstruct_to_split, age_dt=ages)
setnames(split_df, "year_match", "year_id")

################################################################################################################################################
# which locations and years need split? 
pop_locs <- unique(split_df$location_id)
pop_years <- unique(split_df$year_id)
pop_years <- unique(round_any(pop_years, 5))

# get age pattern 
get_age_pattern <- function(locs, id, age_groups){
  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = id, 
                           location_id = locs, source = "epi", measure_id=6,
                           version_id = version_id, sex_id = 2,  release_id=16, 
                           age_group_id = age_groups, year_id = pop_years) 
  
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, year_id, age_group_id, location_id, se_dismod, rate_dis)]
  
  ## CASES AND SAMPLE SIZE
  age_pattern[, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
  age_pattern[, cases_us := sample_size_us * rate_dis]
  age_pattern[is.nan(sample_size_us), sample_size_us := 0] 
  age_pattern[is.nan(cases_us), cases_us := 0]
  
  age_pattern[, super_region_id := location_id]
  age_pattern <- age_pattern[ ,.(age_group_id, year_id, sex_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
  return(age_pattern)
}

age_pattern <- get_age_pattern(locs = pop_locs, id = gbd_id, age_groups = age)

age_pattern[,location_id:=super_region_id]
age_pattern1 <- copy(age_pattern)
setnames(split_df, "year_id", "orig_year_id")
split_df <- split_df[, year_id:=round_any(orig_year_id, 5)]
split_df <- merge(split_df, age_pattern1, by = c("age_group_id", "year_id", "location_id"))

# get live births info to create incidence ratios
get_births_structure <- function(locs, years, age_groups){
  population <- get_population(location_id = locs, year_id = years, release_id=16, #decomp_step = "step2",
                               sex_id = 2, age_group_id = age_groups)
  asfr <- get_covariate_estimates(covariate_id=13, age_group_id = age_groups, location_id=locs, year_id=years,
                                  release_id=16, sex_id=2)
  setnames(asfr, "mean_value", "asfr")
  demogs <- merge(asfr, population, by=c("location_id", "year_id", "age_group_id"))
  demogs[, live_births:=asfr*population]
  demogs[, c("location_id", "year_id", "age_group_id", "live_births")]
  return(demogs)
}

births_structure <- get_births_structure(locs = pop_locs, years = pop_years, age_groups = age)

# merge on births
split_df <- merge(split_df, births_structure, by = c("location_id", "year_id", "age_group_id"))

#################################################################################################################################################
# age-split data
split_data <- function(raw_dt){
  dt1 <- copy(raw_dt)
  dt1[, total_births := sum(live_births), by = "id"]
  dt1[, sample_size := (live_births / total_births) * sample_size]
  dt1[, cases_dis := sample_size * rate_dis]
  dt1[, total_cases_dis := sum(cases_dis), by = "id"]
  dt1[, total_sample_size := sum(sample_size), by = "id"]
  dt1[, all_age_rate := total_cases_dis/total_sample_size]
  dt1[, ratio := mean / all_age_rate]
  dt1[, mean := ratio * rate_dis ]
  dt1 <- dt1[mean < 1, ]
  dt1[, cases := mean * sample_size]
  return(dt1)
}

split_df <- split_data(split_df)
#revert to original year
split_df[, year_id:=orig_year_id]

# re-calculate se with split sample sizes
split_df[, standard_error := NA][, upper:=NA][, lower:=NA][, uncertainty_type_value:=NA]
split_df <- get_se(split_df)

# label split rows
split_df <- split_df[, split_row:=1]

# combine with data that were already split
obstruct <- rbind(obstruct_already_split, split_df, fill=TRUE)

# label with parent_seqs & save for upload
split_ids <- unique(obstruct$id)
obstruct$crosswalk_parent_seq <- NA
obstruct$crosswalk_parent_seq <- as.numeric(obstruct$crosswalk_parent_seq)
obstruct[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
obstruct[, seq:=NA]
obstruct <- subset(obstruct, !(mean==0 & is.na(lower) & is.na(cases) & is.na(sample_size) & is.na(standard_error) & is.na(effective_sample_size)))
obstruct[, sex:="Female"]
obstruct <- obstruct[group_review==1 | is.na(group_review)]
obstruct <- obstruct[measure=="incidence"]

# outlier clinical envelope sources
envelope_new <- read.xlsx("FILEPATH")
envelope_new <- envelope_new%>%mutate(nid=ifelse(merged_nid=="na",nid,merged_nid))
envelope_distinct_new <- envelope_new %>% distinct(nid, uses_env,.keep_all = TRUE)
envelope_distinct_new$nid <- as.numeric(envelope_distinct_new$nid)

obstruct <- as.data.table(merge(obstruct, envelope_distinct_new[, c("uses_env", "nid")], by="nid", all.x=TRUE))
obstruct$note_modeler <- as.character(obstruct$note_modeler)
obstruct <- obstruct[uses_env==1, is_outlier:=1][uses_env==1, note_modeler:="outliered because inpatient source uses envelope"]

# outlier terminal age groups
obstruct <- obstruct[age_start==10, is_outlier:=1][age_start==10, note_modeler:="outliered terminal age group"]
obstruct <- obstruct[age_start==50, is_outlier:=1][age_start==50, note_modeler:="outliered terminal age group"]

# outlier near miss 
obstruct <- obstruct[cv_diag_severe==1, is_outlier:=1][cv_diag_severe==1, note_modeler:="outliered for near-miss case definition"]

# outlier marketscan
ms_nids <- c(244369, 244370, 244371, 336847, 336848, 336849, 336850, 408680, 433114, 494351, 494352)
obstruct <- obstruct[nid%in%ms_nids, is_outlier:=1][nid%in%ms_nids, note_modeler:="outlier marketscan"]

write.xlsx(obstruct, "FILEPATH")
write.csv(obstruct, "FILEPATH")
