###############################################################################################
## Purpose: Calculate diarrhea PAFs including DisMod output, misclassification
## correction, and odds ratios from GEMS and MALED. This particular version of this
##  file is for case definition of qPCR Ct value below lowest inversion in accuracy.
## It also uses the fixed effects only from the mixed effects logistic regression models.
##########################################################################################
rm(list = ls())
library(data.table)
library(openxlsx)
library(plyr)
library(boot)
library(msm)
library(expm)
library(assertable)


windows <- Sys.info()[1][["sysname"]]=="Windows"
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))

location <- commandArgs(trailingOnly=TRUE)[1] 
version <- commandArgs(trailingOnly = T)[2]
run_eti <- commandArgs(trailingOnly = T)[3]
print(commandArgs()[1])

diarrhea_dir <- paste0("/FILEPATH", version, "/")

invisible(sapply(list.files("/FILEPATH", full.names = T), source))
age_map <- read.csv("/FILEPATH/age_mapping_current.csv") 


## Define these here so they are written out just once.
age_group_id <- age_map$age_group_id[age_map$age_pull == 1]
sex_id <- c(1,2)
year_id <- c(1990, 1995, 2000, 2005, 2010, 2015, 2020, 2022, 2023, 2024)
release_id <- 16

yll_version <- 449 
yld_version <- 1531 

## functions --------------------------
agg_age_sex <- function(paf_df, metric, metric_df, pop_df = population){
  metric_df <- melt.data.table(metric_df, id.vars = names(metric_df)[!grepl("draw", names(metric_df))],
                               measure.vars = names(metric_df)[grepl("draw", names(metric_df))], 
                               variable.name = "draw", value.name = "metric")
  
  if(metric == "yld"){
    
    metric_df <- merge(metric_df, pop_df, by = c("age_group_id", "location_id", "year_id", "sex_id"), all.x = T)
    metric_df[,metric := metric * population] #get out of rate space for ylds
    
  }

  paf_df <- merge(paf_df, metric_df, by = c("age_group_id", "location_id", "sex_id", "draw", "year_id"), all.x = T)
  
  if(metric == "yll") setnames(paf_df, "draw_fatal", "paf_draw")
  if(metric == "yld") setnames(paf_df, "draw_nonfatal", "paf_draw")
  
  # get into count space
  paf_df[,count := metric * paf_draw]
  
  
  # aggregate to both sexes
  both_sexes <- copy(paf_df)
  both_sexes <- both_sexes[,.(count = sum(count)), by = c("age_group_id", "location_id", "draw", "year_id", "modelable_entity_id")]
  both_sexes[,sex_id := 3]
  
  paf_df <- rbind(paf_df, both_sexes, fill = T, use.names = T)
  
  # aggregate to all ages
  all_ages <- copy(paf_df)
  all_ages <- all_ages[,.(count = sum(count)), by = c("sex_id", "location_id", "draw", "year_id", "modelable_entity_id")]
  all_ages[,age_group_id := 22]
  
  # aggregate to under 5
  under5_ages <- c(2,3,238,388,389,34)
  u5 <- paf_df[age_group_id %in% under5_ages]
  u5 <- u5[,.(count = sum(count)), by = c("sex_id", "location_id", "draw", "year_id", "modelable_entity_id")]
  u5[,age_group_id := 1]
  
  paf_df <- rbindlist(list(paf_df, all_ages, u5), fill =T, use.names = T)
  
  # drop metric column and re-merge
  
  paf_df <- paf_df[,.(age_group_id, location_id, sex_id, draw, year_id, modelable_entity_id, paf_draw, count)]
  paf_df[,location_id := as.integer(location_id)]
  metric_df[,location_id := as.integer(location_id)]
  paf_df <- merge(paf_df, metric_df, by = c("age_group_id", "location_id", "sex_id", "draw", "year_id"), all.x = T)
  
  # get back into paf space
  
  paf_df[is.na(paf_draw), paf_draw := count / metric]
  paf_df <- paf_df[,.(age_group_id, sex_id, year_id, draw, location_id, modelable_entity_id, paf_draw)]
  
  # do checks
  
  #checking we have all the rows we want
  ids <- list(location_id = as.integer(location), age_group_id = c(2,3,6:20,30,31,32,238,388,389,34,235,22,1),
              sex_id = c(1:3), year_id = c(year_id, 2019, 2021, 2024),
              draw = paste0('draw_',0:999))
  assertable::assert_ids(paf_df, id_vars = ids)
  
  stopifnot(2024 %in% paf_df$year_id)
  
  # checking there are no NA values
  assertable::assert_values(paf_df, "paf_draw", test = "not_na")
  
  # checking there are no values greater than 1
  assert_values(paf_df, "paf_draw", test = "lte", test_val = 1,)
  
  if(metric == "yll") setnames(paf_df,"paf_draw", "draw_fatal")
  if(metric == "yld") setnames(paf_df, "paf_draw", "draw_nonfatal")
  
  return(paf_df)
}


##  read in files #### ------------------------
odds_ratios <- read.csv(paste0(diarrhea_dir, "/used_odds_ratios_gbd2020.csv"))

rv_impact_filename <- paste0(diarrhea_dir, "/rotavirus_vaccine_adjustment/", location, "_rv_adjustment.csv")

if (run_eti == "eti_diarrhea_rotavirus") {
  if (file.exists(rv_impact_filename)) {
    rv_impact <- fread(rv_impact_filename) 
  }
}

## Import etiology meta-data ##
eti_meta <- read.csv("/FILEPATH/eti_rr_me_ids.csv")
eti_meta <- subset(eti_meta, source=="GEMS")
eti_meta <- as.data.table(eti_meta)
eti_meta <- eti_meta[rei == run_eti]

## Import MR_BRT covariates for inpatient sample population ##
mrbrt_covs <- data.table(read.csv(paste0(diarrhea_dir, "/inpatient_scalar_mrbrt_draws.csv")))
# rename
for(i in 1:1000){
  setnames(odds_ratios, paste0("odds_",i), paste0("odds_",i-1))
}
odds_gems <- subset(odds_ratios, study=="GEMS")
odds_maled <- subset(odds_ratios, study=="MALED")

############################################################################################

for(m in unique(eti_meta$modelable_entity_id)){ 
  name <- eti_meta[modelable_entity_id == m, modelable_entity_name]
  eti_dir <- eti_meta[modelable_entity_id == m, rei]
  bundle <- eti_meta[modelable_entity_id == m, bundle_id]
  
  mv_sheet <- read.xlsx("/FILEPATH/gbd23_dismod_mvs_FINAL_GBD2023.xlsx")
  setDT(mv_sheet)
  model_ver <- mv_sheet[bundle_id==bundle]$model_version_id
  
  eti_cov <- mrbrt_covs[modelable_entity_id == m]
  
  print(paste0("Calculating attributable fractions for ",name))
  
  # Get etiology proportion draws for DisMod model #
  eti_draws <- get_draws(source="epi", 
                         version=model_ver, 
                         gbd_id_type="modelable_entity_id", 
                         gbd_id=m, 
                         sex_id=sex_id, 
                         location_id=location, 
                         year_id=c(year_id, 2024), 
                         age_group_id=age_group_id, 
                         release_id=release_id)
  
  eti_interpolate <- interpolate(
    gbd_id_type = "modelable_entity_id",
    gbd_id = m,
    source = "epi",
    version_id = model_ver, 
    measure_id = 18,
    location_id = location,
    sex_id = sex_id,
    age_group_id=age_group_id,
    release_id = release_id,
    reporting_year_start = 2015,
    reporting_year_end = 2022)
  
  eti_draws <- rbind(eti_draws, eti_interpolate[year_id %in% c(2019, 2021)])
  # Find the median, take 1% of that for the floor (consistent with DisMod default for linear floor)
  
  eti_draws$floor <- apply(eti_draws[,paste0("draw_", 0:999)], 1, median)
  eti_draws$floor <- eti_draws$floor * 0.01
  
  eti_fatal <- join(eti_draws, odds_gems, by=c("age_group_id","modelable_entity_id"))
  eti_non_fatal <- join(eti_draws, odds_maled, by=c("age_group_id","modelable_entity_id"))
  
  ## Do PAF calculation ## ---------------------------
  
  ## data manipulation/all the reshaping
  eti <- melt.data.table(eti_fatal, id.vars = names(eti_fatal)[!grepl("draw|odds", names(eti_fatal))], 
                         measure.vars = names(eti_fatal)[grepl("draw|odds", names(eti_fatal))])
  nonfatal_odds <- melt.data.table(eti_non_fatal, id.vars = names(eti_non_fatal)[!grepl("odds|draw", names(eti_non_fatal))], 
                                   measure.vars = names(eti_non_fatal)[grepl("odds|draw", names(eti_non_fatal))])
  
  eti <- eti[!variable %in% c("odds_upper", "odds_lower", "odds")]
  eti[grepl("odds", variable), variable := gsub("_", "_fatal_", variable)]
  eti[grepl("draw", variable), variable := gsub("_", "_nonfatal_", variable)]
  
  nonfatal_odds <- nonfatal_odds[!grepl("draw", variable)]
  nonfatal_odds <- nonfatal_odds[!variable %in% c("odds_upper", "odds_lower", "odds")]
  nonfatal_odds[, variable := gsub("_", "_nonfatal_", variable)]
  
  eti <- rbind(eti, nonfatal_odds, use.names = T)
  
  eti[,draw := unlist(lapply(strsplit(as.character(variable), split="_"), "[", 3))]
  eti[,temp1 := paste(lapply(strsplit(as.character(variable), split="_"), "[", 1))]
  eti[,temp2 := paste(lapply(strsplit(as.character(variable), split="_"), "[", 2))]
  eti[,variable := paste0(temp1, "_", temp2)]
  
  eti[,temp1 := NULL][,temp2 := NULL]
  
  eti <- eti[,.(age_group_id, location_id, sex_id, year_id, floor, lnor, errors, pathogen, pvalue, study, variable, value, draw, modelable_entity_id, rei_id)]
  eti <- dcast(eti, age_group_id + location_id + sex_id + year_id + floor + pathogen + draw + modelable_entity_id ~ variable, value.var = "value")
  
  ## merge on scalar here
  scalar <- melt.data.table(eti_cov, id.vars = names(eti_cov)[!grepl("scalar", names(eti_cov))], 
                            measure.vars = names(eti_cov)[grepl("scalar", names(eti_cov))]) 
  scalar[,draw := unlist(lapply(strsplit(as.character(variable), split="_"), "[", 2))]
  setnames(scalar, "value", "scalar")
  eti <- merge(eti, scalar[,.(scalar, draw, modelable_entity_id, rei_id)], by = c("draw", "modelable_entity_id"))
  
  # do calculations
  eti[,draw_fatal := inv.logit(logit(draw_nonfatal) + scalar)]
  eti[,draw_fatal := draw_fatal * (1 - 1/odds_fatal)]
  eti[,draw_nonfatal := draw_nonfatal * (1 - 1/odds_nonfatal)]
  
  eti[draw_fatal <0, draw_fatal := floor]
  eti[draw_nonfatal <0, draw_nonfatal := floor]
  
  # If rotavirus, join with impact draws and take them into consideration
  if(m == 1219 & file.exists(rv_impact_filename)){
    rv_impact <- melt.data.table(rv_impact, id.vars = names(rv_impact)[!grepl("rv_impact", names(rv_impact))], 
                                 measure.vars = names(rv_impact)[grepl("rv_impact", names(rv_impact))], value.name = "rv_impact") 
    rv_impact[,draw := unlist(lapply(strsplit(as.character(variable), split="_"), "[", 3))]
    
    eti <- merge(eti, rv_impact[,.(year_id, rv_impact, draw)], by= c("year_id", "draw"))
    
    eti[age_group_id %in% c(388,389,34,238), draw_fatal := draw_fatal * (1-rv_impact) / (1 - draw_fatal * rv_impact)]
    eti[age_group_id %in% c(388,389,34,238), draw_nonfatal := draw_nonfatal * (1 - rv_impact) / (1- draw_nonfatal * rv_impact)]
    eti$rv_impact_included <- 1
  }
  
  ## prepping to save out --------------------------------
  
  # pull ylls, ylds, population to weight paf draws for age and sex aggregation
  
  ages_to_pull <- c(2,3,6:20,30,31,32,238,388,389,34,235, 1, 22)
  
  yll_draws <- get_draws(source = "codcorrect", gbd_id_type = "cause_id", gbd_id = 302,
                         measure_id = 1,metric_id = 1,
                         location_id = location, 
                         year_id = c(year_id, 2019, 2021, 2024), sex_id = c(1, 2, 3), age_group_id = ages_to_pull,
                         release_id=16,version = yll_version)
  

  
  
  yld_draws <- get_draws(source="como", gbd_id_type="cause_id", gbd_id=302,
                         measure_id=6, metric_id=3,
                         location_id = location, year_id = c(year_id, 2019, 2021), sex_id=c(1,2,3), age_group_id=ages_to_pull,
                         release_id=16,version= yld_version)
  

  
  print("TEMP: duplicating draws to 1000 from 250")
  dummy_draws <- yld_draws[,paste0("draw_",0:249)]
  setnames(dummy_draws, paste0("draw_",0:249), paste0("draw_",250:499))
  yld_draws <- cbind(yld_draws, dummy_draws)
  setnames(dummy_draws, paste0("draw_",250:499), paste0("draw_",500:749))
  yld_draws <- cbind(yld_draws, dummy_draws)
  setnames(dummy_draws, paste0("draw_",500:749), paste0("draw_",750:999))
  yld_draws <- cbind(yld_draws, dummy_draws)
  
  print("TEMP: duplicating 2023 como draws for 2024")
  yld_2023 <- yld_draws[year_id==2023]
  yld_2023$year_id <- 2024
  yld_draws <- rbind(yld_draws, yld_2023)  
  
  population <- get_population(location_id = location,
                               age_group_id = ages_to_pull,
                               sex_id=c(1,2,3),
                               year_id= c(year_id, 2019, 2021, 2024),
                               release_id = 16)
  
  # FATAL
  
  fatal_df <- eti[,.(modelable_entity_id, age_group_id, location_id, sex_id, year_id, draw_fatal, draw)]
  fatal_df[,draw := paste0("draw_", draw)]
  
  # create fatal aggregates
  fatal_df <- agg_age_sex(paf_df = fatal_df, metric = "yll", metric_df = yll_draws)
  
  fatal_df <- dcast.data.table(fatal_df, modelable_entity_id + year_id + age_group_id + location_id + sex_id ~ draw, value.var = "draw_fatal")
  fatal_df[,cause_id := 302]
  
  write.csv(fatal_df, paste0("/FILEPATH", version, "/", eti_dir, "/", location, "_yll.csv"), row.names=F)
  
  # NONFATAL
  non_fatal_df <- eti[,.(modelable_entity_id, age_group_id, location_id, sex_id, year_id, draw_nonfatal, draw)]
  non_fatal_df[,draw := paste0("draw_", draw)]
  
  # create the nonfatal aggregations
  non_fatal_df <- agg_age_sex(paf_df = non_fatal_df, metric = "yld", metric_df = yld_draws)
  
  non_fatal_df <- dcast.data.table(non_fatal_df, modelable_entity_id + year_id + age_group_id + location_id + sex_id ~ draw, value.var = "draw_nonfatal")
  non_fatal_df[,cause_id := 302]
  write.csv(non_fatal_df, paste0("/FILEPATH", version, "/", eti_dir, "/",location,"_yld.csv"), row.names=F)
  
}