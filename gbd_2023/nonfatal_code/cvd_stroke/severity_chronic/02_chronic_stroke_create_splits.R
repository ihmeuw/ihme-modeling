##
## Author: USERNAME
## Date: 
##
## Purpose: Chronic severity splits for stroke (including stroke + dementia + HF) sequelae
##

library(data.table)
library(gtools)

# pull in parameters
args <- commandArgs(trailingOnly = TRUE)
print(args)
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))

loc_path <- args[1]
loc_args <- fread(loc_path)
location <- loc_args[task_id, location_id]
print(paste0("Location_id is ", location))

release_id <- as.numeric(args[2])
output_folder <- as.character(args[3])
input_folder <- as.character(args[4])

# source central functions
central <- "FILEPATH"
for (func in paste0(central, list.files(central))) source(paste0(func))

# pull in region id of specified location id

message(paste0('PROCESSING...'),location)

locs <- get_location_metadata(location_set_id=35, release_id = release_id)
region_loc <- locs[location_id==location]$region_id

# pull in GBD 2020 ages
ages <- get_age_metadata(release_id = release_id)
setnames(ages, old=c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))

##################################################################
if(release_id==24){
  ## model versions
  stroke_mvs <- get_best_model_versions(entity = 'modelable_entity',ids=c(10837,10836,18733), release_id = 16)
  
  path_to_draws_isch <- 'FILEPATH'
  path_to_draws_read_isch <- paste0(path_to_draws_isch,'meid_10837_mvid_',stroke_mvs[modelable_entity_id==10837,model_version_id],'/')
  
  
  path_to_draws_cerhem <- 'FILEPATH'
  path_to_draws_read_cerhem <- paste0(path_to_draws_cerhem,'meid_10836_mvid_',stroke_mvs[modelable_entity_id==10836,model_version_id],'/')
  
  path_to_draws_subhem <- 'FILEPATH'
  path_to_draws_read_subhem <- paste0(path_to_draws_subhem,'meid_18733_mvid_',stroke_mvs[modelable_entity_id==18733,model_version_id],'/')
  
  age_sex_combos_m <- paste0(paste(location,paste(ages$age_group_id, c(1),sep = '_'),sep="_"),'.csv')
  age_sex_combos_f <- paste0(paste(location,paste(ages$age_group_id, c(2),sep = '_'),sep="_"),'.csv')
  age_sex_combos <- c(age_sex_combos_m,age_sex_combos_f)
  
  files_to_read_isch <- paste0(path_to_draws_read_isch, age_sex_combos)
  files_to_read_cerhem <- paste0(path_to_draws_read_cerhem, age_sex_combos)
  files_to_read_subhem <- paste0(path_to_draws_read_subhem, age_sex_combos)
  
  isch_stroke_draws <- list()
  for(f in files_to_read_isch){
    isch_chronic_stroke_draw <- data.table(read.csv(f))
    isch_stroke_draws[[f]] <- isch_chronic_stroke_draw
  }
  chronic_is <- data.table(rbindlist(isch_stroke_draws))
  
  cerhem_stroke_draws <- list()
  for(f in files_to_read_cerhem){
    cerhem_chronic_stroke_draw <- data.table(read.csv(f))
    cerhem_stroke_draws[[f]] <- cerhem_chronic_stroke_draw
  }
  chronic_ich <- data.table(rbindlist(cerhem_stroke_draws))
  
  subhem_stroke_draws <- list()
  for(f in files_to_read_subhem){
    subhem_chronic_stroke_draw <- data.table(read.csv(f))
    subhem_stroke_draws[[f]] <- subhem_chronic_stroke_draw
  }
  chronic_sah <- data.table(rbindlist(subhem_stroke_draws))
  
}else{
  # pull in draws from chronic ischemic (is) stroke model
  chronic_is <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 10837, source="epi",
                          release_id = release_id, status="best",
                          measure_id = c(5, 6), location_id = location, age_group_id = unique(ages$age_group_id), sex_id = c(1, 2))
  
  # pull in draws from chronic intracerebral (ich) stroke model
  chronic_ich <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 10836, source="epi",
                           release_id = release_id, status="best",
                           measure_id = c(5, 6), location_id = location, age_group_id = unique(ages$age_group_id), sex_id = c(1, 2))
  
  # pull in draws from chronic subarachnoid (sah) stroke model
  chronic_sah <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 18733, source="epi",
                           release_id = release_id, status="best",
                           measure_id = c(5, 6), location_id = location, age_group_id = unique(ages$age_group_id), sex_id = c(1, 2))  
  
}


##################################################################
# pull in draws for dementia due to stroke for specified location
dementia_due_to_stroke <- readRDS(paste0(input_folder, "full_draws/", location, ".rds"))[["stroke"]]

# reshape stroke model data
chronic_is <- melt(chronic_is, measure.vars = names(chronic_is)[grepl("draw", names(chronic_is))], variable.name = "draw", value.name = "is_draw")
chronic_ich <- melt(chronic_ich, measure.vars = names(chronic_ich)[grepl("draw", names(chronic_ich))], variable.name = "draw", value.name = "ich_draw")
chronic_sah <- melt(chronic_sah, measure.vars = names(chronic_sah)[grepl("draw", names(chronic_sah))], variable.name = "draw", value.name = "sah_draw")

setDT(chronic_is)
setDT(chronic_ich)
setDT(chronic_sah)

# merge data from 3 stroke subtype models
chronic_stroke <- merge(chronic_is, chronic_ich, by=c("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id", "draw"))
chronic_stroke <- merge(chronic_stroke, chronic_sah, by=c("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id", "draw"))
setDT(chronic_stroke)

# calculate proportion of stroke subtypes (subtype prevalence/total stroke prevalence)
chronic_stroke[ ,  `:=` (is_draw_prop = is_draw/(is_draw + ich_draw + sah_draw), ich_draw_prop = ich_draw/(is_draw + ich_draw + sah_draw), sah_draw_prop = sah_draw/(is_draw + ich_draw + sah_draw))]

# reshape dementia due to stroke data
dementia_due_to_stroke <- melt(dementia_due_to_stroke, measure.vars = names(dementia_due_to_stroke)[grepl("stroke", names(dementia_due_to_stroke))], variable.name = "draw", value.name = "dem_due_stroke_draw")
setDT(dementia_due_to_stroke)
# rename draws to match stroke draw data
dementia_due_to_stroke[, draw := gsub("stroke_", "draw_", draw)]

# merge stroke model data with dementia due to stroke data
dementia_due_to_stroke <- merge(chronic_stroke, dementia_due_to_stroke, by=c("age_group_id", "sex_id", "year_id", "location_id", "measure_id", "draw"))

# calculate dementia due to 3 stroke subtypes (dementia due to stroke * subtype proportion)
dementia_due_to_stroke[, `:=` (dem_due_to_is_draw = (dem_due_stroke_draw * is_draw_prop), dem_due_to_ich_draw = (dem_due_stroke_draw * ich_draw_prop), dem_due_to_sah_draw = (dem_due_stroke_draw * sah_draw_prop))]
dementia_due_to_stroke <- dementia_due_to_stroke[ , c("age_group_id", "sex_id", "year_id", "location_id", "measure_id", "metric_id", "draw", "dem_due_to_is_draw", "dem_due_to_ich_draw", "dem_due_to_sah_draw")]

##################################################################

# set incidence to be 0 (since these are chronic models)
chronic_is[measure_id==6, is_draw := 0]
chronic_ich[measure_id==6, ich_draw := 0]
chronic_sah[measure_id==6, sah_draw := 0]

dementia_due_to_stroke[measure_id==6, `:=` (dem_due_to_is_draw = 0, dem_due_to_ich_draw = 0, dem_due_to_sah_draw = 0)]

##################################################################

# pull in me id names to add to stroke severity data
me_ids <- get_ids("modelable_entity")
setnames(me_ids, c("modelable_entity_id", "modelable_entity_name"), c("target_meid", "target_meid_name"))

##################################################################

# pull severity splits for dementia
dementia_split <- get_severity_split(source_meid=24352, release_id = 9)

# reassign age_end of 95+ age group to match GBD 2020 age metadata
dementia_split[age_end==100, age_end := 125]

# merge stroke severity split data with age metadata to get age_group_id
dementia_split <- merge(dementia_split, ages[,c("age_group_id", "age_start", "age_end")], by=c("age_start", "age_end"), all.x=TRUE)

# pull in me id names and add to stroke severity data
dementia_split <- merge(dementia_split, me_ids, by="target_meid")

# Expand into draw-space
set.seed(12345)
for (row in 1:nrow(dementia_split)) {
  
  if(dementia_split[row, mean]==dementia_split[row, upper]&dementia_split[row, mean]==dementia_split[row, lower]){
    dementia_split[row, paste0("draw_", 0:999) := dementia_split[row, mean]]
  } else {
    sd <- (dementia_split[row, upper]-dementia_split[row, lower])/(2*1.96)
    sample_size <- dementia_split[row, mean]*(1-dementia_split[row, mean])/sd^2
    alpha <- dementia_split[row, mean]*sample_size
    beta <- (1-dementia_split[row, mean])*sample_size
    
    draws <- rbeta(1000, alpha, beta)
    
    dementia_split[row, paste0("draw_", 0:999) := lapply(X = 1:1000, FUN = function(x) draws[x])]
  }
}

dementia_split <- dementia_split[, c("target_meid", "target_meid_name", "age_group_id", "sex_id", "measure_id", paste0("draw_", 0:999))]

# reshape data
dementia_split <- melt(dementia_split, measure.vars = names(dementia_split)[grepl("draw", names(dementia_split))], variable.name = "draw", value.name = "dem_split_prop")

##################################################################

# pull HF severity splits (asymptomatic, mild, moderate, severe HF) 

# Expand into draw-space
set.seed(12345)
for (row in 1:nrow(HF_split)) {
  sd <- (HF_split[row, upper]-HF_split[row, lower])/(2*1.96)
  sample_size <- HF_split[row, mean]*(1-HF_split[row, mean])/sd^2
  alpha <- HF_split[row, mean]*sample_size
  beta <- (1-HF_split[row, mean])*sample_size
  
  draws <- rbeta(1000, alpha, beta)
  
  HF_split[row, paste0("draw_", 0:999) := lapply(X = 1:1000, FUN = function(x) draws[x])]
  
}

# keep severity names
HF_split <- merge(HF_split, me_ids, by="target_meid")
HF_split[target_meid_name%like%"Asymptomatic and mild heart failure", target_meid_name := gsub("Asymptomatic and mild heart failure", "Mild heart failure", target_meid_name)]
HF_split[, target_severity := gsub(" due to chronic obstructive pulmonary disease", "", tolower(target_meid_name))]

HF_split <- HF_split[, c("target_severity", "measure_id", paste0("draw_", 0:999))]

# reshape data
HF_split <- melt(HF_split, measure.vars = names(HF_split)[grepl("draw", names(HF_split))], variable.name = "draw", value.name = "HF_split_prop")

####################################################################################################################################
# ------------ ISCHEMIC STROKE, INTRACEREBRAL HEMORRAGE, SUBARACHNOID HEMORRHAGE SPLITS  ----------------------------------
####################################################################################################################################

for(parent in c(10837, 10836, 18733)){
  
  if(parent==10837){
    abbr <- "is"
    full <- "Chronic ischemic stroke"
  }
  if(parent==10836){
    abbr <- "ich"
    full <- "Chronic intracerebral hemorrhage"
  }
  if(parent==18733){
    abbr <- "sah"
    full <- "Chronic subarachnoid hemorrhage"
  }
  
  message(paste0("-------- ", full, " ----------"))
  
  # pull in stroke severity splits
  stroke_split <- get_severity_split(source_meid = parent, release_id = 6) 
  
  # restrict stroke severity splits to the location id's region
  stroke_split <- stroke_split[location_id==region_loc]
  
  # reassign age_end of 95+ age group to match GBD 2020 age metadata
  stroke_split[age_end==100, age_end := 125]
  # merge stroke severity split data with age metadata to get age_group_id 
  stroke_split <- merge(stroke_split, ages[,c("age_group_id", "age_start", "age_end")], by=c("age_start", "age_end"), all.x=TRUE)
  
  # reassign one copy of ages 0.07671233 to 1 to create age_group_id 388 (1 to 5 months) and ages 1 to 5 to create age_group_id 238 (1 to 2)
  fix_ages_1 <- stroke_split[is.na(age_group_id)]
  fix_ages_1 <- fix_ages_1[, age_group_id := ifelse(age_end==1, 388, 238)]
  # reassign another copy of ages 0.07671233 to 1 to create age_group_id 389 (6 to 11 months) and ages 1 to 5 to create age_group_id 34 (2 to 4)
  fix_ages_2 <- stroke_split[is.na(age_group_id)]
  fix_ages_2 <- fix_ages_2[, age_group_id := ifelse(age_end==1, 389, 34)]
  # recombine stroke severity split age data
  stroke_split <- rbind(stroke_split[!is.na(age_group_id)], fix_ages_1, fix_ages_2)
  
  # add me id names to stroke severity data
  stroke_split <- merge(stroke_split, me_ids, by="target_meid")
  
  # Expand into draw-space
  set.seed(12345)
  for (row in 1:nrow(stroke_split)) {
    sd <- (stroke_split[row, upper]-stroke_split[row, lower])/(2*1.96)
    sample_size <- stroke_split[row, mean]*(1-stroke_split[row, mean])/sd^2
    alpha <- stroke_split[row, mean]*sample_size
    beta <- (1-stroke_split[row, mean])*sample_size
    
    draws <- rbeta(1000, alpha, beta)
    
    stroke_split[row, paste0("draw_", 0:999) := lapply(X = 1:1000, FUN = function(x) draws[x])]
    
  }
  
  stroke_split <- stroke_split[, c("target_meid", "target_meid_name", "age_group_id", "sex_id", paste0("draw_", 0:999))]
  
  # reshape data
  stroke_split <- melt(stroke_split, measure.vars = names(stroke_split)[grepl("draw", names(stroke_split))], variable.name = "draw", value.name = "stroke_split_prop")
  
  ##################################################################
  
  # pull in draws from hf due to stroke model
  hf_due_stroke_meid <- me_ids[target_meid_name==paste0("Heart failure due to ", capwords(full))]$target_meid
  
  hf_due_stroke <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = hf_due_stroke_meid, source="epi",
                             release_id=release_id, status="best",
                             measure_id = c(5, 6), location_id = location, age_group_id = unique(ages$age_group_id), sex_id = c(1, 2))
  
  # reshape the data
  hf_due_stroke <- melt(hf_due_stroke, measure.vars = names(hf_due_stroke)[grepl("draw", names(hf_due_stroke))], variable.name = "draw", value.name = "hf_due_stroke_draw")
  
  ##################################################################
  
  # rescale stroke severities for severities 2-5 (since there is no HF in stroke severity 0 or 1) to use for splitting HF due to stroke into stroke levels
  stroke_split_rescaled <- dcast(stroke_split, age_group_id + sex_id + draw ~ target_meid, value.var = "stroke_split_prop")
  setDT(stroke_split_rescaled)
  
  sev_0 <- as.character(me_ids[target_meid_name==paste0("Asymptomatic ", tolower(full))]$target_meid)
  sev_1 <- as.character(me_ids[target_meid_name==paste0(full, " severity level 1")]$target_meid)
  sev_2 <- as.character(me_ids[target_meid_name==paste0(full, " severity level 2")]$target_meid)
  sev_3 <- as.character(me_ids[target_meid_name==paste0(full, " severity level 3")]$target_meid)
  sev_4 <- as.character(me_ids[target_meid_name==paste0(full, " severity level 4")]$target_meid)
  sev_5 <- as.character(me_ids[target_meid_name==paste0(full, " severity level 5")]$target_meid)
  
  stroke_split_rescaled[, rescaler := 1/(get(sev_2) +  get(sev_3) + get(sev_4) + get(sev_5))]
  
  stroke_split_rescaled[, paste0(sev_0) := NULL]
  stroke_split_rescaled[, paste0(sev_1) := NULL]
  
  stroke_split_rescaled[, paste0(sev_2) := get(sev_2) * rescaler]
  stroke_split_rescaled[, paste0(sev_3) := get(sev_3) * rescaler]
  stroke_split_rescaled[, paste0(sev_4) := get(sev_4) * rescaler]
  stroke_split_rescaled[, paste0(sev_5) := get(sev_5) * rescaler]
  
  stroke_split_rescaled[, rescaler := NULL]
  
  stroke_split_rescaled <- melt(stroke_split_rescaled, measure.vars = c(sev_2, sev_3, sev_4, sev_5), variable.name = "target_meid", value.name = "stroke_split_prop_rescaled")
  
  ##################################################################
  
  # merge hf due to stroke model data with stroke severity splits 
  hf_due_stroke_split <- merge(hf_due_stroke, stroke_split_rescaled, by=c("age_group_id", "sex_id", "draw"), allow.cartesian = TRUE)
  
  if(F){
    # quick check -- hf_due_stroke_split should be 4x hf_due_stroke for the 4 severity splits
    table(hf_due_stroke$age_group_id)
    table(hf_due_stroke_split$age_group_id)
  }
  
  # multiply hf due to stroke data by the stroke severity split value
  setDT(hf_due_stroke_split)
  hf_due_stroke_split[, hf_stroke_split_draw := hf_due_stroke_draw * stroke_split_prop_rescaled]
  
  hf_due_stroke_split <- hf_due_stroke_split[, c("age_group_id", "sex_id", "draw", 
                                                 "location_id", "measure_id", "metric_id", 
                                                 "year_id", "target_meid", "hf_stroke_split_draw")]
  
  hf_due_stroke_split[, target_meid := as.numeric(as.character(target_meid))]
  
  ##################################################################
  
  # merge chronic stroke model data with stroke severity splits 
  chronic_subtype_split <- merge(get(paste0("chronic_", abbr)), stroke_split, by=c("age_group_id", "sex_id", "draw"), allow.cartesian = TRUE)
  
  if(F){
    # quick check -- chronic_subtype_split should be 6x chronic_<subtype> for the 6 severity splits
    table(get(paste0("chronic_", abbr))$age_group_id)
    table(chronic_subtype_split$age_group_id)
  }
  
  # multiply chronic stroke data by the stroke severity split value
  chronic_subtype_split[, stroke_split_draw := get(paste0(abbr, "_draw")) * stroke_split_prop]
  
  chronic_subtype_split <- merge(chronic_subtype_split, hf_due_stroke_split, by=c("age_group_id", "sex_id", "draw", 
                                                                                  "location_id", "measure_id", "metric_id", 
                                                                                  "year_id", "target_meid"), all.x = TRUE)
  
  # fill in NA HF values (no incidence data for HF due to stroke, also NA values for severity level 1 and asymptomatic) with 0
  chronic_subtype_split[is.na(hf_stroke_split_draw), hf_stroke_split_draw := 0]
  
  # calculate chronic stroke without heart failure data by subtracting hf due to stroke from stroke
  chronic_subtype_split[, stroke_no_hf_draw := stroke_split_draw - hf_stroke_split_draw]
  
  # assign draws that have more HF due to stroke than stroke to be 0
  #chronic_subtype_split[stroke_no_hf_draw<0, stroke_no_hf_draw := 0]
  
  # stroke severity levels 0, 1, 2, and 4 do not have any dementia
  for (severity in c(paste0("Asymptomatic ", tolower(full)), paste0(full, " severity level ", c(1,2,4)))){
    
    severity_subset <- chronic_subtype_split[target_meid_name==severity]
    
    # stroke severity level 0 and 1 have no dementia or HF
    if (severity %in% c(paste0("Asymptomatic ", tolower(full)), paste0(full, " severity level 1"))){
      
      # reshape chronic stroke subtype data
      severity_subset <- dcast(severity_subset, location_id + age_group_id + sex_id + year_id + measure_id + metric_id + target_meid + target_meid_name ~ draw, value.var = "stroke_split_draw")
      
      severity_subset <- as.data.table(severity_subset)
      
      sev_meid <- unique(severity_subset$target_meid)
      severity_subset$target_meid=NULL
      severity_subset$target_meid_name=NULL
      
      if(any(severity_subset[, paste0('draw_', 0:999), with=F]<0)){
        stop(paste0('Negative values detected for ME ID ', sev_meid))
      }
      
      folder <- paste0(output_folder, "me_", sev_meid, "/")
      dir.create(folder, showWarnings = FALSE)
      write.csv(severity_subset, file = 
                  paste0(folder, location, ".csv"), row.names = FALSE)
    } else {
      
      # account for negative values
      if(nrow(severity_subset[stroke_no_hf_draw<0,])>0){
        message("Severity level 2/4: Setting draws below 0 to be 0")
        message(paste0("This occurs in ", nrow(severity_subset[stroke_no_hf_draw<0,]), " row(s) out of ", nrow(severity_subset), " rows, the smallest one being ", 
                       min(severity_subset[stroke_no_hf_draw<0,stroke_no_hf_draw])))
      }
      severity_subset[stroke_no_hf_draw<0, stroke_no_hf_draw:=0]
      
      # save stroke with no HF
      stroke_without_HF <- dcast(severity_subset, location_id + age_group_id + sex_id + year_id + measure_id + metric_id ~ draw, value.var = "stroke_no_hf_draw")
      
      stroke_without_HF <- as.data.table(stroke_without_HF)
      
      sev_meid <- me_ids[target_meid_name==paste0(severity, ", without heart failure")]$target_meid
      
      if(any(stroke_without_HF[, paste0('draw_', 0:999), with=F]<0)){
        stop(paste0('Negative values detected for ME ID ', sev_meid))
      }
      
      folder <- paste0(output_folder, "me_", sev_meid, "/")
      dir.create(folder, showWarnings = FALSE)
      write.csv(stroke_without_HF, file = 
                  paste0(folder, location, ".csv"), row.names = FALSE)
      
      
      # save stroke with HF severities
      # merge data with HF severity splits
      severity_subset <- severity_subset[, c("age_group_id", "sex_id", "draw", "measure_id", "location_id", "year_id", "metric_id", "hf_stroke_split_draw")]
      severity_subset <- merge(severity_subset, HF_split, by=c("measure_id", "draw"), allow.cartesian = TRUE)
      
      # multiply chronic stroke with hf data by HF severity split
      severity_subset[, hf_sev_stroke_split_draw := hf_stroke_split_draw * HF_split_prop]
      
      # add in me name and me id
      severity_subset[, target_meid_name := paste0(severity, ", with ", target_severity)]
      severity_subset <- merge(severity_subset, me_ids, by="target_meid_name")
      
      # subset and save each hf severity level
      for (meid in unique(severity_subset$target_meid)){
        final <- severity_subset[target_meid==meid]
        
        final <- dcast(final, location_id + age_group_id + sex_id + year_id + measure_id + metric_id ~ draw, value.var = "hf_sev_stroke_split_draw")
        final <- as.data.table(final)
        
        if(any(final[, paste0('draw_', 0:999), with=F]<0)){
          stop(paste0('Negative values detected for ME ID ', meid))
        }
        
        folder <- paste0(output_folder, "me_", meid, "/")
        dir.create(folder, showWarnings = FALSE)
        write.csv(final, file = 
                    paste0(folder, location, ".csv"), row.names = FALSE)
      }
      
    }
  }
  
  ##################################################################
  
  # separate stroke severity levels 3 and 5 to be split into moderate/severe stroke with cognitive impairment and moderate/severe stroke with dementia
  severities_3_and_5 <- chronic_subtype_split[target_meid_name%in%c(paste0(full, " severity level ", c(3,5)))]
  
  # calculate the proportion with and without HF
  severities_3_and_5[, prop_hf := ifelse(stroke_split_draw==0, 0, hf_stroke_split_draw/stroke_split_draw)]
  severities_3_and_5[, prop_no_hf := ifelse(stroke_split_draw==0, 0, stroke_no_hf_draw/stroke_split_draw)]
  setDT(severities_3_and_5)
  # reshape stroke data
  library(data.table)
  severities_3_and_5 <- dcast.data.table(severities_3_and_5, 
                                         age_group_id + sex_id + measure_id + metric_id + location_id + year_id + draw ~ target_meid_name, 
                                         value.var=c("stroke_split_draw", "prop_hf", "prop_no_hf"))
  
  # merge stroke severity levels 3 and 5 data with dementia due to stroke data
  severities_3_and_5 <- merge(dementia_due_to_stroke, severities_3_and_5, by=c("age_group_id", "sex_id", "year_id", "location_id", "measure_id", "metric_id", "draw"), all.y = TRUE)
  
  # for NA dementia due to stroke values (which would be younger ages groups where dementia due to stroke is not possible), assign dementia due to stroke as 0
  severities_3_and_5[is.na(get(paste0("dem_due_to_", abbr, "_draw"))), paste0("dem_due_to_", abbr, "_draw") := 0]
  
  # assign 10% of severity level 5 to severe chronic stroke with cognitive impairment for age groups in which dementia due to stroke is possible; for young age groups, assign all of severity level 5 to severe chronic stroke with cognitive impairment 
  severities_3_and_5[, severe_w_cogn_imp := ifelse(age_group_id%in%dementia_due_to_stroke$age_group_id, get(paste0("stroke_split_draw_", full, " severity level 5"))*.1, get(paste0("stroke_split_draw_", full, " severity level 5")))]
  # assign 90% of severity level 5 to severe chronic stroke with dementia for age groups in which dementia due to stroke is possible; for young age groups, assign 0 to severe chronic stroke with dementia
  severities_3_and_5[, severe_w_dem := ifelse(age_group_id%in%dementia_due_to_stroke$age_group_id, get(paste0("stroke_split_draw_", full, " severity level 5"))*.9, 0)]
  
  # assign 10% of dementia due to stroke to be dementia due to moderate stroke
  severities_3_and_5[ , paste0("dem_due_to_mod_", abbr, "_draw") := get(paste0("dem_due_to_", abbr, "_draw")) * .1]
  # assign 90% of dementia due to stroke to be dementia due to severe stroke
  severities_3_and_5[ , paste0("dem_due_to_severe_", abbr, "_draw") := get(paste0("dem_due_to_", abbr, "_draw")) * .9]
  
  # calculate the difference between severe chronic stroke with dementia and dementia due to severe stroke
  severities_3_and_5[ , diff := severe_w_dem - get(paste0("dem_due_to_severe_", abbr, "_draw"))]
  
  # if we have more prevalence in severe chronic stroke with dementia than dementia due to severe stroke, we remove the remainder from severe chronic stroke with dementia and reassign to severe chronic stroke with cognitive impairment
  severities_3_and_5[diff>0, severe_w_dem := severe_w_dem-diff]
  severities_3_and_5[diff>0, severe_w_cogn_imp := severe_w_cogn_imp+diff]
  # if we have less prevalence in severe chronic stroke with dementia than dementia due to severe stroke, we add the leftover to dementia due to moderate stroke (also run for cases where differences is 0, which will just result in no changes -- this will include young age groups where there is no dementia as both severe chronic stroke due to dementia and dementia due to severe stroke will have 0 prevalence)
  severities_3_and_5[diff<=0, paste0("dem_due_to_mod_", abbr, "_draw") := get(paste0("dem_due_to_mod_", abbr, "_draw"))+(diff*-1)]
  
  # use dementia due to moderate stroke prevalence for moderate chronic stroke with dementia
  severities_3_and_5[ , moderate_w_dem := get(paste0("dem_due_to_mod_", abbr, "_draw"))]
  # subtract moderate chronic stroke with dementia from total level 3 severity to obtain moderate chronic stroke with cognitive impairment
  severities_3_and_5[ , moderate_w_cogn_imp := get(paste0("stroke_split_draw_", full, " severity level 3")) - moderate_w_dem]
  # we occasionally have a high draw of moderate chronic stroke with dementia paired with a low draw of total level 3 severity, resulting in a negative prevalence -- set to 0 in these cases
  #severities_3_and_5[moderate_w_cogn_imp < 0, moderate_w_cogn_imp := 0]
  
  if(F){
    # quick check -- severe_w_dem + severe_w_cogn_imp should = Chronic <subtype> severity level 5, moderate_w_dem + moderate_w_cogn_imp should = Chronic <subtype> severity level 3, severe_w_dem + moderate_w_dem should = dem_due_to_<subtype>_draw
    severities_3_and_5[ , check1 := get(paste0("stroke_split_draw_", full, " severity level 5")) - (severe_w_dem + severe_w_cogn_imp)]
    max(severities_3_and_5$check1)
    min(severities_3_and_5$check1)
    severities_3_and_5[ , check2 := get(paste0("stroke_split_draw_", full, " severity level 3")) - (moderate_w_dem + moderate_w_cogn_imp)]
    max(severities_3_and_5$check2)
    min(severities_3_and_5$check2)
    severities_3_and_5[ , check3 := get(paste0("dem_due_to_", abbr, "_draw")) - (moderate_w_dem + severe_w_dem)]
    max(severities_3_and_5$check3)
    min(severities_3_and_5$check3)
  }
  
  # save sequela with cognitive impairment
  for(severity in c(paste0(full, " severity level ", c(3,5)))){
    
    if (severity==paste0(full, " severity level 3")){
      column <- "moderate_w_cogn_imp"
    } else{
      column <- "severe_w_cogn_imp"
    }
    
    # save for without HF
    severities_3_and_5[, paste0(column, "_no_hf") := get(column)*get(paste0("prop_no_hf_", severity))]
    
    # account for negative values
    if(nrow(severities_3_and_5[get(paste0(column, "_no_hf"))<0,])>0){
      message("Severity level 3/5 without heart failure: Setting draws below 0 to be 0")
      message(paste0("This occurs in ", nrow(severities_3_and_5[get(paste0(column, "_no_hf"))<0,]), " row(s) out of ", nrow(severities_3_and_5), " rows, the smallest one being ", 
                     min(severities_3_and_5[get(paste0(column, "_no_hf"))<0,get(paste0(column, "_no_hf"))])))
    }
    severities_3_and_5[get(paste0(column, "_no_hf"))<0, paste0(column, "_no_hf"):=0]
    
    severities_3_and_5_no_hf <- dcast(severities_3_and_5, age_group_id + sex_id + location_id + year_id + measure_id + metric_id ~ draw, value.var = 
                                        paste0(column, "_no_hf"))
    
    # save as csv
    me_id <- me_ids[target_meid_name==paste0(severity, ", without heart failure, no dementia")]$target_meid
    
    severities_3_and_5_no_hf <- as.data.table(severities_3_and_5_no_hf)
    
    if(any(severities_3_and_5_no_hf[, paste0('draw_', 0:999), with=F]<0)){
      stop(paste0('Negative values detected for ME ID ', me_id))
    }
    
    folder <- paste0(output_folder, "me_", me_id, "/")
    dir.create(folder, showWarnings = FALSE)
    write.csv(severities_3_and_5_no_hf, file = 
                paste0(folder, location, ".csv"), row.names = FALSE)
    
    # save for with HF
    severities_3_and_5[, paste0(column, "_with_hf") := get(column)*get(paste0("prop_hf_", severity))]
    
    cols_to_keep <- c("age_group_id", "sex_id", "year_id", "location_id", "measure_id", "metric_id", "draw", paste0(column, "_with_hf"))
    
    severities_3_and_5_with_hf <- severities_3_and_5[, ..cols_to_keep]
    
    # merge data with HF severity splits
    severities_3_and_5_with_hf <- merge(severities_3_and_5_with_hf, HF_split, by=c("measure_id", "draw"), allow.cartesian = TRUE)
    
    # multiply chronic stroke with hf data by HF severity split
    severities_3_and_5_with_hf[, hf_sev_stroke_split_draw := get(paste0(column, "_with_hf")) * HF_split_prop]
    
    # add in me name and me id
    severities_3_and_5_with_hf[, target_meid_name := paste0(severity, ", with ", target_severity, ", no dementia")]
    severities_3_and_5_with_hf <- merge(severities_3_and_5_with_hf, me_ids, by="target_meid_name")
    
    # subset and save each hf severity level
    for (meid in unique(severities_3_and_5_with_hf$target_meid)){
      final <- severities_3_and_5_with_hf[target_meid==meid]
      
      # account for negative values
      if(nrow(final[hf_sev_stroke_split_draw<0,])>0){
        message("Severity level 3/5 with heart failure: Setting draws below 0 to be 0")
        message(paste0("This occurs in ", nrow(final[hf_sev_stroke_split_draw<0,]), " row(s) out of ", nrow(final), " rows, the smallest one being ", 
                       min(final[hf_sev_stroke_split_draw<0,hf_sev_stroke_split_draw])))
      }
      final[hf_sev_stroke_split_draw<0, hf_sev_stroke_split_draw:=0]
      
      final <- dcast(final, location_id + age_group_id + sex_id + year_id + measure_id + metric_id ~ draw, value.var = "hf_sev_stroke_split_draw")
      final <- as.data.table(final)
      
      if(any(final[, paste0('draw_', 0:999), with=F]<0)){
        stop(paste0('Negative values detected for ME ID ', meid))
      }
      
      folder <- paste0(output_folder, "me_", meid, "/")
      dir.create(folder, showWarnings = FALSE)
      write.csv(final, file = 
                  paste0(folder, location, ".csv"), row.names = FALSE)
      
    }
    
  }
  
  ##################################################################
  
  # split moderate and severe stroke with dementia into dementia severities
  
  for(severity in c("Moderate", "Severe")){
    
    if(severity=="Moderate"){
      sev_level <- 3
    } else {
      sev_level <- 5
    }
    
    cols_to_keep <- c("age_group_id", "sex_id", "location_id", "year_id", "measure_id", "metric_id", "draw", paste0(tolower(severity), "_w_dem"), paste0("prop_hf_", full, " severity level ", sev_level), paste0("prop_no_hf_", full, " severity level ", sev_level))
    severity_subset <- severities_3_and_5[,..cols_to_keep]
    
    # separate out young age groups that are not modeled for dementia
    severity_subset_young_ages <- severity_subset[!(age_group_id%in%dementia_split$age_group_id)]
    setnames(severity_subset_young_ages, paste0(tolower(severity), "_w_dem"), "dementia_split_stroke_draw")
    
    if(F){
      # quick check
      table(severity_subset$age_group_id)
    }
    
    # merge split stroke data with dementia severity splits -- youngest age groups will be dropped 
    severity_subset <- merge(severity_subset, dementia_split, by=c("age_group_id", "sex_id", "measure_id", "draw"), allow.cartesian = TRUE)
    
    if(F){
      # quick check -- merged stroke data should be 3x original stroke data for the 3 severity splits
      table(dementia_split$age_group_id)
      table(severity_subset$age_group_id)
    }
    
    # multiply chronic stroke with dementia data by dementia severity split
    severity_subset[, dementia_split_stroke_draw := get(paste0(tolower(severity), "_w_dem")) * dem_split_prop]
    severity_subset[, paste0(tolower(severity), "_w_dem") := NULL]
    severity_subset[, dem_split_prop := NULL]
    
    # split into 3 datasets for mild, moderate, severe dementia and add back young ages that were dropped in merge with dementia severity splits
    for(dem_sev in c(1944, 1945, 1946)){
      severity_subset_dementia <- severity_subset[target_meid==dem_sev]
      
      if (dem_sev==1944){
        dem_name <- "mild dementia"
      }
      if (dem_sev==1945){
        dem_name <- "moderate dementia"
      }
      if (dem_sev==1946){
        dem_name <- "severe dementia"
      }
      
      severity_subset_dementia$target_meid=NULL
      severity_subset_dementia$target_meid_name=NULL
      
      severity_subset_dementia <- rbind(severity_subset_dementia, severity_subset_young_ages)
      
      # save for without HF
      severity_subset_dementia[, no_hf_dementia_split_stroke_draw := dementia_split_stroke_draw*get(paste0("prop_no_hf_", full, " severity level ", sev_level))]
      
      # account for negative values
      if(nrow(severity_subset_dementia[no_hf_dementia_split_stroke_draw<0,])>0){
        message("Severity level 3/5 with dementia: Setting draws below 0 to be 0")
        message(paste0("This occurs in ", nrow(severity_subset_dementia[no_hf_dementia_split_stroke_draw<0,]), " row(s) out of ", nrow(severity_subset_dementia), " rows, the smallest one being ", 
                       min(severity_subset_dementia[no_hf_dementia_split_stroke_draw<0,no_hf_dementia_split_stroke_draw])))
      }
      severity_subset_dementia[no_hf_dementia_split_stroke_draw<0, no_hf_dementia_split_stroke_draw:=0]
      
      severity_subset_dementia_no_hf <- dcast(severity_subset_dementia, age_group_id + sex_id + location_id + year_id + measure_id + metric_id ~ draw, value.var = 
                                                "no_hf_dementia_split_stroke_draw")
      
      # save as csv
      meid <- me_ids[target_meid_name==paste0(full, " severity level ", sev_level, ", without heart failure, with ", dem_name)]$target_meid
      
      # fix for misspelling of subarachnoid in ME ID name
      if (length(meid)==0){
        meid <- me_ids[target_meid_name==gsub("subarachnoid", "subarachwithid", paste0(full, " severity level ", sev_level, ", without heart failure, with ", dem_name))]$target_meid
      }
      
      severity_subset_dementia_no_hf <- as.data.table(severity_subset_dementia_no_hf)
      
      if(any(severity_subset_dementia_no_hf[, paste0('draw_', 0:999), with=F]<0)){
        stop(paste0('Negative values detected for ME ID ', meid))
      }
      
      folder <- paste0(output_folder, "me_", meid, "/")
      dir.create(folder, showWarnings = FALSE)
      write.csv(severity_subset_dementia_no_hf, file = 
                  paste0(folder, location, ".csv"), row.names = FALSE)
      
      # save for with HF
      severity_subset_dementia[, hf_dementia_split_stroke_draw := dementia_split_stroke_draw*get(paste0("prop_hf_", full, " severity level ", sev_level))]
      
      severity_subset_dementia_with_hf <- severity_subset_dementia[, c("age_group_id", "sex_id", "year_id", "location_id", "measure_id", "metric_id", "draw", "hf_dementia_split_stroke_draw")]
      
      # merge data with HF severity splits
      severity_subset_dementia_with_hf <- merge(severity_subset_dementia_with_hf, HF_split, by=c("measure_id", "draw"), allow.cartesian = TRUE)
      
      # multiply chronic stroke with hf data by HF severity split
      severity_subset_dementia_with_hf[, hf_sev_dementia_split_stroke_draw := hf_dementia_split_stroke_draw * HF_split_prop]
      
      # add in me name and me id
      severity_subset_dementia_with_hf[, target_meid_name := paste0(full, " severity level ", sev_level, ", with ", target_severity, ", with ", dem_name)]
      # fix for misspelling of subarachnoid in ME ID name
      me_ids[target_meid_name%like%"subarachwithid", target_meid_name:=gsub("subarachwithid", "subarachnoid", target_meid_name)]
      severity_subset_dementia_with_hf <- merge(severity_subset_dementia_with_hf, me_ids, by="target_meid_name")
      
      # subset and save each hf severity level
      for (meid in unique(severity_subset_dementia_with_hf$target_meid)){
        final <- severity_subset_dementia_with_hf[target_meid==meid]
        
        final <- dcast(final, location_id + age_group_id + sex_id + year_id + measure_id + metric_id ~ draw, value.var = "hf_sev_dementia_split_stroke_draw")
        
        final <- as.data.table(final)
        
        if(any(final[, paste0('draw_', 0:999), with=F]<0)){
          stop(paste0('Negative values detected for ME ID ', meid))
        }
        
        folder <- paste0(output_folder, "me_", meid, "/")
        dir.create(folder, showWarnings = FALSE)
        write.csv(final, file = 
                    paste0(folder, location, ".csv"), row.names = FALSE)
        
      }
      
    }
    
  }
  
}


