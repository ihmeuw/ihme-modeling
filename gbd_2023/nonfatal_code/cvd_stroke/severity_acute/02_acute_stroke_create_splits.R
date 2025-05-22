## Acute stroke severity splits
## Author: USERNAME
## Date: 
##
## Purpose: Acute severity splits for stroke (including stroke + HF) sequelae
##
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

# source central functions
central <- "FILEPATH"
for (func in paste0(central, list.files(central))) source(paste0(func))

##################################################################

# pull in ages
ages <- get_age_metadata(release_id = release_id)
setnames(ages, old=c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))

# pull in me id names to add to severity split data
me_ids <- get_ids("modelable_entity")
setnames(me_ids, c("modelable_entity_id", "modelable_entity_name"), c("target_meid", "target_meid_name"))

##################################################################

# pull HF severity splits (asymptomatic, mild, moderate, severe HF)
HF_split <- get_severity_split(source_meid=9569, release_id = 9)

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

##################################################################

for(parent in c(24714, 24706, 24710)){
  
  if(parent==24714){
    full <- "Acute ischemic stroke"
  }
  if(parent==24706){
    full <- "Acute intracerebral hemorrhage"
  }
  if(parent==24710){
    full <- "Acute subarachnoid hemorrhage"
  }
  
  message(paste0("-------- ", full, " ----------"))
  
  # pull in acute stroke severity splits (severity levels 1-5)
  stroke_split <- get_severity_split(source_meid=parent, gbd_round_id=6, decomp_step="step4")
  
  # reassign age_end of 95+ age group to match age metadata
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
  
  stroke_split <- stroke_split[, c("target_meid", "measure_id", "age_group_id", "sex_id", paste0("draw_", 0:999))]
  
  # reshape data
  stroke_split <- melt(stroke_split, measure.vars = names(stroke_split)[grepl("draw", names(stroke_split))], variable.name = "draw", value.name = "stroke_split_prop")
  
  # pull stroke severity level me ids
  sev_1 <- as.character(me_ids[target_meid_name==paste0(full, " severity level 1")]$target_meid)
  sev_2 <- as.character(me_ids[target_meid_name==paste0(full, " severity level 2")]$target_meid)
  sev_3 <- as.character(me_ids[target_meid_name==paste0(full, " severity level 3")]$target_meid)
  sev_4 <- as.character(me_ids[target_meid_name==paste0(full, " severity level 4")]$target_meid)
  sev_5 <- as.character(me_ids[target_meid_name==paste0(full, " severity level 5")]$target_meid)
  
  ##################################################################

    
  #message(path_to_draws_read)
  age_sex_combos_m <- paste0(paste(location,paste(ages$age_group_id, c(1),sep = '_'),sep="_"),'.csv')
  age_sex_combos_f <- paste0(paste(location,paste(ages$age_group_id, c(2),sep = '_'),sep="_"),'.csv')
  age_sex_combos <- c(age_sex_combos_m,age_sex_combos_f)
  files_to_read <- paste0(path_to_draws_read, age_sex_combos)
    
  stroke_draws <- list()
  for(f in files_to_read){
    acute_stroke_draw <- data.table(read.csv(f))
    stroke_draws[[f]] <- acute_stroke_draw
  }
  acute_stroke <- data.table(rbindlist(stroke_draws))
    
  }else{
    acute_stroke <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = parent, source="epi",
                              release_id = release_id, status = "best",
                              measure_id = c(5, 6), location_id = location, age_group_id = unique(ages$age_group_id), sex_id = c(1, 2))
  }
  # reshape data
  acute_stroke <- melt(acute_stroke, measure.vars = names(acute_stroke)[grepl("draw", names(acute_stroke))], variable.name = "draw", value.name = "acute_stroke_draw")
  
  # merge acute stroke model data with stroke severity splits 
  acute_stroke_split <- data.table(merge(acute_stroke, stroke_split, by=c("age_group_id", "sex_id", "draw", "measure_id"), allow.cartesian = TRUE))
  
  if(F){
    table(acute_stroke$age_group_id)
    table(acute_stroke_split$age_group_id)
  }
  
  # multiply acute stroke data by the stroke severity split value
  acute_stroke_split[, stroke_split_draw := acute_stroke_draw * stroke_split_prop]
  
  # save acute stroke severity level 1, since this has no heart failure
  acute_stroke_level_1 <- acute_stroke_split[target_meid==sev_1]
  
  acute_stroke_level_1 <- dcast(acute_stroke_level_1, location_id + age_group_id + sex_id + year_id + measure_id + metric_id ~ draw, value.var = "stroke_split_draw")
  setDT(acute_stroke_level_1)
  if(any(acute_stroke_level_1[, paste0('draw_', 0:999), with=F]<0)){
    stop(paste0('Negative values detected for ME ID ', sev_1))
  }
  
  folder <- paste0(output_folder, "me_", sev_1, "/")
  dir.create(folder, showWarnings = FALSE)
  write.csv(acute_stroke_level_1, file = 
              paste0(folder, location, ".csv"), row.names = FALSE)
  
  # remove stroke severity level 1 from stroke data
  acute_stroke_split <- acute_stroke_split[target_meid!=sev_1]
  
  ##################################################################
  
  # pull in draws from HF due to stroke model
  hf_due_stroke_meid <- me_ids[target_meid_name==paste0("Heart failure due to ", capwords(full))]$target_meid
  
  # reshape the data
  hf_due_stroke <- melt(hf_due_stroke, measure.vars = names(hf_due_stroke)[grepl("draw", names(hf_due_stroke))], variable.name = "draw", value.name = "hf_due_stroke_draw")
  
  ##################################################################
  
  # rescale stroke severities for severities 2-5 (since there is no HF in stroke severity 1) to use for splitting HF due to stroke into stroke levels
  stroke_split <- dcast(stroke_split, measure_id + age_group_id + sex_id + draw ~ target_meid, value.var = "stroke_split_prop")
  setDT(stroke_split)
  stroke_split[, rescaler := 1/(get(sev_2) +  get(sev_3) + get(sev_4) + get(sev_5))]
  
  stroke_split[, paste0(sev_1) := NULL]
  
  stroke_split[, paste0(sev_2) := get(sev_2) * rescaler]
  stroke_split[, paste0(sev_3) := get(sev_3) * rescaler]
  stroke_split[, paste0(sev_4) := get(sev_4) * rescaler]
  stroke_split[, paste0(sev_5) := get(sev_5) * rescaler]
  
  stroke_split[, rescaler := NULL]
  
  stroke_split <- melt(stroke_split, measure.vars = c(sev_2, sev_3, sev_4, sev_5), variable.name = "target_meid", value.name = "stroke_split_prop")
  
  ##################################################################
  
  # merge hf due to stroke model data with stroke severity splits 
  hf_due_stroke_split <- merge(hf_due_stroke, stroke_split, by=c("age_group_id", "sex_id", "draw", "measure_id"), allow.cartesian = TRUE)
  
  if(F){
    table(hf_due_stroke$age_group_id)
    table(hf_due_stroke_split$age_group_id)
  }
  
  # multiply hf due to stroke data by the stroke severity split value
  setDT(hf_due_stroke_split)
  hf_due_stroke_split[, hf_stroke_split_draw := hf_due_stroke_draw * stroke_split_prop]
  
  ##################################################################
  
  acute_stroke_split <- acute_stroke_split[, c("age_group_id", "sex_id", "draw", "measure_id", "location_id", "year_id", "metric_id", 
                                               "target_meid", "stroke_split_draw")]
  
  hf_due_stroke_split <- hf_due_stroke_split[, c("age_group_id", "sex_id", "draw", "measure_id", "location_id", "year_id", "metric_id", 
                                                 "target_meid", "hf_stroke_split_draw")]
  hf_due_stroke_split[, target_meid := as.numeric(as.character(target_meid))]
  
  # merge acute stroke data and hf due to stroke data (severities 2-5)
  hf_and_stroke <- merge(acute_stroke_split, hf_due_stroke_split, by=c("age_group_id", "sex_id", "draw", "measure_id", "location_id", 
                                                                       "year_id", "metric_id","target_meid"), all=TRUE)
  
  # fill in NA HF values (no incidence data for HF due to stroke) with 0
  hf_and_stroke[is.na(hf_stroke_split_draw), hf_stroke_split_draw := 0]
  
  # calculate acute stroke without heart failure data by substracting hf due to stroke from stroke
  hf_and_stroke[, stroke_no_hf_draw := stroke_split_draw - hf_stroke_split_draw]
  
  # assign draws that have more HF due to stroke than stroke to be 0 
  hf_and_stroke[stroke_no_hf_draw<0, stroke_no_hf_draw := 0]
  
  for (sev in c(2:5)){
    
    # subset the data to a specific stroke severity level
    subset <- hf_and_stroke[target_meid==get(paste0("sev_", sev))]
    
    # save acute stroke with no HF
    acute_stroke_no_hf <- dcast(subset, location_id + age_group_id + sex_id + year_id + measure_id + metric_id ~ draw, value.var = "stroke_no_hf_draw")
    no_hf_meid <- me_ids[target_meid_name==paste0(full, " severity level ", sev, ", without heart failure")]$target_meid
    setDT(acute_stroke_no_hf)
    if(any(acute_stroke_no_hf[, paste0('draw_', 0:999), with=F]<0)){
      stop(paste0('Negative values detected for ME ID ', no_hf_meid))
    }
    
    folder <- paste0(output_folder, "me_", no_hf_meid, "/")
    dir.create(folder, showWarnings = FALSE)
    write.csv(acute_stroke_no_hf, file = 
                paste0(folder, location, ".csv"), row.names = FALSE)
    
    # merge data with HF severity splits
    subset <- subset[, c("age_group_id", "sex_id", "draw", "measure_id", "location_id", "year_id", "metric_id", "hf_stroke_split_draw")]
    subset <- merge(subset, HF_split, by=c("measure_id", "draw"), allow.cartesian = TRUE)
    
    # multiply acute stroke with hf data by HF severity split
    subset[, hf_sev_stroke_split_draw := hf_stroke_split_draw * HF_split_prop]
    
    # add in me name and me id
    subset[, target_meid_name := paste0(full, " severity level ", sev, ", with ", target_severity)]
    
    # "fix" me names that are inconsistent (Acute ischemic stroke severity level 5) before merging
    fix_name <- function(x){
      temp <- unlist(strsplit(x, " due to "))
      return(paste0(temp[2], ", with ", tolower(temp[1])))
    }
    me_ids[target_meid_name%like%"heart failure due to Acute", target_meid_name := as.character(lapply(target_meid_name, fix_name))]
    
    subset <- merge(subset, me_ids, by="target_meid_name")
    
    # subset and save each hf severity level
    for (meid in unique(subset$target_meid)){
      final <- subset[target_meid==meid]
      
      final <- dcast(final, location_id + age_group_id + sex_id + year_id + measure_id + metric_id ~ draw, value.var = "hf_sev_stroke_split_draw")
      setDT(final)
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
