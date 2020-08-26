arguments <- commandArgs()[-(1:5)]

#Source packages
library(plyr)
library(data.table)
library(dplyr)
library(magrittr)
library(tidyr)
source('FILEPATH')
source('FILEPATH')
source('FILEPATH')

#Read arguments and make necessary variables
paf_directory <- arguments[1]

param_map <- fread('FILEPATH')
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))

location   <- param_map[task_id, location]
years         <- c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)
measures      <- c(3, 4) #yld & yll
sexes         <- c(1, 2)
causes        <- c(420, 524) #liver cancer & cirrhosis (here, due to alcohol because that is how they are defined upstream)
ages          <- c(1, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235)

#Read in data

# Cirrhosis; ylds
cirr_b_yld      <- interpolate(gbd_id_type='cause_id', gbd_id=522, source='como', measure_id = 3, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", reporting_year_start=1990, reporting_year_end=2019)
cirr_c_yld      <- interpolate(gbd_id_type='cause_id', gbd_id=523, source='como', measure_id = 3, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", reporting_year_start=1990, reporting_year_end=2019)
cirr_alc_yld    <- interpolate(gbd_id_type='cause_id', gbd_id=524, source='como', measure_id = 3, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", reporting_year_start=1990, reporting_year_end=2019)
cirr_other_yld  <- interpolate(gbd_id_type='cause_id', gbd_id=525, source='como', measure_id = 3, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", reporting_year_start=1990, reporting_year_end=2019)
cirr_nash_yld   <- interpolate(gbd_id_type='cause_id', gbd_id=971, source='como', measure_id = 3, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", reporting_year_start=1990, reporting_year_end=2019)

# Cirrhosis; ylls
cirr_b_yll     <- interpolate(gbd_id_type='cause_id', gbd_id=522, source='codcorrect', measure_id = 4, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", version = 101, reporting_year_start=1990, reporting_year_end=2019)
cirr_c_yll     <- interpolate(gbd_id_type='cause_id', gbd_id=523, source='codcorrect', measure_id = 4, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", version = 101, reporting_year_start=1990, reporting_year_end=2019)
cirr_alc_yll   <- interpolate(gbd_id_type='cause_id', gbd_id=524, source='codcorrect', measure_id = 4, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", version = 101, reporting_year_start=1990, reporting_year_end=2019)
cirr_other_yll <- interpolate(gbd_id_type='cause_id', gbd_id=525, source='codcorrect', measure_id = 4, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", version = 101, reporting_year_start=1990, reporting_year_end=2019)
cirr_nash_yll  <- interpolate(gbd_id_type='cause_id', gbd_id=971, source='codcorrect', measure_id = 4, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", version = 101, reporting_year_start=1990, reporting_year_end=2019)

# Liver Cancer; ylds
lc_b_yld     <- interpolate(gbd_id_type='cause_id', gbd_id=418, source='como', measure_id = 3, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", reporting_year_start=1990, reporting_year_end=2019)
lc_c_yld     <- interpolate(gbd_id_type='cause_id', gbd_id=419, source='como', measure_id = 3, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", reporting_year_start=1990, reporting_year_end=2019)
lc_alc_yld   <- interpolate(gbd_id_type='cause_id', gbd_id=420, source='como', measure_id = 3, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", reporting_year_start=1990, reporting_year_end=2019)
lc_other_yld <- interpolate(gbd_id_type='cause_id', gbd_id=421, source='como', measure_id = 3, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", reporting_year_start=1990, reporting_year_end=2019)
lc_nash_yld  <- interpolate(gbd_id_type='cause_id', gbd_id=996, source='como', measure_id = 3, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", reporting_year_start=1990, reporting_year_end=2019)

# Liver Cancer; ylls
lc_b_yll     <- interpolate(gbd_id_type='cause_id', gbd_id=418, source='codcorrect', measure_id = 4, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", version = 101, reporting_year_start=1990, reporting_year_end=2019)
lc_c_yll     <- interpolate(gbd_id_type='cause_id', gbd_id=419, source='codcorrect', measure_id = 4, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", version = 101, reporting_year_start=1990, reporting_year_end=2019)
lc_alc_yll   <- interpolate(gbd_id_type='cause_id', gbd_id=420, source='codcorrect', measure_id = 4, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", version = 101, reporting_year_start=1990, reporting_year_end=2019)
lc_other_yll <- interpolate(gbd_id_type='cause_id', gbd_id=421, source='codcorrect', measure_id = 4, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", version = 101, reporting_year_start=1990, reporting_year_end=2019)
lc_nash_yll  <- interpolate(gbd_id_type='cause_id', gbd_id=996, source='codcorrect', measure_id = 4, location_id= location, sex_id = c(1,2), age_group_id = ages, gbd_round_id=6, decomp_step = "step4", version = 101, reporting_year_start=1990, reporting_year_end=2019)

lc_other_yld <- lc_other_yld[,cause_id := 1021] #no global ylls or ylds from cause_id 1005 available and will have same result
lc_other_yll <- lc_other_yll[,cause_id := 1021]

table(lc_other_yll$cause_id)


all_negatives <- NULL


for (sex in sexes){
  for (year in years){
    for (measure in measures){
      
m <- ifelse(measure == 3, "yld", "yll")      

# make sure that output file has not been started ("started") and output file hasn't yet been fully written ("incomplete")
make <- paste0("paf_",m,"_",location,"_",year,"_",sex,".csv")

started <- 0
incomplete <- 1

#has output file been created?
if (length(grep(list.files('FILEPATH'), pattern = make)) > 0){
  
  started <- 1
  paf_test <- fread('FILEPATH')
  
  # is output complete?
  if (nrow(paf_test) == 731 | nrow(paf_test) == 851 | nrow(paf_test) == 919){ 
  
    incomplete <- 0
  }
}

message(started)
message(incomplete)

if (started == 1 & incomplete == 0){
  print("done!")
  
  
} else {
  
  paf_full <- fread('FILEPATH')
  
  for (cause in causes){
    
    # call in PAFs 
    paf <- paf_full[cause_id == cause]
    
    paf[cause_id == 524, cause_id := 521]
    paf[cause_id == 420, cause_id := 417]
    
    paf      <- melt(paf, id.vars = c("year_id", "age_group_id", "cause_id"))
    paf      <- setnames(paf, old = c("variable", "value"), new = c("draw", "paf"))
    paf$draw <- gsub("paf_","",paf$draw)
    paf$draw <- as.numeric(paf$draw)
    
    # call in ylls or ylds
    ages <- unique(paf$age_group_id)
    
    if (measure == 3){
      pops   <- get_population(location_id=location, year_id=year, sex_id = sex, age_group_id = ages, gbd_round_id = 6, decomp_step = "step4")
      pops$run_id <- NULL
      
      
      if (cause == 524){
        
        cause_b     <- cirr_b_yld[year_id == year & sex_id == sex]
        cause_c     <- cirr_c_yld[year_id == year & sex_id == sex]
        cause_alc   <- cirr_alc_yld[year_id == year & sex_id == sex]
        cause_other <- cirr_other_yld[year_id == year & sex_id == sex]
        cause_nash  <- cirr_nash_yld[year_id == year & sex_id == sex]
        
      }
      
      if (cause == 420){
        
        
        cause_b     <- lc_b_yld[year_id == year & sex_id == sex]
        cause_c     <- lc_c_yld[year_id == year & sex_id == sex]
        cause_alc   <- lc_alc_yld[year_id == year & sex_id == sex]
        cause_other <- lc_other_yld[year_id == year & sex_id == sex]
        cause_nash  <- lc_nash_yld[year_id == year & sex_id == sex]
        
      }
    
      cause_b     <- melt(cause_b, id.vars = c("age_group_id", "cause_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id"))
      cause_c     <- melt(cause_c, id.vars = c("age_group_id", "cause_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id"))
      cause_alc   <- melt(cause_alc, id.vars = c("age_group_id", "cause_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id"))
      cause_other <- melt(cause_other, id.vars = c("age_group_id", "cause_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id"))
      cause_nash  <- melt(cause_nash, id.vars = c("age_group_id", "cause_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id"))
      
      cause_b     <- merge(cause_b, pops, by = c("age_group_id", "location_id", "sex_id", "year_id"), all = T)
      cause_c     <- merge(cause_c, pops, by = c("age_group_id", "location_id", "sex_id", "year_id"), all = T)
      cause_alc   <- merge(cause_alc, pops, by = c("age_group_id", "location_id", "sex_id", "year_id"), all = T)
      cause_other <- merge(cause_other, pops, by = c("age_group_id", "location_id", "sex_id", "year_id"), all = T)
      cause_nash  <- merge(cause_nash, pops, by = c("age_group_id", "location_id", "sex_id", "year_id"), all = T)
      
      cause_b[, value := value*population]
      cause_b$population <- NULL
      
      cause_c[, value := value*population]
      cause_c$population <- NULL
      
      cause_alc[, value := value*population]
      cause_alc$population <- NULL
      
      cause_other[, value := value*population]
      cause_other$population <- NULL
      
      cause_nash[, value := value*population]
      cause_nash$population <- NULL
      
      
    } else if (measure == 4){
      
      if (cause == 524){
        

        cause_b     <- cirr_b_yll[year_id == year & sex_id == sex]
        cause_c     <- cirr_c_yll[year_id == year & sex_id == sex]
        cause_alc   <- cirr_alc_yll[year_id == year & sex_id == sex]
        cause_other <- cirr_other_yll[year_id == year & sex_id == sex]
        cause_nash  <- cirr_nash_yll[year_id == year & sex_id == sex]
      }
      
      if (cause == 420){
        
        cause_b     <- lc_b_yll[year_id == year & sex_id == sex]
        cause_c     <- lc_c_yll[year_id == year & sex_id == sex]
        cause_alc   <- lc_alc_yll[year_id == year & sex_id == sex]
        cause_other <- lc_other_yll[year_id == year & sex_id == sex]
        cause_nash  <- lc_nash_yll[year_id == year & sex_id == sex]
        
      }
      

      cause_b     <- melt(cause_b, id.vars = c("age_group_id", "cause_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id"))
      cause_c     <- melt(cause_c, id.vars = c("age_group_id", "cause_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id"))
      cause_alc   <- melt(cause_alc, id.vars = c("age_group_id", "cause_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id"))
      cause_other <- melt(cause_other, id.vars = c("age_group_id", "cause_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id"))
      cause_nash  <- melt(cause_nash, id.vars = c("age_group_id", "cause_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id"))
      
    }
    
    
    # some data manipulation
    cause_b <- setnames(cause_b, old = "value", new = "hep_b")
    cause_b[,c("cause_id")] <- NULL
    
    cause_c <- setnames(cause_c, old = "value", new = "hep_c")
    cause_c[,c("cause_id")] <- NULL
    
    cause_alc <- setnames(cause_alc, old = "value", new = "alcohol")
    cause_alc[,c("cause_id")] <- NULL
    
    cause_other <- setnames(cause_other, old = "value", new = "other")
    cause_other[,c("cause_id")] <- NULL
    
    cause_nash <- setnames(cause_nash, old = "value", new = "nash")
    cause_nash[,c("cause_id")] <- NULL
    
    # compile all of them for each location/ age/ sex/ year
    cause_total <- merge(cause_b, cause_c, by = c("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id", "variable"), all = T)
    cause_total <- merge(cause_total, cause_alc, by = c("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id", "variable"), all = T)
    cause_total <- merge(cause_total, cause_other, by = c("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id", "variable"), all = T)
    cause_total <- merge(cause_total, cause_nash, by = c("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id", "variable"), all = T)
    
    # calculate proportion of total cirrhosis ylls that are attributable to each cause
    cause_total[, total := rowSums(cause_total[,c("hep_b", "hep_c", "alcohol", "other", "nash")], na.rm = T)]
    
    cause_total[, hep_b_percent := hep_b/total]
    cause_total[, hep_c_percent := hep_c/total]
    cause_total[, alcohol_percent := alcohol/total]
    cause_total[, other_percent := other/total]
    cause_total[, nash_percent := nash/total]
    
    # some data manipulation to allow for merging
    cause_total$variable <- gsub("draw_", "", cause_total$variable)
    cause_total <- setnames(cause_total, old = "variable", new = "draw")
    cause_total$draw <- as.numeric(cause_total$draw)
    
    
    # merge the proportions onto the actual all-cirrhosis pafs & subset them to make the file easier to read
    paf <- merge(paf, cause_total, by = c("age_group_id", "year_id", "draw"), all.x = T)
    
    # calculate the portion of the all cirrhosis pafs that should be applied to each of the sub-causes
    paf[,paf_without_alc := paf - alcohol_percent]
    
    # account for negative pafs; they are logically implausible so we will need to discuss with nonfatal teams down the line to get in alignment w/ envelope sizes
    negatives <- paf[paf_without_alc < 0]
    negatives <- negatives[,c("age_group_id", "year_id", "draw", "location_id", "sex_id")]
    negatives <- negatives %>% dplyr::group_by(age_group_id, year_id, location_id, sex_id) %>% dplyr::summarise(draws = n())
    all_negatives <- rbind(all_negatives, negatives)
    
    paf[paf_without_alc < 0, paf := alcohol_percent]
    paf[paf_without_alc < 0, paf_without_alc := 0]
    
    # redistribute
    paf[,paf_portion_alcohol := alcohol_percent]
    paf[,paf_portion_nash := 0]
    paf[,paf_portion_hepb := paf_without_alc* (hep_b_percent/ (hep_b_percent + hep_c_percent + other_percent))]
    paf[,paf_portion_hepc := paf_without_alc* (hep_c_percent/ (hep_b_percent + hep_c_percent + other_percent))]
    paf[,paf_portion_other := paf_without_alc* (other_percent/ (hep_b_percent + hep_c_percent + other_percent))]
    
    if (cause == 524){
      
      # rescale the pafs to be out of each sub-cause envelope rather than the all-cirrhosis envelope
      paf[,paf_524 := paf_portion_alcohol/alcohol_percent]
      paf[,paf_971 := paf_portion_nash/nash_percent]
      paf[,paf_522 := paf_portion_hepb/hep_b_percent]
      paf[,paf_523 := paf_portion_hepc/hep_c_percent]
      paf[,paf_525 := paf_portion_other/other_percent]
      
      paf[alcohol_percent == 0,paf_524 := 0]
      paf[nash_percent == 0   ,paf_971 := 0]
      paf[hep_b_percent == 0  ,paf_522 := 0]
      paf[hep_c_percent == 0  ,paf_523 := 0]
      paf[other_percent == 0  ,paf_525 := 0]
      
      paf[paf_524 > 1 ,paf_524 := 1]
      paf[paf_971 > 1 ,paf_971 := 1]
      paf[paf_522 > 1 ,paf_522 := 1]
      paf[paf_523 > 1 ,paf_523 := 1]
      paf[paf_525 > 1 ,paf_525 := 1]
      
    }
    
    if (cause == 420){
      
      # rescale the pafs to be out of each sub-cause envelope rather than the all-cirrhosis envelope
      paf[,paf_420 := paf_portion_alcohol/alcohol_percent]
      paf[,paf_996 := paf_portion_nash/nash_percent]
      paf[,paf_418 := paf_portion_hepb/hep_b_percent]
      paf[,paf_419 := paf_portion_hepc/hep_c_percent]
      paf[,paf_1021 := paf_portion_other/other_percent]
      
      paf[alcohol_percent == 0,paf_420 := 0]
      paf[nash_percent == 0   ,paf_996 := 0]
      paf[hep_b_percent == 0  ,paf_418 := 0]
      paf[hep_c_percent == 0  ,paf_419 := 0]
      paf[other_percent == 0  ,paf_1021 := 0]
      
      paf[paf_420 > 1 ,paf_420 := 1]
      paf[paf_996 > 1 ,paf_996 := 1]
      paf[paf_418 > 1 ,paf_418 := 1]
      paf[paf_419 > 1 ,paf_419 := 1]
      paf[paf_1021 > 1 ,paf_1021 := 1]
      
    }
    
    paf[,c("paf_portion_alcohol", "paf_portion_nash","paf_portion_hepb","paf_portion_hepc", "paf_portion_other", "paf_without_alc", "paf", "location_id", "measure_id", "sex_id", "metric_id",
           "hep_c", "hep_b", "alcohol", "other", "nash", "total", "hep_b_percent", "hep_c_percent", "alcohol_percent", "other_percent", "nash_percent")] <- NULL
    paf$cause_id <- NULL
    
    paf <- melt(paf, id.vars = c("age_group_id", "year_id", "draw"))
    paf$cause_id <- gsub("paf_","",paf$variable)
    paf$variable <- NULL
    
    paf$draw <- paste0("paf_", paf$draw)
    paf <- dcast(paf, age_group_id + year_id + cause_id ~ draw, value.var = "value")
    
    paf_full <- paf_full[cause_id != cause]
    paf_full <- rbind(paf_full, paf)
    
    
  }
  
  paf_full <- paf_full[cause_id != 521 & cause_id != 704 & cause_id != 417]
  table(paf_full$cause_id)
  write.csv(paf_full, 'FILEPATH', row.names = F)
  
  
  
}
  
}
}
}

if (nrow(all_negatives > 0)){
  write.csv(all_negatives, 'FILEPATH', row.names = F)
}

