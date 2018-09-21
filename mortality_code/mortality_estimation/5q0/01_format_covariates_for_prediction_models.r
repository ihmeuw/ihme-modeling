################################################################################
## Description: Formats 5q0 data and covariates for the first and second
##              stage models
################################################################################

  rm(list=ls())
  
  library(foreign); library(reshape); library(plyr); library(data.table); library(haven)

if (Sys.info()[1] == 'Windows') {
  username <- "USERNAME"
  root <- "FILEPATH"
  code_dir <- ""
  source("FILEPATH/shared/functions/get_locations.r")
  hivsims <- F
  rnum <- 1
} else {
  username <- commandArgs()[5]
  root <- "FILEPATH"
  code_dir <- "FILEPATH" 
  source(paste0("FILEPATH/get_locations.r"))
  rnum <- commandArgs()[3]
  hivsims <- as.integer(commandArgs()[4])
}

  print(hivsims)
  print(rnum)
  print(username)

####################
## Load covariates 
####################

## make a square dataset onto which we'll merge all covariates and data
  data <- read.csv("FILEPATH/locations.csv")
  data <- data[data$level_all == 1 | data$ihme_loc_id=="IND_44849",]
  data <- unique(na.omit(data[,c("ihme_loc_id","region_name","super_region_name","location_id","location_name")]))
  all_locs <- length(unique(data$ihme_loc_id))

  data$region_name <- gsub(" ", "_", gsub(" / ", "_", gsub(", ", "_", data$region_name)))
  data$super_region_name <- gsub(" ", "_", gsub("/", "_", gsub(", ", "_", data$super_region_name)))
  data$region_name[data$ihme_loc_id %in% c("GUY","TTO","BLZ","JAM","ATG","BHS","BMU","BRB","DMA","GRD","VCT","LCA","PRI")] <- "CaribbeanI"
  data <- merge(data, data.frame(year=1950:2016))

## get placeholder hiv numbers  
  hiv <- read_dta("FILEPATH/compiled_hiv_summary.dta")
  setnames(hiv, old="hiv_cdr", new="hiv")
  setnames(hiv, old="iso3", new="ihme_loc_id")
  hiv <- hiv[hiv$sex=="both",]
  hiv <- hiv[,c("year","ihme_loc_id","hiv")]
  
  ## population weight AP and Telangana hiv CDR to get old AP hiv CDR
  hiv_ap <- hiv[hiv$ihme_loc_id %in% c("IND_4841", "IND_4871"),]
  pop_ap <- fread(paste0(root, "FILEPATH/population_gbd2016_M.csv"))
  pop_ap <- pop_ap[sex=="both" & age_group_id==1]
  pop_ap <- pop_ap[,.(ihme_loc_id, year, pop)]
  
  hiv_ap <- data.table(hiv_ap)
  hiv_ap <- merge(hiv_ap, pop_ap, by=c("ihme_loc_id", "year"))
  hiv_ap[,hiv := hiv*pop]
  setkey(hiv_ap, year)
  hiv_ap <- hiv_ap[,.(hiv=sum(hiv), pop=sum(pop)), by=key(hiv_ap)]
  hiv_ap[,hiv := hiv/pop]
  hiv_ap[,ihme_loc_id := "IND_44849"]
  hiv_ap[,pop:=NULL]
  hiv <- rbind(hiv, hiv_ap, use.names=T)
  hiv <- as.data.frame(hiv)

  
  data <- merge(data,hiv,by=c("ihme_loc_id","year"), all.x=T)  
  data$hiv[is.na(data$hiv)] <- 0
  t1 <- dim(data)
  
## need to get covariates from database query function written by collaborator
  source("FILEPATH/WORK/01_covariates/common/r_functions/load_cov_functions.r"))
  ldi <- data.frame(get_cov_estimates('LDI_pc'))
  ldi_model_number <- unique(ldi$model_version_id)
  ldi <- ldi[,c("location_id","year_id","mean_value")]
  names(ldi) <- c("location_id","year","LDI_id")
  

  
  ## population weight AP and Telangana ldi to get old AP ldi
  ldi_ap <- ldi[ldi$location_id %in% c("4841", "4871"),]
  popldi_ap <- fread("FILEPATH/population_gbd2016_M.csv")
  popldi_ap <- popldi_ap[sex=="both" & age_group_id==22]
  popldi_ap <- popldi_ap[,.(location_id, year, pop)]
  
  ldi_ap <- data.table(ldi_ap)
  ldi_ap <- merge(ldi_ap, popldi_ap, by=c("location_id", "year"))
  ldi_ap[,LDI_id := LDI_id*pop]
  setkey(ldi_ap, year)
  ldi_ap <- ldi_ap[,.(LDI_id=sum(LDI_id), pop=sum(pop)), by=key(ldi_ap)]
  ldi_ap[,LDI_id := LDI_id/pop]
  ldi_ap[,location_id := 44849]
  ldi_ap[,pop:=NULL]
  ldi <- rbind(ldi, ldi_ap, use.names=T)
  ldi <- as.data.frame(ldi)

  data <- merge(data,ldi,by=c("location_id","year"),all.x=T)
  stopifnot(length(unique(data$ihme_loc_id))==all_locs)
  
  educ <- data.frame(get_cov_estimates('maternal_educ_yrs_pc'))
  educ_model_number <- unique(educ$model_version_id)
  educ <- educ[,c("location_id","year_id","mean_value")]
  names(educ) <- c("location_id","year","maternal_educ")
  
  
  ## population weight AP and Telangana mat ed to get old AP mat ed
  ## weight by female 15-49 population
  
  educ_ap <- educ[educ$location_id %in% c("4841", "4871"),]
  popeduc_ap <- fread("FILEPATH/population_gbd2016_M.csv")
  popeduc_ap <- popeduc_ap[sex=="female" & age_group_name=="15-49 years"]
  popeduc_ap <- popeduc_ap[,.(location_id, year, pop)]
  
  educ_ap <- data.table(educ_ap)
  educ_ap <- merge(educ_ap, popeduc_ap, by=c("location_id", "year"))
  educ_ap[,maternal_educ := maternal_educ*pop]
  setkey(educ_ap, year)
  educ_ap <- educ_ap[,.(maternal_educ=sum(maternal_educ), pop=sum(pop)), by=key(educ_ap)]
  educ_ap[,maternal_educ := maternal_educ/pop]
  educ_ap[,location_id := 44849]
  educ_ap[,pop:=NULL]
  educ <- rbind(educ, educ_ap, use.names=T)
  educ <- as.data.frame(educ)
  
  
  data <- merge(data,educ,by=c("location_id","year"),all.x=T)
  stopifnot(length(unique(data$ihme_loc_id))==all_locs)

stopifnot(!is.na(data$hiv))
stopifnot(!is.na(data$LDI_id))
stopifnot(!is.na(data$maternal_educ))

## load 5q0 data (now from 02_adjust_biased_vr)-cal
  q5.data <- read.table("FILEPATH/raw.5q0.adjusted.txt", 
                    sep="\t", header=T, stringsAsFactors=F)
  ## 
  q5.data$source.date <- as.numeric(q5.data$source.date)
  names(q5.data)[names(q5.data) == "source.date"] <-"num_survey_date"
   
  #format
  names(q5.data)[names(q5.data) == "num_survey_date"] <- "source.yr" 
  q5.data$year <- floor(q5.data$year) + 0.5
                    

####################
## Merge everything together
####################
  data$year <- data$year + 0.5

## merge in 5q0 data
  data <- merge(data, q5.data[,names(q5.data) != "gbd_region"], by=c("ihme_loc_id", "year","location_name"), all.x=T)
  data$data <- as.numeric(!is.na(data$mort))      # this is an indicator for data avaialability across years -- still want to keep cy's where data is not available

## create variable for survey series indicator for survey random effects
  #this will identify 
  #SBH points by source, source year, and type (indirect)
  #CBH points by source and type (as we combine CBH data across source-years, and the years in source.yr don't actually relate to the survey dates)
  #VR points by source (just VR, assuming correlated across all years of VR)
  #NA points (SRS, CENSUSES, compiled estimates) by source (again, assuming correlated across all years of these estimates)
  #HH points by source (again, assuming correlated across years of estimation from one source)
  data$source1 <- rep(0, length(data$source))
  
  #sbh ind vector
  sbh_ind <- grepl("indirect", data$type, ignore.case = T)
  data$source1[sbh_ind] <- paste(data$source[sbh_ind], data$source.yr[sbh_ind], data$type[sbh_ind])
    
  #cbh indicator vector
  cbh_ind <- grepl("direct", data$type, ignore.case = T) & !grepl("indirect", data$type, ignore.case = T)
  data$source1[cbh_ind] <- paste(data$source[cbh_ind], data$type[cbh_ind]) 
  
  #hh indicator vector
  hh_ind <- grepl("hh", data$type, ignore.case = T)
  data$source1[hh_ind] <- paste(data$source[hh_ind], data$type[hh_ind])
  
  #everything else, only source
  data$source1[data$source1 == 0] <- data$source[data$source1 == 0]

  #fix for in-depth DHS's so that they have the same RE as normal DHS's (only PHL right now)
  data$source1[data$source1 == "DHS IN direct"] <- "DHS direct"
  data$source1[grepl("DHS SP",data$source1) & grepl("BGD|GHA|UZB",data$ihme_loc_id)] <- gsub("DHS SP","DHS", data$source1[grepl("DHS SP",data$source1) & grepl("BGD|GHA|UZB",data$ihme_loc_id)]) 
  
  #classify DHS completes from reports same as our DHS completes
  data$source1[grepl("DHS",data$source1, ignore.case = T) & grepl("report",data$source1, ignore.case = T) & grepl("direct",data$source1, ignore.case = T) & !grepl("indirect",data$source1, ignore.case = T)] <- "DHS direct"

  #classify province (and country) level DSP into before and after 2004
  #before = 0, after = 1
  dsp.ind <- data$source1 %in% c("DSP","China DSP hh")
  data$source1[dsp.ind] <- paste(data$source1[dsp.ind], as.numeric(data$year[dsp.ind] > 2004), sep = "_")
  
## format and save
  data <- data[order(data$ihme_loc_id, data$year),
    c("super_region_name", "region_name", "ihme_loc_id", "year", "LDI_id", "maternal_educ", "hiv", "mort", "category", "corr_code_bias","to_correct","source", "source.yr", "source1", "vr", "data","ptid","log10.sd.q5","location_name","type")]
  

  write.csv(data, ifelse(hivsims, "FILEPATH/prediction_input_data_",rnum,".txt",
                         "FILEPATH/prediction_input_data.txt", sep=""),
            row.names=F)

  if(!hivsims) write.csv(data,"FILEPATH/prediction_input_data_", Sys.Date(), ".txt",row.names=F)


