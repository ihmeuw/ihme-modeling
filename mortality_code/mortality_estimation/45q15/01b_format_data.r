################################################################################
## Description: Formats 45q15 data and covariates for the first and second
##              stage models
################################################################################

rm(list=ls())

library(foreign); library(plyr); library(reshape); library(data.table);library(haven); library(assertable); library(data.table)
if (Sys.info()[1]=="Windows") {
  root <- "filepath" 
  hivsims <- F
  hivupdate <- F
} else {
  root <- "filepath"
  print(commandArgs())
  hivsims <- as.logical(as.numeric(commandArgs()[3]))
  hivupdate <- as.logical(as.numeric(commandArgs()[4]))
  hivscalars <- as.logical(as.numeric(commandArgs()[5])) 
}

## Get covariates loading functions
source("filepath")

## set country list
codes <- read.csv("filepath")
iso3_map <- codes[,c("local_id_2013","ihme_loc_id")]
codes <- codes[,c("location_id","ihme_loc_id")]
years <- c(1950:2016)

####################
## Load covariates 
####################

## make a square dataset onto which we'll merge all covariates and data
data <- read.csv("filepath")
data <- data[,c("ihme_loc_id","location_id","location_name","super_region_name","region_name","parent_id")]
data$region_name <- gsub(" ", "_", gsub(" / ", "_", gsub(", ", "_", gsub("-","_",data$region_name))))
data$super_region_name <- gsub(" ", "_", gsub("/", "_", gsub(", ", "_", data$super_region_name)))
data <- merge(data, data.frame(year=1950:2016))

## load pop data, only for weighting the mean of education over ages.
source(filepath)
pop <- as.data.table(get_population(status = "recent", location_id = unique(data$location_id), year_id = years, sex_id = c(1:2), age_group_id = c(8:16)))
pop_ap <- as.data.table(get_population(status = "recent", location_id = c("4841", "4871"), year_id = c(1950:2016), sex_id = c(1:2), age_group_id = c(8:16)))
pop_ap <- pop_ap[,list(population = sum(population), location_id = 44849), by = c("age_group_id", "year_id", "sex_id", "process_version_map_id") ]
pop <-rbind(pop, pop_ap, use.names = T)
setnames(pop, c("year_id", "population"), c("year", "pop"))
pop <- pop[sex_id == 1,  sex := "male"]
pop <- pop[sex_id == 2, sex := "female"]
pop <- pop[,list(location_id,year,sex,age_group_id,pop)]

## load LDI, edu, and HIV from covariates DB
LDI <- as.data.frame(get_cov_estimates('LDI_pc'))
LDI <- LDI[,c("location_id","year_id","mean_value")]
names(LDI)[names(LDI)=="year_id"] <- "year"
names(LDI)[names(LDI)=="mean_value"] <- "LDI_id"

pop_ap <- as.data.table(get_population(status = "recent", location_id = c("4841", "4871"), year_id = c(1950:2016), sex_id = c(1:2), age_group_id = c(8:16)))
setnames(pop_ap, c("year_id"), c("year"))
pop_ap <- as.data.table(pop_ap[,list(pop = sum(population)), by = c("location_id", "year")])
LDI_ap <- as.data.table(LDI[LDI$location_id %in% c(4841, 4871),])
LDI_ap <- merge(LDI_ap, pop_ap, by=c("location_id", "year"))
LDI_ap[,LDI_id := LDI_id * pop]
setkey(LDI_ap, year)
LDI_ap <- LDI_ap[,.(LDI_id = sum(LDI_id), pop = sum(pop)), by = key(LDI_ap)]
LDI_ap[,LDI_id := LDI_id/pop]
LDI_ap[,location_id := 44849]
LDI_ap[,pop:= NULL]
LDI <- rbind(LDI, LDI_ap)

LDI <- merge(LDI,codes, by = c("location_id"), all=FALSE) # Drops China and England


edu <- as.data.frame(get_cov_estimates('education_yrs_pc'))
edu$year <- edu$year_id
edu <- edu[,c("location_id","year","sex_id","age_group_id","mean_value")]
names(edu)[names(edu)=="sex_id"] <- "sex"

edu <- edu[edu$year <=2016 & edu$age_group_id > 7 & edu$age_group_id < 17,]

pop_ap <- as.data.table(get_population(status = "recent", location_id = c("4841", "4871"), year_id = c(1950:2016), sex_id = c(1:2), age_group_id = c(8:16)))
setnames(pop_ap, c("year_id", "sex_id", "population"), c("year", "sex", "pop"))
edu_ap <- as.data.table(edu[edu$location_id %in% c(4841, 4871),])
edu_ap <- merge(edu_ap, pop_ap, by=c("location_id", "year", "sex", "age_group_id"))
edu_ap[,mean_value := mean_value * pop]
setkey(edu_ap, year, age_group_id, sex)
edu_ap <- edu_ap[,.(mean_value = sum(mean_value), pop = sum(pop)), by = key(edu_ap)]
edu_ap[,mean_value := mean_value/pop]
edu_ap[,location_id := 44849]
edu_ap[,pop:= NULL]
edu <- rbind(edu, edu_ap)

edu <- merge(edu,codes,all=FALSE, by = "location_id") # Drops China and England

hiv <- read_dta("filepath")

hiv <- hiv[hiv$agegroup == "45q15" & hiv$year >= 1970, c("iso3","sex","year","hiv_cdr")]
hiv$sex_new[hiv$sex == "male"] <- 1
hiv$sex_new[hiv$sex == "female"] <- 2
hiv$sex <- hiv$sex_new
hiv$sex_new <- NULL
names(hiv)[names(hiv)=="iso3"] <- "ihme_loc_id"
names(hiv)[names(hiv)=="hiv_cdr"] <- "death_rt_1559_mean"

hiv_ap <- hiv[hiv$ihme_loc_id %in% c("IND_4841", "IND_4871"),]
pop_ap <- as.data.table(get_population(status = "recent", location_id = c("4841", "4871"), year_id = c(1950:2016), sex_id = c(1:2), age_group_id = c(8:16)))
pop_ap[,ihme_loc_id := paste0("IND_", location_id)]
pop_ap <- pop_ap[,list(pop = sum(population)), by = c("ihme_loc_id", "year_id", "sex_id")]
setnames(pop_ap, c("year_id", "sex_id"), c("year", "sex"))

hiv_ap <- data.table(hiv_ap)
hiv_ap <- merge(hiv_ap, pop_ap, by=c("ihme_loc_id", "year", "sex"))
hiv_ap[,hiv := death_rt_1559_mean*pop]
setkey(hiv_ap, year, sex)
hiv_ap <- hiv_ap[,.(hiv=sum(hiv), pop=sum(pop)), by=key(hiv_ap)]
hiv_ap[,death_rt_1559_mean := hiv/pop]
hiv_ap[,ihme_loc_id := "IND_44849"]
hiv_ap[,pop:=NULL]
hiv_ap[,hiv:=NULL]
hiv <- rbind(hiv, hiv_ap, use.names=T)


# Add years before each country's min year, which had no HIV
hiv_rest <- data.table(hiv)
setkey(hiv_rest,ihme_loc_id,sex)
hiv_rest <- as.data.frame(hiv_rest[,
                                   list(year=1950:(min(year)-1),
                                        death_rt_1559_mean=0),
                                   key(hiv_rest)])
hiv <- rbind(hiv,hiv_rest)

# merge together
codmod <- merge(edu,hiv,by=c("ihme_loc_id","year","sex"), all.x = T)
codmod <- merge(codmod,LDI,by=c("location_id","ihme_loc_id","year"))
codmod$sex <- ifelse(codmod$sex==1,"male","female")

# format
codmod <- codmod[!is.na(codmod$age_group_id) & codmod$year <= 2016,]  
codmod$age_group_id <- as.numeric(codmod$age_group_id)
codmod <- codmod[codmod$age_group_id > 7 & codmod$age_group_id < 17 & codmod$ihme_loc_id %in% unique(data$ihme_loc_id),]
codmod <- merge(codmod, pop, by=c("location_id","sex","year","age_group_id"), all.x=T)
codmod <- ddply(codmod, c("ihme_loc_id", "year", "sex"), 
                function(x) { 
                  if (sum(is.na(x$pop))==0) w <- x$pop else w <- rep(1,length(x$pop))
                  data.frame(LDI_id = x$LDI_id[1], 
                             mean_yrs_educ = weighted.mean(x$mean_value, w),
                             hiv = ifelse(is.na(x$death_rt_1559_mean[1]), 0, x$death_rt_1559_mean[1])
                  )
                }
)
assert_values(codmod, colnames(codmod), "not_na")
id_vars <- list(year = years, sex = c("female", "male"), ihme_loc_id = unique(data$ihme_loc_id))
assert_ids(codmod, id_vars)
####################
## Prep data  
####################

## load 45q15 data, drop shocks and exclusions 
raw <- read.csv(paste("filepath", sep=""), header=T, stringsAsFactors=F)
raw <- subset(raw, raw$shock == 0 & raw$exclude == 0 & raw$adj45q15 < 1)
raw$year <- floor(raw$year)

## duplicate data in countries with only one data point (to avoid pinching in the GPR estimates) 
single <- table(raw$ihme_loc_id, raw$sex)
single <- melt(single)
single <- single[single$value == 1,]
single$ihme_loc_id <- as.character(single$Var.1)
single$sex <- as.character(single$Var.2)
for (ii in 1:nrow(single)) { 
  add <- subset(raw, raw$ihme_loc_id == single$ihme_loc_id[ii] & raw$sex == single$sex[ii]) 
  add$adj45q15 <- add$adj45q15*1.25
  raw <- rbind(raw, add) 
  add$adj45q15 <- (add$adj45q15/1.25)*0.75
  raw <- rbind(raw, add)
} 
raw <- raw[!is.na(raw$year),]
n_raw <- nrow(raw) # Find the number of data rows before merging, to make sure none get dropped prior to regression

## assign data categories 
## category I: Complete
raw$category[raw$adjust == "complete"] <- "complete" 
## category II: DDM adjusted (include all subnational)
raw$category[raw$adjust == "ddm_adjusted"] <- "ddm_adjust"
## category III: GB adjusted
raw$category[raw$adjust == "gb_adjusted"] <- "gb_adjust" 
## category IV: Unadjusted 
raw$category[raw$adjust == "unadjusted"] <- "no_adjust"
## category V: Sibs
raw$category[raw$source_type == "SIBLING_HISTORIES"] <- "sibs" 

## assign each country to a data group 
raw$vr <- as.numeric(grepl("VR|SRS", raw$source_type))
types <- ddply(raw, c("location_id","ihme_loc_id","sex"),
               function(x) {
                 cats <- unique(x$category)
                 vr <- mean(x$vr)
                 vr.max <- ifelse(vr == 0, 0, max(x$year[x$vr==1]))
                 vr.num <- sum(x$vr==1)
                 if (length(cats) == 1 & cats[1] == "complete" & vr == 1 & vr.max > 1980 & vr.num > 10) type <- "complete VR only"
                 else if (("ddm_adjust" %in% cats | "gb_adjust" %in% cats) & vr == 1 & vr.max > 1980 & vr.num > 10) type <- "VR only"
                 else if ((vr < 1 & vr > 0) | (vr == 1 & (vr.max <= 1980 | vr.num <= 10))) type <- "VR plus"
                 else if ("sibs" %in% cats & vr == 0) type <- "sibs"
                 else if (!"sibs" %in% cats & vr == 0) type <- "other"
                 else type <- "none"
                 return(data.frame(type=type, stringsAsFactors=F))
               })     



## drop excess variables
names(raw) <- gsub("adj45q15", "mort", names(raw))
names(raw) <- gsub("sd", "adjust.sd", names(raw))
raw <- raw[order(raw$ihme_loc_id, raw$year, raw$sex, raw$category),c("location_id","ihme_loc_id", "year", "sex", "obs45q15", "mort", "source_type", "category", "vr", "exposure", "comp", "adjust.sd")]

####################
## Merge everything together
####################

## merge all covariates to build a square dataset
data <- merge(data, codmod, all.x=T, by = c("ihme_loc_id","year"))

## merge in 45q15 data
data <- merge(data, raw, by=c("location_id","ihme_loc_id", "sex", "year"), all.x=T)
data$data <- as.numeric(!is.na(data$mort))
data$year <- data$year + 0.5

## merge in country data classification 
data <- merge(data, types, by=c("location_id","ihme_loc_id","sex"), all.x=T)
data$type <- as.character(data$type)
data$type[is.na(data$type)] <- "no data"

## Identify number of years covered by VR within each country after 1970
new_data <- data.table(unique(data[data$data==1 & !is.na(data$vr) & data$vr==1 & data$year>=1970,c("ihme_loc_id","year")]))
setkey(new_data,ihme_loc_id)
data_length <- as.data.frame(new_data[,length(year),by=key(new_data)])
names(data_length) <- c("ihme_loc_id","covered_years")

## Designate a cutoff point of covered years under which we will put into their own category
req_years <- 20
sibs_years <- 21 
data <- merge(data,data_length,by="ihme_loc_id",all.x=T)
data[is.na(data$covered_years),]$covered_years <- 0
data$ihme_loc_id <- as.character(data$ihme_loc_id)
data$type[data$type != "no data" & data$type != "sibs" & data$covered_years >= req_years] <- data$ihme_loc_id[data$type != "no data" & data$type != "sibs" & data$covered_years >= req_years]
data$type[data$type != "no data" & data$type != "sibs" & data$covered_years < req_years] <- paste0("sparse_data_",data$type[data$type != "no data" & data$type != "sibs" & data$covered_years < req_years])
data$type[data$type == "sibs" & data$covered_years >= sibs_years] <- "sibs_large"
data$type[data$type == "sibs" & data$covered_years < sibs_years] <- "sibs_small"

data$type[data$ihme_loc_id == "ALB"] <- "sparse_data_VR only" 
data$type[data$ihme_loc_id == "IND_44849"] <- "sparse_data_VR only"
for(cc in unique(data$ihme_loc_id[substr(data$ihme_loc_id, 1, 3) == "SAU"])) {
  data$type[data$ihme_loc_id == cc] <- cc
}

n_formatted <- nrow(data[!is.na(data$mort),])
if(n_raw != n_formatted) {
  stop(paste0("Number raw is ",n_raw," and number formatted is ", n_formatted))
}

## format and save
data <- data[order(data$ihme_loc_id, data$sex, data$year),
             c("location_id","location_name","super_region_name", "region_name",
               "parent_id", "ihme_loc_id", "sex", "year", "LDI_id", "mean_yrs_educ", "hiv", "obs45q15", "mort", "source_type", 
               "comp", "adjust.sd", "type", "category", "vr", "data", "exposure")]

write.csv(data, paste("filepath", sep=""),row.names=F)
write.csv(data, paste("filepath", sep=""),row.names=F)


## if we're going to use hiv sims, we need to save those here
if (hivsims & hivupdate) {
  shiv <- read.dta("filepath")

  shiv <- shiv[shiv$agegroup == "45q15" & shiv$year >= 1970,]
  shiv <- data.table(shiv)
  shiv <- shiv[,d_old := NULL]

  shiv_ap <- shiv[shiv$iso3 %in% c("IND_4841", "IND_4871"),]
  shiv_ap[,hiv_cdr := hiv_cdr*population]
  setkey(shiv_ap, year, sex, draw, agegroup)
  shiv_ap <- shiv_ap[,.(hiv_cdr=sum(hiv_cdr), population=sum(population)), by=key(shiv_ap)]
  shiv_ap[,hiv_cdr := hiv_cdr/population]
  shiv_ap[,iso3 := "IND_44849"]
  shiv <- rbind(shiv, shiv_ap, use.names=T)
  shiv <- as.data.frame(shiv)

  shiv$agegroup <- NULL
  names(shiv)[names(shiv) == "draw"] <- "sim"
  ## the hiv sims aren't numbered 1-250, so we need to make sure they are
  combos <- length(unique(shiv$year))*length(unique(shiv$sex))*length(unique(shiv$iso3))
  ## make square dataset because things may be missing, so we need to identify them
  iso_sim <- paste0(shiv$iso3,"&&",shiv$sim)
  sqr <- expand.grid(unique(shiv$year),unique(shiv$sex),unique(iso_sim))
  names(sqr) <- c("year","sex","iso_sim")
  sqr$iso_sim <- as.character(sqr$iso_sim)
  sqr$iso3 <- sapply(strsplit(sqr$iso_sim,"&&"),"[",1)
  sqr$sim <- as.numeric(sapply(strsplit(sqr$iso_sim,"&&"),"[",2))
  sqr$iso_sim <- NULL
  shiv <- merge(sqr,shiv,all=T,by=c("year","sex","iso3","sim"))
  stopifnot(combos*250==length(shiv$year))
  
  shiv <- shiv[order(shiv$iso3,shiv$sex,shiv$sim,shiv$year),]
  
  ## renumber the sims so that saving them in simfiles for 45q15 works
  shiv <- shiv[order(shiv$iso3,shiv$sex,shiv$year,shiv$sim),]
  shiv$sim <- rep(1:250,combos)
  

  stopifnot(length(shiv$year)==length(unique(shiv$year))*length(unique(shiv$sex))*length(unique(shiv$iso3))*length(unique(shiv$sim)))
  
  
  ## loop over sims and save simfiles so that they can be used to run models
  for (i in sort(unique(shiv$sim))) {
    print(i)
    temp <- shiv[shiv$sim == i,]
    stopifnot(length(temp$year)==length(unique(shiv$year))*length(unique(shiv$iso3))*length(unique(shiv$sex)))
    if (length(temp$year)!=length(unique(shiv$year))*length(unique(shiv$iso3))*length(unique(shiv$sex))) print(paste0(i," doesn't have right observations"))
    write.csv(temp,paste0("filepath"),row.names=F)
  }
  
} 
  

