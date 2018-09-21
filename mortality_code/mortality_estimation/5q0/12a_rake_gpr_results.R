###################
## Purpose: Compile 5q0 GPR results, scale subnational to national
## Rationale: We use model at all levels (for the most part), and therefore the modelled national results and modelled subnational
##            results are not consistent. Because we believe the national data more than the subnational data, we scale the 
##            subnational data to the national data to make it consistent instead of aggregating subnational data.
##            We then replace the subnational GPR results with the final raked reseults, The execption is in South Africa, 
##            where the subnational data better captures the HIV epidemic so we aggregate and replace the national
##            GPR results. 
## Steps:
##    0. Make sure that the correct files are being read in-- 2015 locations from the 2015 folder, and the rest from the "final"
##       folder. This is done by removing 2015 locations from the "final" folder and then copying the results from the 2015 folder into the final folder
##    1. Getting location lists for raking-- lists with just the lower level subnational, and also the subnational with its parent
##    2. Prepping populations and births-- subsetting to both sexes, aggregating population to under5 pops
##    3. Aggregating subnational to national for South Africa exception 
##    4. Raking level 2 locations (first level subnationals) to level 1 locations (national locs)
##          A. Weight subnationals by chosen metric (population in this code), aggregate, and create scaling factor
##             Multiply original subnational estimates by scaling factor
##          B. Addressing England Exception:
##                - because of data availability concerns, we are raking only England and Wales to the UK minus England and 
##                  Wales before 1981. We rake all 4 subnationals (England, Wales, Northern Ireland, Scotland) to the UK 1981-2016
##    5. Raking level 3 locations (second level subnationals) to level 2 locations (first level subnats)
##    6. Raking level 4 locations (third level subnationals) to level 3 locations (second level subnats) 
################################

rm(list=ls())
library(foreign); library(data.table);library(haven)

if (Sys.info()[1]=="Windows"){
  root <- "FILEPATH"
  username <- "USERNAME"
  nat_ihme <- 95 ## test location, UK
  nat_ihme_loc_id <- "GBR"
} else {
  root <- "/home/j"
  username <- commandArgs()[3]
  nat_ihme <- as.integer(commandArgs()[4])
  nat_ihme_loc_id <- commandArgs()[5]
  code_dir <-  "FILEPATH"
}
print(username)
print(nat_ihme)
print(code_dir)

## working dir  
setwd("FILEPATH")  
data_dir <- "FILEPATH"

## functions
source("FILEPATH/shared/functions/get_locations.r")
source("FILEPATH/get_population.R")

## function that gets from 5q0s of nationals and subnationals to a scaling factor that you can multiply the subnationals 
## by in order to rake. Have a variable called ind and upper corresponds to the higher level while lower corresponds to lower level
get_scalar <- function(data){
  data[,mort := mort * scale_var]
  setkey(data, sim, year_id, ind, parent_id)
  data <- data[,list(mort=sum(mort), scale_var=sum(scale_var)), by=key(data)]
  data[, mort := mort/scale_var]
  
  ## create scalar to multiply lower level subnats by
  data[,scale_var:=NULL]
  data <- dcast.data.table(data, parent_id+sim+year_id~ind, value.var="mort")
  data[,scalar:= upper/lower]
  data <- data[,c("upper", "lower"):=NULL]
  return(data)
}

## options
aggnats <- c("ZAF")
mx <- T
use_births <- F 

## loading in data, location lists
est_locs <- data.table(read.csv("FILEPATH/locations_est.csv", stringsAsFactors = F)) # 2016 estimation 
parent_locs <- est_locs[,list(parent_id, ihme_loc_id)]

years <- c(1950:2016)
pop <- get_population(status = "recent", year_id = years, sex_id = c(3), age_group_id=c(5, 28), location_id=-1, location_set_id=21)
pop[,process_version_map_id := NULL]
setnames(pop, "population", "pop")


births <- data.table(read_dta("FILEPATH/births_gbd2016.dta",sep=""))


######################################
## Step 1: Getting location lists with all the levels
#######################################

## for raking first level subnational to national
lev2_1_locs <- copy(est_locs)
if(nat_ihme_loc_id=="IND") lev2_1_locs <- lev2_1_locs[location_id!=44849,] # drop Old AP from second level because we don't want it included in the raking up to India
lev2_1_locs <- lev2_1_locs[parent_id==nat_ihme | location_id==nat_ihme]
l2_locs <- lev2_1_locs[location_id!=nat_ihme]


## for raking second level subnational to first level subnational 
lev3_2_locs <- copy(est_locs)
l3_locs <- lev3_2_locs[parent_id %in% l2_locs$location_id]
lev3_2_locs <- lev3_2_locs[parent_id %in% l2_locs$location_id | location_id %in% unique(l3_locs$parent_id)]

## for raking third level subnational to second level subnational (just UK)
lev4_3_locs <- copy(est_locs)
l4_locs <- lev4_3_locs[parent_id %in% l3_locs$location_id]
lev4_3_locs <- lev4_3_locs[parent_id %in% l3_locs$location_id | location_id %in% unique(l4_locs$parent_id)]



################################
## Step 2: Prep population and births
################################

pop <- pop[age_group_id %in% c(5, 28)]
pop[,age_group_id:=1]
pop <- pop[sex_id==3]
setkey(pop, location_id, year_id, sex_id, age_group_id)
pop <- pop[,list(pop=sum(pop)), by=key(pop)]
pop <- pop[,c("age_group_id", "sex_id") :=NULL]
setnames(pop, "pop", "scale_var")

births <- births[sex_id==3]
births <- births[,.(location_id, year, births)]
setnames(births, "births", "scale_var")

if(use_births==T){
  pop <- copy(births)
}

#####################################
## Step 2.5 - Rake New AP and Telangana to Old AP
#####################################

if(nat_ihme_loc_id=="IND"){
  
  # get oap pop by adding telangana and new AP
  oap_pop <- copy(pop[location_id %in% c(4871, 4841)])
  oap_pop <- oap_pop[,list(scale_var= sum(scale_var)), by=list(year_id)]
  oap_pop[,location_id:=44849]
  
  ## bring in level 1 location, merging on population
  oap_ihme = 44849
  oap_ihme_loc_id <- est_locs$ihme_loc_id[est_locs$location_id==oap_ihme]
  oap <- fread(paste0(data_dir, "gpr_", oap_ihme_loc_id, "_sim.txt"))
  oap[,location_id:=oap_ihme]
  oap[,year_id := floor(year)]
  oap[,year:=NULL]
  
  oap <- merge(oap, oap_pop, by=c("year_id", "location_id"))
 
  oap[,ind:="upper"]
  
  ## bring in level 2 locations
  suboap1 <- list()
  missing_files <- c()
  
  for(cc in c("IND_4871", "IND_4841")){
    file <- paste0(data_dir, "gpr_", cc, "_sim.txt")
    if(file.exists(file)){
      suboap1[[paste0(cc)]] <- fread(file)
      suboap1[[paste0(cc)]]$location_id <- l2_locs$location_id[l2_locs$ihme_loc_id==cc]
    } else {
      missing_files <- c(missing_files, file)
    }
  }
  if(length(missing_files)>0) stop("Level 2 files are missing")
  suboap1 <- rbindlist(suboap1, use.names=T)
  suboap1[,year_id:=floor(year)]
  suboap1[,year:=NULL]
  suboap1[,ind:="lower"]
  
  ## subset to relevant populations and merge 
  ## keep suboap1 looking like this for the next level
  suboap1 <- merge(suboap1, pop, by=c("location_id", "year_id"))
  
  
  # bind on New AP and Old AP
  ## append on level 1 loc
  l2_1_oap <- rbind(suboap1, oap)
  
  ## Replace mort with mx if you want to weight using mx
  if(mx==T){
    l2_1_oap[,mort := log(1-mort)/-5]
  }
  l2_1_oap[,parent_id:=44849]
  ## weight the subnationals by chosen metric
  scalar1 <- get_scalar(data=l2_1_oap)
  
  ## merge on scalars to lower level
  l2_oap <- copy(suboap1)
  
  if(mx==T){
    l2_oap[,mort := log(1-mort)/-5]
  }
  
  l2_oap <- l2_oap[,.(year_id, ihme_loc_id, sim, mort)]
  l2_oap[,parent_id:=44849]
  l2_oap <- merge(l2_oap, scalar1, by=c("year_id", "sim", "parent_id"))
  l2_oap[,mort := mort * scalar]
  l2_oap[,scalar:=NULL]
  
  if(mx == T){
    l2_oap[, mort := 1 - exp(-5 * mort)]
  }
  
  ## revert to format of GPR results
  l2_oap[,year:=year_id+0.5]
  l2_oap[,c("year_id", "parent_id"):=NULL]
  
  
  
  # replace files with raked results
  
  for(locs in c("IND_4871", "IND_4841")){
    temp <- l2_oap[ihme_loc_id==locs]
    
    ## draw level
    file.remove(paste0(data_dir, "gpr_", locs, "_sim.txt"))
    write.csv(temp, paste0(data_dir, "gpr_", locs, "_sim.txt"), row.names=F)

    ## mean level
    sum_agg <- copy(temp)
    setkey(sum_agg, ihme_loc_id, year)
    sum_agg <- sum_agg[,list(med=mean(mort), lower=quantile(mort, probs=c(.025)), upper=quantile(mort, probs=c(0.975))), 
                       by=key(sum_agg)]
    
    ## deleting and saving summary file
    file.remove(paste0(data_dir, "gpr_", locs, ".txt"))
    write.csv(sum_agg, paste0(data_dir, "gpr_", locs, ".txt"), row.names=F)
  }
  
  
  
}


#####################################
## Step 3-4 (A): Level 2 to level 1
######################################

## bring in relevant level 2 and level 1 locations

## bring in level 1 location, merging on population
nat_ihme_loc_id <- est_locs$ihme_loc_id[est_locs$location_id==nat_ihme]
nat <- fread(paste0(data_dir, "gpr_", nat_ihme_loc_id, "_sim.txt"))
nat[,location_id:=nat_ihme]
nat[,year_id := floor(year)]
nat[,year:=NULL]

nat <- merge(nat, pop, by=c("year_id", "location_id"))
nat[,ind:="upper"]

## bring in level 2 locations
subnat1 <- list()
missing_files <- c()

for(cc in l2_locs$ihme_loc_id){
  file <- paste0(data_dir, "gpr_", cc, "_sim.txt")
  if(file.exists(file)){
    subnat1[[paste0(cc)]] <- fread(file)
    subnat1[[paste0(cc)]]$location_id <- l2_locs$location_id[l2_locs$ihme_loc_id==cc]
  } else {
    missing_files <- c(missing_files, file)
  }
}
if(length(missing_files)>0) stop("Level 2 files are missing")
subnat1 <- rbindlist(subnat1, use.names=T)
subnat1[,year_id:=floor(year)]
subnat1[,year:=NULL]
subnat1[,ind:="lower"]

## subset to relevant populations and merge 
subnat1 <- merge(subnat1, pop, by=c("location_id", "year_id"))

##########################
## Step 3: Aggregating ZAF 
###########################

## aggregating up to nationals for chosen locations
if(nat_ihme_loc_id %in% aggnats){
  
  
  ## Replace mort with mx if you want to weight using mx
  if(mx==T){
    subnat1[,mort := log(1-mort)/-5]
  }
  
  subnat1[,mort := mort * scale_var]
  subnat1[,ind := NULL]
  
  #aggregating
  setkey(subnat1, sim, year_id)
  subnat1 <- subnat1[,list(mort=sum(mort), scale_var=sum(scale_var)), by=key(subnat1)]
  subnat1[,mort := mort/scale_var]
  
  if(mx == T){
    subnat1[, mort := 1 - exp(-5 * mort)]
  }
  
  #formatting
  subnat1[,ihme_loc_id:=nat_ihme_loc_id]
  subnat1[,year:=year_id+0.5]
  subnat1[,c("year_id", "scale_var") := NULL]
  
  ##deleting and saving draw level file
  file.remove(paste0(data_dir, "gpr_", nat_ihme_loc_id, "_sim.txt"))
  write.csv(subnat1, paste0(data_dir, "gpr_", nat_ihme_loc_id, "_sim.txt"), row.names=F)
  
  ## summarizing
  sum_agg <- copy(subnat1)
  setkey(sum_agg, ihme_loc_id, year)
  sum_agg <- sum_agg[,list(med=mean(mort), lower=quantile(mort, probs=c(.025)), upper=quantile(mort, probs=c(0.975))), 
                     by=key(sum_agg)]
  
  ## deleting and saving summary file
  file.remove(paste0(data_dir, "gpr_", nat_ihme_loc_id, ".txt"))
  write.csv(sum_agg, paste0(data_dir, "gpr_", nat_ihme_loc_id, ".txt"), row.names=F)
}

#########################################
## Step 4A: Raking level 2 to level 1
#########################################

## append on level 1 loc
l2_1 <- rbind(subnat1, nat)


l2_1 <- merge(l2_1, parent_locs, by = c("ihme_loc_id"))
l2_1 <- l2_1[ind =="upper", parent_id := location_id]

## Replace mort with mx if you want to weight using mx
if(mx==T){
  l2_1[,mort := log(1-mort)/-5]
}
## weight the subnationals by chosen metric
scalar1 <- get_scalar(data=l2_1)

## merge on scalars to lower level
l2 <- copy(subnat1)

if(mx==T){
  l2[,mort := log(1-mort)/-5]
}

l2 <- l2[,.(year_id, ihme_loc_id, sim, mort)]
l2 <- merge(l2, parent_locs, by = c("ihme_loc_id"))
l2 <- merge(l2, scalar1, by=c("year_id", "sim", "parent_id"))
l2[,mort := mort * scalar]
l2[,scalar:=NULL]

if(mx == T){
  l2[, mort := 1 - exp(-5 * mort)]
}

## revert to format of GPR results
l2[,year:=year_id+0.5]
l2[,c("year_id", "parent_id"):=NULL]


############################
## Step 4B Level 2 to 1 UK exeption
############################

## in the UK, pre-1981 we just want to rake England and Wales to the UK minus Scotland and Northern Ireland
## post-1981 we want to rake all 4 level 2 subnations to the UK
if(nat_ihme==95){

  pre_1981_nat <- rbind(nat, subnat1)
  pre_1981_nat <- pre_1981_nat[!ihme_loc_id %in% c("GBR_4749", "GBR_4636")] ## subsetting to UK, Scotland, N. Ireland
  pre_1981_nat <- pre_1981_nat[year_id<=1980]
  
  ## for later appending of the predicted unraked Scotland and N Ireland
  scot_ni <- copy(pre_1981_nat)
  scot_ni <- scot_ni[!ihme_loc_id %in% "GBR"]
  scot_ni[,c("location_id", "scale_var", "ind"):= NULL]
  
  if(mx==T){
    pre_1981_nat[,mort := log(1-mort)/-5]
  }
  
  
  pre_1981_nat[,mort := scale_var*mort] ## weighting mx/qx by births/pop
  pre_1981_nat[,c("ind"):=NULL]
  
  pre_1981_nat <- dcast.data.table(pre_1981_nat, year_id+sim~ihme_loc_id, value.var=c("mort", "scale_var"))
  
  ## subtracting population and weighted mortality of Northern Ireland and Scotland from UK
  ## in order to create fake "national" for England and Scotland to scale to
  pre_1981_nat[,mort := mort_GBR - mort_GBR_434 - mort_GBR_433]
  pre_1981_nat[,scale_var := scale_var_GBR - scale_var_GBR_434 - scale_var_GBR_433]
  pre_1981_nat <- pre_1981_nat[,.(year_id, sim, mort, scale_var)]
  pre_1981_nat[,mort := mort/scale_var]
  pre_1981_nat[,scale_var:=NULL]
  setnames(pre_1981_nat, "mort", "l1")
  
  ## weighting England and Scotland
  pre_1981_sub <- copy(subnat1)
  pre_1981_sub <- pre_1981_sub[year_id<=1980]
  pre_1981_sub <- pre_1981_sub[ihme_loc_id %in% c("GBR_4749", "GBR_4636")]
  pre_1981_sub <- pre_1981_sub[,c("location_id", "ind"):=NULL]
  
  temp_scalar <- copy(pre_1981_sub)
  
  if(mx==T){
    temp_scalar[,mort := log(1-mort)/-5]
  }
  
  temp_scalar[, mort:= mort*scale_var]
  setkey(temp_scalar, year_id, sim)
  temp_scalar <- temp_scalar[,list(mort=sum(mort), scale_var=sum(scale_var)), by=key(temp_scalar)]
  temp_scalar[,mort := mort/scale_var]
  temp_scalar[,scale_var:=NULL]
  setnames(temp_scalar, "mort", "l2")
  
  ## Create scalar for England and Wales
  pre_1981 <- merge(temp_scalar, pre_1981_nat, by=c("year_id", "sim"))
  pre_1981[,scalar := l1/l2]
  pre_1981[,c("l1", "l2", "scale_var"):=NULL]
  
  ## merge on scalars to lower level
  pre_1981 <- merge(pre_1981, pre_1981_sub, by=c("year_id", "sim"))
  
  if(mx==T){
    pre_1981[,mort := log(1-mort)/-5]
  }
  
  pre_1981[,mort := mort * scalar]
  
  
  if(mx == T){
    pre_1981[, mort := 1 - exp(-5 * mort)]
  }
  
  ## merge on unraked Scotland and NIreland
  pre_1981 <- rbind(pre_1981, scot_ni, fill=T, use.names=T)
  pre_1981[,year:=year_id+0.5]
  pre_1981[,c("scalar", "scale_var", "year_id"):=NULL]
  
  ## drop 1980 and earlier from previous results, append on from this
  l2 <- l2[year>1981]
  l2 <- rbind(l2, pre_1981)
}

## delete pre-raked results, save final raked results by location (l2)
for(locs in l2_locs$ihme_loc_id){
  temp <- l2[ihme_loc_id==locs]
  
  ## draw level
  file.remove(paste0(data_dir, "gpr_", locs, "_sim.txt"))
  write.csv(temp, paste0(data_dir, "gpr_", locs, "_sim.txt"), row.names=F)
  
  ## mean level
  sum_agg <- copy(temp)
  setkey(sum_agg, ihme_loc_id, year)
  sum_agg <- sum_agg[,list(med=mean(mort), lower=quantile(mort, probs=c(.025)), upper=quantile(mort, probs=c(0.975))), 
                     by=key(sum_agg)]
  
  ## deleting and saving summary file
  file.remove(paste0(data_dir, "gpr_", locs, ".txt"))
  write.csv(sum_agg, paste0(data_dir, "gpr_", locs, ".txt"), row.names=F)
}

####################################
## Step 5: Rake Level 3 to Level 2
####################################

## subnat1 contains all level 2 locations
if(nrow(l3_locs)>0){
  ## read in level 3 locations
  subnat2 <- list()
  missing_files <- c()
  
  for(cc in l3_locs$ihme_loc_id){
    file <- paste0(data_dir, "gpr_", cc, "_sim.txt")
    if(file.exists(file)){
      subnat2[[paste0(cc)]] <- fread(file)
      subnat2[[paste0(cc)]]$location_id <- l3_locs$location_id[l3_locs$ihme_loc_id==cc]
    } else {
      missing_files <- c(missing_files, file)
    }
  }
  if(length(missing_files)>0) stop("Level 3 files are missing")
  subnat2 <- rbindlist(subnat2, use.names=T)
  subnat2[,ind := "lower"]
  
  ## subsetting the level 2 locations to just the parents of level 3 locations (for UK, only England has children)
  
  l2[,location_id := sapply(strsplit(ihme_loc_id, "_"), "[", 2)]
  l2 <- l2[location_id %in% l3_locs$parent_id]
  l2[, ind := "upper"]
  l3_2 <- rbind(l2, subnat2)
  
  ## merging on population or births
  l3_2[,year_id := floor(year)]
  l3_2[,year:=NULL]
  l3_2[,location_id := as.numeric(location_id)]
  l3_2 <- merge(l3_2, pop, by=c("location_id", "year_id"))
  l3_2 <- merge(l3_2, parent_locs, by = c("ihme_loc_id"))
  l3_2 <- l3_2[ind =="upper", parent_id := location_id]
  
  ## Replace mort with mx if you want to weight using mx
  if(mx==T){
    l3_2[,mort := log(1-mort)/-5]
  }
  
  ## weighting mortality by chosen metric, aggregating, unweighting
  ## create scalar to multiply lower level subnats by
  scalar2 <- get_scalar(data=l3_2)
  
  ## merge scalar to lower level locs
  l3 <- copy(subnat2)
  
  if(mx==T){
    l3[,mort := log(1-mort)/-5]
  }
  
  
  l3[,year_id:= floor(year)]
  l3 <- l3[,.(year_id, ihme_loc_id, sim, mort)]
  l3 <- merge(l3, parent_locs, by = c("ihme_loc_id"))
  l3 <- merge(l3, scalar2, by=c("year_id", "sim", "parent_id"))
  l3[,mort := mort * scalar]
  l3[,scalar:=NULL]
  
  if(mx == T){
    l3[, mort := 1 - exp(-5 * mort)]
  }
  
  ## revert to format of GPR results
  l3[,year:=year_id+0.5]
  l3[,c("year_id", "parent_id") :=NULL]
  
  ## delete pre-raked results, save final raked results by location (l3)
  for(locs in l3_locs$ihme_loc_id){
    temp <- l3[ihme_loc_id==locs]
    
    ## draw level
    file.remove(paste0(data_dir, "gpr_", locs, "_sim.txt"))
    write.csv(temp, paste0(data_dir, "gpr_", locs, "_sim.txt"), row.names=F)
    
    ## mean level
    sum_agg <- copy(temp)
    setkey(sum_agg, ihme_loc_id, year)
    sum_agg <- sum_agg[,list(med=mean(mort), lower=quantile(mort, probs=c(.025)), upper=quantile(mort, probs=c(0.975))), 
                       by=key(sum_agg)]
    
    ## deleting and saving summary file
    file.remove(paste0(data_dir, "gpr_", locs, ".txt"))
    write.csv(sum_agg, paste0(data_dir, "gpr_", locs, ".txt"), row.names=F)
  }
}


####################################
## Step 6: Rake level 4 to level 3
###################################

if(nrow(l4_locs)>0){
  ## read in level 3 locations
  subnat3 <- list()
  missing_files <- c()
  
  for(cc in l4_locs$ihme_loc_id){
    file <- paste0(data_dir, "gpr_", cc, "_sim.txt")
    if(file.exists(file)){
      subnat3[[paste0(cc)]] <- fread(file)
      subnat3[[paste0(cc)]]$location_id <- l4_locs$location_id[l4_locs$ihme_loc_id==cc]
    } else {
      missing_files <- c(missing_files, file)
    }
  }
  if(length(missing_files)>0) stop("Level 4 files are missing")
  subnat3 <- rbindlist(subnat3, use.names=T)
  subnat3[,ind := "lower"]
  
  ## subsetting the level 3 locations to just the parents of level 4 locations 
  
  l3[,location_id := sapply(strsplit(ihme_loc_id, "_"), "[", 2)]
  l3 <- l3[location_id %in% l4_locs$parent_id]
  l3[, ind := "upper"]
  l4_3 <- rbind(l3, subnat3)
  
  
  ## merging on population or births
  l4_3[,year_id := floor(year)]
  l4_3[,year:=NULL]
  l4_3[,location_id := as.numeric(location_id)]
  l4_3 <- merge(l4_3, pop, by=c("location_id", "year_id"))
  l4_3 <- merge(l4_3, parent_locs, by = c("ihme_loc_id"))
  l4_3 <- l4_3[ind =="upper", parent_id := location_id]
  
  ## weighting mortality by chosen metric, aggregating, unweighting
  
  ## Replace mort with mx if you want to weight using mx
  if(mx==T){
    l4_3[,mort := log(1-mort)/-5]
  }
  
  ## create scalar to multiply lower level subnats by
  scalar3 <- get_scalar(data=l4_3)
  
  ## merge scalar to lower level locs
  l4 <- copy(subnat3)
  
  if(mx==T){
    l4[,mort := log(1-mort)/-5]
  }
  
  
  l4[,year_id:= floor(year)]
  l4 <- l4[,.(year_id, ihme_loc_id, sim, mort)]
  l4 <- merge(l4, parent_locs, by = c("ihme_loc_id"))
  l4 <- merge(l4, scalar3, by=c("year_id", "sim", "parent_id"))
  l4[,mort := mort * scalar]
  l4[,scalar:=NULL]
  
  if(mx == T){
   l4[, mort := 1 - exp(-5 * mort)]
  }
  
  ## revert to format of GPR results
  l4[,year:=year_id+0.5]
  l4[,c("year_id", "parent_id") :=NULL]
  
  ## delete pre-raked results, save final raked results by location (l4)
  for(locs in l4_locs$ihme_loc_id){
    temp <- l4[ihme_loc_id==locs]
    
    ## draw level
    file.remove(paste0(data_dir, "gpr_", locs, "_sim.txt"))
    write.csv(temp, paste0(data_dir, "gpr_", locs, "_sim.txt"), row.names=F)
    
    ## mean level
    sum_agg <- copy(temp)
    setkey(sum_agg, ihme_loc_id, year)
    sum_agg <- sum_agg[,list(med=mean(mort), lower=quantile(mort, probs=c(.025)), upper=quantile(mort, probs=c(0.975))), 
                       by=key(sum_agg)]
    
    ## deleting and saving summary file
    file.remove(paste0(data_dir, "gpr_", locs, ".txt"))
    write.csv(sum_agg, paste0(data_dir, "gpr_", locs, ".txt"), row.names=F)
  }
}


