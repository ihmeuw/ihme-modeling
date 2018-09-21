###################

## Purpose: Scale subnational GPR results to national
##          NOTE: This code was written to run only on parent locations - e.g. "SWE", "GBR"  
##                Adult mortality run_all and 08b_compile_all are meant to run on non-parent locations          
## Rationale: We use model at all levels (for the most part), and therefore the modelled national results and modelled subnational
##            results are not consistent. Because we believe the national data more than the subnational data, we scale the 
##            subnational data to the national data to make it consistent instead of aggregating subnational data.
##            We then replace the subnational GPR results with the final raked reseults, The execptioni s in South Africa, 
##            where the subnational data better captures the HIV epidemic so we aggregate and replace the national
##            GPR results. 
## Steps:
##    1. Getting location lists for raking-- lists with just the lower level subnational, and also the subnational with its parent
##    2. Prepping populations -- subsetting to both sexes, aggregating population to under5 pops
##    3. Aggregating subnational to national for South Africa exception 
##    4. Raking level 2 locations (first level subnationals) to level 1 locations (nationa locs)
##          A. Weight subnationals by chosen metric (population in this code), aggregate, and create scaling factor
##             Multiply original subnational estimates by scaling factor
##          B. Addressing England Execption:
##                - because of data availability concerns, we are raking only England and Wales to the UK minus England and 
##                  Wales before 1981. We rake all 4 subnationals (England, Wales, Northern Ireland, Scotland) to the UK 1981-2016
##    5. Raking level 3 locations (second level subnationals) to level 2 locations (first level subnats)


################################

rm(list=ls())
library(foreign); library(data.table);library(haven)

if (Sys.info()[1]=="Windows"){
  root <- "filepath"

} else {
  root <- "filepath"
  username <- Sys.getenv("USER")
  nat_ihme <- as.integer(commandArgs()[3])
  nat_ihme_loc_id <- commandArgs()[4]
  hivuncert <- as.logical(as.numeric(commandArgs()[5]))
  gpr_sex <- commandArgs()[6]

  code_dir <- paste("filepath",sep="") 
}

## working dir
setwd(paste("filepath", sep=""))
if (hivuncert) {
  data_dir<- paste0("filepath")
  } else {
  data_dir<- paste0("filepath")
  }

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


## loading in data, location lists
est_locs <- fread(paste0("filepath")) # 2016 estimation 
parent_locs <- est_locs[,list(parent_id, ihme_loc_id)]

source(paste0("filepath"))
pop <- as.data.table(get_population(status = "recent", location_id = unique(est_locs$location_id), year_id = c(1950:2016), sex_id = c(1:2), age_group_id = c(8:16)))


######################################
## Step 1: Getting location lists with all the levels
#######################################

## for raking first level subnational to national
lev2_1_locs <- copy(est_locs)
lev2_1_locs <- lev2_1_locs[parent_id==nat_ihme | location_id==nat_ihme]
lev2_1_locs <- lev2_1_locs[location_id != 44849,]
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
## Step 2: Prep population
################################
#creating old andhra prUSER
pop_weight <- as.data.table(get_population(status = "recent", location_id = c(4841, 4871), year_id = c(1950:2016), sex_id = c(1:2), age_group_id = c(8:16)))
setkey(pop_weight, sex_id, year_id)
pop_weight <- pop_weight[,list(population = sum(population), location_id = 44849, age_group_id = 1), by = key(pop_weight)]

pop[,process_version_map_id := NULL]
pop[,age_group_id:=1]
pop  <- rbind(pop, pop_weight, use.names = T)
if(gpr_sex == "male"){
  pop <- pop[sex_id == 1,]
} else{
  pop <- pop[sex_id == 2,]
}
setkey(pop, location_id, year_id, sex_id, age_group_id)
pop <- pop[,list(scale_var = sum(population)), by=key(pop)]
pop <- pop[,c("age_group_id", "sex_id") :=NULL]

####################################
## India exception - raking to Old Andhra PrUSER
####################################
if(nat_ihme_loc_id == "IND"){
  old_ap <-  fread(paste0("filepath"))
  old_ap[,location_id:=44849]
  old_ap[,parent_id := 44849]
  old_ap[,year_id := floor(year)]
  old_ap[,year:=NULL]
  old_ap[,groups := NULL]
  old_ap <- merge(old_ap, pop, by=c("year_id", "location_id"))
  old_ap[,ind := "upper"]

  sub_ap_locs <- copy(est_locs)
  sub_ap_locs <- sub_ap_locs[ihme_loc_id == "IND_4841" | ihme_loc_id == "IND_4871",]
  sub_ap <- list()
  missing_files <- c()
  for (cc in sub_ap_locs$ihme_loc_id){
     file <- paste0( "filepath")
    if(file.exists(file)){
      sub_ap[[paste0(cc)]] <- fread(file)
      sub_ap[[paste0(cc)]]$location_id <- sub_ap_locs$location_id[sub_ap_locs$ihme_loc_id==cc]
      sub_ap[[paste0(cc)]]$groups <- NULL
    } else {
      missing_files <- c(missing_files, file)
    }
  }
  if(length(missing_files)>0) stop(paste("Andhra PrUSER or Telangana missing", missing_files))
  sub_ap <- rbindlist(sub_ap, use.names = T)
  sub_ap[,year_id:=floor(year)]
  sub_ap[,year:=NULL]
  sub_ap[,ind:="lower"]
  sub_ap <- merge(sub_ap, pop, by = c("location_id", "year_id"))
  sub_ap[,parent_id := 44849]

  all_ap <- rbind(sub_ap, old_ap, use.names = T)
  ## Replace mort with mx if you want to weight using mx
  if(mx==T){
    all_ap[,mort := log(1-mort)/-45]
  }

  ## weight the subnationals by chosen metric
  scalar_ap <- get_scalar(data=all_ap)

  ## merge on scalars to lower level
  new_ap <- copy(sub_ap)
  new_ap <- new_ap[,.(year_id, ihme_loc_id, sim, mort, sex, hiv, pred.1.wRE, pred.1.noRE, pred.2.final)]
  new_ap <- merge(new_ap, scalar_ap, by=c("year_id", "sim"))
  if(mx==T){
    new_ap[,mort := log(1-mort)/-45]
  }
  new_ap[,mort := mort * scalar]
  if(mx == T){
    new_ap[, mort := 1 - exp(-45 * mort)]
  }
  new_ap[,scalar:=NULL]

  ## revert to format of GPR results
  new_ap[,year:=year_id+0.5]
  new_ap[,c("year_id", "parent_id"):=NULL]

   for(locs in sub_ap_locs$ihme_loc_id){
      temp <- new_ap[ihme_loc_id==locs]
      
      ## draw level
      write.csv(temp, paste0("filepath"), row.names=F)
      
      ## mean level
      sum_agg <- copy(temp)
      setkey(sum_agg, ihme_loc_id, year, sex)
      sum_agg <- sum_agg[,list(mort_med = mean(mort),mort_lower = quantile(mort,probs=.025),
                                     mort_upper = quantile(mort,probs=.975),
                                     med_hiv=quantile(hiv,.5),mean_hiv=mean(hiv),
                                     med_stage1=quantile(pred.1.noRE,.5),
                                     med_stage2 =quantile(pred.2.final,.5)),
                         by=key(sum_agg)]
      
      write.csv(sum_agg, paste0("filepath"), row.names=F) 
    }
    file.copy(from=paste("filepath",sep=""),
                  to=paste("filepath",sep=""))
    file.copy(from=paste("filepath",sep=""),
                  to=paste("filepath",sep=""))

}

#####################################
## Step 3-4 (A): Level 2 to level 1
######################################

## bring in relevant level 2 and level 1 locations

## bring in level 1 location, merging on population
nat_ihme_loc_id <- est_locs$ihme_loc_id[est_locs$location_id==nat_ihme]
nat <- fread(paste0( "filepath"))

nat[,location_id:=nat_ihme]
nat[,year_id := floor(year)]
nat[,year:=NULL]
nat[,groups := NULL]

nat <- merge(nat, pop, by=c("year_id", "location_id"))
nat[,ind:="upper"]

## bring in level 2 locations
subnat1 <- list()
missing_files <- c()

for(cc in l2_locs$ihme_loc_id){
  file <- paste0( "filepath")
  if(file.exists(file)){
    subnat1[[paste0(cc)]] <- fread(file)
    subnat1[[paste0(cc)]]$location_id <- l2_locs$location_id[l2_locs$ihme_loc_id==cc]
    subnat1[[paste0(cc)]]$groups <- NULL
  } else {
    missing_files <- c(missing_files, file)
  }
}
if(length(missing_files)>0) stop(paste("Level 2 files are missing", missing_files))
subnat1 <- rbindlist(subnat1, use.names=T)
subnat1[,year_id:=floor(year)]
subnat1[,year:=NULL]
subnat1[,ind:="lower"]

## subset to relevant populations and merge 
## keep subnat1 looking like this for the next level
subnat1 <- merge(subnat1, pop, by=c("location_id", "year_id"))
 
##########################
## Step 5: Aggregating ZAF 
###########################

## aggregating up to nationals for chosen locations
## mostly just South Africa because of HIV shenanigans in the subnational data
if(nat_ihme_loc_id %in% aggnats){
  to_merge <- copy(nat)
  to_merge <- to_merge[,.(hiv, pred.1.wRE, pred.1.noRE, pred.2.final, sim, year_id)]
  if(mx==T){
    subnat1[,mort := log(1-mort)/-45]
  }
  subnat1[,mort := mort * scale_var]
  subnat1[,ind := NULL]
  
  #aggregating
  subnat1[,ihme_loc_id:=nat_ihme_loc_id]
  sum_agg <- copy(subnat1)
  setkey(subnat1, sim, year_id, sex, ihme_loc_id)
  subnat1 <- subnat1[,list(mort=sum(mort), scale_var=sum(scale_var)), by=key(subnat1)]
  subnat1[,mort := mort/scale_var]
  if(mx == T){
    subnat1[, mort := 1 - exp(-45 * mort)]
  }
  ## merging first and second stage predictions back on
  subnat1 <- merge(subnat1, to_merge, by=c("sim", "year_id"), all=T)
  
  #formatting
  subnat1[,year := year_id + 0.5]
  subnat1[,c("year_id", "scale_var") := NULL]
  
  ##saving draw level file
  write.csv(subnat1, paste0(  "filepath"), row.names=F)
  
  ## summarizing
  sum_agg <- copy(subnat1)
  setkey(sum_agg, ihme_loc_id, year, sex)
  sum_agg <- sum_agg[,list(mort_med = mean(mort),mort_lower = quantile(mort,probs=.025),
                                 mort_upper = quantile(mort,probs=.975),
                                 med_hiv=quantile(hiv,.5),mean_hiv=mean(hiv),
                                 med_stage1=quantile(pred.1.noRE,.5),
                                 med_stage2 =quantile(pred.2.final,.5)),
                     by=key(sum_agg)] 


  
  ## saving summary file
  write.csv(sum_agg, paste0( "filepath"), row.names=F) 
  #changing names of subnational files now that we've scaled national
  for (cc in l2_locs$ihme_loc_id){
      cat(paste(cc,"\n")); flush.console()
      file.copy(from=paste("filepath",sep=""),
                to=paste("filepath",sep=""))
      file.copy(from=paste("filepath",sep=""),
                to=paste("filepath",sep=""))

  }


}

#########################################
## Step 5A: Rakinging level 2 to level 1
#########################################

if(!nat_ihme_loc_id %in% aggnats){
  ## append on level 1 loc
  l2_1 <- rbind(subnat1, nat)
  l2_1 <- merge(l2_1, parent_locs, by = c("ihme_loc_id"))
  l2_1 <- l2_1[ind =="upper", parent_id := location_id]
  ## Replace mort with mx if you want to weight using mx
  if(mx==T){
    l2_1[,mort := log(1-mort)/-45]
  }
  
  ## weight the subnationals by chosen metric
  scalar1 <- get_scalar(data=l2_1)
  
  ## merge on scalars to lower level
  l2 <- copy(subnat1)
  l2 <- l2[,.(year_id, ihme_loc_id, sim, mort, sex, hiv, pred.1.wRE, pred.1.noRE, pred.2.final)]
  l2 <- merge(l2, parent_locs, by = c("ihme_loc_id"))
  l2 <- merge(l2, scalar1, by=c("year_id", "sim", "parent_id"))
  if(mx==T){
    l2[,mort := log(1-mort)/-45]
  }
  l2[,mort := mort * scalar]
  if(mx == T){
    l2[, mort := 1 - exp(-45 * mort)]
  }

  l2[,scalar:=NULL]
  
  ## revert to format of GPR results
  l2[,year:=year_id+0.5]
  l2[,c("year_id", "parent_id"):=NULL]
  
  
  ############################
  ## Step 5B Level 2 to 1 UK exeption
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
      pre_1981_nat[,mort := log(1-mort)/-45]
    }


    pre_1981_nat[,mort := scale_var*mort] ## weighting mx/qx by pop
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
      temp_scalar[,mort := log(1-mort)/-45]
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
    pre_1981[,c("l1", "l2"):=NULL]
    
    ## merge on scalars to lower level
    pre_1981 <- merge(pre_1981, pre_1981_sub, by=c("year_id", "sim"))
    if(mx==T){
      pre_1981[,mort := log(1-mort)/-45]
    }
    pre_1981[,mort := mort * scalar]
    if(mx == T){
      pre_1981[, mort := 1 - exp(-45 * mort)]
  }
    ## merge on unraked Scotland and NIreland
    pre_1981 <- rbind(pre_1981, scot_ni, fill=T, use.names=T)
    pre_1981[,year:=year_id+0.5]
    pre_1981[,c("scalar", "scale_var", "year_id"):=NULL]
    pre_1981[,groups:=NULL]
    
    ## drop 1980 and earlier from previous results, append on from this
    l2 <- l2[year>1981]
    l2 <- rbind(l2, pre_1981)
  }
  
  ## delete pre-raked results, save final raked results by location (l2)
  for(locs in l2_locs$ihme_loc_id){
    temp <- l2[ihme_loc_id==locs]
    
    ## draw level
    write.csv(temp, paste0(  "filepath"), row.names=F)
    
    ## mean level
    sum_agg <- copy(temp)
    setkey(sum_agg, ihme_loc_id, year, sex)
    sum_agg <- sum_agg[,list(mort_med = mean(mort),mort_lower = quantile(mort,probs=.025),
                                   mort_upper = quantile(mort,probs=.975),
                                   med_hiv=quantile(hiv,.5),mean_hiv=mean(hiv),
                                   med_stage1=quantile(pred.1.noRE,.5),
                                   med_stage2 =quantile(pred.2.final,.5)),
                       by=key(sum_agg)]

  
    write.csv(sum_agg, paste0( "filepath"), row.names=F) 
  }
  
  ############################33
  ## Step 6: Rake Level 3 to Level 2
  ############################
  
  ## subnat1 contains all level 2 locations
  if(nrow(l3_locs)>0){
    ## read in level 3 locations
    subnat2 <- list()
    missing_files <- c()
    
    for(cc in l3_locs$ihme_loc_id){
      file <- paste0( "filepath")
      if(file.exists(file)){
        subnat2[[paste0(cc)]] <- fread(file)      
        subnat2[[paste0(cc)]]$location_id <- l3_locs$location_id[l3_locs$ihme_loc_id==cc]
        subnat2[[paste0(cc)]]$groups <- NULL
      } else {
        missing_files <- c(missing_files, file)
      }
    }
    if(length(missing_files)>0) stop(paste("Level 3 files are missing", missing_files))
    subnat2 <- rbindlist(subnat2, use.names=T)
    subnat2[,ind := "lower"]
  #  subnat2 <- subnat2[, c("year", "ihme_loc_id", "location_id", "mort", "ind", "sim", "sex"), with = F]
  
    
    ## subsetting the level 2 locations to just the parents of level 3 locations (for UK, only England has children)
    
    l2[,location_id := sapply(strsplit(ihme_loc_id, "_"), "[", 2)]
    l2 <- l2[location_id %in% l3_locs$parent_id]
    l2[, ind := "upper"]
    l3_2 <- rbind(l2, subnat2)
    
    ## merging on population 
    l3_2[,year_id := floor(year)]
    l3_2[,year:=NULL]
    l3_2[,location_id := as.numeric(location_id)]
    l3_2 <- merge(l3_2, pop, by=c("location_id", "year_id"))
    l3_2 <- merge(l3_2, parent_locs, by = c("ihme_loc_id"))
    l3_2 <- l3_2[ind =="upper", parent_id := location_id]

    if(mx==T){
      l3_2[,mort := log(1-mort)/-45]
  }
    
    ## weighting mortality by chosen metric, aggregating, unweighting
    ## create scalar to multiply lower level subnats by
    scalar2 <- get_scalar(data=l3_2)
    
    ## merge scalar to lower level locs
    l3 <- copy(subnat2)
    l3[,year_id:= floor(year)]
    l3 <- l3[,.(year_id, ihme_loc_id, sim, mort, sex, hiv, pred.1.wRE, pred.1.noRE, pred.2.final)]
    l3 <- merge(l3, parent_locs, by = c("ihme_loc_id"))
    l3 <- merge(l3, scalar2, by=c("year_id", "sim", "parent_id"))
    if(mx==T){
    l3[,mort := log(1-mort)/-45]
  }
    l3[,mort := mort * scalar]
    if(mx == T){
      l3[, mort := 1 - exp(-45 * mort)]
  }
    l3[,scalar:=NULL]
    
    ## revert to format of GPR results
    l3[,year:=year_id+0.5]
    l3[,c("year_id", "parent_id") :=NULL]
    
    ## delete pre-raked results, save final raked results by location (l3)
    for(locs in l3_locs$ihme_loc_id){
      temp <- l3[ihme_loc_id==locs]
  
      ## draw level
      write.csv(temp, paste0(  "filepath"), row.names=F)
    
      ## mean level
      sum_agg <- copy(temp)
      setkey(sum_agg, ihme_loc_id, year, sex)
      sum_agg <- sum_agg[,list(mort_med = mean(mort),mort_lower = quantile(mort,probs=.025),
                                   mort_upper = quantile(mort,probs=.975),
                                   med_hiv=quantile(hiv,.5),mean_hiv=mean(hiv),
                                   med_stage1=quantile(pred.1.noRE,.5),
                                   med_stage2 =quantile(pred.2.final,.5)),
                       by=key(sum_agg)]

      write.csv(sum_agg, paste0( "filepath"), row.names=F) 
  
    }
  }
  
  
  ###########################
  ## Step 7: Rake level 4 to level 3
  ###########################
  
  if(nrow(l4_locs)>0){
    ## read in level 3 locations
    subnat3 <- list()
    missing_files <- c()
    
    for(cc in l4_locs$ihme_loc_id){
      file <- paste0( "filepath")
      if(file.exists(file)){
        subnat3[[paste0(cc)]] <- fread(file)
        subnat3[[paste0(cc)]]$location_id <- l4_locs$location_id[l4_locs$ihme_loc_id==cc]
        subnat3[[paste0(cc)]]$groups <- NULL
      } else {
        missing_files <- c(missing_files, file)
      }
    }
    if(length(missing_files)>0) stop(paste("Level 4 files are missing", missing_files))
    subnat3 <- rbindlist(subnat3, use.names=T)
    subnat3[,ind := "lower"]
    #subnat3 <- subnat3[, c("year", "ihme_loc_id", "location_id", "mort", "ind", "sim", "sex"), with = F]
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

  if(mx==T){
    l4_3[,mort := log(1-mort)/-45]
  }

    ## weighting mortality by chosen metric, aggregating, unweighting
    ## create scalar to multiply lower level subnats by
    scalar3 <- get_scalar(data=l4_3)
    
    ## merge scalar to lower level locs
    l4 <- copy(subnat3)
    l4[,year_id:= floor(year)]
    l4 <- l4[,.(year_id, ihme_loc_id, sim, mort, sex, hiv, pred.1.wRE, pred.1.noRE, pred.2.final)]
    l4 <- merge(l4, parent_locs, by = c("ihme_loc_id"))
    l4 <- merge(l4, scalar3, by=c("year_id", "sim", "parent_id"))
    if(mx==T){
    l4[,mort := log(1-mort)/-45]
  }
    l4[,mort := mort * scalar]
    if(mx == T){
      l4[, mort := 1 - exp(-45 * mort)]
  }
    l4[,scalar:=NULL]
    
    ## revert to format of GPR results
    l4[,year:=year_id+0.5]
    l4[,c("year_id", "parent_id") :=NULL]
    
    ## delete pre-raked results, save final raked results by location (l4)
    for(locs in l4_locs$ihme_loc_id){
      temp <- l4[ihme_loc_id==locs]
      
      ## draw level
      write.csv(temp, paste0(  "filepath"), row.names=F)
    
      ## mean level
      sum_agg <- copy(temp)
      setkey(sum_agg, ihme_loc_id, year, sex)
      sum_agg <- sum_agg[,list(mort_med = mean(mort),mort_lower = quantile(mort,probs=.025),
                                   mort_upper = quantile(mort,probs=.975),
                                   med_hiv=quantile(hiv,.5),mean_hiv=mean(hiv),
                                   med_stage1=quantile(pred.1.noRE,.5),
                                   med_stage2 =quantile(pred.2.final,.5)),
                       by=key(sum_agg)]

  
      write.csv(sum_agg, paste0( "filepath"), row.names=F) 
    }
  }
  
  ##Copy national files
if(!(nat_ihme_loc_id %in% aggnats)){
    file.copy(from=paste("filepath",sep=""),
                  to=paste("filepath",sep=""))
    file.copy(from=paste("filepath",sep=""),
                  to=paste("filepath",sep=""))
  }
}