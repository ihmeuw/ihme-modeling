#### Setup ####

code_start <- Sys.time()

os <- .Platform$OS.type
if (os == "windows") {
  source("FILEPATH")
} else {
  source("FILEPATH")
}

library(ggplot2,lib="ADDRESS")
library(gridExtra,lib="ADDRESS")
library(metafor, lib="ADDRESS")
library(msm)

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

getMaster <- function(dat,cov_obj,cov_name,covariate_id,round_id=6){
  start <- Sys.time()
  if(!exists(cov_obj)){
    eval(parse(text=paste(cov_obj," <<- get_covariate_estimates(location_id=\"all\",covariate_id=covariate_id,year_id =\"all\",gbd_round_id=round_id,decomp_step=\"iterative\")")))
  }
  for(num in 1:dim(dat)[1]){
    if(dat[num,"location_name"] %in% locs$location_name){
      eval(parse(text=paste("inds <- which(",cov_obj,"$location_name==dat[num,\"location_name\"]&",cov_obj,"$year_id>=dat[num,\"year_start\"]&",cov_obj,"$year_id<=dat[num,\"year_end\"])")))
      cov_range <- eval(parse(text=paste(cov_obj,"[inds,\"mean_value\"]")))
      val <- mean(cov_range$mean_value)
    }
    else if(!is.na(dat[num,"location_id"])){
      eval(parse(text=paste("inds <- which(",cov_obj,"$location_id==dat[num,\"location_id\"]&",cov_obj,"$year_id>=dat[num,\"year_start\"]&",cov_obj,"$year_id<=dat[num,\"year_end\"])")))
      cov_range <- eval(parse(text=paste(cov_obj,"[inds,\"mean_value\"]")))
      val <- mean(cov_range$mean_value)
    }
    #multicenter studies ONLY past this point
    else if(";" %in% unlist(strsplit(dat[num,"location_name"],split=""))){
      #if Singapore/Hong Kong/Taiwan, take an average of each
      if(some_combo(dat[num,"location_name"],"Singapore;Hong Kong Special Administrative Region of China;Taiwan (Province of China)",split=";")){
        row_locs <- data.frame(location_ascii_name=unlist(strsplit(dat[num,"location_name"],split=";")))
        row_locs <- merge(row_locs,hierarchy[,c("location_ascii_name","location_id")],by="location_ascii_name")
        #for each country in list, pull population
        pops <- c()
        for(row in 1:nrow(row_locs)){
          loc_id <- row_locs[row,]$location_id
          year_range <- seq(dat[num,]$year_start,dat[num,]$year_end)
          pop <- subset(pop_master,location_id==loc_id)
          pop <- mean(subset(pop,year_id %in% year_range)$population)
          pops <- c(pops,pop)
        }
        w <- pops/sum(pops)
        #pull covariate values
        vals <- c()
        for(row in 1:nrow(row_locs)){
          eval(parse(text=paste("inds <- which(",cov_obj,"$location_name==row_locs[row,\"location_ascii_name\"]&",cov_obj,"$year_id>=dat[num,\"year_start\"]&",cov_obj,"$year_id<=dat[num,\"year_end\"])")))
          cov_range <- eval(parse(text=paste(cov_obj,"[inds,\"mean_value\"]")))
          vals <- c(vals,mean(cov_range$mean_value))
        }
        #calculate weighted average
        val <- sum(w*vals)/sum(w)
      }
      else{
        #strsplit by semicolon
        row_locs <- data.frame(location_ascii_name=unlist(strsplit(dat[num,"location_name"],split=";")))
        #hard-coding this to avoid mixing up Mexico and Mexico City
        workaround <- merge(row_locs,hierarchy[,c("location_ascii_name","location_id")])
        if(4657 %in% workaround$location_id){
          row_locs <- subset(workaround,location_id!=4657)
        }
        #identify if the locations are in the same region/super-region
        m_region <- sapply(row_locs$location_ascii_name,function(n){
          return(hierarchy[which(hierarchy$location_ascii_name == n),]$region_name)
        })
        m_super <- sapply(row_locs$location_ascii_name,function(n){
          return(hierarchy[which(hierarchy$location_ascii_name == n),]$super_region_name)
        })
        m_region <- unlist(m_region)
        m_super <- unlist(m_super)
        if(all(unlist(m_region)==m_region[1])){
          same_region <- TRUE
        } else {
          same_region <- FALSE
        }
        if(all(unlist(m_super)==m_super[1])){
          same_super_region <- TRUE
        } else{
          same_super_region <- FALSE
        }
        flag <- FALSE
        if(same_region){
          #identify region from data
          #find regional covariate value
          eval(parse(text=paste("inds <- which(",cov_obj,"$location_name==m_region[1]&",cov_obj,"$year_id>=dat[num,\"year_start\"]&",cov_obj,"$year_id<=dat[num,\"year_end\"])")))
          cov_range <- eval(parse(text=paste(cov_obj,"[inds,\"mean_value\"]")))
          val <- mean(cov_range$mean_value)
          if(is.nan(val)){
            flag <- TRUE
          }
        } else if(same_super_region){
          #identify super-region from data
          #find regional covariate value
          eval(parse(text=paste("inds <- which(",cov_obj,"$location_name==m_super[1]&",cov_obj,"$year_id>=dat[num,\"year_start\"]&",cov_obj,"$year_id<=dat[num,\"year_end\"])")))
          cov_range <- eval(parse(text=paste(cov_obj,"[inds,\"mean_value\"]")))
          val <- mean(cov_range$mean_value)
          if(is.nan(val)){
            flag <- TRUE
          }
        }
        if(!(same_super_region|same_region)|flag){
          row_locs <- data.frame(location_ascii_name=unlist(strsplit(dat[num,"location_name"],split=";")))
          row_locs <- merge(row_locs,hierarchy[,c("location_ascii_name","location_id")],by="location_ascii_name")
          #pull weights
          #for each country in list, pull population
          pops <- c()
          for(row in 1:nrow(row_locs)){
            loc_id <- row_locs[row,]$location_id
            year_range <- seq(dat[num,]$year_start,dat[num,]$year_end)
            pop <- subset(pop_master,location_id==loc_id)
            pop <- mean(subset(pop,year_id %in% year_range)$population)
            pops <- c(pops,pop)
          }
          w <- pops/sum(pops)
          #pull covariate values
          vals <- c()
          for(row in 1:nrow(row_locs)){
            eval(parse(text=paste("inds <- which(",cov_obj,"$location_name==row_locs[row,\"location_ascii_name\"]&",cov_obj,"$year_id>=dat[num,\"year_start\"]&",cov_obj,"$year_id<=dat[num,\"year_end\"])")))
            cov_range <- eval(parse(text=paste(cov_obj,"[inds,\"mean_value\"]")))
            vals <- c(vals,mean(cov_range$mean_value))
          }
          #calculate weighted average
          val <- sum(w*vals)/sum(w)
        }
      }
    }
    #assign covariate value to row
    dat[num,cov_name] <- val
  }
  end <- Sys.time()
  print(paste(cov_obj,end-start))
  return(dat)
}

reg_inquiry <- function(row) {
  regnum <- invisible(readline(paste("Number of region containing ",dat[row,"location_name"],": ")))
  if(regnum == 1){
    return("Misc/Mixed")
  }
  else if(regnum >= 2 & regnum <= 8){
    reg <- unique(hierarchy$super_region_name)[as.numeric(regnum)]
    if(is.na(reg)){
      print(paste("Error Line 86, ",regnum))
      reg <- reg_inquiry(row)
    }
    return(reg)
  }
  else{
    print("Error")
    return(reg_inquiry(row))
  }
}

save_results <- function(sdi_fit_RV,fit_RV,fit_d,covs){
  #location id, year id (stratified into 1990, 1995,2000,2005,2010,2017), sdi-only results, and sdi+diarrhea SEV results
  save_covs <- covs[which(covs$year_id%in%c(1990,1995,2000,2005,2010,2017)),]
  sdi_pred <- 1-exp(predict(sdi_fit_RV,newmods=as.matrix(save_covs[,c("sdi")]),type="response")$pred)
  best_pred <- 1-exp(predict(fit_RV,newmods=as.matrix(save_covs[,c("sdi","diarrhea_SEV")]),type="response")$pred)
  PAF_pred <- 1-exp(predict(fit_d,as.matrix(save_covs[,c("sdi","diarrhea_SEV")]),type="response")$pred)
  output <- data.frame(location_id=save_covs$location_id,year_id=save_covs$year_id,sdi=save_covs$sdi,sdi_only=sdi_pred,best=best_pred,PAF=PAF_pred)
  return(output)
}

within_five <- function(x,y){
  out <- FALSE
  if((x$year_start-5 <= y$year_start) & (y$year_start <= x$year_end+5)){
    out <- TRUE
  }
  else if((y$year_start-5 <= x$year_start) & (x$year_start <= y$year_end+5)){
    out <- TRUE
  }
  else if((x$year_start-5 <= y$year_end) & (y$year_end <= x$year_end+5)){
    out <- TRUE
  }
  else if((y$year_start-5 <= x$year_end) & (x$year_end <= y$year_end+5)){
    out <- TRUE
  }
  return(out)
}

#### Loading Extracted Data ####
round_id <- 6

dat <- read.csv("FILEPATH")
dat$location_name <- as.character(dat$location_name)

pop_master <- get_population(location_id="all",year_id="all",decomp_step="iterative",gbd_round_id=round_id)

#Patch: set VE=1 to 0.9999 to avoid issues with log(1-VE)
dat[which(dat$rota_ve_mean>=1),"rota_ve_mean"] <- 0.9999
dat[which(dat$rota_ve_upper>=1),"rota_ve_upper"] <- 0.9999
dat[which(dat$rota_ve_lower>=1),"rota_ve_lower"] <- 0.9999
dat[which(dat$diarrhea_ve_mean>=1),"diarrhea_ve_mean"] <- 0.9999
dat[which(dat$diarrhea_ve_upper>=1),"diarrhea_ve_upper"] <- 0.9999
dat[which(dat$diarrhea_ve_lower>=1),"diarrhea_ve_lower"] <- 0.9999

#Patch: set cv_inpatient=NA to 0 to allow for inpatient adjustment
dat[which(is.na(dat$cv_inpatient)),"cv_inpatient"] <- 0

#Finding location IDs
if(!exists("locs")){
  locs <- get_ids("location")
}

#Finding regions for rows with multiple countries
if(!exists("hierarchy")){
  hierarchy <- get_location_metadata(location_set_id=9, gbd_round_id=round_id)
  #hierarchy <- get_location_metadata(location_set_id=22)
}

#Assigning regions to rows
for(row in 1:dim(dat)[1]){
  if(dat[row,"location_name"] %in% hierarchy$location_name){
    dat[row,"region"] <- hierarchy[which(hierarchy$location_name==dat[row,"location_name"]),"super_region_name"]
    dat[row,"location_id"] <- hierarchy[which(hierarchy$location_name==dat[row,"location_name"]),"ihme_loc_id"]
  }
  else{
    dat[row,"region"] <- "Misc/Mixed"
    #reg <- reg_inquiry(row)
    #dat[which(dat$location_name==dat[row,"location_name"]),"region"] <- reg
  }
}

#Organizing data into dat_RV and dat_d data frames
RV_only <- NA
d_only <- NA
combo <- NA
sev_only <- NA
for(row in 1:dim(dat)[1]){
  if((dat[row,"vaccine_type"]=="Rotarix"&(dat[row,"doses"]==2|dat[row,"doses"]=="Any"))|(dat[row,"vaccine_type"]=="RotaTeq"&(dat[row,"doses"]==3|dat[row,"doses"]=="Any"))|(dat[row,"vaccine_type"]=="Mixed Rotarix/Rotateq"&(dat[row,"doses"]=="2 (RV1)/3 (RV5)"|dat[row,"doses"]=="Any"))){
    if(dat[row,"age_start"]<5&dat[row,"age_start"]>=0&dat[row,"age_end"]<5&dat[row,"age_end"]>=0){
      if(((is.na(dat[row,"diarrhea_ve_sample_size"])==TRUE&is.na(dat[row,"diarrhea_ve_lower"])==TRUE)&(is.na(dat[row,"rota_ve_sample_size"])==FALSE|is.na(dat[row,"rota_ve_lower"])==FALSE))){
        if(is.null(dim(RV_only))){
          RV_only <- dat[row,]
        }
        else{
          RV_only <- rbind(RV_only,dat[row,])
        }
      }
      else if(((is.na(dat[row,"diarrhea_ve_sample_size"])==FALSE|is.na(dat[row,"diarrhea_ve_lower"])==FALSE)&(is.na(dat[row,"rota_ve_sample_size"])==TRUE&is.na(dat[row,"rota_ve_lower"])==TRUE))){
        if(is.null(dim(d_only))){
          d_only <- dat[row,]
        }
        else{
          d_only <- rbind(d_only,dat[row,])
        }
      }
      else if((is.na(dat[row,"diarrhea_ve_sample_size"])==FALSE|is.na(dat[row,"diarrhea_ve_lower"])==FALSE)&(is.na(dat[row,"rota_ve_sample_size"])==FALSE|is.na(dat[row,"rota_ve_lower"])==FALSE)){
        if(is.null(dim(combo))){
          combo <- dat[row,]
        }
        else{
          combo <- rbind(combo,dat[row,])
        }
      }
      if(!is.na(dat[row,"severe_rota_sample_size"])|!is.na(dat[row,"severe_rota_ve_lower"])){
        if(is.null(dim(sev_only))){
          sev_only <- dat[row,]
        }
        else{
          sev_only <- rbind(sev_only,dat[row,])
        }
      }
    }
  }
}
dat_RV <- rbind(RV_only,combo)
dat_RV <- rbind(RV_only,combo)
dat_d <- rbind(d_only,combo)
dat_all <- rbind(dat_RV,d_only)
dat_sev <- sev_only
#### Loading Covariates ####

#sdi
dat_RV <- getMaster(dat_RV,"sdi_master","sdi",881)
# #population density
dat_RV <- getMaster(dat_RV,"den_master","density",118)
# #diarrhea sev
dat_RV <- getMaster(dat_RV,"dSEV_master","diarrhea_SEV",740)
# #discontinued breastfeeding SEV
dat_RV <- getMaster(dat_RV,"dbfSEV_master","breastfeeding_SEV",2091)
# #wasting SEV
dat_RV <- getMaster(dat_RV,"waste_master","wasting_SEV",1234)
# #stunting SEV
dat_RV <- getMaster(dat_RV,"stunt_master","stunting_SEV",1232)
# #underweight SEV
dat_RV <- getMaster(dat_RV,"weight_master","underweight_SEV",1230)
# #zinc deficiency
dat_RV <- getMaster(dat_RV,"zincD_master","zincD",1159)
# #vitamin A deficiency
dat_RV <- getMaster(dat_RV,"vitA_master","vitA",442)

#diarrhea prevalence
min_year <- 1990
max_year <- 2019

#Assigning diarrhea SEV, breastfeeding SEV, zinc deficiency, and vitamin A decifiency to data frames
start_pull <- Sys.time()
if(!exists("pop")){
  pop <- get_population(age_group_id=1,location_id="all",year_id="all",sex_id="all",decomp_step="iterative")
}

#population-weighting diarrhea SEV, breastfeeding SEV, zinc deficiency, and vitamin A deficiency covariates to generate sex_id=3 estimates
male <- subset(pop,sex_id==1)
female <- subset(pop,sex_id==2)
names(male) <- c("age_group_id","location_id","year_id","sex_id","male_population","run_id")
names(female) <- c("age_group_id","location_id","year_id","sex_id","female_population","run_id")
male_SEV <- merge(dSEV_master,male[,c("location_id","year_id","sex_id","male_population")],by=c("sex_id","location_id","year_id"))
male_SEV$mw_mean <- male_SEV$male_population*male_SEV$mean_value
male_SEV$mw_lower <- male_SEV$male_population*male_SEV$lower_value
male_SEV$mw_upper <- male_SEV$male_population*male_SEV$upper_value
female_SEV <- merge(dSEV_master,female[,c("location_id","year_id","sex_id","female_population")],by=c("sex_id","location_id","year_id"))
female_SEV$fw_mean <- female_SEV$female_population*female_SEV$mean_value
female_SEV$fw_lower <- female_SEV$female_population*female_SEV$lower_value
female_SEV$fw_upper <- female_SEV$female_population*female_SEV$upper_value
dSEV_merged <- merge(male_SEV,female_SEV[,c("location_id","year_id","female_population","fw_mean","fw_lower","fw_upper")],by=c("location_id","year_id"))
dSEV_merged$w_mean <- (dSEV_merged$mw_mean+dSEV_merged$fw_mean)/(dSEV_merged$male_population+dSEV_merged$female_population)
dSEV_merged$w_lower <- (dSEV_merged$mw_lower+dSEV_merged$fw_lower)/(dSEV_merged$male_population+dSEV_merged$female_population)
dSEV_merged$w_upper <- (dSEV_merged$mw_upper+dSEV_merged$fw_upper)/(dSEV_merged$male_population+dSEV_merged$female_population)
dSEV_master <- merge(dSEV_master,dSEV_merged[,c("location_id","year_id","w_mean","w_lower","w_upper")],by=c("location_id","year_id"))

male_SEV <- merge(dbfSEV_master,male[,c("location_id","year_id","sex_id","male_population")],by=c("sex_id","location_id","year_id"))
male_SEV$mw_mean <- male_SEV$male_population*male_SEV$mean_value
male_SEV$mw_lower <- male_SEV$male_population*male_SEV$lower_value
male_SEV$mw_upper <- male_SEV$male_population*male_SEV$upper_value
female_SEV <- merge(dbfSEV_master,female[,c("location_id","year_id","sex_id","female_population")],by=c("sex_id","location_id","year_id"))
female_SEV$fw_mean <- female_SEV$female_population*female_SEV$mean_value
female_SEV$fw_lower <- female_SEV$female_population*female_SEV$lower_value
female_SEV$fw_upper <- female_SEV$female_population*female_SEV$upper_value
dbfSEV_merged <- merge(male_SEV,female_SEV[,c("location_id","year_id","female_population","fw_mean","fw_lower","fw_upper")],by=c("location_id","year_id"))
dbfSEV_merged$w_mean <- (dbfSEV_merged$mw_mean+dbfSEV_merged$fw_mean)/(dbfSEV_merged$male_population+dbfSEV_merged$female_population)
dbfSEV_merged$w_lower <- (dbfSEV_merged$mw_lower+dbfSEV_merged$fw_lower)/(dbfSEV_merged$male_population+dbfSEV_merged$female_population)
dbfSEV_merged$w_upper <- (dbfSEV_merged$mw_upper+dbfSEV_merged$fw_upper)/(dbfSEV_merged$male_population+dbfSEV_merged$female_population)
dbfSEV_master <- merge(dbfSEV_master,dbfSEV_merged[,c("location_id","year_id","w_mean","w_lower","w_upper")],by=c("location_id","year_id"))

male_SEV <- merge(zincD_master,male[,c("location_id","year_id","sex_id","male_population")],by=c("sex_id","location_id","year_id"))
male_SEV$mw_mean <- male_SEV$male_population*male_SEV$mean_value
male_SEV$mw_lower <- male_SEV$male_population*male_SEV$lower_value
male_SEV$mw_upper <- male_SEV$male_population*male_SEV$upper_value
female_SEV <- merge(zincD_master,female[,c("location_id","year_id","sex_id","female_population")],by=c("sex_id","location_id","year_id"))
female_SEV$fw_mean <- female_SEV$female_population*female_SEV$mean_value
female_SEV$fw_lower <- female_SEV$female_population*female_SEV$lower_value
female_SEV$fw_upper <- female_SEV$female_population*female_SEV$upper_value
zincD_merged <- merge(male_SEV,female_SEV[,c("location_id","year_id","female_population","fw_mean","fw_lower","fw_upper")],by=c("location_id","year_id"))
zincD_merged$w_mean <- (zincD_merged$mw_mean+zincD_merged$fw_mean)/(zincD_merged$male_population+zincD_merged$female_population)
zincD_merged$w_lower <- (zincD_merged$mw_lower+zincD_merged$fw_lower)/(zincD_merged$male_population+zincD_merged$female_population)
zincD_merged$w_upper <- (zincD_merged$mw_upper+zincD_merged$fw_upper)/(zincD_merged$male_population+zincD_merged$female_population)
zincD_master <- merge(zincD_master,zincD_merged[,c("location_id","year_id","w_mean","w_lower","w_upper")],by=c("location_id","year_id"))

male_SEV <- merge(vitA_master,male[,c("location_id","year_id","sex_id","male_population")],by=c("sex_id","location_id","year_id"))
male_SEV$mw_mean <- male_SEV$male_population*male_SEV$mean_value
male_SEV$mw_lower <- male_SEV$male_population*male_SEV$lower_value
male_SEV$mw_upper <- male_SEV$male_population*male_SEV$upper_value
female_SEV <- merge(vitA_master,female[,c("location_id","year_id","sex_id","female_population")],by=c("sex_id","location_id","year_id"))
female_SEV$fw_mean <- female_SEV$female_population*female_SEV$mean_value
female_SEV$fw_lower <- female_SEV$female_population*female_SEV$lower_value
female_SEV$fw_upper <- female_SEV$female_population*female_SEV$upper_value
vitA_merged <- merge(male_SEV,female_SEV[,c("location_id","year_id","female_population","fw_mean","fw_lower","fw_upper")],by=c("location_id","year_id"))
vitA_merged$w_mean <- (vitA_merged$mw_mean+vitA_merged$fw_mean)/(vitA_merged$male_population+vitA_merged$female_population)
vitA_merged$w_lower <- (vitA_merged$mw_lower+vitA_merged$fw_lower)/(vitA_merged$male_population+vitA_merged$female_population)
vitA_merged$w_upper <- (vitA_merged$mw_upper+vitA_merged$fw_upper)/(vitA_merged$male_population+vitA_merged$female_population)
vitA_master <- merge(vitA_master,vitA_merged[,c("location_id","year_id","w_mean","w_lower","w_upper")],by=c("location_id","year_id"))

end_pull <- Sys.time()
print(paste("dSEV_master",end_pull-start_pull))

#Assembling covariate data frame for all estimated country-years
print("Assembling covariates...")
start_pull <- Sys.time()
covs <- data.frame(location_id=NULL,location_name=NULL,year_id=NULL,cv_inpatient=NULL)
h_rows <- which(hierarchy$level>=3)
find_year <- function(loc_id,find_max){
  year_list <- c()
  if(find_max){
    year_list <- c(year_list,max(subset(sdi_master,location_id==loc_id)$year_id))
    year_list <- c(year_list,max(subset(den_master,location_id==loc_id)$year_id))
    year_list <- c(year_list,max(subset(dSEV_master,location_id==loc_id)$year_id))
    year_list <- c(year_list,max(subset(dbfSEV_master,location_id==loc_id)$year_id))
    year_list <- c(year_list,max(subset(waste_master,location_id==loc_id)$year_id))
    year_list <- c(year_list,max(subset(stunt_master,location_id==loc_id)$year_id))
    year_list <- c(year_list,max(subset(weight_master,location_id==loc_id)$year_id))
    year_list <- c(year_list,max(subset(zincD_master,location_id==loc_id)$year_id))
    year_list <- c(year_list,max(subset(vitA_master,location_id==loc_id)$year_id))
    return(min(year_list))
  } else {
    year_list <- c(year_list,min(subset(sdi_master,location_id==loc_id)$year_id))
    year_list <- c(year_list,min(subset(den_master,location_id==loc_id)$year_id))
    year_list <- c(year_list,min(subset(dSEV_master,location_id==loc_id)$year_id))
    year_list <- c(year_list,min(subset(dbfSEV_master,location_id==loc_id)$year_id))
    year_list <- c(year_list,min(subset(waste_master,location_id==loc_id)$year_id))
    year_list <- c(year_list,min(subset(stunt_master,location_id==loc_id)$year_id))
    year_list <- c(year_list,min(subset(weight_master,location_id==loc_id)$year_id))
    year_list <- c(year_list,min(subset(zincD_master,location_id==loc_id)$year_id))
    year_list <- c(year_list,min(subset(vitA_master,location_id==loc_id)$year_id))
    return(max(year_list))
  }
}
for(row in h_rows){
  loc_id <- hierarchy[4,]$location_id
  min_year <- find_year(loc_id,find_max=FALSE)
  max_year <- find_year(loc_id,find_max=TRUE)
  for(year in seq(min_year,max_year)){
    covs <- rbind(covs,data.frame(location_id=hierarchy[row,]$location_id,location_name=hierarchy[row,]$location_name,year_id=year,cv_inpatient=0))
  }
}
covs <- merge(covs,sdi_master[,c("location_id","location_name","year_id","mean_value")],by=c("location_id","location_name","year_id"))
setnames(covs,"mean_value","sdi")
covs <- merge(covs,den_master[,c("location_id","location_name","year_id","mean_value")],by=c("location_id","location_name","year_id"))
setnames(covs,"mean_value","density")
covs <- merge(covs,subset(dSEV_master,sex_id==1)[,c("location_id","location_name","year_id","w_mean")],by=c("location_id","location_name","year_id"))
setnames(covs,"w_mean","diarrhea_SEV")
covs <- merge(covs,subset(dbfSEV_master,sex_id==1)[,c("location_id","location_name","year_id","w_mean")],by=c("location_id","location_name","year_id"))
setnames(covs,"w_mean","breastfeeding_SEV")
covs <- merge(covs,waste_master[,c("location_id","location_name","year_id","mean_value")],by=c("location_id","location_name","year_id"))
setnames(covs,"mean_value","wasting_SEV")
covs <- merge(covs,stunt_master[,c("location_id","location_name","year_id","mean_value")],by=c("location_id","location_name","year_id"))
setnames(covs,"mean_value","stunting_SEV")
covs <- merge(covs,weight_master[,c("location_id","location_name","year_id","mean_value")],by=c("location_id","location_name","year_id"))
setnames(covs,"mean_value","underweight_SEV")
covs <- merge(covs,subset(zincD_master,sex_id==1)[,c("location_id","location_name","year_id","w_mean")],by=c("location_id","location_name","year_id"))
setnames(covs,"w_mean","zincD")
covs <- merge(covs,subset(vitA_master,sex_id==1)[,c("location_id","location_name","year_id","w_mean")],by=c("location_id","location_name","year_id"))
setnames(covs,"w_mean","vitA")
covs <- covs[,c("location_id","location_name","year_id","sdi","density","diarrhea_SEV","breastfeeding_SEV","wasting_SEV","stunting_SEV","underweight_SEV","zincD","vitA","cv_inpatient")]
end_pull <- Sys.time()
print(paste("Covariate runtime: ",end_pull-start_pull))
print("Covariates assembled. Beginning modeling process.")

#### Crosswalking ####

source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))
source(fix_path("FILEPATH"))

in_dat0 <- dat_RV[which(!is.na(dat_RV$sdi)&dat_RV$rota_ve_mean<=1&dat_RV$rota_ve_mean>=0),]
in_dat0 <- subset(in_dat0,source_type %in% c("RCT","Case-control"))
in_dat0$case_definition <- NA
##Goal: crosswalk case definitions for RCTs
##subset data from rct_nids, rct_alt_nids
rct_nids <- c(24013441,
              22520136,
              24629994,
              19679216,
              20107214,
              22497874,
              23147138,
              21378594,
              21640780,
              22520135,
              18037080,
              18395579,
              27217217,
              28419095,
              23499605,
              22974466,
              17380232,
              16148848)
rct_alt_nids <- c(16394299,
                  19949360,
                  20692031,
                  23732903,
                  22520141,
                  22520140,
                  20559656,
                  18162243,
                  20442684,
                  22520137,
                  15602194,
                  20692030,
                  27755463,
                  28935470,
                  22252206,
                  19879226,
                  10440305)
cc_nids <- c(26680277,
             28693503,
             25638521,
             25303843,
             24508336,
             20047501,
             20083525,
             23776114,
             27059358,
             24699470,
             27059349,
             27059345,
             22753550,
             26935786,
             27059346,
             27059344,
             20551120,
             27059359,
             19491186,
             23783434,
             27815381,
             29655631)
cc_alt_nids <- c(27059357,
                 23583814,
                 23876518,
                 23487388,
                 20637764,
                 24051998,
                 24569388,
                 20634776,
                 26546262,
                 27595446,
                 24569388,
                 27059350,
                 27059351,
                 22875947)
ba_nids <- c(21288843,21048524,21436757,19896451,29162319,29102168)
ba_alt_nids <- c(27367155,21526228,19645646,20587671,21571023,23072242)


#Match RCTs by case definition
rct_gs <- subset(in_dat0,field_citation_value %in% rct_nids)
rct_alt <- subset(in_dat0,field_citation_value %in% rct_alt_nids)
rct_row_matches <- data.frame(
  gs_pmid = NULL,
  gs_loc = NULL,
  gs_year_start = NULL,
  gs_year_end = NULL,
  gs_mean = NULL,
  gs_lower = NULL,
  gs_upper = NULL,
  alt_pmid = NULL,
  alt_loc = NULL,
  alt_year_start = NULL,
  alt_year_end = NULL,
  alt_mean = NULL,
  alt_lower = NULL,
  alt_upper = NULL)

for(n_alt in 1:nrow(rct_alt)){
  row_alt <- rct_alt[n_alt,]
  for(n_gs in 1:nrow(rct_gs)){
    row_gs <- rct_gs[n_gs,]
    cond <- within_five(row_alt,row_gs) & row_alt$location_name == row_gs$location_name
    if(cond){
      row <- data.frame(
        gs_pmid = row_gs$field_citation_value,
        gs_loc = row_gs$location_name,
        gs_year_start = row_gs$year_start,
        gs_year_end = row_gs$year_end,
        gs_mean = row_gs$rota_ve_mean,
        gs_lower = row_gs$rota_ve_lower,
        gs_upper = row_gs$rota_ve_upper,
        alt_pmid = row_alt$field_citation_value,
        alt_loc = row_alt$location_name,
        alt_year_start = row_alt$year_start,
        alt_year_end = row_alt$year_end,
        alt_mean = row_alt$rota_ve_mean,
        alt_lower = row_alt$rota_ve_lower,
        alt_upper = row_alt$rota_ve_upper)
      rct_row_matches <- rbind(rct_row_matches,row)
    }
  }
}
rct_in_dat <- rct_row_matches

#Match CCs by case definition
cc_gs <- subset(in_dat0,field_citation_value %in% cc_nids)
cc_alt <- subset(in_dat0,field_citation_value %in% cc_alt_nids)
cc_row_matches <- data.frame(
  gs_pmid = NULL,
  gs_loc = NULL,
  gs_year_start = NULL,
  gs_year_end = NULL,
  gs_mean = NULL,
  gs_lower = NULL,
  gs_upper = NULL,
  alt_pmid = NULL,
  alt_loc = NULL,
  alt_year_start = NULL,
  alt_year_end = NULL,
  alt_mean = NULL,
  alt_lower = NULL,
  alt_upper = NULL)

for(n_alt in 1:nrow(cc_alt)){
  row_alt <- cc_alt[n_alt,]
  for(n_gs in 1:nrow(cc_gs)){
    row_gs <- cc_gs[n_gs,]
    cond <- within_five(row_alt,row_gs) & row_alt$location_name == row_gs$location_name
    if(cond){
      row <- data.frame(
        gs_pmid = row_gs$field_citation_value,
        gs_loc = row_gs$location_name,
        gs_year_start = row_gs$year_start,
        gs_year_end = row_gs$year_end,
        gs_mean = row_gs$rota_ve_mean,
        gs_lower = row_gs$rota_ve_lower,
        gs_upper = row_gs$rota_ve_upper,
        alt_pmid = row_alt$field_citation_value,
        alt_loc = row_alt$location_name,
        alt_year_start = row_alt$year_start,
        alt_year_end = row_alt$year_end,
        alt_mean = row_alt$rota_ve_mean,
        alt_lower = row_alt$rota_ve_lower,
        alt_upper = row_alt$rota_ve_upper)
      cc_row_matches <- rbind(cc_row_matches,row)
    }
  }
}
cc_in_dat <- cc_row_matches


#Goal: crosswalk alternative case definition RCTs in log-space

#log-transform RR for alternative case definitions
rct_alt$rr <- 1-rct_alt$rota_ve_mean
rct_alt$se <- (rct_alt$rota_ve_lower-rct_alt$rota_ve_upper)/2/qnorm(0.975)
rct_alt$log_rr <- log(rct_alt$rr)
rct_alt$log_se <- sapply(1:nrow(rct_alt),function(n){
  m <- rct_alt[n,"rr"]
  se <- rct_alt[n,"se"]
  return(deltamethod(~log(x1),m,se^2))
})

#take ratios of RR_gs/RR_alt in matched location-years
rct_in_dat$gs_rr <- 1-rct_in_dat$gs_mean
rct_in_dat$alt_rr <- 1-rct_in_dat$alt_mean
rct_in_dat$gs_rr_se <- (rct_in_dat$gs_lower-rct_in_dat$gs_upper)/2/qnorm(0.975)
rct_in_dat$alt_rr_se <- (rct_in_dat$alt_lower-rct_in_dat$alt_upper)/2/qnorm(0.975)
rct_in_dat$ratio <- rct_in_dat$gs_rr/rct_in_dat$alt_rr

#calculate se of ratio
rct_in_dat$ratio_se <- rct_in_dat$ratio*sqrt((rct_in_dat$gs_rr_se/rct_in_dat$gs_rr)^2+(rct_in_dat$alt_rr_se/rct_in_dat$alt_rr)^2)

#log-transform ratio, se
rct_in_dat$log_ratio <- log(rct_in_dat$ratio)
rct_in_dat$log_ratio_se <- sapply(1:nrow(rct_in_dat),function(n){
  m <- rct_in_dat[n,"ratio"]
  se <- rct_in_dat[n,"ratio_se"]
  return(deltamethod(~log(x1),m,se^2))
})

#meta-analysis: estimate log-ratio, log-se
output_dir <- fix_path("ADDRESS")
model_label <- "test"
mean_var <- "log_ratio"
se_var <- "log_ratio_se"

fit <- run_mr_brt(output_dir=output_dir,model_label=model_label,data=rct_in_dat,mean_var=mean_var,se_var=se_var,overwrite_previous=TRUE)
df_pred <- data.frame(intercept=1)
fit_pred <- predict_mr_brt(fit, newdata = df_pred)
pred_object <- load_mr_brt_preds(fit_pred)
preds <- pred_object$model_summaries
out <- data.frame(ratio = preds$Y_mean,
                  se = (preds$Y_mean_hi - preds$Y_mean_lo)/2/qnorm(0.975))
rct_rct_coefs <- data.frame(ratio = preds$Y_mean, lb = preds$Y_mean_lo, ub = preds$Y_mean_hi)
#adjust individual studies in log-space with new ratio
rct_alt$adj_log_rr <- rct_alt$log_rr + out$ratio

#recalculate standard error for each study
rct_alt$adj_log_se <- sqrt(rct_alt$log_se^2 + out$se^2)

#transform gold-standard definitions to log-space
rct_gs$adj_log_rr <- log(1-rct_gs$rota_ve_mean)
rct_gs$adj_log_se <- sapply(1:nrow(rct_gs),function(n){
  deltamethod(~log(x1),(1-rct_gs$rota_ve_mean[n]),with(rct_gs,(rota_ve_upper[n]-rota_ve_lower[n])/2/qnorm(0.975))^2)
})

rct_alt$gs <- 0
rct_gs$gs <- 1
sub_rct_alt <- rct_alt[,names(rct_gs)]
rcts <- rbind(rct_gs,sub_rct_alt)



#Goal: crosswalk alternative case definition CCs in log-space

#log-transform RR for alternative case definitions
cc_alt$rr <- 1-cc_alt$rota_ve_mean
cc_alt$se <- (cc_alt$rota_ve_lower-cc_alt$rota_ve_upper)/2/qnorm(0.975)
cc_alt$log_rr <- log(cc_alt$rr)
cc_alt$log_se <- sapply(1:nrow(cc_alt),function(n){
  m <- cc_alt[n,"rr"]
  se <- cc_alt[n,"se"]
  return(deltamethod(~log(x1),m,se^2))
})

#take ratios of RR_gs/RR_alt in matched location-years
cc_in_dat$gs_rr <- 1-cc_in_dat$gs_mean
cc_in_dat$alt_rr <- 1-cc_in_dat$alt_mean
cc_in_dat$gs_rr_se <- (cc_in_dat$gs_lower-cc_in_dat$gs_upper)/2/qnorm(0.975)
cc_in_dat$alt_rr_se <- (cc_in_dat$alt_lower-cc_in_dat$alt_upper)/2/qnorm(0.975)
cc_in_dat$ratio <- cc_in_dat$gs_rr/cc_in_dat$alt_rr

#calculate se of ratio
cc_in_dat$ratio_se <- cc_in_dat$ratio*sqrt((cc_in_dat$gs_rr_se/cc_in_dat$gs_rr)^2+(cc_in_dat$alt_rr_se/cc_in_dat$alt_rr)^2)

#log-transform ratio, se
cc_in_dat$log_ratio <- log(cc_in_dat$ratio)
cc_in_dat$log_ratio_se <- sapply(1:nrow(cc_in_dat),function(n){
  m <- cc_in_dat[n,"ratio"]
  se <- cc_in_dat[n,"ratio_se"]
  return(deltamethod(~log(x1),m,se^2))
})

#meta-analysis: estimate log-ratio, log-se
output_dir <- fix_path("ADDRESS")
model_label <- "test"
mean_var <- "log_ratio"
se_var <- "log_ratio_se"

fit <- run_mr_brt(output_dir=output_dir,model_label=model_label,data=cc_in_dat,mean_var=mean_var,se_var=se_var,overwrite_previous=TRUE)
df_pred <- data.frame(intercept=1)
fit_pred <- predict_mr_brt(fit, newdata = df_pred)
pred_object <- load_mr_brt_preds(fit_pred)
preds <- pred_object$model_summaries
out <- data.frame(ratio = preds$Y_mean,
                  se = (preds$Y_mean_hi - preds$Y_mean_lo)/2/qnorm(0.975))
cc_cc_coefs <- data.frame(ratio = preds$Y_mean, lb = preds$Y_mean_lo, ub = preds$Y_mean_hi)
#adjust individual studies in log-space with new ratio
cc_alt$adj_log_rr <- cc_alt$log_rr + out$ratio

#recalculate standard error for each study
cc_alt$adj_log_se <- sqrt(cc_alt$log_se^2 + out$se^2)

#transform gold-standard definitions to log-space
cc_gs$adj_log_rr <- log(1-cc_gs$rota_ve_mean)
cc_gs$adj_log_se <- sapply(1:nrow(cc_gs),function(n){
  deltamethod(~log(x1),(1-cc_gs$rota_ve_mean[n]),with(cc_gs,(rota_ve_upper[n]-rota_ve_lower[n])/2/qnorm(0.975))^2)
})

cc_alt$gs <- 0
cc_gs$gs <- 1
sub_cc_alt <- cc_alt[,names(cc_gs)]
ccs <- rbind(cc_gs,sub_cc_alt)


#Match CCs to RCTs
sd_row_matches <- data.frame(
  rct_pmid = NULL,
  rct_loc = NULL,
  rct_year_start = NULL,
  rct_year_end = NULL,
  rct_mean = NULL,
  rct_lower = NULL,
  rct_upper = NULL,
  cc_pmid = NULL,
  cc_loc = NULL,
  cc_year_start = NULL,
  cc_year_end = NULL,
  cc_mean = NULL,
  cc_lower = NULL,
  cc_upper = NULL)

for(n_alt in 1:nrow(ccs)){
  row_alt <- ccs[n_alt,]
  for(n_gs in 1:nrow(rcts)){
    row_gs <- rcts[n_gs,]
    cond <- within_five(row_alt,row_gs) & row_alt$location_name == row_gs$location_name
    if(cond){
      row <- data.frame(
        rct_pmid = row_gs$field_citation_value,
        rct_loc = row_gs$location_name,
        rct_year_start = row_gs$year_start,
        rct_year_end = row_gs$year_end,
        rct_mean = row_gs$rota_ve_mean,
        rct_lower = row_gs$rota_ve_lower,
        rct_upper = row_gs$rota_ve_upper,
        cc_pmid = row_alt$field_citation_value,
        cc_loc = row_alt$location_name,
        cc_year_start = row_alt$year_start,
        cc_year_end = row_alt$year_end,
        cc_mean = row_alt$rota_ve_mean,
        cc_lower = row_alt$rota_ve_lower,
        cc_upper = row_alt$rota_ve_upper)
      sd_row_matches <- rbind(sd_row_matches,row)
    }
  }
}
sd_in_dat <- sd_row_matches

ccs0 <- ccs
rcts0 <- rcts

#Goal: crosswalk study designs

#log-transform RR for alternative case definitions
ccs$rr <- 1-ccs$rota_ve_mean
ccs$se <- (ccs$rota_ve_lower-ccs$rota_ve_upper)/2/qnorm(0.975)
ccs$log_rr <- log(ccs$rr)
ccs$log_se <- sapply(1:nrow(ccs),function(n){
  m <- ccs[n,"rr"]
  se <- ccs[n,"se"]
  return(deltamethod(~log(x1),m,se^2))
})

#take ratios of RR_gs/RR_alt in matched location-years
sd_in_dat$rct_rr <- 1-sd_in_dat$rct_mean
sd_in_dat$cc_rr <- 1-sd_in_dat$cc_mean
sd_in_dat$rct_rr_se <- (sd_in_dat$rct_lower-sd_in_dat$rct_upper)/2/qnorm(0.975)
sd_in_dat$cc_rr_se <- (sd_in_dat$cc_lower-sd_in_dat$cc_upper)/2/qnorm(0.975)
sd_in_dat$ratio <- sd_in_dat$rct_rr/sd_in_dat$cc_rr

#calculate se of ratio
sd_in_dat$ratio_se <- sd_in_dat$ratio*sqrt((sd_in_dat$rct_rr_se/sd_in_dat$rct_rr)^2+(sd_in_dat$cc_rr_se/sd_in_dat$cc_rr)^2)

#log-transform ratio, se
sd_in_dat$log_ratio <- log(sd_in_dat$ratio)
sd_in_dat$log_ratio_se <- sapply(1:nrow(sd_in_dat),function(n){
  m <- sd_in_dat[n,"ratio"]
  se <- sd_in_dat[n,"ratio_se"]
  return(deltamethod(~log(x1),m,se^2))
})

#meta-analysis: estimate log-ratio, log-se
output_dir <- fix_path("ADDRESS")
model_label <- "test"
mean_var <- "log_ratio"
se_var <- "log_ratio_se"

fit <- run_mr_brt(output_dir=output_dir,model_label=model_label,data=sd_in_dat,mean_var=mean_var,se_var=se_var,overwrite_previous=TRUE)
df_pred <- data.frame(intercept=1)
fit_pred <- predict_mr_brt(fit, newdata = df_pred)
pred_object <- load_mr_brt_preds(fit_pred)
preds <- pred_object$model_summaries
out <- data.frame(ratio = preds$Y_mean,
                  se = (preds$Y_mean_hi - preds$Y_mean_lo)/2/qnorm(0.975))
rct_cc_coefs <- data.frame(ratio = preds$Y_mean, lb = preds$Y_mean_lo, ub = preds$Y_mean_hi)
#adjust individual studies in log-space with new ratio
ccs$adj_log_rr <- ccs$log_rr + out$ratio

#recalculate standard error for each study
ccs$adj_log_se <- sqrt(ccs$log_se^2 + out$se^2)

#transform gold-standard definitions to log-space
rcts$adj_log_rr <- log(1-rcts$rota_ve_mean)
rcts$adj_log_se <- sapply(1:nrow(rcts),function(n){
  deltamethod(~log(x1),(1-rcts$rota_ve_mean[n]),with(rcts,(rota_ve_upper[n]-rota_ve_lower[n])/2/qnorm(0.975))^2)
})

sub_ccs <- ccs[,names(rcts)]
in_dat0 <- in_dat <- rbind(rcts,sub_ccs)
#### Model Fitting/Covariate Selection ####
#Goal: use MR-BRT for meta-regression, covariate selection

full_cov_names <- c("sdi","density","diarrhea_SEV","breastfeeding_SEV","wasting_SEV","stunting_SEV","underweight_SEV","vitA","zincD")
cov_names <- full_cov_names
#convert adjusted log-VE to linear space
in_dat$adj_rr <- exp(in_dat$adj_log_rr)
in_dat$adj_se <- sapply(1:nrow(in_dat),function(n){
  m <- in_dat$adj_log_rr[n]
  se <- in_dat$adj_log_se[n]
  deltamethod(~exp(x1),m,se^2)
})

#convert linear-space VE to logit space
in_dat$adj_logit_rr <- logit(in_dat$adj_rr)
in_dat$adj_logit_se <- sapply(1:nrow(in_dat),function(n){
  m <- in_dat$adj_rr[n]
  se <- in_dat$adj_se[n]
  deltamethod(~log(x1/(1-x1)),m,se^2)
})

output_dir <- fix_path("ADDRESS")
model_label <- "test"
mean_var <- "adj_logit_rr"
se_var <- "adj_logit_se"

covs0 <- list()
for(cov in cov_names){
  covs0[length(covs0)+1] <- list(cov_info(cov,"X",type="proportion"))
}

#Normalize variables for MRBRT lasso function
for(covariate in full_cov_names){
  in_dat[,covariate] <- in_dat[,covariate]/max(covs[,covariate])
}

#using leave-one-out cross validation to select the optimum value of lambda for MR-BRT's lasso functionality
run_cross_validation <- FALSE
leave_one_out <- function(dat0,covariates,lambda){
  sum_rss <- 0
  s_df <- data.frame(n=NULL,sum_rss=NULL)
  k <- 86
  in_dat$kfold <- cut(seq(1,nrow(in_dat)),breaks=k,labels=FALSE)
  for(k in 1:k){
    print(k)
    sub_inds <- which(in_dat$kfold==k)
    dat <- dat0[-sub_inds,]
    fit <- run_mr_brt(output_dir=output_dir,model_label=model_label,data=in_dat,mean_var=mean_var,se_var=se_var,covs=covs0,study_id="field_citation_value",overwrite_previous=TRUE,lasso=TRUE,lambda_multiplier=lambda,trim_pct=0.1,method="trim_maxL")
    check_for_outputs(fit)
    df_pred <- covariates
    fit_pred <- predict_mr_brt(fit,newdata = dat0[sub_inds,])
    check_for_preds(fit_pred)
    pred_object <- load_mr_brt_preds(fit_pred)
    preds <- pred_object$model_summaries
    out <- data.frame(adj_log_rr = preds$Y_mean,
                      adj_log_se = (preds$Y_mean_hi - preds$Y_mean_lo)/2/qnorm(0.975))
    sum_rss <- sum_rss + sum((dat0[sub_inds,]$adj_log_rr-out$adj_log_rr)^2)
    s_df <- rbind(s_df,data.frame(n=sub_inds,sum_rss=sum_rss))
  }
  return(sum_rss/nrow(dat0[sub_inds,]))
}

if(run_cross_validation){
  l_df <- data.frame(lambda=NULL,avg_rss=NULL)
  for(lambda in c(0,1E1,1E2,1E3,1E4,1E5,1E6,1E7,1E8,1E9)){
    avg_rss <- leave_one_out(in_dat,covs0,lambda)
    l_df <- rbind(l_df,data.frame(lambda=lambda,avg_rss=avg_rss))
  }
  l_fit <- loess(avg_rss~lambda,data=l_df)
  l_pred <- predict(l_fit)
  l_df$smoothed_rss <- l_pred
  lambda <- l_df[which(l_df$smoothed_rss==min(l_df$smoothed_rss)),"lambda"]
  write.csv(l_df,file="FILEPATH")
} else{
  lambda <- 10000
}

fit <- run_mr_brt(output_dir=output_dir,model_label=model_label,data=in_dat,mean_var=mean_var,se_var=se_var,covs=covs0,study_id="field_citation_value",overwrite_previous=TRUE,lasso=TRUE,lambda_multiplier=lambda,trim_pct=0.1,method="trim_maxL")
check_for_outputs(fit)

#Predict RR in all country-years in GBD
outputs <- covs
df_pred <- covs
df_pred$intercept <- 1
df_pred$year_id <- round(sapply(1:nrow(df_pred),function(n){mean(c(df_pred$year_start[n],df_pred$year_end[n]))}))
for(covariate in full_cov_names){
  df_pred[,covariate] <- df_pred[,covariate]/max(covs[,covariate])
}
fit_pred <- predict_mr_brt(fit, newdata = df_pred)
check_for_preds(fit_pred)
pred_object <- load_mr_brt_preds(fit_pred)
preds <- pred_object$model_summaries
out <- data.frame(adj_logit_rr = preds$Y_mean,
                  adj_logit_se = (preds$Y_mean_hi - preds$Y_mean_lo)/2/qnorm(0.975))
outputs$pred_ve <- 1-inv.logit(out$adj_logit_rr)
outputs$pred_se <- sapply(1:nrow(out),function(n){
  m <- out$adj_logit_rr[n]
  se <- out$adj_logit_se[n]
  return(deltamethod(~exp(x1)/(1+exp(x1)),m,se^2))
})

#calculate upper/lower bounds in logit space
#convert to linear space
outputs$adj_logit_rr_ub <- out$adj_logit_rr + qnorm(0.975)*out$adj_logit_se
outputs$adj_logit_rr_lb <- out$adj_logit_rr - qnorm(0.975)*out$adj_logit_se
outputs$adj_rr_ub <- inv.logit(outputs$adj_logit_rr_ub)
outputs$adj_rr_lb <- inv.logit(outputs$adj_logit_rr_lb)
outputs$pred_ub <- 1-outputs$adj_rr_ub
outputs$pred_lb <- 1-outputs$adj_rr_lb

td <- fit$train_data
td$inv_var <- 1/((td$rota_ve_upper - td$rota_ve_lower)/2/qnorm(0.975))^2
td$year_id <- (td$year_start+td$year_end)/2
td$w <- factor(td$w,levels=c(0,1))


#Predict RR along all values of SDI
sdi_outputs <- data.frame(sdi=seq(0,1,0.001))
df_pred <- data.frame(sdi=seq(0,1,0.001))
df_pred[,c("density","diarrhea_SEV","wasting_SEV","stunting_SEV","underweight_SEV","zincD","vitA","cv_inpatient")] <- 0
df_pred$intercept <- 1
for(covariate in c("sdi")){
  df_pred[,covariate] <- df_pred[,covariate]/max(covs[,covariate])
}
fit_pred <- predict_mr_brt(fit, newdata = df_pred, write_draws = TRUE)
check_for_preds(fit_pred)
pred_object <- load_mr_brt_preds(fit_pred)
inds <- draw_inds(pred_object$model_draws)[[1]]
pred_draws <- 1-inv.logit(pred_object$model_draws[,inds])
sdi_outputs <- cbind(sdi_outputs,pred_draws)


g <- ggplot(outputs,aes(x=sdi,y=pred_ve)) +
  geom_point() +
  theme_bw() +
  geom_ribbon(aes(ymin=pred_lb,ymax=pred_ub),fill="blue",col=NA,alpha=0.15) +
  geom_point(data=td,aes(x=sdi,y=rota_ve_mean,col=source_type,size=inv_var)) +
  geom_point(data=subset(td,w==0),aes(x=sdi,y=rota_ve_mean),shape=4,size=5) +
  ylim(0,1) + xlim(0,1) +
  labs(title="Predicted VE vs. SDI",subtitle=paste("RCTs + Adjusted Case-controls \n λ=",lambda)) + xlab("SDI") + ylab("Predicted VE")
g


###

#### End ####
write.csv(outputs,file=fix_path("FILEPATH"))
code_end <- Sys.time()
print("Total runtime: ")
print(code_end-code_start)
