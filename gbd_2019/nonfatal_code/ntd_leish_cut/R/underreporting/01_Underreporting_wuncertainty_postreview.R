########################################################
########################################################

##	Prep R

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}		

##	Load shared function
library(RColorBrewer)
source(sprintf("%s/FILEPATH",prefix))
source(sprintf("%s/FILEPATH",prefix))
source(sprintf("%s/FILEPATH",prefix))
source(sprintf("%s/FILEPATH",prefix))

ilogit <- function(x)1/(1+exp(-x))

##	Set up directories

in_dir <- sprintf("FILEPATH",prefix)
tmp_in_dir <- sprintf("FILEPATH",prefix)
code_dir <- sprintf("FILEPATH",prefix)
underreport_dir <- sprintf("FILEPATH",prefix)

experiment_name<-"ADDRESS"


##	Create underreporting model

haqi <- get_covariate_estimates(ADDRESS)
sdi <- get_covariate_estimates(ADDRESS)
leish_presence <- get_covariate_estimates(ADDRESS)
leish_endemic_vl <- get_covariate_estimates(ADDRESS)
leish_endemic_cl <- get_covariate_estimates(ADDRESS)

underreport_raw <- read.csv(sprintf("FILEPATH",in_dir))

n_reps<-1000
underreport_list<-list()
year_string<-NA
proportion_string<-NA
proportion_error<-NA
higher_bound<-NA
lower_bound<-NA
proportion_sample<-NA
std_dev<-NA
random_location<-NA
#create a string of unique locations, as want to include effect of including/excluding
uniqlo<-unique(underreport_raw$loc_id)

for (k in 1:n_reps){
  for (j in 1:nrow(underreport_raw)){
    
    if(length(underreport_raw$year_start[j]:underreport_raw$year_end[j])==1){
      year_string[j]<-underreport_raw$year_start[j]
    }else{
      year_string[j]<-sample(underreport_raw$year_start[j]:underreport_raw$year_end[j],1)
    }
    
    proportion_string[j]<-underreport_raw$observed_cases[j]/underreport_raw$true_cases[j]
    proportion_error[j]<-sqrt((proportion_string[j]*(1-proportion_string[j]))/underreport_raw$true_cases[j])
    higher_bound[j]<-proportion_string[j]+(1.96*proportion_error[j])
    lower_bound[j]<-proportion_string[j]-(1.96*proportion_error[j])
    std_dev[j]<-proportion_error[j]*(sqrt(underreport_raw$true_cases[j]))
    proportion_sample[j]<-rnorm(1,proportion_string[j], proportion_error[j])
    
    
  }
  
  
  
  underreport_list[[k]]<-data.frame(location_name=underreport_raw$location_name,
                                    year_start=year_string,
                                    loc_id=underreport_raw$loc_id,
                                    proportion=proportion_string,
                                    prop_low=lower_bound,
                                    prop_upper=higher_bound,
                                    std_dev=std_dev,
                                    proportion_sample=proportion_sample,
                                    pathogen=underreport_raw$pathogen,
                                    observed_cases=underreport_raw$observed_cases,
                                    true_cases=underreport_raw$true_cases
                                    
  )
  random_location[k]<-sample(uniqlo, 1)
  underreport_list[[k]]<-subset(underreport_list[[k]], underreport_list[[k]]$loc_id!=random_location[k])
}

#subset each into a training and a test dataset
#determine 90/10 fraction
fraction<-0.9


underreport_train<-list()
underreport_test<-list()
sampling_frame<-list()
omission_frame<-list()

for (i in 1:n_reps){
  reference_string<-seq(1, nrow(underreport_list[[i]]),1)
  sampling_frame[[i]]<-sample(reference_string,round(nrow(underreport_list[[i]])*fraction))
  omission_frame[[i]]<-setdiff(reference_string, sampling_frame[[i]])
  underreport_train[[i]]<-underreport_list[[i]][sampling_frame[[i]],]
  underreport_test[[i]]<-underreport_list[[i]][omission_frame[[i]],]
}


mod<-list()
pred_test<-list()
for (k in 1:n_reps){
  
  underreport<-underreport_train[[k]]
  
  pulled_year <- pulled_haqi <- pulled_sdi <- pulled_leish_e <- rep(NA,length(underreport[,1]))
  
  for (i in 1:length(underreport[,1])){
    pulled_year[i] <- underreport$year_start[i]
    
    pulled_haqi[i] <- haqi[location_id==underreport$loc_id[i] & year_id ==pulled_year[i]]$mean_value
    
    pulled_sdi[i] <- sdi[location_id==underreport$loc_id[i] & year_id ==pulled_year[i]]$mean_value
    
    if (underreport$pathogen[i]=='cl'){
      pulled_leish_e[i] <- leish_endemic_cl[location_id==underreport$loc_id[i] & year_id ==pulled_year[i]]$mean_value
    }
    if (underreport$pathogen[i]=='vl'){
      pulled_leish_e[i] <- leish_endemic_vl[location_id==underreport$loc_id[i] & year_id ==pulled_year[i]]$mean_value
    }
  }
  
  
  pulled_data <- data.frame(observed = underreport$observed_cases,
                            true = underreport$true_cases,
                            pathogen = as.factor(underreport$pathogen),
                            year = pulled_year,
                            haqi = pulled_haqi,
                            sdi = pulled_sdi,
                            leish_endemic = pulled_leish_e)
  
  mod[[k]] <- glm(observed/true ~ pathogen + year + sdi, data = pulled_data, weights=true,family = binomial())

  LocToPull <- 4844
  pred_year <- 1980:2017
  pred_path <- rep(levels(pulled_data$pathogen)[2], length(pred_year))
  pred_sdi <- sdi[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value

  
  pred_test[[k]] <- predict(mod[[k]],data.frame(year = pred_year, pathogen = pred_path,  sdi = pred_sdi),se=TRUE)
}

#create a named .RData file tagged with experiment_name

save(mod,
     underreport_train,
     underreport_test,
     underreport_raw,
     file = sprintf("%s/%s.RData",underreport_dir,experiment_name))


colors<-brewer.pal(10, "Set3")

plot(pred_year, ilogit(pred_test[[1]]$fit), type='n', ylim=c(0,1), ylab='Proportion reported', xlab='Year', main="Bihar")
for (i in 1:n_reps){
  Upper <- ilogit(pred_test[[i]]$fit + 1.96 * pred_test[[i]]$se.fit)
  Mean <- ilogit(pred_test[[i]]$fit)
  Lower <- ilogit(pred_test[[i]]$fit - 1.96 * pred_test[[i]]$se.fit)
  
  lines(pred_year, Upper, lwd=2,col=rgb(0,0,1,.01))
  lines(pred_year, Lower, lwd=2,col=rgb(0,0,1,.01))
  lines(pred_year, Mean, lwd=2,col=colors[which(uniqlo==random_location[i])])
  #legend currently incorrect
  legend("topleft", legend=uniqlo, lwd=2, col=colors)
}

#generate coefficients summary
coeff_pathogen<-NA
coeff_year<-NA
coeff_sdi<-NA

for (i in 1:n_reps){
  coeff_pathogen[i]<-mod[[i]]$coefficients[2]
  coeff_year[i]<-mod[[i]]$coefficients[3]
  coeff_sdi[i]<-mod[[i]]$coefficients[4]
}


# 
# 
# 
# 
# plot(pred_year, ilogit(pred_test[[1]]$fit), type='n', ylim=c(0,1), ylab='Proportion reported', xlab='Year')
# for (i in 1:n_reps){
#   Upper <- ilogit(pred_test[[i]]$fit + 1.96 * pred_test[[i]]$se.fit)
#   Mean <- ilogit(pred_test[[i]]$fit)
#   Lower <- ilogit(pred_test[[i]]$fit - 1.96 * pred_test[[i]]$se.fit)
#   
#   #lines(pred_year, Upper, lwd=2,col=rgb(0,0,1,.01))
#   #lines(pred_year, Lower, lwd=2,col=rgb(0,0,1,.01))
#   lines(pred_year, Mean, lwd=2,col=colors[which(uniqlo==random_location[i])])
#   
#   #plot Alvar data
#   #for Bihar
#   abline(h=1/4.2,lwd=2, col=3, lty=4)
#   abline(h=1/8.1,lwd=2, col=3, lty=4)
#   
#   #polygon(c(pred_year,rev(pred_year)),c(Upper,rev(Lower)),col=rgb(1,0,0,.1))
#   #Bihar data
  # segments(2002.9,
  #          (8/65)+(1.96*sqrt(((8/65)*(1-(8/65)))/65)),
  #          2002.9,
  #          (8/65)-(1.96*sqrt(((8/65)*(1-(8/65)))/65))
  # 
  # )
  # segments(2003.1,
  #          (109/876)+(1.96*sqrt(((109/876)*(1-(109/876)))/876)),
  #          2003.1,
  #          (109/876)-(1.96*sqrt(((109/876)*(1-(109/876)))/876))
  # 
  # )
  # segments(2007.9,
  #          (111/130)+(1.96*sqrt(((111/130)*(1-(111/130)))/130)),
  #          2007.9,
  #          (111/130)-(1.96*sqrt(((111/130)*(1-(111/130)))/130))
  # 
  # )
  # segments(2008.1,
  #          (119/127)+(1.96*sqrt(((119/127)*(1-(119/127)))/127)),
  #          2008.1,
  #          (119/127)-(1.96*sqrt(((119/127)*(1-(119/127)))/127))
  # 
  # )
  # segments(2006,
  #          (34/177)+(1.96*sqrt(((34/177)*(1-(34/177)))/177)),
  #          2006,
  #          (34/177)-(1.96*sqrt(((34/177)*(1-(34/177)))/177))
  # 
  # )
#   
# }
# #for Bangladesh
# plot(pred_year, ilogit(pred_test[[1]]$fit), type='n', ylim=c(0,1), ylab='Proportion reported', xlab='Year')
# for (i in 1:n_reps){
#   Upper <- ilogit(pred_test[[i]]$fit + 1.96 * pred_test[[i]]$se.fit)
#   Mean <- ilogit(pred_test[[i]]$fit)
#   Lower <- ilogit(pred_test[[i]]$fit - 1.96 * pred_test[[i]]$se.fit)
#   
#   lines(pred_year, Upper, lwd=2,col=rgb(0,0,1,.01))
#   lines(pred_year, Lower, lwd=2,col=rgb(0,0,1,.01))
#   lines(pred_year, Mean, lwd=2,col=rgb(1,0,0,.05))
#   
#   #plot Alvar data
#   #for Bangladesh
#   abline(h=1/2,lwd=2, col=3, lty=4)
#   abline(h=1/4,lwd=2, col=3, lty=4)
#   
#   #polygon(c(pred_year,rev(pred_year)),c(Upper,rev(Lower)),col=rgb(1,0,0,.1))
#   #Bangladesh data
#   segments(2010.6,
#            (29/58)+(1.96*sqrt(((29/58)*(1-(29/58)))/58)),
#            2010.6,
#            (29/58)-(1.96*sqrt(((29/58)*(1-(29/58)))/58))
#            
#   )
#   segments(2010.4,
#            (756/1087)+(1.96*sqrt(((756/1087)*(1-(756/1087)))/1087)),
#            2010.4,
#            (756/1087)-(1.96*sqrt(((756/1087)*(1-(756/1087)))/1087))
#            
#   )
#   segments(2008,
#            (20/32)+(1.96*sqrt(((20/32)*(1-(20/32)))/32)),
#            2008,
#            (20/32)-(1.96*sqrt(((20/32)*(1-(20/32)))/32))
#            
#   )
#   
#   
# }
# #plot each countries underreporting
# 
# #define country set
# underreporting_alvar <- read.csv(sprintf("%s/underreporting_factors_alvar_2012.csv",in_dir))
# alvar_locs_vl<-unique(underreporting_alvar$location_id)
# 
# 
# #import data - vl
# leish_geo<-read.csv(sprintf("%s/Project/NTDs/geographic_restrictions/GBD_2017/datasets/geo_restrict_vl.csv",prefix), stringsAsFactors = FALSE)
# dataset<-leish_geo[,1:48]
# 
# 
# #convert to long
# melt_data<-melt(dataset, id.vars = c('loc_id',
#                                      'parent_id',
#                                      'level',
#                                      'type',
#                                      'loc_nm_sh',
#                                      'loc_name',
#                                      'spr_reg_id',
#                                      'region_id',
#                                      'ihme_lc_id',
#                                      'GAUL_CODE'))
# 
# melt_data$value<-as.character(melt_data$value)
# 
# status_list<-subset(melt_data, type=='status')
# 
# presence_list<-subset(status_list,value=='p' |value=='pp')
# 
# unique_vl_locations<-unique(presence_list$loc_id)
# #skip location_id = 44533 China without HK and Macao - as of 3 January 2018 no HAQI values
# unique_vl_locations<-unique_vl_locations[-which(unique_vl_locations==44533)]
# 
# pred<-list()
# for (alpha in 1:length(unique_vl_locations)){
#   LocToPull <- unique_vl_locations[alpha]
#   Location_name<-unique(presence_list$loc_name[which(presence_list$loc_id==unique_vl_locations[alpha])])
#   pred_year <- 1980:2017
#   pred_path <- rep(levels(pulled_data$pathogen)[2], length(pred_year))
#   pred_haqi <- haqi[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value
#   pred_sdi <- sdi[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value
#   pred_leish_e <- leish_endemic_vl[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value
#   
#   for(k in 1:n_reps){
#     pred[[k]] <- predict(mod[[k]],data.frame(year = pred_year, pathogen = pred_path, haqi = pred_haqi, sdi = pred_sdi, leish_endemic = pred_leish_e),se=TRUE)
#   }
#   
#   pdf(file=paste0(prefix, '/WORK/04_epi/02_models/01_code/06_custom/ntd_leish/gbd_2017/underreporting_outputs/vl/', unique_vl_locations[alpha], '.pdf'))
#   plot(pred_year, ilogit(pred[[1]]$fit), type='n', ylim=c(0,1), main=paste0(Location_name,' ',unique_vl_locations[alpha]),
#        ylab='Proportion reported', xlab='Year')
#   for (i in 1:n_reps){
#     Upper <- ilogit(pred[[i]]$fit + 1.96 * pred[[i]]$se.fit)
#     Mean <- ilogit(pred[[i]]$fit)
#     Lower <- ilogit(pred[[i]]$fit - 1.96 * pred[[i]]$se.fit)
#     lines(pred_year, Mean, lwd=2,col=2)
#     #polygon(c(pred_year,rev(pred_year)),c(Upper,rev(Lower)),col=rgb(1,0,0,.1))
#     if(unique_vl_locations[alpha] %in% alvar_locs_vl){
#       lines(pred_year, rep(1/underreporting_alvar$vl_underreporting_lo[which(underreporting_alvar$location_id==unique_vl_locations[alpha])], length(pred_year)),
#             lwd=2, col=3, lty=4)
#       lines(pred_year, rep(1/underreporting_alvar$vl_underreporting_hi[which(underreporting_alvar$location_id==unique_vl_locations[alpha])], length(pred_year)),
#             lwd=2, col=3, lty=4)
#       abline(v=2010, lty=4, col=3)
#     }
#   }
#   dev.off()
# }
# 
# 
# #bihar points
# points(y=c(8/65,109/876,111/130,119/127),
#        x=c(2003,2003,2008,2008))
# 
# 
# ##############CL mapping
# #define country set
# underreporting_alvar <- read.csv(sprintf("%s/underreporting_factors_alvar_2012.csv",in_dir))
# alvar_locs_vl<-unique(underreporting_alvar$location_id)
# 
# 
# #import data - vl
# leish_geo<-read.csv(sprintf("%s/Project/NTDs/geographic_restrictions/GBD_2017/datasets/geo_restrict_cl.csv",prefix), stringsAsFactors = FALSE)
# dataset<-leish_geo[,1:48]
# 
# 
# #convert to long
# melt_data<-melt(dataset, id.vars = c('loc_id',
#                                      'parent_id',
#                                      'level',
#                                      'type',
#                                      'loc_nm_sh',
#                                      'loc_name',
#                                      'spr_reg_id',
#                                      'region_id',
#                                      'ihme_lc_id',
#                                      'GAUL_CODE'))
# 
# melt_data$value<-as.character(melt_data$value)
# 
# status_list<-subset(melt_data, type=='status')
# 
# presence_list<-subset(status_list,value=='p' |value=='pp')
# 
# unique_vl_locations<-unique(presence_list$loc_id)
# #skip location_id = 44533 China without HK and Macao - as of 3 January 2018 no HAQI values
# unique_vl_locations<-unique_vl_locations[-which(unique_vl_locations==44533)]
# 
# pred<-list()
# for (alpha in 1:length(unique_vl_locations)){
#   LocToPull <- unique_vl_locations[alpha]
#   Location_name<-unique(presence_list$loc_name[which(presence_list$loc_id==unique_vl_locations[alpha])])
#   pred_year <- 1980:2017
#   pred_path <- rep(levels(pulled_data$pathogen)[1], length(pred_year))
#   pred_haqi <- haqi[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value
#   pred_sdi <- sdi[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value
#   pred_leish_e <- leish_endemic_cl[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value
#   
#   for(k in 1:n_reps){
#     pred[[k]] <- predict(mod[[k]],data.frame(year = pred_year, pathogen = pred_path, haqi = pred_haqi, sdi = pred_sdi, leish_endemic = pred_leish_e),se=TRUE)
#   }
#   
#   pdf(file=paste0(prefix, '/WORK/04_epi/02_models/01_code/06_custom/ntd_leish/gbd_2017/underreporting_outputs/cl/', unique_vl_locations[alpha], '.pdf'))
#   plot(pred_year, ilogit(pred[[1]]$fit), type='n', ylim=c(0,1), main=paste0(Location_name,' ',unique_vl_locations[alpha]),
#        ylab='Proportion reported', xlab='Year')
#   for (i in 1:n_reps){
#     Upper <- ilogit(pred[[i]]$fit + 1.96 * pred[[i]]$se.fit)
#     Mean <- ilogit(pred[[i]]$fit)
#     Lower <- ilogit(pred[[i]]$fit - 1.96 * pred[[i]]$se.fit)
#     lines(pred_year, Mean, lwd=2,col=2)
#     #polygon(c(pred_year,rev(pred_year)),c(Upper,rev(Lower)),col=rgb(1,0,0,.1))
#     if(unique_vl_locations[alpha] %in% alvar_locs_vl){
#       lines(pred_year, rep(1/underreporting_alvar$cl_underreporting_lo[which(underreporting_alvar$location_id==unique_vl_locations[alpha])], length(pred_year)),
#             lwd=2, col=3, lty=4)
#       lines(pred_year, rep(1/underreporting_alvar$cl_underreporting_hi[which(underreporting_alvar$location_id==unique_vl_locations[alpha])], length(pred_year)),
#             lwd=2, col=3, lty=4)
#       abline(v=2010, lty=4, col=3)
#     }
#   }
#   dev.off()
# }
# 
# 
# ##	Bring in country-specific underreporting factors as assigned by Alvar et al (PLoS Negl Trop Dis 2012).
# ##	(N.B. the figures in the excel source file are color-coded to indicate whether they were originally
# ##	assigned by Alvar et al or by me (Luc Coffeng), because Alvar et al hadn't assigned any value to a
# ##	specific country.)
# 
# underreporting <- read.csv(sprintf("%s/underreporting_factors_alvar_2012.csv",in_dir))
# 
# ## Convert underreporting factors to proportion reported
# 
# prop_hi <- 1 / underreporting$vl_underreporting_lo
# prop_lo <- 1 / underreporting$vl_underreporting_hi
# 
# ## Estimate parameters for a beta distribution that correspond to the upper and lower bounds
# 
# 
