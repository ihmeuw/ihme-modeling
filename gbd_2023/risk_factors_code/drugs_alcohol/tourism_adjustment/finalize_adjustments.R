########################################################################
#finalize adjustments and output an STGPR input

########################################################################
rm(list=ls())
invisible(sapply(list.files('FILEPATH', full.names = T), source))
pacman::p_load(data.table,plyr,dplyr,magrittr,ggplot2)

alc_lpc_stgpr_version <- version
best_net_tourism_adj <- 'FILEPATH' 

#location
locs <- get_location_metadata(location_set_id = 22,release_id=16)

alc_lpc_stgpr_version <- version
alc_lpc <- fread(paste0('FILEPATH')) 

alc_lpc[,c("age_group_id", "sex_id")] <- NULL
alc_lpc_melt <- melt.data.table(alc_lpc, id.vars=c("location_id", "year_id"),
                                  measure.vars=paste0("draw_", 0:999), value.name='alc_lpc', variable.name="draw")
alc_lpc_melt <- alc_lpc_melt[year_id >= 1980&location_id%in%c(locs[level>=3]$location_id)] # limit years for the covariate


##### Read in tourism adjustment ############################################################
# append and format the net tourism adjustments or read in directly if done previously
if(read_append_net_tourism==T){
  # run this chunk if you need to append the files from 02b to be read in next
  net_tourism_files <- list.files(paste0('FILEPATH'), full.names = T)
  net_tourism_data <- rbindlist(lapply(net_tourism_files, fread))
  net_tourism_data$location_name <- NULL
  fwrite(net_tourism_data,paste0('FILEPATH'))
} 

if(format_new_net_datset==T){
    
  ntd_unformatted <- fread(paste0('FILEPATH'))
  length(unique(ntd_unformatted$location_id))
  head(ntd_unformatted)
  
  # merge template all the years we will need adjustment factors for
  template_missing <- as.data.table(expand.grid(location_id = unique(ntd_unformatted$location_id), 
                                                year_id = c(seq(1980,2024)), 
                                                draw = paste0("draw_", 0:999)))
  net_tourism_data <- merge(ntd_unformatted,template_missing,
                            by=c('location_id','year_id','draw'),all = T) %>%unique()
  net_tourism_data[,check:=mean(net_tourism,na.rm=T),by='year_id']
  unique(net_tourism_data[is.na(check)]$year_id) #only  missing edge years
  net_tourism_data$check <- NULL
  
  # copy out the adjustment factor for tourism in 1995 to the previous years, since 1995 is the earlier year we have tourism data for
  info_1995 <- net_tourism_data[year_id == 1995]
  info_1995$year_id <- NULL
  setnames(info_1995,old=c("net_tourism"),new=c("tourism_1995"))
  net_tourism_data <- merge(net_tourism_data,info_1995,by=c("location_id","draw"))
  net_tourism_data[year_id <= 1994, net_tourism := tourism_1995]
  net_tourism_data$tourism_1995 <- NULL
  
  # do the same for (replicating out for more recent years)
  info_2022 <- net_tourism_data[year_id == 2022]
  info_2022$year_id <- NULL
  setnames(info_2022,old=c("net_tourism"),new=c("tourism_2022"))
  net_tourism_data <- merge(net_tourism_data,info_2022,by=c("location_id","draw"))
  net_tourism_data[year_id > 2022, net_tourism := tourism_2022]
  net_tourism_data$tourism_2022 <- NULL
  
  rm(info_1995,info_2022)
  
  # smooth the tourism adjustment factors with a simple loess
  names(net_tourism_data)
  #takes a few min
  net_tourism_data[, value_loess := {
    # Fit the loess model on the current group/subset
    loess_model <- loess(net_tourism ~ year_id, data = .SD, span = 0.5)
    
    # Predict using the fitted loess model on the same subset
    predicted_values <- predict(loess_model, newdata = .SD)
    
    # Return the predicted values to be assigned to 'value_loess'
    predicted_values
  }, by = .(location_id, draw)]
  
    
  net_tourism_data[,net_tourism:=value_loess] #replace with smoothed net tourism
  net_tourism_data$value_loess <-NULL
  
  #trim outliers
  summary(net_tourism_data$net_tourism)
  q99 <- quantile(net_tourism_data$net_tourism,0.99,na.rm=T)
  q025 <- quantile(net_tourism_data$net_tourism,0.025,na.rm=T)
  message(paste0('net tourism 99th percentile: ',round(q99,4)))
  message(paste0('net tourism 2.5th percentile: ',round(q025,4)))
  
  net_tourism_data[net_tourism>q99,net_tourism:=q99]
  net_tourism_data[net_tourism<q025,net_tourism:=q025]
  summary(net_tourism_data$net_tourism)
  length(unique(net_tourism_data$location_id))
  
  ## fill in missing national locations with 0...
  template <- as.data.table(expand.grid(location_id=unique(locs[level==3]$location_id),
                                        year_id=c(1980:2024),
                                        draw=paste0('draw_',0:999)))
  net_tourism_data <- merge(template,net_tourism_data,by=c("location_id","year_id","draw"),all=T)
  length(unique(net_tourism_data$location_id))
  net_tourism_data[is.na(net_tourism),net_tourism:=0]
  
  net_tourism_data <- net_tourism_data[location_id%in%unique(locs[level==3]$location_id)]
  fwrite(net_tourism_data,paste0('FILEPATH'))
}

net_tourism_formatted <- fread(paste0('FILEPATH'))

##### COMBINE WITH LPC ###################################################################
# adjust LPC when tourism values are present. Otherwise, assume that there is no tourism adjustment
tourism_alc <- merge(alc_lpc_melt,net_tourism_formatted,by=c("location_id","year_id","draw"),all=T)

# Find the percent of LPC that is due to tourism in locations with data - 
# this ratio will be applied to subnats where there is no known net tourism
tourism_alc[,ratio := net_tourism /(alc_lpc + net_tourism)]
summary(tourism_alc[!(is.na(ratio))]$ratio)

#trim outliers
q99 <- quantile(tourism_alc[net_tourism!=0]$ratio,0.99,na.rm=T)
q025<-quantile(tourism_alc[net_tourism!=0]$ratio,0.025,na.rm=T)
message(paste0('ratio 99th percentile: ',round(q99,3)))
message(paste0('ratio 2.5th percentile: ',round(q025,3)))

tourism_alc[ratio>q99,ratio:=q99]
tourism_alc[ratio<q025,ratio:=q025]
summary(tourism_alc[!(is.na(ratio))]$ratio)

## Identify level 3 parent of subnationals & Apply this ratio to subnational lpc to get the net tourism (duplicate across subnats) ######
tourism_alc <- merge(tourism_alc,locs[level>=3,.(ihme_loc_id,location_id,level,parent_id)],by='location_id',all=T)

summary(tourism_alc[!(is.na(ratio))]$ratio)
unique(tourism_alc[(is.na(ratio))&level==3]$year_id)
unique(tourism_alc[(is.na(ratio))&level==3]$location_id)

tourism_alc[level==3,parent_id_3:=location_id]
#make sure parent_id 3 is used for all subnats
tourism_alc[,parent_id_3 := mean(parent_id_3,na.rm=T),by=str_sub(ihme_loc_id,end=3)]
summary(tourism_alc$parent_id_3) #no missing

#apply the ratio to all subnats by parent
tourism_alc[,ratio:=mean(ratio,na.rm=T),by=c("parent_id_3","year_id","draw")]
summary(tourism_alc$ratio) #should be no NAs
(unique(tourism_alc[is.na(ratio)]$location_id)) #should be NAs
unique(tourism_alc$location_id)

#back calculate net tourism from ratio and lpc
tourism_alc[is.na(net_tourism), net_tourism := ((alc_lpc * ratio)/(1 - ratio))]
summary(tourism_alc$net_tourism)

### Arithmetic
# tourism = ratio*(lpc + tourism) = ratio*lpc + ratio*tourism
# tourism - ratio*tourism = ratio*lpc
# tourism*(1-ratio) = ratio * lpc
# tourism = ratio*lpc/(1-ratio)

# if there is no tourism value, then set it to 0
tourism_alc[is.na(net_tourism), net_tourism:=0]

tourism_alc$level <- NULL
tourism_alc$parent_id_3 <- NULL
tourism_alc$parent_id <- NULL

# if the net LPC value is less than 0, then don't adjust at all
tourism_alc[, net_adj_negative := ifelse(alc_lpc + net_tourism <= 0,1,0)]
issue_locs <- unique(tourism_alc[net_adj_negative == 1, .(ihme_loc_id)])
issue_locs #locations where this is happening

# add the lpc and the tourism total together, or if negative,0.01
tourism_alc[net_adj_negative==1,alc_lpc_new:=0.01]
tourism_alc[is.na(alc_lpc_new),alc_lpc_new:=alc_lpc + net_tourism] #fill the rest in normally

### save_for_exp IS THE FINAL OUTPUT read into the exp calc script ###########################################
save_for_exp <- copy(tourism_alc[,.(draw,location_id,year_id,alc_lpc_new)])
setnames(save_for_exp, old = "alc_lpc_new", new = "alc_lpc")
names(save_for_exp)
length(unique(save_for_exp$location_id))
length(unique(save_for_exp$draw))

fwrite(save_for_exp, paste0('FILEPATH'))
paste0('Saved to: ','FILEPATH')

############### unrecorded consumption is applied to alc_adj, and later saved as a covariate ######################

unrecorded <- fread(paste0('FILEPATH'))

# combine with alc
plot_stages <- merge(tourism_alc,unrecorded,by=c("location_id","draw"),all=T)
# if no unrecorded rate, assume it is 1
plot_stages[is.na(unrecorded), unrecorded := 1]

# multiply by unrecorded rate:
plot_stages[,total_lpc := alc_lpc_new * unrecorded]

# summarize the draws to get just the mean values (for plotting purposes)
plot_stages <- plot_stages[,.(mean=mean(total_lpc),
                      mean_original=mean(alc_lpc),
                      mean_tourism=mean(alc_lpc_new)),
                   by=c("location_id", "year_id")]
plot_stages <- unique(plot_stages[,.(location_id,year_id,mean,mean_original,mean_tourism)])

plot_stages_melt <- melt.data.table(plot_stages,measure.vars = c("mean","mean_original","mean_tourism"))
plot_stages_melt <- merge(plot_stages_melt,locs[,.(location_id,location_name)], by="location_id")
plot_stages_melt$round <- 2023
plot_stages_melt$X <- NULL

#save plot_stages_melt
fwrite(plot_stages_melt,paste0('FILEPATH'))


################# adjustment for upload as lpc cov #############################
# only do if in rotation and vetted
if(upload_covariate==T){
  
  # combine with alc
  alc_adj <- merge(tourism_alc,unrecorded,by=c("location_id","draw"),all=T)
  # if no unrecorded rate, assume it is 1
  alc_adj[is.na(unrecorded), unrecorded := 1]
  
  # multiply by unrecorded rate:
  alc_adj[,value := alc_lpc_new * unrecorded]
  
  # summarize the draws to get just the mean values (for plotting purposes)
  alc_adj <- alc_adj[,.(mean_value=mean(value),
                        lower_value=quantile(value, .025, na.rm = T),
                        upper_value=quantile(value, .975, na.rm = T)),
                     by=c("location_id", "year_id")]
  
  # format columns for upload
  alc_adj$age_group_id <- 22
  alc_adj$sex_id <- 3
  
  ################## upload! ######################
  
  fwrite(alc_adj,paste0('FILEPATH'))
  
  source('FILEPATH')
  # upload covariates
  message("Uploading covariate XXXX")
  
  save_results_covariate('FILEPATH',
                         covariate_id=XXXX, 
                         description="lpc, adjusted for tourism and unrecorded, all age and sex",
                         release_id = release,
                         year_id=seq(1980,2024,1))

}

