list.of.packages <- c("reldist","sp","data.table","plyr","ggplot2","haven","parallel","assertable")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
#if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only = TRUE)
#Arguments
jpath <- ifelse(Sys.info()[1]=="Windows", "J:/", "/home/j/")
cores <- 35
start_year <- 1950
end_year <- 2040
model_version <- " "

########################################################################################
#load data no draws
files <- list.files(paste0("...","data/output_data/",model_version,"/gpr/"),full.names=T)
#load data with draws
#files <- list.files(paste0("...","data/output_data/",model_version,"/gpr_draws/"),full.names=T)



data.list <- mclapply(files, function(file) {
  data <- fread(file)
},mc.cores=ifelse(Sys.info()[1]=="Windows", 1, cores))

#data.list<- lapply(data.list, data.table)
estimates <- rbindlist(data.list,fill=T)

#drop duplicates years, we only need estimates
estimates <- estimates[,.(gpr_mean=mean(gpr_mean), gpr_upper = mean(gpr_upper), gpr_lower = mean(gpr_lower)),by=.(location_id,year_id,age_group_id,sex_id)]

#load locs
locs <- get_location_metadata(location_set_id = 22)[level >= 3]
locs <- locs[,.SD,.SDcols=c("location_id","ihme_loc_id","location_name","parent_id","level")]

#load pops
pops1 <- get_population(age_group_id = 'all', sex_id = c(1:2), year_id = 'all', location_id = locs$location_id, run_id = 98)

#Extend pops for 2018-2040
pops <- pops1[age_group_id %in% unique(estimates$age_group_id)]
pops.extend <- pops[year_id==2017]
for (y in 2018:2040) {
  new_pop <- pops.extend
  new_pop[,year_id:=y]
  pops <- rbind(pops,new_pop)
}
#merge pops and locs onto data
pops <- pops[location_id %in% unique(estimates$location_id) & sex_id < 3]

est.pop <- data.table(merge(pops,estimates,by=c("location_id","year_id","age_group_id","sex_id"),all.y=T))
est.pop.loc <- data.table(merge(est.pop,locs,by=c("location_id"),all.x=T))


#Make New Var to hold raked estimates
est.pop.loc[,gpr_raked:=gpr_mean]

assert_ids(estimates, id_vars = list(location_id = locs$location_id, age_group_id = c(6:20, 30:32, 235), sex_id = 1:2, year_id = 1950:2040), assert_dups = T)

for (c.lvl in 4:6) {
  #make aggs
  aggs <- est.pop.loc[level==c.lvl]
  aggs <- aggs[,.(agg_mean=weighted.mean(x=gpr_raked,w=population)),by=.(parent_id,year_id,age_group_id,sex_id)]
  setnames(aggs,"parent_id","location_id")
  rake <- data.table(merge(est.pop.loc,aggs,by=c("location_id","year_id","age_group_id","sex_id")))
  #calculate raking factor
  rake[,rake.factor:= gpr_raked / agg_mean]
  rake <- rake[,.SD,.SDcols=c("location_id","year_id","age_group_id","sex_id","rake.factor")]
  setnames(rake,"location_id","parent_id")
  #merge onto estimates
  est.pop.loc <- data.table(merge(est.pop.loc,rake,by=c("parent_id","year_id","age_group_id","sex_id"),all=T, allow.cartesian = F))
  #rake
  est.pop.loc[level== c.lvl, gpr_raked:= gpr_raked * rake.factor ]
  setnames(est.pop.loc,"rake.factor",paste0("rake.factor",c.lvl))
}
#copy level estimates into raked var
est.pop.loc[level==3,gpr_raked:=gpr_mean]
est.pop.loc[,gpr_upper_raked:=(gpr_raked - gpr_mean + gpr_upper)]
est.pop.loc[,gpr_lower_raked:=(gpr_raked - gpr_mean + gpr_lower)]


#Test that it worked
test <- est.pop.loc[level==4]
test <- test[,.(test_mean=weighted.mean(x=gpr_raked,w=population)),by=.(parent_id,year_id,age_group_id,sex_id)] 

#Save estimates for vizualization and graphing 
raked.data <- est.pop.loc[,.SD,.SDcols=c("location_id","year_id","age_group_id","sex_id","gpr_raked","gpr_lower_raked", "gpr_upper_raked")]
estimates <- rbindlist(data.list,fill=T)
estimates <- data.table(merge(estimates, raked.data , by=c("location_id","year_id","age_group_id","sex_id"),all.x = T))

#clean up types
#Save loc specific for viz
dir.create(paste0("...","data/output_data/",model_version,"/raked/"),showWarnings = F)

#Save all raked estimates
savefiles <-function(c.ihme_loc_id){
  write.csv(estimates[ihme_loc_id==c.ihme_loc_id],paste0("...","data/output_data/",model_version,"/raked/",c.ihme_loc_id,".csv"),row.names=F)
}

mclapply(unique(estimates$ihme_loc_id), savefiles, mc.cores = cores)

aw <- fread("GBD_2017_ageweights_20171024.csv") 
#aw16 <- aw[gbd_round_id==4]
esties17 <- estimates[,.(mean17=mean(gpr_raked), upper17 = mean(gpr_upper_raked), lower17 = mean(gpr_lower_raked)),by=.(location_id,year_id,age_group_id,sex_id)]
esties17 <- data.table(merge(esties17,aw,by='age_group_id'))
#format off 2015 estimates
#make maternal estimates for 2016 estimates
mat <- esties17[sex_id==2 & age_group_id > 7 & age_group_id < 15]
mat <- mat[,.(mat17=weighted.mean(x=mean17,w=age_group_weight_value), matlower17 =weighted.mean(x = lower17,w=age_group_weight_value), matupper17 = weighted.mean(x = upper17,w=age_group_weight_value)),by=.(location_id,year_id)]

########################################################################################
###create full uploads by using maternal average education for ages younger than 5 years old
########################################################################################

upload.ests <- esties17[year_id <= 2017, .(location_id, year_id, age_group_id, sex_id, mean17,upper17,lower17)]
#use maternal for less than 15
childages <- expand.grid(age_group_id = 2:5, sex_id = 1:2, location_id = unique(upload.ests$location_id), year_id = unique(upload.ests$year_id)) %>% as.data.table
upload.ests <- rbind(upload.ests, childages, use.names = T, fill = T)
upload.ests <- data.table(merge(upload.ests,mat,by=c('location_id','year_id'),all.x=T))

upload.ests[age_group_id<8,mean17:=mat17]
upload.ests[age_group_id<8,lower17:=matlower17]
upload.ests[age_group_id<8,upper17:=matupper17]

upload.ests[,mean_value:= mean17]
upload.ests <- upload.ests[order(location_id,sex_id,age_group_id,year_id)]

# COVARIATE INFO
upload.ests[, c("covariate_id", "covariate_name_short") := .(33, 'education_yrs_pc')]

#subset
upload.ests[,upper_value:=upper17]
upload.ests[,lower_value:=lower17]
upload.ests <- upload.ests[,.SD,.SDcols=c("location_id","year_id","age_group_id","sex_id","covariate_id","covariate_name_short","mean_value","upper_value", "lower_value")]

assert_ids(upload.ests, id_vars = list(location_id = locs$location_id, year_id = 1950:2017, age_group_id = c(2:20, 30:32, 235), sex_id = 1:2))
write.csv(upload.ests,paste0("...","data/output_data/",model_version,"/education_upload.csv"),row.names = F)
