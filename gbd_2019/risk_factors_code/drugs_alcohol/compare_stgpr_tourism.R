source("/home/j/temp/central_comp/libraries/current/r/get_location_metadata.R")
source('/ihme/code/st_gpr/central/stgpr/r_functions/utilities/utility.r')
library(data.table)
library(ggplot2)

gbd_num <- 6
locs <- get_location_metadata(location_set_id = 22, gbd_round_id = gbd_num)[, .(location_id, location_name)]

# compare the new tourist total values with the old ones:
old_tourism <- fread("/home/j/WORK/05_risk/risks/TEAM/sub_risks/alcohol/raw_data/exposure/tourism/tourism_gpr_2016.csv")
#new_tourism <- model_load(57254,obj="raked")
new_tourism <- model_load(60221, obj="raked")
#new_tourism_data <- model_load(57254,obj="data")
new_tourism_data <- model_load(60221,obj="data")
new_tourism_data[,lower := data - 1.96*sqrt(variance)]
new_tourism_data[,upper := data + 1.96*sqrt(variance)]
new_tourism_data[,data:=data*10000]
new_tourism_data[,lower:=lower*10000]
new_tourism_data[,upper:=upper*10000]


# take mean, lower, and upper of the old tourism file and compare to the new file

# if similar, we can proceed with running 1000 draws of the new tourism calculations

drawvars<-c(paste0("draw_", 0:999))
idvars<-c("location_id", "year_id", "age_group_id", "sex_id")

old_tourism_long<-melt(old_tourism, id.vars = idvars, measure.vars = drawvars)
setnames(old_tourism_long, "value", "total_tourists")

old_tourism_long <- old_tourism_long[, c("total_tourists_mean", "total_tourists_low", "total_tourists_up"):=as.list(c(mean(get("total_tourists")), quantile(get("total_tourists"), c(0.025, 0.975),na.rm=T) )), by=c("location_id","year_id","sex_id","age_group_id")]
old_tourism_unique <- unique(old_tourism_long[,.(location_id,year_id,age_group_id,sex_id,total_tourists_mean,total_tourists_low,total_tourists_up)])


combined_tourism <- merge(new_tourism,old_tourism_unique,by=c("location_id","year_id","age_group_id","sex_id"),all=T)
combined_tourism[,gpr_mean := gpr_mean * 10000] # multiple by 10,000 because we ran ST-GPR in units of 10,000
# and adjust the upper and lower because of this 10,000 transformation. but not sure if I am doing this correctly:
combined_tourism[,gpr_lower := gpr_lower * 10000]
combined_tourism[,gpr_upper := gpr_upper * 10000]

combined_tourism <- merge(combined_tourism,locs,by='location_id')

combined_tourism <- merge(combined_tourism,new_tourism_data,by=c("location_id","year_id","age_group_id","sex_id"),all=T)

l <- 102
l <- 130

l <- 51
#combined_tourism <- merge(combined_tourism,locs,by="location_id")
#write.csv(combined_tourism,"/share/gbd/WORK/05_risk/TEAM/sub_risks/tobacco/code/parkesk/gbd2019_alcohol/new_old_tourism_combined.csv")

#combined_tourism <- as.data.table(read.csv("/share/gbd/WORK/05_risk/TEAM/sub_risks/tobacco/code/parkesk/gbd2019_alcohol/new_old_tourism_combined.csv"))
pdf("/share/gbd/WORK/05_risk/TEAM/sub_risks/tobacco/code/parkesk/gbd2019_alcohol/tourism_comparison_plots_withRussia.pdf",width=12,height = 8)
for(l in unique(combined_tourism$location_id)){
  message(l)
  dt <- combined_tourism[location_id == l]
  print(ggplot(data=dt,aes(x=year_id)) + geom_line(aes(y=gpr_mean,color="New")) + geom_ribbon(aes(ymin=gpr_lower,ymax=gpr_upper),fill="red",alpha=0.2)+
          geom_line(aes(y=total_tourists_mean,color="Old")) + geom_ribbon(aes(ymin=total_tourists_low,ymax=total_tourists_up),fill="blue",alpha = 0.2)+
          scale_color_manual(values = c(Old = "blue",New="red"))+
          geom_point(aes(y=data)) + geom_errorbar(aes(ymin=lower,ymax=upper),width = .001) + ggtitle(unique(dt$location_name)))
}
dev.off()



print(ggplot(data=dt,aes(x=year_id)) + geom_line(aes(y=gpr_mean/10000),color="red") + geom_ribbon(aes(ymin=gpr_lower/10000,ymax=gpr_upper/10000),fill="red",alpha=0.2)+
  geom_point(aes(y=data/10000)) + geom_errorbar(aes(ymin=lower/10000,ymax=upper/10000),width = .001))


## look at some of the intermediary files to make sure everything is being included in the input data:
tt <- fread("/share/gbd/WORK/05_risk/TEAM/covariates/alcohol/liters_per_capita/tourism_adjustment_output/tourist_proportions.csv")
head(tt[location_name %like% "Russ"])
post_merge <- fread("/home/j/WORK/05_risk/risks/TEAM/sub_risks/alcohol/raw_data/exposure/tourism/tourist_tot_preSTGPR_PK.csv")
