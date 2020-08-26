#create full covariate set 
source("FILEPATH")

source("FILEPATH")

d_locs1<-get_location_metadata(gbd_round_id = 6,location_set_id = 22)
d_locs1<-d_locs1[d_locs1$is_estimate==1,]
#all locations
location_list<-unique(d_locs1$location_id)

csmr<-get_covariate_estimates(covariate_id=ADDRESS,gbd_round=6, location_id=location_list, decomp_step='iterative')
csmr$csmr_cov<-csmr$mean_value

#just keep males
csmr<-csmr[csmr$sex_id==1,]

deng_prob<-get_covariate_estimates(covariate_id=ADDRESS, gbd_round_id=6,location_id=location_list,decomp_step="step4")
deng_prob$deng_prob_cov<-deng_prob$mean_value

test504<-deng_prob[deng_prob$location_id==504,]

#get populations 
years<-seq(1980,2019,1)
pops<-get_population(age_group_id = 22,location_id=location_list,year_id=years, gbd_round_id=6, decomp_step="step4")


covs<-merge(pops,csmr,by=c("location_id","year_id"))
covs2<-merge(covs,deng_prob, by=c("location_id","year_id"))

#output the covariate file - 
write.csv(covs2,"FILEPATH")


dt<-read.csv("FILEPATH")
covs2$age_group_id.x<-NULL
covs2$age_group_id.y<-NULL
covs2$sex_id.x<-NULL
covs2$sex_id.y<-NULL
ur<-merge(dt, covs2,by=c("location_id","year_id"),all=TRUE)


write.csv(ur,"FILEPATH",na = "")

table(ur$location_id)



pops<-get_population(sex_id=c(1,2), age_group_i="all",location_id=unique_d_locations,year_id=c(1990,1995,2000,2005,2010,2015,2017,2019), gbd_round_id=6, decomp_step="step4")
pops<-pops[pops$age_group_id>1 & pops$age_group_id<=235,]
pops<-pops[pops$age_group_id<=32 | pops$age_group_id==235,]
pops<-pops[pops$age_group_id<=20 | pops$age_group_id>=30,]
write.csv(pops,"FILEPATH")
