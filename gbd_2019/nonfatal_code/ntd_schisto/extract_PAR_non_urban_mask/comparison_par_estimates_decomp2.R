rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}		

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
##all but China!

########DECOMP 2#######
par<-read.csv("FILEPATH")

draws<-subset(par, select = grep("^[prop]", names(par))) 
locs1<-par[,1:3]

means_nf<-rowMeans(draws,na.rm=FALSE, dims=1)
nf_draws_loc2<-cbind(locs1,means_nf)

#merge with all age population
pops<-get_population(age_group_id="ADDRESS", decomp_step="step1", year_id=2017, location_id = "all")

par2<-merge(nf_draws_loc2,pops,by="location_id")

par2$pop_at_risk<-par2$means_nf*par2$population

write.csv(par2,"FILEPATH")


#GBD 2017 par estimates-------
par_17<-read.csv("FILEPATH")

draws<-subset(par_17, select = grep("^[prop]", names(par_17))) 
locs17<-par_17[,1:3]

draws2<-draws[,-1]

means_17<-rowMeans(draws2,na.rm=FALSE, dims=1)
nf_draws_loc17_2<-cbind(locs17,means_17)

#merge with all age population
pops<-get_population(age_group_id="ADDRESS", decomp_step="step1", year_id=2000, location_id = "all")

par17_2<-merge(nf_draws_loc17_2,pops,by="location_id")

par17_2$pop_at_risk<-par17_2$means_17*par17_2$population

write.csv(par17_2,"FILEPATH")



test<-merge(par17_2,par2, by="location_id", all=T)

write.csv(test,"FILEPATH")


##decomp 1 estimates

par_decomp1<- read.csv("FILEPATH")

draws<-subset(par_decomp1, select = grep("^[prop]", names(par_decomp1))) 
locs1<-par_decomp1[,1:3]

draws2<-draws[,-1]

means_1<-rowMeans(draws2,na.rm=FALSE, dims=1)
nf_draws_loc1_2<-cbind(locs1,means_1)


#merge with all age population

pops<-get_population(age_group_id="ADDRESS", decomp_step="step1", year_id=2000, location_id = "all")

par1_2<-merge(nf_draws_loc1_2,pops,by="location_id")

par1_2$pop_at_risk<-par1_2$means_1*par1_2$population

write.csv(par1_2,"FILEPATH")


###comparison files 

#gbd 2017 versus decomp 1
test<-merge(par1_2,par17_2, by="location_id", all=T)
write.csv(test,"FILEPATH")

#decomp 1 versus decomp 2

test<-merge(par1_2,par2, by="location_id", all=T)
write.csv(test,"FILEPATH")


########################################################
######## CHINA SUBNATS ##################
######################################################



########DECOMP 2#######
par<-read.csv("FILEPATH")



draws<-subset(par, select = grep("^[prop]", names(par))) 
locs1<-par[,1:3]

means_nf<-rowMeans(draws,na.rm=FALSE, dims=1)
nf_draws_loc2<-cbind(locs1,means_nf)


#merge with all age population

pops<-get_population(age_group_id="ADDRESS", decomp_step="step1", year_id=2017, location_id = "all")

par2<-merge(nf_draws_loc2,pops,by="location_id")

par2$pop_at_risk<-par2$means_nf*par2$population

write.csv(par2,"FILEPATH")

#GBD 2017 par estimates-------
par_17<-read.csv("FILEPATH")
draws<-subset(par_17, select = grep("^[prop]", names(par_17))) 
locs17<-par_17[,1:3]

draws2<-draws[,-1]

means_17<-rowMeans(draws2,na.rm=FALSE, dims=1)
nf_draws_loc17_2<-cbind(locs17,means_17)


#merge with all age population
pops<-get_population(age_group_id="ADDRESS", decomp_step="step1", year_id=2000, location_id = "all")

par17_2<-merge(nf_draws_loc17_2,pops,by="location_id")
par17_2$pop_at_risk <- par17_2$means_17*par17_2$population
write.csv(par17_2,"FILEPATH")

test<-merge(par17_2,par1_2, by="location_id", all=T)
write.csv(test,"FILEPATH")


##decomp 1 estimates

par_decomp1<- read.csv("FILEPATH")

draws<-subset(par_decomp1, select = grep("^[prop]", names(par_decomp1))) 
locs1<-par_decomp1[,1:3]

draws2<-draws[,-1]

means_1<-rowMeans(draws2,na.rm=FALSE, dims=1)
nf_draws_loc1_2<-cbind(locs1,means_1)


#merge with all age population

pops<-get_population(age_group_id="ADDRESS", decomp_step="step1", year_id=2000, location_id = "all")

par1_2<-merge(nf_draws_loc1_2,pops,by="location_id")

par1_2$pop_at_risk<-par1_2$means_1*par1_2$population

write.csv(par1_2,"FILEPATH")


###comparison files 

#gbd 2017 versus decomp 1
test<-merge(par1_2,par17_2, by="location_id", all=T)
write.csv(test,"FILEPATH")

#decomp 1 versus decomp 2
test<-merge(par1_2,par2, by="location_id", all=T)
write.csv(test,"FILEPATH")
