# Purpose: subnational splits for YF

source("FILEPATH/get_population.R")

subs<-read.csv("FILEPATH")

#keep location_id
myvars<- c("location_id","parent_id")
subs <- as.data.frame(subs)[myvars]

locs<-unique(subs$location_id)
years<-(1980:2024)
release_id <- ADDRESS

pops<-get_population(age_group_id=2, sex_id=3, release_id = release_id,location_id=locs, year_id=years)
total_pops<-get_population(age_group_id=2, sex_id=3,release_id = release_id,location_id=c(179,214,180), year_id=years)

#calculate proportion of subnational population
#merge pops with subs
subs2<-merge(subs,pops,by="location_id")

#change total_pops location_id to parent id
total_pops2 <-total_pops %>%                     
  rename(parent_id="location_id", total_pop="population")

#merge total_pops2 to subs2
subs3<-merge(subs2,total_pops2,by=c("parent_id","year_id"))

myvars<- c("location_id","parent_id","population","year_id","total_pop")
subs3 <- as.data.frame(subs3)[myvars]
subs3$proportion<-subs3$population/subs3$total_pop
subs3$se<-sqrt((subs3$proportion*(1-subs3$proportion))/subs3$population)

#generate 1000 draws of proportion - with uncertainty
#generate from beta distribution
draws.required <- 1000
draw.cols <- paste0("braSubPr_", 0:999)

subs4<-data.table::setDT(subs3)
subs4[, id := .I]
subs5<-subs4[, (draw.cols) := as.list(rnorm(draws.required, proportion, se)), by=id]


library(foreign)
write.dta(subs5,"FILEPATH")
