########################################################
##############  Code for Ebola non-fatal processing
########################################################

rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}
source(sprintf("FILEPATH/get_demographics.R",prefix))
source(sprintf("FILEPATH/get_population.R",prefix))

outdir<-paste0(prefix, "FILEPATH")

# check if a full set of csvs exists for all reporting locations, formatted as "loc_year.csv"
existing_files<-list.files(path=paste0(outdir,"/9668"), pattern = "csv")
if(!is.numeric(existing_files)){
  existing_files<-1
}

epi_demographics<-get_demographics('ADDRESS')
#we expect every year from 1990 to be present, for every GBD location [2018 included because 2017 cases will go into 2018]
year_string<-seq(1990,2018,1)
expected_files<-length(year_string)*length(epi_demographics$location_id)

#if absent, construct said csvs
if(existing_files != expected_files){
  #create age/sex/epi matrix
  age_sex_epi<-expand.grid(age_group_id = epi_demographics$age_group_id, sex_id = epi_demographics$sex_id, measure_id = c(5,6))
  #create a zero set with 1,000 colums, and nrow(age_sex_epi)
  blank_set<-matrix(0,
                    nrow = nrow(age_sex_epi),
                    ncol = 1000)
  blank_set<-as.data.frame(blank_set)

  name_prefix<-"draw"
  name_suffix<-seq(1:1000)-1
  names(blank_set)<-paste(name_prefix, name_suffix, sep="_")

  common_blank<-cbind(age_sex_epi, blank_set)

  for(i in 1:length(epi_demographics$location_id)){
    location_blank<-data.frame(location_id = rep(epi_demographics$location_id[i], nrow(common_blank)),
                               common_blank)

  for(k in 1:length(year_string)){
    year_blank<-data.frame(year_id = rep(year_string[k], nrow(location_blank)),
                           location_blank)
    acute_blank<-data.frame(modelable_entity_id = rep(9668, nrow(year_blank)),
                            year_blank)
    chronic_blank<-data.frame(modelable_entity_id = rep(9669, nrow(year_blank)),
                              year_blank)
    write.csv(acute_blank, paste0(outdir, "/9668/", epi_demographics$location_id[i], "_", year_string[k],".csv"), row.names=FALSE)
    write.csv(chronic_blank, paste0(outdir, "/9669/", epi_demographics$location_id[i], "_", year_string[k],".csv"), row.names=FALSE)
  }
  print(paste0("Completed location ", i, " of ", length(epi_demographics$location_id)))
}
}else{
  print("You have the necessary number of csvs in place")
}

##################################################
# Create Ebola parameters and their draws if absent and save them
##################################################

######################################################################################################################################case-fatality rate
cfr_present<-list.files(path=paste0(outdir,"/parameters"), pattern = "evd_cfr_draws")

cfr_files<-cfr_present=="evd_cfr_draws.csv"

if(length(cfr_files)==0){

cfr_raw<-read.csv(paste0(prefix, "FILEPATH/evd_cfr.csv"))

under_15_mean<-cfr_raw$CFR_mean[cfr_raw$Grouping=="<15 yrs"]
under_15_se<-(cfr_raw$CFR_UCI[cfr_raw$Grouping=="<15 yrs"]-cfr_raw$CFR_LCI[cfr_raw$Grouping=="<15 yrs"])/(2*1.96)

over_45_mean<-cfr_raw$CFR_mean[cfr_raw$Grouping==">=45 yrs"]
over_45_se<-(cfr_raw$CFR_UCI[cfr_raw$Grouping==">=45 yrs"]-cfr_raw$CFR_LCI[cfr_raw$Grouping==">=45 yrs"])/(2*1.96)

middle_mean<-cfr_raw$CFR_mean[cfr_raw$Grouping=="15-44 yrs"]
middle_se<-(cfr_raw$CFR_UCI[cfr_raw$Grouping=="15-44 yrs"]-cfr_raw$CFR_LCI[cfr_raw$Grouping=="15-44 yrs"])/(2*1.96)

all_mean<-cfr_raw$CFR_mean[cfr_raw$Grouping == "All cases"]
all_se<-(cfr_raw$CFR_UCI[cfr_raw$Grouping=="All cases"]-cfr_raw$CFR_LCI[cfr_raw$Grouping=="All cases"])/(2*1.96)

#for each of the GBD age_group_ids, create 1,000 draws all-age added too
age_sex<-expand.grid(age_group_id = c(epi_demographics$age_group_id, 22), sex_id = epi_demographics$sex_id)

blank_cfr_param<-matrix(0,
                  nrow = nrow(age_sex),
                  ncol = 1000)
blank_cfr_param<-as.data.frame(blank_cfr_param)

name_prefix<-"draw"
name_suffix<-seq(1:1000)-1
names(blank_cfr_param)<-paste(name_prefix, name_suffix, sep="_")

cfr_param<-cbind(age_sex, blank_cfr_param)

under_15_draw<-rnorm(1000, under_15_mean, under_15_se)
over_45_draw<-rnorm(1000, over_45_mean, over_45_se)
middle_draw<-rnorm(1000, middle_mean, middle_se)
all_draw<-rnorm(1000, all_mean, all_se)

for(j in 1:nrow(cfr_param)){
  if(cfr_param$age_group_id[j] %in% c(2,3,4,5,6,7)){
    cfr_param[j,3:1002]<-under_15_draw
  }
  if(cfr_param$age_group_id[j] %in% c(8,9,10,11,12,13)){
    cfr_param[j,3:1002]<-middle_draw
  }
  if(cfr_param$age_group_id[j] %in% c(14,15,16,17,18,19,20,30,31,32,235)){
    cfr_param[j,3:1002]<-over_45_draw
  }
  if(cfr_param$age_group_id[j] %in% c(22)){
    cfr_param[j,3:1002]<-all_draw
  }
}
write.csv(cfr_param, paste0(outdir, "/parameters/evd_cfr_draws.csv"), row.names = FALSE)
}

######################################################################################################################################acute duration

acute_duration_present<-list.files(path=paste0(outdir,"/parameters"), pattern = "evd_acute_duration_draws")

acute_duration_files<-acute_duration_present == "evd_acute_duration_draws.csv"

if(length(acute_duration_files) == 0){
acute_duration_raw<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_acute_duration.csv"))

acute_death_mean<-acute_duration_raw$mean[acute_duration_raw$Grouping == "onset_to_death"]
acute_death_se<-(acute_duration_raw$mean_UCI[acute_duration_raw$Grouping == "onset_to_death"]-acute_duration_raw$mean_LCI[acute_duration_raw$Grouping == "onset_to_death"])/(2*1.96)
#parameters in day space - need to be in years
acute_death_draw<-rnorm(1000, acute_death_mean, acute_death_se)/365

acute_survival_mean<-acute_duration_raw$mean[acute_duration_raw$Grouping == "onset_to_discharge"]
acute_survival_se<-(acute_duration_raw$mean_UCI[acute_duration_raw$Grouping == "onset_to_discharge"]-acute_duration_raw$mean_LCI[acute_duration_raw$Grouping == "onset_to_discharge"])/(2*1.96)
#parameters in day space - need to be in years
acute_survival_draw<-rnorm(1000, acute_survival_mean, acute_survival_se)/365

acute_duration<-rbind(acute_death_draw, acute_survival_draw)

write.csv(acute_duration, paste0(outdir, "/parameters/evd_acute_duration_draws.csv"))
}

######################################################################################################################################chronic duration
#bootstrap across parameter values, not predicted values

chronic_duration_present<-list.files(path=paste0(outdir,"/parameters"), pattern = "evd_chronic_duration_draws")

chronic_duration_files<-"evd_chronic_duration_draws_year1.csv" %in% chronic_duration_present

if(length(chronic_duration_files) == 0){
#load in data
evd_data<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration.csv"))

#exclude Wendo 2001 data due to limited metadata and therefore outlier
evd_data<-evd_data[evd_data$study!='Wendo_2001',]

#using means
dataset<-data.frame(y=NA, x=NA)
for (i in 1:nrow(evd_data)){
  input_expanded<-cbind(y=mean(c(evd_data$value_min[i], evd_data$value_max[i])),x=log(mean(c(evd_data$time_start[i], evd_data$time_end[i]))))
  dataset<-rbind(dataset, input_expanded)
}


dataset<-na.omit(dataset)
model<-lm(y~x, data=dataset)

# new_data<-data.frame(x=log(seq(0.01,3,0.01)))
# results<-predict(model, new_data, interval='prediction')
#
# plot_me<-data.frame(x=seq(0.01, 3, 0.01), y=results[,1])
# plot(y~x, data=plot_me, type='l', ylim=c(0,1), col="red")
# points(y~exp(x), data=dataset)
# lines(y=results[,2], x=seq(0.01, 3, 0.01), col='red')
# lines(y=results[,3], x=seq(0.01, 3, 0.01), col='red')

years<-seq(0, 20, 0.05)
#for each year, calculate the value with CI
new_data<-data.frame(x=log(years))

intercept<-coef(summary(model))[1,1]
intercept_ste<-coef(summary(model))[1,2]
mean_ste<-coef(summary(model))[2,2]
mean<-coef(summary(model))[2,1]

mean_draws<-runif(1000, mean-1.96*mean_ste, mean+1.96*mean_ste)
intercept_draws<-runif(1000, intercept-1.96*intercept_ste, intercept+1.96*intercept_ste)

results<-list()
result_draw<-data.frame(x=years,y=rep(NA,length(years) ))

for (i in 1:1000){

  for(j in 1:nrow(result_draw)){
    result_draw$y[j]<-(mean_draws[i]*log(result_draw$x[j]))+intercept_draws[i]
    result_draw$y[1]<-1
  }
  results[[i]]<-result_draw
}

for (i in 1:1000){
  for (j in 1:nrow(results[[i]]-1)){

    results[[i]]$value[j]<-((results[[i]]$x[j+1]-results[[i]]$x[1])*(results[[i]]$y[j]-results[[i]]$y[j+1]))/2

  }
  results[[i]]$value[nrow(results[[i]])]<-0
}
duration<-NA

for (i in 1:1000){
  duration[i]<-sum(results[[i]]$value)
}

mean(duration)
quantile(duration, probs=c(0.05, 0.5,0.95))

central_tendency<-result_draw
for(j in 1:nrow(central_tendency)){
  central_tendency$y[j]<-(mean*log(central_tendency$x[j]))+intercept
  central_tendency$y[1]<-1
}

#code to plot

#
# plot(y~exp(x), data=dataset, type='p', ylim=c(0,1), xlim=c(0,5),col="red",xaxs='i',yaxs='i', ylab='Proportion of survivors', xlab='Time (years)')
#
# for (i in 1:1000){
#   lines(y~x, data=results[[i]], col=adjustcolor('black', alpha.f=0.1))
# }
# points(y~exp(x), data=dataset, pch=16, col='red')
#
# lines(y~x, data=central_tendency, col='red')

year1<-duration
for(i in 1:length(duration)){
if(year1[i]>1){year1[i]<-1}
}

year2<-duration-1
for(i in 1:length(duration)){
  if(year2[i]<0){year2[i]<-0}
}

write.csv(t(year1), paste0(outdir, "/parameters/evd_chronic_duration_draws_year1.csv"))
write.csv(t(year2), paste0(outdir, "/parameters/evd_chronic_duration_draws_year2.csv"))
}

######################################################################################################################################underreporting cases
underreporting_cases_present<-list.files(path=paste0(outdir,"/parameters"), pattern = "evd_underreporting_cases_draws")

if(length(underreporting_cases_present) == 0){
  underreporting_cases_raw<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_underreporting_cases.csv"))
  under_cases_mean<-underreporting_cases_raw$Mean[underreporting_cases_raw$Correction.factor == "Cases"]
  under_cases_se<-(underreporting_cases_raw$Upper[underreporting_cases_raw$Correction.factor == "Cases"] - underreporting_cases_raw$Lower[underreporting_cases_raw$Correction.factor == "Cases"])/(2*1.96)
  under_cases_draw<-rnorm(1000, under_cases_mean, under_cases_se)
  write.csv(t(under_cases_draw), paste0(outdir, "/parameters/evd_underreporting_cases_draws.csv"))
  under_death_mean<-underreporting_cases_raw$Mean[underreporting_cases_raw$Correction.factor == "Deaths"]
  under_death_se<-(underreporting_cases_raw$Upper[underreporting_cases_raw$Correction.factor == "Deaths"] - underreporting_cases_raw$Lower[underreporting_cases_raw$Correction.factor == "Deaths"])/(2*1.96)
  under_death_draw<-rnorm(1000, under_death_mean, under_death_se)
  write.csv(t(under_death_draw), paste0(outdir, "/parameters/evd_underreporting_deaths_draws.csv"))
}


##################################################
# Ingest West African death data and calculate cases using case fatality rate
# Post-GBD2017 update, this approach is now irrelevant to generate survivors; cfr is still useful
##################################################
ebola_death<-read.csv(paste0(prefix, "FILEPATH/ebola_shock_dataset_11May2018.csv"))

#subset to just SLE/GIN/LBR 2014 and 2015 data
wafrica_death<-subset(ebola_death, ebola_death$iso %in% c("LBR", "GIN", "SLE"))
wafrica_death<-subset(wafrica_death, wafrica_death$year %in% c(2014,2015))

wafrica_death<-data.frame(iso = wafrica_death$iso,
                          year = wafrica_death$year,
                          age_group_id = wafrica_death$age_group_id,
                          sex_id = wafrica_death$sex_id,
                          deaths = wafrica_death$best)

#generate 1000 draws of case counts based upon what is present in cfr_draws
blank_wafro_case<-matrix(0,
                        nrow = nrow(wafrica_death),
                        ncol = 1000)
blank_wafro_case<-as.data.frame(blank_wafro_case)

name_prefix<-"draw"
name_suffix<-seq(1:1000)-1
names(blank_wafro_case)<-paste(name_prefix, name_suffix, sep="_")

blank_wafro_survivor<-blank_wafro_case

#load in cfr draws
cfr_draws<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_cfr_draws.csv"))
for (i in 1:nrow(wafrica_death)){
  ref_age<-wafrica_death$age_group_id[i]
  if(ref_age %in% c(33,48)){ref_age<-235}
  ref_sex<-wafrica_death$sex_id[i]
  ref_death<-wafrica_death$deaths[i]
  blank_wafro_case[i,]<-(ref_death/cfr_draws[which(cfr_draws$age_group_id == ref_age & cfr_draws$sex_id == ref_sex),3:1002])*100
  blank_wafro_survivor[i,]<-((ref_death/cfr_draws[which(cfr_draws$age_group_id == ref_age & cfr_draws$sex_id == ref_sex),3:1002])*100)-ref_death
}

west_africa_cases<-cbind(wafrica_death, blank_wafro_case)
west_africa_survivors<-cbind(wafrica_death, blank_wafro_survivor)
#save cases
# write.csv(west_africa_cases, paste0(prefix,"FILEPATH/ebola_wafrica_cases.csv"))
# write.csv(west_africa_survivors, paste0(prefix,"FILEPATH/ebola_wafrica_survivors.csv"))

#use the survivors to age/sex split

age_sex_survivor<-age_sex

for(i in 1:nrow(age_sex_survivor)){
  ref_age<-age_sex_survivor$age_group_id[i]
  ref_sex<-age_sex_survivor$sex_id[i]

  if(ref_age %in% c(2,3)){

    for (j in 1:1000){
      age_sex_survivor[i,j+2]<-0
    }
  }

  if(ref_age %in% c(235)){
    collapsed_group<-subset(west_africa_survivors, west_africa_survivors$age_group_id %in% c(235,33,48) & west_africa_survivors$sex_id == ref_sex)
    for (j in 1:1000){
      age_sex_survivor[i,j+2]<-sum(collapsed_group[,j+5])
    }
  }
  if (ref_age %in% c(22, 4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32)) {
    collapsed_group<-subset(west_africa_survivors, west_africa_survivors$age_group_id == ref_age & west_africa_survivors$sex_id == ref_sex)
    for (j in 1:1000){
      age_sex_survivor[i,j+2]<-sum(collapsed_group[,j+5])
    }
  }
}

age_sex_survivor<-subset(age_sex_survivor, age_sex_survivor$age_group_id != 22)
age_sex_male<-subset(age_sex_survivor, age_sex_survivor$sex_id == 1)
age_sex_female<-subset(age_sex_survivor, age_sex_survivor$sex_id == 2)

age_sex_proportion<-age_sex
age_sex_proportion<-subset(age_sex_proportion, age_sex_proportion$age_group_id != 22)
age_sex_prop_male<-subset(age_sex_proportion, age_sex_proportion$sex_id == 1)
age_sex_prop_female<-subset(age_sex_proportion, age_sex_proportion$sex_id == 2)

for (i in 3:ncol(age_sex_survivor)){
  age_sex_proportion[,i]<-age_sex_survivor[,i]/sum(age_sex_survivor[,i])
}
for (i in 3:ncol(age_sex_male)){
  age_sex_prop_male[,i]<-age_sex_male[,i]/sum(age_sex_male[,i])
}
for (i in 3:ncol(age_sex_female)){
  age_sex_prop_female[,i]<-age_sex_female[,i]/sum(age_sex_female[,i])
}

write.csv(age_sex_proportion, paste0(outdir, "/parameters/evd_age_sex_split.csv"), row.names = FALSE)
write.csv(age_sex_prop_male, paste0(outdir, "/parameters/evd_age_sex_split_male.csv"), row.names = FALSE)
write.csv(age_sex_prop_female, paste0(outdir, "/parameters/evd_age_sex_split_female.csv"), row.names = FALSE)

############################################################################################################################################################
#calculate death proportion splits

ebola_death<-read.csv(paste0(prefix, "FILEPATH/ebola_shock_dataset_11May2018.csv"))

#subset to just SLE/GIN/LBR 2014 and 2015 data
wafrica_death<-subset(ebola_death, ebola_death$iso %in% c("LBR", "GIN", "SLE"))
wafrica_death<-subset(wafrica_death, wafrica_death$year %in% c(2014,2015))

wafrica_death<-data.frame(iso = wafrica_death$iso,
                          year = wafrica_death$year,
                          age_group_id = wafrica_death$age_group_id,
                          sex_id = wafrica_death$sex_id,
                          deaths = wafrica_death$best)

age_sex_death<-age_sex

for(i in 1:nrow(age_sex_death)){
  ref_age<-age_sex_death$age_group_id[i]
  ref_sex<-age_sex_death$sex_id[i]

  if(ref_age %in% c(2,3)){


      age_sex_death[i,3]<-0

  }

  if(ref_age %in% c(235)){
    collapsed_group<-subset(wafrica_death, wafrica_death$age_group_id %in% c(235,33,48) & wafrica_death$sex_id == ref_sex)

      age_sex_death[i,3]<-sum(collapsed_group[,5])

  }
  if (ref_age %in% c(22, 4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32)) {
    collapsed_group<-subset(wafrica_death, wafrica_death$age_group_id == ref_age & wafrica_death$sex_id == ref_sex)

      age_sex_death[i,3]<-sum(collapsed_group[,5])

  }
}

age_sex_death_abs<-subset(age_sex_death, age_sex_death$age_group_id != 22)
age_sex_death_male<-subset(age_sex_death_abs, age_sex_death_abs$sex_id == 1)
age_sex_death_female<-subset(age_sex_death_abs, age_sex_death_abs$sex_id == 2)

for(i in 1:999){
  age_sex_death_abs<-cbind(age_sex_death_abs, age_sex_death_abs[,3])
  age_sex_death_male<-cbind(age_sex_death_male, age_sex_death_male[,3])
  age_sex_death_female<-cbind(age_sex_death_female, age_sex_death_female[,3])
}

age_sex_prop_death<-age_sex_death_abs
age_sex_prop_death_male<-age_sex_death_male
age_sex_prop_death_female<-age_sex_death_female

for (i in 3:ncol(age_sex_death_abs)){
  age_sex_prop_death[,i]<-age_sex_death_abs[,i]/sum(age_sex_death_abs[,i])
}
for (i in 3:ncol(age_sex_prop_death_male)){
  age_sex_prop_death_male[,i]<-age_sex_death_male[,i]/sum(age_sex_death_male[,i])
}
for (i in 3:ncol(age_sex_prop_death_female)){
  age_sex_prop_death_female[,i]<-age_sex_death_female[,i]/sum(age_sex_death_female[,i])
}

write.csv(age_sex_prop_death, paste0(outdir, "/parameters/evd_age_sex_split_death.csv"), row.names = FALSE)
write.csv(age_sex_prop_death_male, paste0(outdir, "/parameters/evd_age_sex_split_male_death.csv"), row.names = FALSE)
write.csv(age_sex_prop_death_female, paste0(outdir, "/parameters/evd_age_sex_split_female_death.csv"), row.names = FALSE)

#########################################################################################################################################################################################
#########################################################################################################################################################################################
#########################################################################################################################################################################################
#########################################################################################################################################################################################
#########################################################################################################################################################################################
#########################################################################################################################################################################################
#########################################################################################################################################################################################

#load in survivors values
evd_survivor_list<-read.csv(paste0(prefix, "FILEPATH/ebola_survivors_dataset_23May2018.csv"), stringsAsFactors = FALSE)

#drop the 0 survivors
evd_survivor_list<-subset(evd_survivor_list, evd_survivor_list$best != 0)

outbreak_list<-unique(evd_survivor_list$event_name)

for(i in 1:length(outbreak_list)){
  survivor_subset<-subset(evd_survivor_list, evd_survivor_list$event_name == outbreak_list[i])
  ref_locs<-unique(survivor_subset$iso)
  print(paste0("On event ", outbreak_list[[i]]))
  for(k in 1:nrow(survivor_subset)){
    ref_sex<-survivor_subset$sex_id[k]
    ref_age<-survivor_subset$age_group_id[k]
    ref_loc<-survivor_subset$iso[k]
    ref_best<-survivor_subset$best[k]
    ref_year<-survivor_subset$year[k]
    if(ref_loc == "GAB"){ref_loc <- 173}
    if(ref_loc == "CIV"){ref_loc <- 205}
    if(ref_loc == "COD"){ref_loc <- 171}
    if(ref_loc == "COG"){ref_loc <- 170}
    if(ref_loc == "GIN"){ref_loc <- 208}
    if(ref_loc == "LBR"){ref_loc <- 210}
    if(ref_loc == "SLE"){ref_loc <- 217}
    if(ref_loc == "SSD"){ref_loc <- 435}
    if(ref_loc == "UGA"){ref_loc <- 190}

    if(ref_sex == 3 & ref_age == 22){
      #load in proportion split for all age both sexes
      all_prop<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_age_sex_split.csv"), stringsAsFactors = FALSE)
      apportioned_survivors<-ref_best*all_prop[,3:1002]
      apportioned_survivors<-cbind(all_prop[,c(1,2)],apportioned_survivors)

      #inflate using cases scalar
      underreport<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_underreporting_cases_draws.csv"), stringsAsFactors = FALSE)
      for(beta in 1:nrow(apportioned_survivors)){
      apportioned_survivors[beta,3:1002]<-apportioned_survivors[beta,3:1002]*underreport[2:1001]
      }

      #get population
      population<-get_population(location_id = ref_loc,
                                 year_id = ref_year,
                                 sex_id = apportioned_survivors$sex_id,
                                 age_group_id = apportioned_survivors$age_group_id)
      #convert into incidence (by using population denominator)
      #convert into prevalence (by using population denominator and duration)
      #load in duration acute
      duration_acute<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_acute_duration_draws.csv"), stringsAsFactors = FALSE)
      duration_chronic_year1<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year1.csv"), stringsAsFactors = FALSE)
      duration_chronic_year2<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year2.csv"), stringsAsFactors = FALSE)
      evd_incidence<-apportioned_survivors
      evd_prevalence_acute<-apportioned_survivors
      evd_prevalence_chronic_year1<-apportioned_survivors
      evd_prevalence_chronic_year2<-apportioned_survivors
      for (q in 1:nrow(evd_incidence)){
        evd_incidence[q,3:1002]<-apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_incidence$age_group_id[q] & population$sex_id == evd_incidence$sex_id[q])]
        evd_prevalence_acute[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_acute$age_group_id[q] & population$sex_id == evd_prevalence_acute$sex_id[q])])*duration_acute[2,2:1001]
        evd_prevalence_chronic_year1[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_chronic_year1$age_group_id[q] & population$sex_id == evd_prevalence_chronic_year1$sex_id[q])])*duration_chronic_year1[,2:1001]
        evd_prevalence_chronic_year2[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_chronic_year2$age_group_id[q] & population$sex_id == evd_prevalence_chronic_year2$sex_id[q])])*duration_chronic_year2[,2:1001]
      }
      #load in the relevant files
      me_9668<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)
      me_9669<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)
      me_9669_yr2<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year+1, ".csv"), stringsAsFactors = FALSE)

      for(w in 1:nrow(evd_incidence)){
        me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
                        me_9668$sex_id==evd_incidence$sex_id[w] &
                        me_9668$measure_id==6),7:1006]<-me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
                                                                        me_9668$sex_id==evd_incidence$sex_id[w] &
                                                                        me_9668$measure_id==6),7:1006] + evd_incidence[w,3:1002]
        me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
                        me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
                        me_9668$measure_id==5),7:1006]<-me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
                                                                        me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
                                                                        me_9668$measure_id==5),7:1006] + evd_prevalence_acute[w,3:1002]
        me_9669[which(me_9669$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
                        me_9669$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
                        me_9669$measure_id==5),7:1006]<-me_9669[which(me_9669$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
                                                                        me_9669$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
                                                                        me_9669$measure_id==5),7:1006] + evd_prevalence_chronic_year1[w,3:1002]
        me_9669_yr2[which(me_9669_yr2$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
                        me_9669_yr2$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
                        me_9669_yr2$measure_id==5),7:1006]<-me_9669_yr2[which(me_9669_yr2$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
                                                                        me_9669_yr2$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
                                                                        me_9669_yr2$measure_id==5),7:1006] + evd_prevalence_chronic_year2[w,3:1002]

      }
      write.csv(me_9668, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)
      write.csv(me_9669, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)
      write.csv(me_9669_yr2, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year+1, ".csv"),row.names = FALSE)
    }

    if(ref_age == 22 & ref_sex == 1 || ref_age == 22 & ref_sex == 2){
      if(ref_sex == 1) {all_prop<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_age_sex_split_male.csv"), stringsAsFactors = FALSE)}
      if(ref_sex == 2) {all_prop<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_age_sex_split_female.csv"), stringsAsFactors = FALSE)}
      apportioned_survivors<-ref_best*all_prop[,3:1002]
      apportioned_survivors<-cbind(all_prop[,c(1,2)],apportioned_survivors)

      #inflate using cases scalar
      underreport<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_underreporting_cases_draws.csv"), stringsAsFactors = FALSE)
      for(beta in 1:nrow(apportioned_survivors)){
        apportioned_survivors[beta,3:1002]<-apportioned_survivors[beta,3:1002]*underreport[2:1001]
      }

      population<-get_population(location_id = ref_loc,
                                 year_id = ref_year,
                                 sex_id = ref_sex,
                                 age_group_id = apportioned_survivors$age_group_id)

      #convert into incidence (by using population denominator)
      #convert into prevalence (by using population denominator and duration)
      #load in duration acute
      duration_acute<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_acute_duration_draws.csv"), stringsAsFactors = FALSE)
      duration_chronic_year1<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year1.csv"), stringsAsFactors = FALSE)
      duration_chronic_year2<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year2.csv"), stringsAsFactors = FALSE)
      evd_incidence<-apportioned_survivors
      evd_prevalence_acute<-apportioned_survivors
      evd_prevalence_chronic_year1<-apportioned_survivors
      evd_prevalence_chronic_year2<-apportioned_survivors
      for (q in 1:nrow(evd_incidence)){
        evd_incidence[q,3:1002]<-apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_incidence$age_group_id[q] & population$sex_id == evd_incidence$sex_id[q])]
        evd_prevalence_acute[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_acute$age_group_id[q] & population$sex_id == evd_prevalence_acute$sex_id[q])])*duration_acute[2,2:1001]
        evd_prevalence_chronic_year1[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_chronic_year1$age_group_id[q] & population$sex_id == evd_prevalence_chronic_year1$sex_id[q])])*duration_chronic_year1[,2:1001]
        evd_prevalence_chronic_year2[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_chronic_year2$age_group_id[q] & population$sex_id == evd_prevalence_chronic_year2$sex_id[q])])*duration_chronic_year2[,2:1001]
      }
      #load in the relevant files
      me_9668<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)
      me_9669<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)
      me_9669_yr2<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year+1, ".csv"), stringsAsFactors = FALSE)

      for(w in 1:nrow(evd_incidence)){
        me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
                        me_9668$sex_id==evd_incidence$sex_id[w] &
                        me_9668$measure_id==6),7:1006]<-me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
                                                                        me_9668$sex_id==evd_incidence$sex_id[w] &
                                                                        me_9668$measure_id==6),7:1006] + evd_incidence[w,3:1002]
        me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
                        me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
                        me_9668$measure_id==5),7:1006]<-me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
                                                                        me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
                                                                        me_9668$measure_id==5),7:1006] + evd_prevalence_acute[w,3:1002]
        me_9669[which(me_9669$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
                        me_9669$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
                        me_9669$measure_id==5),7:1006]<-me_9669[which(me_9669$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
                                                                        me_9669$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
                                                                        me_9669$measure_id==5),7:1006] + evd_prevalence_chronic_year1[w,3:1002]
        me_9669_yr2[which(me_9669_yr2$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
                            me_9669_yr2$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
                            me_9669_yr2$measure_id==5),7:1006]<-me_9669_yr2[which(me_9669_yr2$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
                                                                                    me_9669_yr2$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
                                                                                    me_9669_yr2$measure_id==5),7:1006] + evd_prevalence_chronic_year2[w,3:1002]

      }
      write.csv(me_9668, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)
      write.csv(me_9669, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)
      write.csv(me_9669_yr2, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year+1, ".csv"),row.names = FALSE)
    }



    if(ref_age != 22){
      #inflate using cases scalar
      underreport<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_underreporting_cases_draws.csv"), stringsAsFactors = FALSE)
      #get population
      population<-get_population(location_id = ref_loc,
                                 year_id = ref_year,
                                 sex_id = ref_sex,
                                 age_group_id = ref_age)
      #convert into incidence (by using population denominator)
      #convert into prevalence (by using population denominator and duration)
      #load in duration acute
      duration_acute<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_acute_duration_draws.csv"), stringsAsFactors = FALSE)
      duration_chronic_year1<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year1.csv"), stringsAsFactors = FALSE)
      duration_chronic_year2<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year2.csv"), stringsAsFactors = FALSE)

      scaled_counts<-underreport[,2:1001]*ref_best
      evd_incidence<-scaled_counts/population$population
      evd_prevalence_acute<-evd_incidence*duration_acute[2, 2:1001]
      evd_prevalence_chronic_year1<-evd_incidence*duration_chronic_year1[,2:1001]
      evd_prevalence_chronic_year2<-evd_incidence*duration_chronic_year2[,2:1001]

      #load in the relevant files
      me_9668<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)
      me_9669<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)
      me_9669_yr2<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year+1, ".csv"), stringsAsFactors = FALSE)

      me_9668[which(me_9668$age_group_id==ref_age &
                      me_9668$sex_id==ref_sex &
                      me_9668$measure_id==6),7:1006]<-me_9668[which(me_9668$age_group_id==ref_age &
                                                                      me_9668$sex_id==ref_sex &
                                                                      me_9668$measure_id==6),7:1006] + evd_incidence
      me_9668[which(me_9668$age_group_id==ref_age &
                      me_9668$sex_id==ref_sex &
                      me_9668$measure_id==5),7:1006]<-me_9668[which(me_9668$age_group_id==ref_age &
                                                                      me_9668$sex_id==ref_sex &
                                                                      me_9668$measure_id==5),7:1006] + evd_prevalence_acute

      me_9669[which(me_9669$age_group_id==ref_age &
                      me_9669$sex_id==ref_sex &
                      me_9669$measure_id==5),7:1006]<-me_9669[which(me_9669$age_group_id==ref_age &
                                                                      me_9669$sex_id== ref_sex &
                                                                      me_9669$measure_id==5),7:1006] + evd_prevalence_chronic_year1

      me_9669_yr2[which(me_9669_yr2$age_group_id==ref_age &
                          me_9669_yr2$sex_id==ref_sex &
                          me_9669_yr2$measure_id==5),7:1006]<-me_9669_yr2[which(me_9669_yr2$age_group_id==ref_age &
                                                                              me_9669_yr2$sex_id==ref_sex &
                                                                              me_9669_yr2$measure_id==5),7:1006] + evd_prevalence_chronic_year2
      write.csv(me_9668, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)
      write.csv(me_9669, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)
      write.csv(me_9669_yr2, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year+1, ".csv"),row.names = FALSE)
    }

  }
}
###################################################################################################################################################################################################
#load in survivors values - West Arica 2014/2015
#not necessary post 26th May as integrated into survivor list now
# evd_survivor_list<-read.csv(paste0(prefix, "FILEPATH/ebola_wafrica_survivors.csv"), stringsAsFactors = FALSE)
# survivor_subset<-evd_survivor_list
# for(k in 1:nrow(survivor_subset)){
#   ref_sex<-survivor_subset$sex_id[k]
#   ref_age<-survivor_subset$age_group_id[k]
#   if(ref_age %in% c(33,48)){ref_age<-235}
#   ref_loc<-survivor_subset$iso[k]
#   ref_year<-survivor_subset$year[k]
#   if(ref_loc == "GIN"){ref_loc <- 208}
#   if(ref_loc == "SLE"){ref_loc <- 217}
#   if(ref_loc == "LBR"){ref_loc <- 210}
#
#   if(ref_age == 22 & ref_sex == 1 || ref_age == 22 & ref_sex == 2){
#     if(ref_sex == 1) {all_prop<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_age_sex_split_male.csv"), stringsAsFactors = FALSE)}
#     if(ref_sex == 2) {all_prop<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_age_sex_split_female.csv"), stringsAsFactors = FALSE)}
#
#     survivor_df<-evd_survivor_list[k,7:1006]
#     for (gamma in 1:nrow(all_prop)){
#       survivor_df[gamma,]<-evd_survivor_list[k,7:1006]
#     }
#
#     apportioned_survivors<-survivor_df*all_prop[,3:1002]
#     apportioned_survivors<-cbind(all_prop[,c(1,2)],apportioned_survivors)
#     #inflate using cases scalar
#     underreport<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_underreporting_cases_draws.csv"), stringsAsFactors = FALSE)
#     for(beta in 1:nrow(apportioned_survivors)){
#       apportioned_survivors[beta,3:1002]<-apportioned_survivors[beta,3:1002]*underreport[2:1001]
#     }
#
#     population<-get_population(location_id = ref_loc,
#                                year_id = ref_year,
#                                sex_id = ref_sex,
#                                age_group_id = apportioned_survivors$age_group_id)
#
#     #convert into incidence (by using population denominator)
#     #convert into prevalence (by using population denominator and duration)
#     #load in duration acute
#     duration_acute<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_acute_duration_draws.csv"), stringsAsFactors = FALSE)
#     duration_chronic_year1<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year1.csv"), stringsAsFactors = FALSE)
#     duration_chronic_year2<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year2.csv"), stringsAsFactors = FALSE)
#     evd_incidence<-apportioned_survivors
#     evd_prevalence_acute<-apportioned_survivors
#     evd_prevalence_chronic_year1<-apportioned_survivors
#     evd_prevalence_chronic_year2<-apportioned_survivors
#     for (q in 1:nrow(evd_incidence)){
#       evd_incidence[q,3:1002]<-apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_incidence$age_group_id[q] & population$sex_id == evd_incidence$sex_id[q])]
#       evd_prevalence_acute[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_acute$age_group_id[q] & population$sex_id == evd_prevalence_acute$sex_id[q])])*duration_acute[2,2:1001]
#       evd_prevalence_chronic_year1[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_chronic_year1$age_group_id[q] & population$sex_id == evd_prevalence_chronic_year1$sex_id[q])])*duration_chronic_year1[,2:1001]
#       evd_prevalence_chronic_year2[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_chronic_year2$age_group_id[q] & population$sex_id == evd_prevalence_chronic_year2$sex_id[q])])*duration_chronic_year2[,2:1001]
#     }
#     #load in the relevant files
#     me_9668<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)
#     me_9669<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)
#     me_9669_yr2<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year+1, ".csv"), stringsAsFactors = FALSE)
#
#     for(w in 1:nrow(evd_incidence)){
#       me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
#                       me_9668$sex_id==evd_incidence$sex_id[w] &
#                       me_9668$measure_id==6),7:1006]<-me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
#                                                                       me_9668$sex_id==evd_incidence$sex_id[w] &
#                                                                       me_9668$measure_id==6),7:1006] + evd_incidence[w,3:1002]
#       me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
#                       me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
#                       me_9668$measure_id==5),7:1006]<-me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
#                                                                       me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
#                                                                       me_9668$measure_id==5),7:1006] + evd_prevalence_acute[w,3:1002]
#       me_9669[which(me_9669$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
#                       me_9669$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
#                       me_9669$measure_id==5),7:1006]<-me_9669[which(me_9669$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
#                                                                       me_9669$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
#                                                                       me_9669$measure_id==5),7:1006] + evd_prevalence_chronic_year1[w,3:1002]
#       me_9669_yr2[which(me_9669_yr2$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
#                           me_9669_yr2$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
#                           me_9669_yr2$measure_id==5),7:1006]<-me_9669_yr2[which(me_9669_yr2$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
#                                                                                   me_9669_yr2$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
#                                                                                   me_9669_yr2$measure_id==5),7:1006] + evd_prevalence_chronic_year2[w,3:1002]
#
#     }
#     write.csv(me_9668, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)
#     write.csv(me_9669, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)
#     write.csv(me_9669_yr2, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year+1, ".csv"),row.names = FALSE)
#   }
#   if(ref_age != 22){
#     #inflate using cases scalar
#     underreport<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_underreporting_cases_draws.csv"), stringsAsFactors = FALSE)
#     #get population
#     population<-get_population(location_id = ref_loc,
#                                year_id = ref_year,
#                                sex_id = ref_sex,
#                                age_group_id = ref_age)
#     #convert into incidence (by using population denominator)
#     #convert into prevalence (by using population denominator and duration)
#     #load in duration acute
#     duration_acute<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_acute_duration_draws.csv"), stringsAsFactors = FALSE)
#     duration_chronic_year1<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year1.csv"), stringsAsFactors = FALSE)
#     duration_chronic_year2<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year2.csv"), stringsAsFactors = FALSE)
#
#     scaled_counts<-underreport[,2:1001]*evd_survivor_list[k,7:1006]
#     evd_incidence<-scaled_counts/population$population
#     evd_prevalence_acute<-evd_incidence*duration_acute[2, 2:1001]
#     evd_prevalence_chronic_year1<-evd_incidence*duration_chronic_year1[,2:1001]
#     evd_prevalence_chronic_year2<-evd_incidence*duration_chronic_year2[,2:1001]
#
#     #load in the relevant files
#     me_9668<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)
#     me_9669<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)
#     me_9669_yr2<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year+1, ".csv"), stringsAsFactors = FALSE)
#
#     me_9668[which(me_9668$age_group_id==ref_age &
#                     me_9668$sex_id==ref_sex &
#                     me_9668$measure_id==6),7:1006]<-me_9668[which(me_9668$age_group_id==ref_age &
#                                                                     me_9668$sex_id==ref_sex &
#                                                                     me_9668$measure_id==6),7:1006] + evd_incidence
#     me_9668[which(me_9668$age_group_id==ref_age &
#                     me_9668$sex_id==ref_sex &
#                     me_9668$measure_id==5),7:1006]<-me_9668[which(me_9668$age_group_id==ref_age &
#                                                                     me_9668$sex_id==ref_sex &
#                                                                     me_9668$measure_id==5),7:1006] + evd_prevalence_acute
#
#     me_9669[which(me_9669$age_group_id==ref_age &
#                     me_9669$sex_id==ref_sex &
#                     me_9669$measure_id==5),7:1006]<-me_9669[which(me_9669$age_group_id==ref_age &
#                                                                     me_9669$sex_id== ref_sex &
#                                                                     me_9669$measure_id==5),7:1006] + evd_prevalence_chronic_year1
#
#     me_9669_yr2[which(me_9669_yr2$age_group_id==ref_age &
#                         me_9669_yr2$sex_id==ref_sex &
#                         me_9669_yr2$measure_id==5),7:1006]<-me_9669_yr2[which(me_9669_yr2$age_group_id==ref_age &
#                                                                                 me_9669_yr2$sex_id==ref_sex &
#                                                                                 me_9669_yr2$measure_id==5),7:1006] + evd_prevalence_chronic_year2
#     write.csv(me_9668, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)
#     write.csv(me_9669, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)
#     write.csv(me_9669_yr2, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year+1, ".csv"),row.names = FALSE)
#   }
# }
#

########################################################################################################################
#add in deaths

#load in deaths
evd_survivor_list<-read.csv(paste0(prefix, "FILEPATH/ebola_shock_dataset_22May2018.csv"), stringsAsFactors = FALSE)

#drop the 0 deaths
evd_survivor_list<-subset(evd_survivor_list, evd_survivor_list$best != 0)


outbreak_list<-unique(evd_survivor_list$event_name)

for(i in 1:length(outbreak_list)){
  survivor_subset<-subset(evd_survivor_list, evd_survivor_list$event_name == outbreak_list[i])
  ref_locs<-unique(survivor_subset$iso)
  print(paste0("On event ", outbreak_list[[i]]))
  for(k in 1:nrow(survivor_subset)){
    ref_sex<-survivor_subset$sex_id[k]
    ref_age<-survivor_subset$age_group_id[k]
    if(ref_age %in% c(33,48)){ref_age<-235}
    ref_loc<-survivor_subset$iso[k]
    ref_best<-survivor_subset$best[k]
    ref_year<-survivor_subset$year[k]
    if(ref_loc == "GAB"){ref_loc <- 173}
    if(ref_loc == "CIV"){ref_loc <- 205}
    if(ref_loc == "COD"){ref_loc <- 171}
    if(ref_loc == "COG"){ref_loc <- 170}
    if(ref_loc == "GIN"){ref_loc <- 208}
    if(ref_loc == "LBR"){ref_loc <- 210}
    if(ref_loc == "SLE"){ref_loc <- 217}
    if(ref_loc == "SSD"){ref_loc <- 435}
    if(ref_loc == "UGA"){ref_loc <- 190}

    if(ref_sex == 3 & ref_age == 22){
      #load in proportion split for all age both sexes
      all_prop<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_age_sex_split_death.csv"), stringsAsFactors = FALSE)
      apportioned_survivors<-ref_best*all_prop[,3:1002]
      apportioned_survivors<-cbind(all_prop[,c(1,2)],apportioned_survivors)

      #inflate using cases scalar
      underreport<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_underreporting_deaths_draws.csv"), stringsAsFactors = FALSE)
      for(beta in 1:nrow(apportioned_survivors)){
        apportioned_survivors[beta,3:1002]<-apportioned_survivors[beta,3:1002]*underreport[2:1001]
      }

      #get population
      population<-get_population(location_id = ref_loc,
                                 year_id = ref_year,
                                 sex_id = apportioned_survivors$sex_id,
                                 age_group_id = apportioned_survivors$age_group_id)
      #convert into incidence (by using population denominator)
      #convert into prevalence (by using population denominator and duration)
      #load in duration acute
      duration_acute<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_acute_duration_draws.csv"), stringsAsFactors = FALSE)
      duration_chronic_year1<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year1.csv"), stringsAsFactors = FALSE)
      duration_chronic_year2<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year2.csv"), stringsAsFactors = FALSE)
      evd_incidence<-apportioned_survivors
      evd_prevalence_acute<-apportioned_survivors
      evd_prevalence_chronic_year1<-apportioned_survivors
      evd_prevalence_chronic_year2<-apportioned_survivors
      for (q in 1:nrow(evd_incidence)){
        evd_incidence[q,3:1002]<-apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_incidence$age_group_id[q] & population$sex_id == evd_incidence$sex_id[q])]
        evd_prevalence_acute[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_acute$age_group_id[q] & population$sex_id == evd_prevalence_acute$sex_id[q])])*duration_acute[1,2:1001]
        }
      #load in the relevant files
      me_9668<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)


      for(w in 1:nrow(evd_incidence)){
        me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
                        me_9668$sex_id==evd_incidence$sex_id[w] &
                        me_9668$measure_id==6),7:1006]<-me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
                                                                        me_9668$sex_id==evd_incidence$sex_id[w] &
                                                                        me_9668$measure_id==6),7:1006] + evd_incidence[w,3:1002]
        me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
                        me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
                        me_9668$measure_id==5),7:1006]<-me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
                                                                        me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
                                                                        me_9668$measure_id==5),7:1006] + evd_prevalence_acute[w,3:1002]


      }
      write.csv(me_9668, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)

    }

    if(ref_age == 22 & ref_sex == 1 || ref_age == 22 & ref_sex == 2){
      if(ref_sex == 1) {all_prop<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_age_sex_split_male_death.csv"), stringsAsFactors = FALSE)}
      if(ref_sex == 2) {all_prop<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_age_sex_split_female_death.csv"), stringsAsFactors = FALSE)}
      apportioned_survivors<-ref_best*all_prop[,3:1002]
      apportioned_survivors<-cbind(all_prop[,c(1,2)],apportioned_survivors)

      #inflate using cases scalar
      underreport<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_underreporting_deaths_draws.csv"), stringsAsFactors = FALSE)
      for(beta in 1:nrow(apportioned_survivors)){
        apportioned_survivors[beta,3:1002]<-apportioned_survivors[beta,3:1002]*underreport[2:1001]
      }

      population<-get_population(location_id = ref_loc,
                                 year_id = ref_year,
                                 sex_id = ref_sex,
                                 age_group_id = apportioned_survivors$age_group_id)

      #convert into incidence (by using population denominator)
      #convert into prevalence (by using population denominator and duration)
      #load in duration acute
      duration_acute<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_acute_duration_draws.csv"), stringsAsFactors = FALSE)
      duration_chronic_year1<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year1.csv"), stringsAsFactors = FALSE)
      duration_chronic_year2<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year2.csv"), stringsAsFactors = FALSE)
      evd_incidence<-apportioned_survivors
      evd_prevalence_acute<-apportioned_survivors

      for (q in 1:nrow(evd_incidence)){
        evd_incidence[q,3:1002]<-apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_incidence$age_group_id[q] & population$sex_id == evd_incidence$sex_id[q])]
        evd_prevalence_acute[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_acute$age_group_id[q] & population$sex_id == evd_prevalence_acute$sex_id[q])])*duration_acute[1,2:1001]
        }
      #load in the relevant files
      me_9668<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)


      for(w in 1:nrow(evd_incidence)){
        me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
                        me_9668$sex_id==evd_incidence$sex_id[w] &
                        me_9668$measure_id==6),7:1006]<-me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
                                                                        me_9668$sex_id==evd_incidence$sex_id[w] &
                                                                        me_9668$measure_id==6),7:1006] + evd_incidence[w,3:1002]
        me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
                        me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
                        me_9668$measure_id==5),7:1006]<-me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
                                                                        me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
                                                                        me_9668$measure_id==5),7:1006] + evd_prevalence_acute[w,3:1002]


      }
      write.csv(me_9668, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)

    }



    if(ref_age != 22){
      #inflate using cases scalar
      underreport<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_underreporting_deaths_draws.csv"), stringsAsFactors = FALSE)
      #get population
      population<-get_population(location_id = ref_loc,
                                 year_id = ref_year,
                                 sex_id = ref_sex,
                                 age_group_id = ref_age)
      #convert into incidence (by using population denominator)
      #convert into prevalence (by using population denominator and duration)
      #load in duration acute
      duration_acute<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_acute_duration_draws.csv"), stringsAsFactors = FALSE)
      duration_chronic_year1<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year1.csv"), stringsAsFactors = FALSE)
      duration_chronic_year2<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year2.csv"), stringsAsFactors = FALSE)

      scaled_counts<-underreport[,2:1001]*ref_best
      evd_incidence<-scaled_counts/population$population
      evd_prevalence_acute<-evd_incidence*duration_acute[1, 2:1001]


      #load in the relevant files
      me_9668<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)

      me_9668[which(me_9668$age_group_id==ref_age &
                      me_9668$sex_id==ref_sex &
                      me_9668$measure_id==6),7:1006]<-me_9668[which(me_9668$age_group_id==ref_age &
                                                                      me_9668$sex_id==ref_sex &
                                                                      me_9668$measure_id==6),7:1006] + evd_incidence
      me_9668[which(me_9668$age_group_id==ref_age &
                      me_9668$sex_id==ref_sex &
                      me_9668$measure_id==5),7:1006]<-me_9668[which(me_9668$age_group_id==ref_age &
                                                                      me_9668$sex_id==ref_sex &
                                                                      me_9668$measure_id==5),7:1006] + evd_prevalence_acute


      write.csv(me_9668, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)

    }

  }
}

##############################################################
#deal with imported cases
#imported survivors and deaths do not have underreporting upscaling


#load in imported survivors values
evd_survivor_list<-read.csv(paste0(prefix, "FILEPATH/data/ebola_imported_survivor_dataset_26May2018.csv"), stringsAsFactors = FALSE)

#drop the 0 survivors
evd_survivor_list<-subset(evd_survivor_list, evd_survivor_list$best != 0)

outbreak_list<-unique(evd_survivor_list$event_name)

for(i in 1:length(outbreak_list)){
  survivor_subset<-subset(evd_survivor_list, evd_survivor_list$event_name == outbreak_list[i])
  ref_locs<-unique(survivor_subset$iso)
  print(paste0("On event ", outbreak_list[[i]]))
  for(k in 1:nrow(survivor_subset)){
    ref_sex<-survivor_subset$sex_id[k]
    ref_age<-survivor_subset$age_group_id[k]
    ref_loc<-survivor_subset$iso[k]
    ref_best<-survivor_subset$best[k]
    ref_year<-survivor_subset$year[k]
    if(ref_loc == "ITA"){ref_loc <- 86}
    if(ref_loc == "MLI"){ref_loc <- 211}
    if(ref_loc == "NGA"){ref_loc <- 214}
    if(ref_loc == "SEN"){ref_loc <- 216}
    if(ref_loc == "SPA"){ref_loc <- 92}
    if(ref_loc == "Scotland"){ref_loc <- 434}
    if(ref_loc == "Texas"){ref_loc <- 566}
    if(ref_loc == "NewYork"){ref_loc <- 555}

    if(ref_sex == 3 & ref_age == 22){
      #load in proportion split for all age both sexes
      all_prop<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_age_sex_split.csv"), stringsAsFactors = FALSE)
      apportioned_survivors<-ref_best*all_prop[,3:1002]
      apportioned_survivors<-cbind(all_prop[,c(1,2)],apportioned_survivors)

      #inflate using cases scalar
      underreport<-1
      for(beta in 1:nrow(apportioned_survivors)){
        apportioned_survivors[beta,3:1002]<-apportioned_survivors[beta,3:1002]*underreport
      }

      #get population
      population<-get_population(location_id = ref_loc,
                                 year_id = ref_year,
                                 sex_id = apportioned_survivors$sex_id,
                                 age_group_id = apportioned_survivors$age_group_id)
      #convert into incidence (by using population denominator)
      #convert into prevalence (by using population denominator and duration)
      #load in duration acute
      duration_acute<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_acute_duration_draws.csv"), stringsAsFactors = FALSE)
      duration_chronic_year1<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year1.csv"), stringsAsFactors = FALSE)
      duration_chronic_year2<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year2.csv"), stringsAsFactors = FALSE)
      evd_incidence<-apportioned_survivors
      evd_prevalence_acute<-apportioned_survivors
      evd_prevalence_chronic_year1<-apportioned_survivors
      evd_prevalence_chronic_year2<-apportioned_survivors
      for (q in 1:nrow(evd_incidence)){
        evd_incidence[q,3:1002]<-apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_incidence$age_group_id[q] & population$sex_id == evd_incidence$sex_id[q])]
        evd_prevalence_acute[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_acute$age_group_id[q] & population$sex_id == evd_prevalence_acute$sex_id[q])])*duration_acute[2,2:1001]
        evd_prevalence_chronic_year1[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_chronic_year1$age_group_id[q] & population$sex_id == evd_prevalence_chronic_year1$sex_id[q])])*duration_chronic_year1[,2:1001]
        evd_prevalence_chronic_year2[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_chronic_year2$age_group_id[q] & population$sex_id == evd_prevalence_chronic_year2$sex_id[q])])*duration_chronic_year2[,2:1001]
      }
      #load in the relevant files
      me_9668<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)
      me_9669<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)
      me_9669_yr2<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year+1, ".csv"), stringsAsFactors = FALSE)

      for(w in 1:nrow(evd_incidence)){
        me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
                        me_9668$sex_id==evd_incidence$sex_id[w] &
                        me_9668$measure_id==6),7:1006]<-me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
                                                                        me_9668$sex_id==evd_incidence$sex_id[w] &
                                                                        me_9668$measure_id==6),7:1006] + evd_incidence[w,3:1002]
        me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
                        me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
                        me_9668$measure_id==5),7:1006]<-me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
                                                                        me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
                                                                        me_9668$measure_id==5),7:1006] + evd_prevalence_acute[w,3:1002]
        me_9669[which(me_9669$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
                        me_9669$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
                        me_9669$measure_id==5),7:1006]<-me_9669[which(me_9669$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
                                                                        me_9669$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
                                                                        me_9669$measure_id==5),7:1006] + evd_prevalence_chronic_year1[w,3:1002]
        me_9669_yr2[which(me_9669_yr2$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
                            me_9669_yr2$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
                            me_9669_yr2$measure_id==5),7:1006]<-me_9669_yr2[which(me_9669_yr2$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
                                                                                    me_9669_yr2$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
                                                                                    me_9669_yr2$measure_id==5),7:1006] + evd_prevalence_chronic_year2[w,3:1002]

      }
      write.csv(me_9668, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)
      write.csv(me_9669, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)
      write.csv(me_9669_yr2, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year+1, ".csv"),row.names = FALSE)
    }

    if(ref_age == 22 & ref_sex == 1 || ref_age == 22 & ref_sex == 2){
      if(ref_sex == 1) {all_prop<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_age_sex_split_male.csv"), stringsAsFactors = FALSE)}
      if(ref_sex == 2) {all_prop<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_age_sex_split_female.csv"), stringsAsFactors = FALSE)}
      apportioned_survivors<-ref_best*all_prop[,3:1002]
      apportioned_survivors<-cbind(all_prop[,c(1,2)],apportioned_survivors)

      #inflate using cases scalar
      underreport<-1
      for(beta in 1:nrow(apportioned_survivors)){
        apportioned_survivors[beta,3:1002]<-apportioned_survivors[beta,3:1002]*underreport
      }

      population<-get_population(location_id = ref_loc,
                                 year_id = ref_year,
                                 sex_id = ref_sex,
                                 age_group_id = apportioned_survivors$age_group_id)

      #convert into incidence (by using population denominator)
      #convert into prevalence (by using population denominator and duration)
      #load in duration acute
      duration_acute<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_acute_duration_draws.csv"), stringsAsFactors = FALSE)
      duration_chronic_year1<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year1.csv"), stringsAsFactors = FALSE)
      duration_chronic_year2<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year2.csv"), stringsAsFactors = FALSE)
      evd_incidence<-apportioned_survivors
      evd_prevalence_acute<-apportioned_survivors
      evd_prevalence_chronic_year1<-apportioned_survivors
      evd_prevalence_chronic_year2<-apportioned_survivors
      for (q in 1:nrow(evd_incidence)){
        evd_incidence[q,3:1002]<-apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_incidence$age_group_id[q] & population$sex_id == evd_incidence$sex_id[q])]
        evd_prevalence_acute[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_acute$age_group_id[q] & population$sex_id == evd_prevalence_acute$sex_id[q])])*duration_acute[2,2:1001]
        evd_prevalence_chronic_year1[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_chronic_year1$age_group_id[q] & population$sex_id == evd_prevalence_chronic_year1$sex_id[q])])*duration_chronic_year1[,2:1001]
        evd_prevalence_chronic_year2[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_chronic_year2$age_group_id[q] & population$sex_id == evd_prevalence_chronic_year2$sex_id[q])])*duration_chronic_year2[,2:1001]
      }
      #load in the relevant files
      me_9668<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)
      me_9669<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)
      me_9669_yr2<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year+1, ".csv"), stringsAsFactors = FALSE)

      for(w in 1:nrow(evd_incidence)){
        me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
                        me_9668$sex_id==evd_incidence$sex_id[w] &
                        me_9668$measure_id==6),7:1006]<-me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
                                                                        me_9668$sex_id==evd_incidence$sex_id[w] &
                                                                        me_9668$measure_id==6),7:1006] + evd_incidence[w,3:1002]
        me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
                        me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
                        me_9668$measure_id==5),7:1006]<-me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
                                                                        me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
                                                                        me_9668$measure_id==5),7:1006] + evd_prevalence_acute[w,3:1002]
        me_9669[which(me_9669$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
                        me_9669$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
                        me_9669$measure_id==5),7:1006]<-me_9669[which(me_9669$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
                                                                        me_9669$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
                                                                        me_9669$measure_id==5),7:1006] + evd_prevalence_chronic_year1[w,3:1002]
        me_9669_yr2[which(me_9669_yr2$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
                            me_9669_yr2$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
                            me_9669_yr2$measure_id==5),7:1006]<-me_9669_yr2[which(me_9669_yr2$age_group_id==evd_prevalence_chronic_year1$age_group_id[w] &
                                                                                    me_9669_yr2$sex_id==evd_prevalence_chronic_year1$sex_id[w] &
                                                                                    me_9669_yr2$measure_id==5),7:1006] + evd_prevalence_chronic_year2[w,3:1002]

      }
      write.csv(me_9668, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)
      write.csv(me_9669, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)
      write.csv(me_9669_yr2, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year+1, ".csv"),row.names = FALSE)
    }



    if(ref_age != 22){
      #inflate using cases scalar
      underreport<-1
      #get population
      population<-get_population(location_id = ref_loc,
                                 year_id = ref_year,
                                 sex_id = ref_sex,
                                 age_group_id = ref_age)
      #convert into incidence (by using population denominator)
      #convert into prevalence (by using population denominator and duration)
      #load in duration acute
      duration_acute<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_acute_duration_draws.csv"), stringsAsFactors = FALSE)
      duration_chronic_year1<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year1.csv"), stringsAsFactors = FALSE)
      duration_chronic_year2<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year2.csv"), stringsAsFactors = FALSE)

      scaled_counts<-underreport*ref_best
      evd_incidence<-scaled_counts/population$population
      evd_prevalence_acute<-evd_incidence*duration_acute[2, 2:1001]
      evd_prevalence_chronic_year1<-evd_incidence*duration_chronic_year1[,2:1001]
      evd_prevalence_chronic_year2<-evd_incidence*duration_chronic_year2[,2:1001]

      #load in the relevant files
      me_9668<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)
      me_9669<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)
      me_9669_yr2<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year+1, ".csv"), stringsAsFactors = FALSE)

      me_9668[which(me_9668$age_group_id==ref_age &
                      me_9668$sex_id==ref_sex &
                      me_9668$measure_id==6),7:1006]<-me_9668[which(me_9668$age_group_id==ref_age &
                                                                      me_9668$sex_id==ref_sex &
                                                                      me_9668$measure_id==6),7:1006] + evd_incidence
      me_9668[which(me_9668$age_group_id==ref_age &
                      me_9668$sex_id==ref_sex &
                      me_9668$measure_id==5),7:1006]<-me_9668[which(me_9668$age_group_id==ref_age &
                                                                      me_9668$sex_id==ref_sex &
                                                                      me_9668$measure_id==5),7:1006] + evd_prevalence_acute

      me_9669[which(me_9669$age_group_id==ref_age &
                      me_9669$sex_id==ref_sex &
                      me_9669$measure_id==5),7:1006]<-me_9669[which(me_9669$age_group_id==ref_age &
                                                                      me_9669$sex_id== ref_sex &
                                                                      me_9669$measure_id==5),7:1006] + evd_prevalence_chronic_year1

      me_9669_yr2[which(me_9669_yr2$age_group_id==ref_age &
                          me_9669_yr2$sex_id==ref_sex &
                          me_9669_yr2$measure_id==5),7:1006]<-me_9669_yr2[which(me_9669_yr2$age_group_id==ref_age &
                                                                                  me_9669_yr2$sex_id==ref_sex &
                                                                                  me_9669_yr2$measure_id==5),7:1006] + evd_prevalence_chronic_year2
      write.csv(me_9668, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)
      write.csv(me_9669, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)
      write.csv(me_9669_yr2, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year+1, ".csv"),row.names = FALSE)
    }

  }
}

#now process imported deaths

#load in deaths
evd_survivor_list<-read.csv(paste0(prefix, "FILEPATH/ebola_imported_death_dataset_26May2018.csv"), stringsAsFactors = FALSE)

#drop the 0 deaths
evd_survivor_list<-subset(evd_survivor_list, evd_survivor_list$best != 0)


outbreak_list<-unique(evd_survivor_list$event_name)

for(i in 1:length(outbreak_list)){
  survivor_subset<-subset(evd_survivor_list, evd_survivor_list$event_name == outbreak_list[i])
  ref_locs<-unique(survivor_subset$iso)
  print(paste0("On event ", outbreak_list[[i]]))
  for(k in 1:nrow(survivor_subset)){
    ref_sex<-survivor_subset$sex_id[k]
    ref_age<-survivor_subset$age_group_id[k]
    if(ref_age %in% c(33,48)){ref_age<-235}
    ref_loc<-survivor_subset$iso[k]
    ref_best<-survivor_subset$best[k]
    ref_year<-survivor_subset$year[k]
    if(ref_loc == "MLI"){ref_loc <- 211}
    if(ref_loc == "NGA"){ref_loc <- 214}
    if(ref_loc == "Texas"){ref_loc <- 566}

    if(ref_sex == 3 & ref_age == 22){
      #load in proportion split for all age both sexes
      all_prop<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_age_sex_split_death.csv"), stringsAsFactors = FALSE)
      apportioned_survivors<-ref_best*all_prop[,3:1002]
      apportioned_survivors<-cbind(all_prop[,c(1,2)],apportioned_survivors)

      #inflate using cases scalar
      underreport<-1
      for(beta in 1:nrow(apportioned_survivors)){
        apportioned_survivors[beta,3:1002]<-apportioned_survivors[beta,3:1002]*underreport
      }

      #get population
      population<-get_population(location_id = ref_loc,
                                 year_id = ref_year,
                                 sex_id = apportioned_survivors$sex_id,
                                 age_group_id = apportioned_survivors$age_group_id)
      #convert into incidence (by using population denominator)
      #convert into prevalence (by using population denominator and duration)
      #load in duration acute
      duration_acute<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_acute_duration_draws.csv"), stringsAsFactors = FALSE)
      duration_chronic_year1<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year1.csv"), stringsAsFactors = FALSE)
      duration_chronic_year2<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year2.csv"), stringsAsFactors = FALSE)
      evd_incidence<-apportioned_survivors
      evd_prevalence_acute<-apportioned_survivors
      evd_prevalence_chronic_year1<-apportioned_survivors
      evd_prevalence_chronic_year2<-apportioned_survivors
      for (q in 1:nrow(evd_incidence)){
        evd_incidence[q,3:1002]<-apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_incidence$age_group_id[q] & population$sex_id == evd_incidence$sex_id[q])]
        evd_prevalence_acute[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_acute$age_group_id[q] & population$sex_id == evd_prevalence_acute$sex_id[q])])*duration_acute[1,2:1001]
      }
      #load in the relevant files
      me_9668<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)


      for(w in 1:nrow(evd_incidence)){
        me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
                        me_9668$sex_id==evd_incidence$sex_id[w] &
                        me_9668$measure_id==6),7:1006]<-me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
                                                                        me_9668$sex_id==evd_incidence$sex_id[w] &
                                                                        me_9668$measure_id==6),7:1006] + evd_incidence[w,3:1002]
        me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
                        me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
                        me_9668$measure_id==5),7:1006]<-me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
                                                                        me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
                                                                        me_9668$measure_id==5),7:1006] + evd_prevalence_acute[w,3:1002]


      }
      write.csv(me_9668, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)

    }

    if(ref_age == 22 & ref_sex == 1 || ref_age == 22 & ref_sex == 2){
      if(ref_sex == 1) {all_prop<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_age_sex_split_male_death.csv"), stringsAsFactors = FALSE)}
      if(ref_sex == 2) {all_prop<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_age_sex_split_female_death.csv"), stringsAsFactors = FALSE)}
      apportioned_survivors<-ref_best*all_prop[,3:1002]
      apportioned_survivors<-cbind(all_prop[,c(1,2)],apportioned_survivors)

      #inflate using cases scalar
      underreport<-1
      for(beta in 1:nrow(apportioned_survivors)){
        apportioned_survivors[beta,3:1002]<-apportioned_survivors[beta,3:1002]*underreport
      }

      population<-get_population(location_id = ref_loc,
                                 year_id = ref_year,
                                 sex_id = ref_sex,
                                 age_group_id = apportioned_survivors$age_group_id)

      #convert into incidence (by using population denominator)
      #convert into prevalence (by using population denominator and duration)
      #load in duration acute
      duration_acute<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_acute_duration_draws.csv"), stringsAsFactors = FALSE)
      duration_chronic_year1<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year1.csv"), stringsAsFactors = FALSE)
      duration_chronic_year2<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year2.csv"), stringsAsFactors = FALSE)
      evd_incidence<-apportioned_survivors
      evd_prevalence_acute<-apportioned_survivors

      for (q in 1:nrow(evd_incidence)){
        evd_incidence[q,3:1002]<-apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_incidence$age_group_id[q] & population$sex_id == evd_incidence$sex_id[q])]
        evd_prevalence_acute[q,3:1002]<-(apportioned_survivors[q,3:1002]/population$population[which(population$age_group_id == evd_prevalence_acute$age_group_id[q] & population$sex_id == evd_prevalence_acute$sex_id[q])])*duration_acute[1,2:1001]
      }
      #load in the relevant files
      me_9668<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)


      for(w in 1:nrow(evd_incidence)){
        me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
                        me_9668$sex_id==evd_incidence$sex_id[w] &
                        me_9668$measure_id==6),7:1006]<-me_9668[which(me_9668$age_group_id==evd_incidence$age_group_id[w] &
                                                                        me_9668$sex_id==evd_incidence$sex_id[w] &
                                                                        me_9668$measure_id==6),7:1006] + evd_incidence[w,3:1002]
        me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
                        me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
                        me_9668$measure_id==5),7:1006]<-me_9668[which(me_9668$age_group_id==evd_prevalence_acute$age_group_id[w] &
                                                                        me_9668$sex_id==evd_prevalence_acute$sex_id[w] &
                                                                        me_9668$measure_id==5),7:1006] + evd_prevalence_acute[w,3:1002]


      }
      write.csv(me_9668, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)

    }



    if(ref_age != 22){
      #inflate using cases scalar
      underreport<-1
      #get population
      population<-get_population(location_id = ref_loc,
                                 year_id = ref_year,
                                 sex_id = ref_sex,
                                 age_group_id = ref_age)
      #convert into incidence (by using population denominator)
      #convert into prevalence (by using population denominator and duration)
      #load in duration acute
      duration_acute<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_acute_duration_draws.csv"), stringsAsFactors = FALSE)
      duration_chronic_year1<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year1.csv"), stringsAsFactors = FALSE)
      duration_chronic_year2<-read.csv(paste0(prefix, "FILEPATH/parameters/evd_chronic_duration_draws_year2.csv"), stringsAsFactors = FALSE)

      scaled_counts<-underreport*ref_best
      evd_incidence<-scaled_counts/population$population
      evd_prevalence_acute<-evd_incidence*duration_acute[1, 2:1001]


      #load in the relevant files
      me_9668<-read.csv(paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"), stringsAsFactors = FALSE)

      me_9668[which(me_9668$age_group_id==ref_age &
                      me_9668$sex_id==ref_sex &
                      me_9668$measure_id==6),7:1006]<-me_9668[which(me_9668$age_group_id==ref_age &
                                                                      me_9668$sex_id==ref_sex &
                                                                      me_9668$measure_id==6),7:1006] + evd_incidence
      me_9668[which(me_9668$age_group_id==ref_age &
                      me_9668$sex_id==ref_sex &
                      me_9668$measure_id==5),7:1006]<-me_9668[which(me_9668$age_group_id==ref_age &
                                                                      me_9668$sex_id==ref_sex &
                                                                      me_9668$measure_id==5),7:1006] + evd_prevalence_acute


      write.csv(me_9668, paste0(prefix, "FILEPATH",ref_loc,"_",ref_year, ".csv"),row.names = FALSE)

    }

  }
}
