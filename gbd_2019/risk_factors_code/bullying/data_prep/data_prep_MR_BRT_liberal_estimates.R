library(metafor)
library(data.table)
library(openxlsx)
library(ggplot2)

source("FILEPATH/or_to_rr.R")

seq_c <- function(start, finish){for(i in c(1:length(start))){
  temp_c <- seq(start[i], finish[i], 1)
  if(i==1){c <- temp_c} else {c <- cbind(c, temp_c)}
}
  return(unname(c))
}

#################################
##### Load Bullying dataset #####
#################################

bully_data_dep <- as.data.table(read.xlsx("FILEPATH.xlsx", sheet="Depressive disorders extraction"))
bully_data_dep <- bully_data_dep[include==1,                        
                         .(author=paste(First.Author, Publication.year), cohort = Name.of.Cohort, Adjusted, 
                         sex=Sex, age_start_min = Youngest.age.at.intake, age_start_max = Oldest.age.at.intake, age_start=Mean.age.at.intake, age_end=Mean.age.at.follow.up, time=Mean.years.follow.up, 
                         parameter=Parameter.type, value=Parameter.value, lower=Lower.UI, upper=Upper.UI, sd=Standard.deviation, 
                         p=p.value, exposed_cases=Exposed.cases, exposed_noncases=`Exposed.non-cases`, nonexposed_cases=`Non-exposed.cases`,
                         nonexposed_noncases=`Non-exposed.non-cases`, exposed_sample_size=`Sample.size.(exposed)`, 
                         nonexposed_sample_size=`Sample.size.(non-exposed)`, parameter_sample_size=`Sample.size.(exposed.+.non-exposed)`,
                         study_sample_size=`Study.sample.size.(total.N)`, cv_symptoms, cv_unadjusted, cv_b_parent_only, cv_or, cv_multi_reg, cv_cyberbullying, cv_low_threshold_bullying, cv_child_baseline, cv_retrospective, cv_baseline_adjust=Adjusted.for.outcome.at.baseline)]
bully_data_dep <- bully_data_dep[cv_retrospective==0, ]
bully_data_dep[is.na(age_start), age_start := (age_start_min + age_start_max) / 2]

##### back calculate required data #####
bully_data_dep[is.na(exposed_sample_size), exposed_sample_size:=exposed_cases+exposed_noncases]
bully_data_dep[is.na(nonexposed_sample_size), nonexposed_sample_size:=nonexposed_cases+nonexposed_noncases]
bully_data_dep[is.na(parameter_sample_size), parameter_sample_size:=exposed_sample_size+nonexposed_sample_size]
bully_data_dep[, `:=` (cases_total=exposed_cases+nonexposed_cases)]

# study specific adjustments
bully_data_dep[author=="Hemphill 2015", `:=` (cases_total = 174)]
bully_data_dep[author=="Hemphill 2011", `:=` (cases_total = parameter_sample_size*0.281)]
bully_data_dep[author=="Farrington 2011" & age_start==10 & time==1.5, `:=` (parameter_sample_size = 503, cases_total = (503*0.264 + 503*0.312)/2, exposed_sample_size = 503*0.253)]
bully_data_dep[author=="Farrington 2011" & age_start==10 & time==3.5, `:=` (parameter_sample_size = 503, cases_total = (503*0.204 + 503*0.223)/2, exposed_sample_size = 503*0.253)]
bully_data_dep[author=="Farrington 2011" & age_start==10 & time==5.5, `:=` (parameter_sample_size = 503, cases_total = (503*0.222 + 503*0.239)/2, exposed_sample_size = 503*0.253)]
bully_data_dep[author=="Farrington 2011" & age_start==11.5 & time==2, `:=` (parameter_sample_size = 503, cases_total = (503*0.204 + 503*0.223)/2, exposed_sample_size = (503*0.222 + 503*0.147)/2)]
bully_data_dep[author=="Farrington 2011" & age_start==11.5 & time==4, `:=` (parameter_sample_size = 503, cases_total = (503*0.222 + 503*0.239)/2, exposed_sample_size = (503*0.222 + 503*0.147)/2)]
bully_data_dep[author=="Farrington 2011" & age_start==13.5 & time==2, `:=` (parameter_sample_size = 503, cases_total = (503*0.222 + 503*0.239)/2, exposed_sample_size = (503*0.088 + 503*0.058)/2)]
bully_data_dep[author=="Vassallo 2014", `:=` (cases_total = parameter_sample_size*0.2)]
bully_data_dep[author=="Fahy 2016", `:=` (cases_total = parameter_sample_size*0.248)]
bully_data_dep[author=="Pirkola 2005" & sex == "Male", `:=` (parameter_sample_size = 4076/2, cases_total = (4076/2)*0.055, exposed_sample_size = (4076/2)*0.156)]
bully_data_dep[author=="Pirkola 2005" & sex == "Female", `:=` (parameter_sample_size = 4076/2, cases_total = (4076/2)*0.092, exposed_sample_size = (4076/2)*0.156)]
bully_data_dep[author=="Rothon 2011", `:=` (parameter_sample_size = study_sample_size, cases_total = study_sample_size*0.273, exposed_sample_size = study_sample_size*0.091)]
bully_data_dep[author=="Zwierzynska 2013", `:=` (value=1.74, lower=0.64, upper=4.75, parameter_sample_size=study_sample_size, exposed_sample_size=1706, cases_total=24)]

##### Load Anxiety  dataset #####
bully_data_anx <- as.data.table(read.xlsx("FILEPATH.xlsx", sheet="Anxiety disorders extraction"))
bully_data_anx <- bully_data_anx[include==1, 
                                                                      .(author=paste(First.Author, Publication.year), cohort = Name.of.Cohort, Adjusted, 
                                                                        sex=Sex, age_start_min = Youngest.age.at.intake, age_start_max = Oldest.age.at.intake, age_start=Mean.age.at.intake, age_end=Mean.age.at.follow.up, time=Mean.years.follow.up, 
                                                                     parameter=Parameter.type, value=Parameter.value, lower=Lower.UI, upper=Upper.UI, sd=Standard.deviation, 
                                                                     p=p.value, exposed_cases=Exposed.cases, exposed_noncases=`Exposed.non-cases`, nonexposed_cases=`Non-exposed.cases`,
                                                                     nonexposed_noncases=`Non-exposed.non-cases`, exposed_sample_size=`Sample.size.(exposed)`, 
                                                                     nonexposed_sample_size=`Sample.size.(non-exposed)`, parameter_sample_size=`Sample.size.(exposed.+.non-exposed)`,
                                                                     study_sample_size=`Study.sample.size.(total.N)`, cv_symptoms, cv_unadjusted, cv_b_parent_only, cv_or, cv_multi_reg, cv_cyberbullying, cv_low_threshold_bullying, cv_child_baseline, cv_retrospective, cv_baseline_adjust=Adjusted.for.outcome.at.baseline)]
bully_data_anx <- bully_data_anx[cv_retrospective==0, ]
bully_data_anx[is.na(age_start), age_start := (age_start_min + age_start_max) / 2]

##### back calculate required data #####
bully_data_anx[is.na(exposed_sample_size), exposed_sample_size:=exposed_cases+exposed_noncases]
bully_data_anx[is.na(nonexposed_sample_size), nonexposed_sample_size:=nonexposed_cases+nonexposed_noncases]
bully_data_anx[is.na(parameter_sample_size), parameter_sample_size:=exposed_sample_size+nonexposed_sample_size]
bully_data_anx[, `:=` (cases_total=exposed_cases+nonexposed_cases)]

# study specific adjustments
bully_data_anx[author=="Zwierzynska 2013", `:=` (parameter_sample_size=study_sample_size, cases_total = 37)]
bully_data_anx[author=="Schwartz 2015", `:=` (cases_total = 296*0.1224)]
bully_data_anx[author=="Wichstrøm 2013", `:=` (cases_total = 90)]

####### Combine Datasets #######
bully_data <- rbind(bully_data_dep[,`:=` (cv_dep=1, cv_anx=0)], bully_data_anx[,`:=` (cv_dep=0, cv_anx=1)])
bully_data[author=="Copeland  2013", cv_low_threshold_bullying:=0]
bully_data[author=="Rothon 2011", cv_low_threshold_bullying:=1]

########################
##### Estimate RRs #####
########################

bully_data[parameter == "OR", `:=` (rr = (exposed_cases/(exposed_cases+exposed_noncases))/(nonexposed_cases/(nonexposed_cases+nonexposed_noncases)),
                   rr_se = sqrt(1/exposed_cases + 1/nonexposed_cases - 1/exposed_sample_size - 1/nonexposed_sample_size))]
bully_data[parameter == "OR", `:=` (arr = or_2_rr(value, cases_total/parameter_sample_size, exposed_sample_size/parameter_sample_size),
                   arr_low = or_2_rr(lower, cases_total/parameter_sample_size, exposed_sample_size/parameter_sample_size),
                   arr_high = or_2_rr(upper, cases_total/parameter_sample_size, exposed_sample_size/parameter_sample_size)), by=c("author", "value")]

bully_data_rr <- copy(bully_data)
bully_data_rr[, arr_se := (log(arr_high) - log(arr_low)) / (qnorm(0.975,0, 1)*2)]

#If missing RR, make OR = RR
bully_data_rr[author=="Schoon 1997", `:=` (arr = value, arr_se = sd)]
bully_data_rr[cv_or == 1 & !is.na(arr), cv_or := 0]
bully_data_rr[is.na(arr), `:=` (missing_rr=1, arr=value)]
bully_data_rr[is.na(arr_se), arr_se := ifelse(is.na(sd), (log(upper) - log(lower))/(qnorm(0.975, 0, 1)*2), sd)]


## Prepare csv for MR-BRT
bully_data_rr[, `:=` (log_effect_size = log(arr), log_effect_size_se = arr_se)]

bully_data_rr <- bully_data_rr[,.(author, cohort, log_effect_size, log_effect_size_se, intercept = 1, time, cv_anx, cv_low_threshold_bullying,cv_symptoms, cv_baseline_adjust, cv_or, cv_adjusted)]
bully_data_rr[, `:=` (cv_baseline_adjust = ifelse(cv_baseline_adjust == "Y", 0, 1))]

## Estiamte attrition covariate
bully_data_rr[author == "Copeland  2013", attrition := 1273/1420] 
bully_data_rr[author == "Farrington 2011", attrition := 0.82]
bully_data_rr[author == "Fekkes 2006", attrition := 1118/1552]
bully_data_rr[author == "Gibb 2011", attrition := 0.78] 
bully_data_rr[author == "Moore 2014", attrition := (1590)/(0.649*2868)] 
bully_data_rr[author == "Zwierzynska 2013", attrition := 3692 / 10286] # Restricted analysis to completed cases and does not report attrition
bully_data_rr[author == "Takizawa 2014", attrition := 0.78] 
bully_data_rr[author == "Kaltiala-Heino 2010", attrition := 0.631]
bully_data_rr[author == "Rothon 2011", attrition := 1794 / 2734]
bully_data_rr[author == "Silberg 2016", attrition := 0.817]
bully_data_rr[author == "Sourander 2007", attrition := 2540/2946]
bully_data_rr[author == "Patton 2008", attrition := 0.978]
bully_data_rr[author == "Hemphill 2011" & time == 1, attrition := 0.85 / 0.97]
bully_data_rr[author == "Hemphill 2011" & time == 4, attrition := 0.85/ 0.89]
bully_data_rr[author == "Hemphill 2015" & time == 1, attrition := 680/ 825]
bully_data_rr[author == "Hemphill 2015" & time == 2, attrition := 651/ 805]
bully_data_rr[author == "Bowes 2015", attrition := 2668/6838]
bully_data_rr[author == "Kumpulainen 2000", attrition := 1157/1268]
bully_data_rr[author == "Fahy 2016", attrition := 2480 / 3213]
bully_data_rr[author == "Sigurdson 2015", attrition := 1266 / 2532]
bully_data_rr[author == "Vassallo 2014", attrition := 993 / 1359]
bully_data_rr[author == "Schoon 1997", attrition := 9005 / 14000]
bully_data_rr[author == "Lereya 2015b", attrition := 4101 / 5217]
bully_data_rr[author == "Lereya 2015a", attrition := 4101 / 5217]
bully_data_rr[author == "Wichstrøm 2013", attrition := (797-41) / 1000]
bully_data_rr[author == "Stapinski 2014", attrition := 3629 / 6208]
bully_data_rr[author == "Haavisto 2004", attrition := 2348 / 2946]
bully_data_rr[author == "Klomek 2008", attrition := 2348 / 2946]

bully_data_rr[attrition > 0.95, cv_selection_bias := 0]
bully_data_rr[is.na(cv_selection_bias) & attrition > 0.85, cv_selection_bias := 0]
bully_data_rr[is.na(cv_selection_bias), cv_selection_bias := 1]

bully_data_rr[is.na(cohort), cohort := author]
bully_data_rr[, `:=` (cv_male = 0, cv_female = 0)]
bully_data_rr[sex == "Male", cv_male := 1]
bully_data_rr[sex == "Female", cv_female := 1]
bully_data_rr <- bully_data_rr[,.(author, cohort, log_effect_size, log_effect_size_se, intercept = 1, time, cv_symptoms, cv_unadjusted, cv_b_parent_only, cv_or, cv_multi_reg, cv_cyberbullying, cv_low_threshold_bullying, cv_baseline_adjust, cv_anx, cv_selection_bias, cv_male, cv_female, cv_child_baseline, attrition, age_start)]

write.csv(bully_data_rr, "FILEPATH.csv", row.names=F)



