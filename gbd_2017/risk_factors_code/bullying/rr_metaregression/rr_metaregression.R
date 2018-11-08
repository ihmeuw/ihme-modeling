library(metafor)
library(data.table)
library(openxlsx)
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

bully_data_dep <- as.data.table(read.xlsx("FILEPATH", sheet="Depressive disorders extraction"))
bully_data_dep <- bully_data_dep[`Included.in.the.OR.meta-analysis`=="Y",
                         .(author=paste(First.Author, Publication.year), Adjusted,
                         sex=Sex, age_start=Mean.age.at.intake, age_end=Mean.age.at.follow.up, time=Mean.years.follow.up,
                         parameter=Parameter.type, value=Parameter.value, lower=Lower.UI, upper=Upper.UI, sd=Standard.deviation,
                         p=p.value, exposed_cases=Exposed.cases, exposed_noncases=`Exposed.non-cases`, nonexposed_cases=`Non-exposed.cases`,
                         nonexposed_noncases=`Non-exposed.non-cases`, exposed_sample_size=`Sample.size.(exposed)`,
                         nonexposed_sample_size=`Sample.size.(non-exposed)`, parameter_sample_size=`Sample.size.(exposed.+.non-exposed)`,
                         study_sample_size=`Study.sample.size.(total.N)`, cv_symptoms, cv_low_threshold_bullying, cv_child_baseline, cv_retrospective, cv_baseline_adjust=Adjusted.for.outcome.at.baseline)]
bully_data_dep <- bully_data_dep[cv_retrospective==0, ]

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
bully_data_anx <- as.data.table(read.xlsx("FILEPATH", sheet="Anxiety disorders extraction"))
bully_data_anx <- bully_data_anx[`Included.in.the.OR.meta-analysis`=="Y",
                                                                      .(author=paste(First.Author, Publication.year), Adjusted,
                                                                     sex=Sex, age_start=Mean.age.at.intake, age_end=Mean.age.at.follow.up, time=Mean.years.follow.up,
                                                                     parameter=Parameter.type, value=Parameter.value, lower=Lower.UI, upper=Upper.UI, sd=Standard.deviation,
                                                                     p=p.value, exposed_cases=Exposed.cases, exposed_noncases=`Exposed.non-cases`, nonexposed_cases=`Non-exposed.cases`,
                                                                     nonexposed_noncases=`Non-exposed.non-cases`, exposed_sample_size=`Sample.size.(exposed)`,
                                                                     nonexposed_sample_size=`Sample.size.(non-exposed)`, parameter_sample_size=`Sample.size.(exposed.+.non-exposed)`,
                                                                     study_sample_size=`Study.sample.size.(total.N)`, cv_symptoms, cv_low_threshold_bullying, cv_child_baseline, cv_retrospective, cv_baseline_adjust=Adjusted.for.outcome.at.baseline)]
bully_data_anx <- bully_data_anx[cv_retrospective==0, ]

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

########################
##### Estimate RRs #####
########################

bully_data[, `:=` (rr = (exposed_cases/(exposed_cases+exposed_noncases))/(nonexposed_cases/(nonexposed_cases+nonexposed_noncases)),
                   rr_se = sqrt(1/exposed_cases + 1/nonexposed_cases - 1/exposed_sample_size - 1/nonexposed_sample_size))]
bully_data[, `:=` (arr = or_2_rr(value, cases_total/parameter_sample_size, exposed_sample_size/parameter_sample_size),
                   arr_low = or_2_rr(lower, cases_total/parameter_sample_size, exposed_sample_size/parameter_sample_size),
                   arr_high = or_2_rr(upper, cases_total/parameter_sample_size, exposed_sample_size/parameter_sample_size)), by=c("author", "value")]

bully_data_rr <- copy(bully_data)
bully_data_rr[, arr_se := (log(arr_high) - log(arr_low)) / (qnorm(0.975,0, 1)*2)]

#If missing RR, make OR = RR
bully_data_rr[author=="Schoon 1997", `:=` (arr = value, arr_se = sd)]
bully_data_rr[is.na(arr), `:=` (missing_rr=1, arr=value)]
bully_data_rr[is.na(arr_se), arr_se := ifelse(is.na(sd), (log(upper) - log(lower))/(qnorm(0.975, 0, 1)*2), sd)]

########################################
##### Pooled overall meta-analysis #####
########################################
bully_data_rr[, `:=` (yi = log(arr), vi = arr_se^2)]
meta_rr_overall <- rma(measure="RR", yi=yi, vi=vi, slab=author, data=bully_data_rr, method="DL")
predict(meta_rr_overall, digits=6, transf=transf.exp.int)
meta_rr_bydisorder <- rma(measure="RR", yi=yi, vi=vi, slab=author, data=bully_data_rr, mods=~cv_anx, method="DL")
predict(meta_rr_bydisorder, newmods=rbind(0, 1), digits=6, transf=transf.exp.int)

########################################################
##### Pooled post-bullying-threshold meta-analysis #####
########################################################

## Apply low bullying threshold crosswalks
bully_data_rr[cv_anx == 0 & cv_low_threshold_bullying==1, `:=` (arr = arr*1.469779, arr_se = sqrt((arr_se^2)*(0.0966926^2) + (arr_se^2)*(1.469779^2) + (0.0966926^2)*(arr^2)))]
bully_data_rr[cv_anx == 1 & cv_low_threshold_bullying==1, `:=` (arr = arr*1.310574, arr_se = sqrt((arr_se^2)*(0.06983203^2) + (arr_se^2)*(1.310574^2) + (0.06983203^2)*(arr^2)))]

##### meta-analysis #####
# yi = log(rr)
# vi = rr_se^2
bully_data_rr[, `:=` (yi = log(arr), vi = arr_se^2)]

meta_rr_overall <- rma(measure="RR", yi=yi, vi=vi, slab=author, data=bully_data_rr, method="DL")
predict(meta_rr_overall, digits=6, transf=transf.exp.int)
meta_rr_bydisorder <- rma(measure="RR", yi=yi, vi=vi, slab=author, data=bully_data_rr, mods=~cv_anx, method="DL")
predict(meta_rr_bydisorder, newmods=rbind(0, 1), digits=6, transf=transf.exp.int)

############################################################
##### Meta-regression with follow-up time as moderator #####
############################################################

meta_rr <- rma(measure="RR", yi=yi, vi=vi, slab=author, subset=(time<25 & author!="Copeland  2013") , data=bully_data_rr, method="DL", mods=~time)
meta_rr <- rma(measure="RR", yi=yi, vi=vi, slab=author, subset=(time<25 & author!="Copeland  2013") , data=bully_data_rr, method="DL", mods=~time+cv_symptoms+cv_baseline_adjust)
predicted <- predict(meta_rr, newmods=seq_c(c(0, 0, 0), c(100, 0, 0)), digits=6, transf=transf.exp.int)
reg_results <- data.table(time=c(0:100), rr=predicted$pred, lower=predicted$ci.lb, upper=predicted$ci.ub)
reg_results <- reg_results[rr>1,]

write.csv(reg_results, "FILEPATH", row.names=F)

## logged RRs
predicted_log <- predict(meta_rr, newmods=seq_c(c(0, 0, 0), c(100, 0, 0)), digits=6)
reg_results_log <- data.table(time=c(0:100), rr=predicted_log$pred, lower=predicted_log$ci.lb, upper=predicted_log$ci.ub)
reg_results_log <- reg_results_log[exp(rr)>1,]

write.csv(reg_results_log, "FILEPATH", row.names=F)
