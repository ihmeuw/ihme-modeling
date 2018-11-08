# VL_stgpr_dataprep

rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}

##	Load shared functions
source(sprintf("FILEPATH/get_location_metadata.R",prefix))
source(sprintf("FILEPATH/get_epi_data.R",prefix))
source(sprintf("FILEPATH/get_demographics.R",prefix))
source(sprintf("FILEPATH/get_population.R",prefix))

#load in relevant data, vl bundle id = 53
leish_data<-get_epi_data(53)

#AIM IS TO GET LOCATION SPECIFIC CASE COUNTS
#drop redundant rows - Cross-sectional surveys
leish_trim<-subset(leish_data,
                   leish_data$source_type!="Survey - cross-sectional")

#drop Pigott et al. citation [is geographic restrictions]
leish_trim<-subset(leish_trim,
                   leish_trim$nid!=281876
)

#drop prevalence data - incidence only
leish_trim<-subset(leish_trim,
                   leish_trim$measure!='prevalence')

#make aged_end 100 = 99 to match standardization of GBD all-age category of 0-99
for(i in 1:nrow(leish_trim)){
  if(leish_trim$age_end[i] == 100){
    leish_trim$age_end[i]<-99
  }
}

#make age_end 120 = 99 for Brazil subnationals
for(i in 1:nrow(leish_trim)){
  if(leish_trim$age_end[i] == 120){
    leish_trim$age_end[i]<-99
  }
}


#extract the all age records - i.e. 0 to 99 years
AllAge<-which(leish_trim$age_start == 0 & leish_trim$age_end==99)
leishAA<-leish_trim[AllAge,]


if(is.null(nrow(AllAge))){
  leishNAA<-leish_trim
}else{
  leishNAA<-leish_trim[-AllAge,]
}


#check for partitioning age bins, re-aggreagate them and drop everything else
Keep <- {}
ULoc <- unique(leishNAA$location_id)
UYears <- unique(leishNAA$year_start[leishNAA$year_start==leishNAA$year_end])

for (tmploc in ULoc){
  for (tmpyear in UYears){
    tmprows <- which(leishNAA$location_id == tmploc & leishNAA$year_start == tmpyear)
    UGender <- unique(leishNAA$sex[tmprows])
    if (length(tmprows)){
      for (tmpGender in UGender){
        tmpnewrows <- which(leishNAA$location_id == tmploc & leishNAA$year_start == tmpyear & leishNAA$sex == tmpGender)
        if (length(tmpnewrows) > 1){
          tmpAges <- as.vector(sapply(tmpnewrows,function(x)c(leishNAA$age_start[x],leishNAA$age_end[x])))
          tmpAges<-sort(tmpAges)
          tmpMaxGap <- max(diff(tmpAges)[seq(2,length(tmpAges)-1,by=2)])
          if (tmpAges[1] == 0 & tmpAges[length(tmpAges)] == 99 & tmpMaxGap <= 1){
            Keep <- c(Keep, tmpnewrows[1])
            leishNAA$age_start[tmpnewrows[1]] <- 0
            leishNAA$age_end[tmpnewrows[1]] <- 99
            leishNAA$cases[tmpnewrows[1]] <- sum(leishNAA$cases[tmpnewrows])
            leishNAA$sample_size[tmpnewrows[1]] <- sum(leishNAA$sample_size[tmpnewrows])
            leishNAA$mean[tmpnewrows[1]] <- leishNAA$cases[tmpnewrows[1]] / leishNAA$sample_size[tmpnewrows[1]]
            leishNAA$lower[tmpnewrows[1]] <- NA
            leishNAA$upper[tmpnewrows[1]] <- NA
            leishNAA$standard_error[tmpnewrows[1]] <- NA
            leishNAA$effective_sample_size[tmpnewrows[1]] <- NA
          }
        }
      }
    }
  }
}

leishAA <- rbind(leishAA, leishNAA[Keep,])

# Pull just those that are all age and all sex combined

AllAgeSex <- which(leishAA$sex == "Both")
leishAAS <- leishAA[AllAgeSex,]

## Where we have all-age, sex-sepcific data points for a location/year, combine them to be both sex and drop all other points

leishNAAS <- leishAA[-AllAgeSex,]

HasBoth <- {}
UYears <- unique(leishNAAS$year_start[leishNAAS$year_start == leishNAAS$year_end])
ULoc <- unique(leishNAAS$location_id)

for (tmploc in ULoc){
  for (tmpyear in UYears){
    tmpmatch <- which(leishNAAS$year_start == tmpyear & leishNAAS$location_id == tmploc)
    if (length(tmpmatch)){
      tmpmale <- which(leishNAAS$sex[tmpmatch] == "Male")
      tmpfemale <- which(leishNAAS$sex[tmpmatch] == "Female")
      if (length(tmpmale) == 1 & length(tmpfemale) == 1){
        HasBoth <- c(HasBoth,tmpmatch[1])
        leishNAAS$sex[tmpmatch[1]] <- "Both"
        leishNAAS$cases[tmpmatch[1]] <- sum(leishNAAS$cases[tmpmatch])
        leishNAAS$sample_size[tmpmatch[1]] <- sum(leishNAAS$sample_size[tmpmatch])
        leishNAAS$mean[tmpmatch[1]] <- leishNAAS$cases[tmpmatch[1]] / leishNAAS$sample_size[tmpmatch[1]]
        leishNAAS$lower[tmpmatch[1]] <- NA
        leishNAAS$upper[tmpmatch[1]] <- NA
        leishNAAS$standard_error[tmpmatch[1]] <- NA
        leishNAAS$effective_sample_size[tmpmatch[1]] <- NA
      }
    }
  }
}

leishAAS <- rbind(leishAAS,leishNAAS[HasBoth,])

#subset to post 1980 data
leishAAS<-subset(leishAAS, leishAAS$year_start>1979)
#remove the zeroes
leishAAS<-subset(leishAAS, leishAAS$cases>0)

write.csv(leishAAS,
          "FILEPATH/collpase_3Mar.csv")

####################################
# incorporate underreporting scalars
####################################

#load in a 100/0 underreporting model set

load(sprintf("FILEPATH/full_model_90split10_27Feb2017.RData",prefix))

#for each record in leishAAS, need to calculate the underreported fraction and scale up accordingly

#load in get covariates function and necessary covariates
source(sprintf("FILEPATH/get_covariate_estimates.R",prefix))
ilogit <- function(x)1/(1+exp(-x))
haqi <- get_covariate_estimates(1099)
sdi <- get_covariate_estimates(881)
leish_presence <- get_covariate_estimates(211)
leish_endemic <- get_covariate_estimates(213)

#set number of repeats
n_reps<-1000

draw_string<-seq(1,n_reps,1)
draw_names<-paste0("draw_", draw_string)
draw_df<-data.frame(matrix(nrow=nrow(leishAAS), ncol=length(draw_string)))

#introduce a floor value
floor_threshold<-0.1

predicted_draws<-draw_df
predicted_cases<-draw_df
for (j in 1:nrow(leishAAS)){
  LocToPull<-leishAAS$location_id[j]
  pred_year<-leishAAS$year_start[j]
  pred_path<-as.factor('vl')
  pred_haqi<-haqi[location_id==LocToPull & year_id==pred_year]$mean_value
  pred_sdi<-sdi[location_id==LocToPull & year_id==pred_year]$mean_value
  pred_leish_e<-leish_endemic[location_id==LocToPull & year_id==pred_year]$mean_value

  for (k in 1:n_reps){
    pred<-predict(mod[[k]], data.frame(year=pred_year, pathogen=pred_path, sdi=pred_sdi), se=TRUE)

    # if(ilogit(pred$fit)>=floor_threshold){
    #   predicted_draws[j,k]<-ilogit(pred$fit)
    #   predicted_cases[j,k]<-leishAAS$cases[j]/ilogit(pred$fit)
    # }else{
    #   predicted_draws[j,k]<-floor_threshold
    #   predicted_cases[j,k]<-leishAAS$cases[j]/floor_threshold
    # }
    predicted_draws[j,k]<-ilogit(rnorm(1,
                                      mean = pred$fit,
                                      sd = (1.96*pred$se.fit)))
    predicted_cases[j,k]<-leishAAS$cases[j]/predicted_draws[j,k]
  }
  print(paste0("Completed ",j, " of ", nrow(leishAAS), ' draws'))
}

#need to translate into incidence - ideally update with most recent demographics team estimates hence this step RATHER than use the imputed cases done in previous cycles
demographics_denominators<-NA
for (k in 1:nrow(leishAAS)){
  demographics<-get_population(age_group_id=22,
                               location_id=leishAAS$location_id[k],
                               year_id=leishAAS$year_start[k]
  )
  demographics_denominators[k]<-demographics$population
  print(paste0("Pulling ", k, " of ", nrow(leishAAS)))
}

demographics_denominators<-data.frame(demographics_denominators)
demographics_denominators<-t(demographics_denominators)

#calculated incidence for each of the predicted_cases draws
predicted_incidence<-predicted_cases/demographics_denominators

#calculate the mean and the variance of the draws in terms of cases and incidence
summary_incidence<-data.frame(mean=rep(NA, nrow(predicted_incidence)),
                              variance=rep(NA, nrow(predicted_incidence)))
summary_cases<-data.frame(mean=rep(NA, nrow(predicted_incidence)),
                          variance=rep(NA, nrow(predicted_incidence)))

for (s in 1:nrow(summary_incidence)){
  summary_incidence$mean[s]<-mean(t(predicted_incidence)[,s])
  summary_incidence$variance[s]<-var(t(predicted_incidence)[,s])
}
for (s in 1:nrow(summary_cases)){
  summary_cases$mean[s]<-mean(t(predicted_cases)[,s])
  summary_cases$variance[s]<-var(t(predicted_cases)[,s])
}

#produce an output for st-gpr
#currently have 1,000 as sample_size.
st_gpr_input<-data.frame(me_name=rep("ntd_vl", nrow(summary_incidence)),
                         location_id=leishAAS$location_id,
                         nid=leishAAS$nid,
                         year_id=leishAAS$year_start,
                         age_group_id=rep(22, nrow(leishAAS)),
                         sex_id=rep(3, nrow(leishAAS)),
                         data=summary_incidence$mean,
                         variance=summary_incidence$variance,
                         sample_size=rep(1000, nrow(leishAAS)))

case_count_output<-data.frame(me_name=rep("case_count", nrow(summary_incidence)),
                              location_id=leishAAS$location_id,
                              nid=leishAAS$nid,
                              year_id=leishAAS$year_start,
                              age_group_id=rep(22, nrow(leishAAS)),
                              sex_id=rep(3, nrow(leishAAS)),
                              data=summary_cases$mean,
                              variance=summary_cases$variance,
                              sample_size=rep(1000, nrow(leishAAS)),
                              raw_cases=leishAAS$cases)

#SAVE OUTPUTS as an RDATA
save(st_gpr_input, case_count_output,
     file=sprintf("FILEPATH/st_gpr_inputs_3Mar.RData", prefix))


     #SAVE OUTPUTS FOR ST_GPR
     write.csv(st_gpr_input,
               file=sprintf("FILEPATH/st_gpr_inputs_3Mar.csv", prefix))
