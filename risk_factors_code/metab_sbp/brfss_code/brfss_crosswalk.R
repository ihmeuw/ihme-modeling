################################
##author:USERNAME
##date: 1/27/17
##purpose:crosswalks brfss to nhanes, produces diagnostic plots
##notes:  -Disorganized system for testing different datasets and diagnostic criteria, 
##          -Can run on a brfss dataset with only collapsed self-reported disease (hypertension or hyperchol)...
##             -OR can run on a dataset with collapsed imputed probability of hypertensive status (imputation done by us counties team)
##
##
##
##
##          -Can look at NHANES with treatment in or out of diag criteria (rx T or F), or as self-report (rx=="self_report")
##              -If crosswalking imputed brfss, need to run with rx==F
##              -If crosswalking self-report, need to run with rx=="self_report"
##              -rx==T is just for looking at NHANES right now, haven't collapsed brfss to be crosswalked with that criteria
##
##
##                        -
##     ##THIS IS ONLY CW FOR GBD 2016 AS OF 03/24/2017
##     ##NECESSARY SCRIPT
#######################################



################### SETUP #########################################
######################################################


rm(list=ls())
os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH"
  j<- "J:/"
  h<-"H:/"
} else {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
}


library(ggplot2)
library(data.table)
library(haven)
library(wesanderson)
library(dplyr)
library(lme4)
library(matrixStats)
library(stringr)


################### PATHS AND ARGS #########################################
######################################################
##USERNAME:args
me<-"sbp" #"chl" or "sbp"


##USERNAME:here, can set diagnostic criteria by changing 'cutoff' and 'variant cutoff'
   ####  cutoff<-6.2
variant_cutoff<-90    ##variant_cutoff<-4.1 (for ldl)
rx<-F  ##USERNAME: T, F or self_report    self_rprt is misleading!! it's self-report of been diagnosed, NOT self report of hypertension!  Just using it as an rx option to simplify code
##USERNAME:if using imputed, rx needs to be 'F'
imputed<-T
##USERNAME:path to R central functions
central<-paste0(j, "FILEPATH")

##USERNAME:path to files with useful data
meta_path<-paste0(j, "FILEPATH")

##USERNAME:path with all collapsed microdata
micro_path<-paste0(j, "FILEPATH")



if(imputed==F){
##USERNAME:this is brfss is collapsed and only has self-reported disease status
brfss_in<-paste0(j, "FILEPATH")
}

if(imputed==T){
##USERNAME:this brfss is collapsed and has imputed values for diagnostic criteria  (imputation done by us counties team).  Need to change whether you read in imputed or self-report version down below (around line 610)
brfss_in<-paste0(j, "FILEPATH")
}



##USERNAME:path to diagnostic criteria, not using this anymore May reconsider using again at some point
daig_path<-paste0(j, "FILEPATH")


#outputs
cw_output<-paste0(j, "FILEPATH")
plot_output<-paste0(j, "FILEPATH")
plot_output<-paste0(j, "FILEPATH")

output<-paste0(j, "FILEPATH")





######################## GET LOC DATA ###################################
#################################################################  

if(F){

source(paste0(central, "get_location_metadata.R"))
locs<-get_location_metadata(version_id=149)
locs<-locs[, c("ihme_loc_id", "location_name","location_id", "region_id", "region_name", "super_region_id", "super_region_name", "level", "is_estimate"), with=F]

}



################### CONDITIONALS #########################################
######################################################

##USERNAME:here, can set diagnostic criteria by changing 'cutoff' and 'variant cutoff'

if(me=="sbp"){
  units<-"mmHg"
  variant<-"dbp"
  var_names<-c("diastolic", "systolic")
  plot_breaks<-seq(0,260, by=2)
  cutoff<-140
}
if(me=="chl"){
  units<-"mmol/L"
  variant<-"ldl"
  var_names<-c("ldl", "total_chl")
  plot_breaks<-seq(0, 15, by=0.2)
  cutoff<-6.2

}




################### GET METADATA AND NHANES #########################################
######################################################
meta<-fread(meta_path)
meta<-meta[iso3=="USA",]


micro<-fread(micro_path)
##USERNAME:limit micro to nhanes
nhanes<-c(48604, 52110, 49205, 47962, 47478, 25914, 48332, 110300, 165892)
micro<-micro[nid %in% nhanes,]

if(F){##USERNAME: only have 1 diagnostic criteria for NHANES
##USERNAME: read in diagnostic criteria specific to me, and create the 'dc_short' column
diag <- fread(daig_path)[dc_condition != ""  &  me_name==me,] 
diag <- diag[, dc_short := gsub(">|[|]", "_", dc_condition)] 
diag <- diag[, dc_short := gsub("rx==1", "rx", dc_short)]
diag <- diag[me_name == me, dc_condition := gsub("rx", paste0(me, "_rx"), dc_condition)] 
}



################### IF INTERACTIVE #########################################
######################################################
if(F){
  file<-meta$files[6]
    sex<-1
  
  
  
}

################### START LOOP #########################################
######################################################
#####################################################



prev_data<-NULL

pdf(file=plot_output)
for(file in meta$files){
  if(j=="J:/"){
    file<-gsub("FILEPATH", j, file)
  }

  
  
  ################### READ IN FILES AND CLEAN #########################################
  ######################################################
  
  df <- read_stata(file)
  
  ##USERNAME:check for variant(dbp or ldl)
  df<-as.data.table(df)

  ##USERNAME: make year_id for merging on with microdata later
  df[, year_id:=floor((year_start+year_end)/2)]
  
  
  ##USERNAME:restructure the age column
  ## Keep if age >= 25
  df <- df[age_year >=15,]
  
  df[, age_group_id:= floor((age_year+25)/5)]
  df[age_year>=80, age_group_id:=30]
  df[age_year>=85, age_group_id:=31]
  df[age_year>=90, age_group_id:=32]
  df[age_year>=95, age_group_id:=235]
  
  if(nrow(df[age_year>120,])>0){
    stop("Ages over 120, need to take a look")
    ##USERNAME: run this line by hand
  df<-df[!age_year>120,]
  }
  
  
  ##USERNAME: reset drug binary column names
  old <- c("hypertension_drug", "hyperchol_drug")
  new <- c("sbp_rx", "chl_rx")
  setnames(df, old, new)
  
  
  
  

  
  
  ######################## CHECK OUT DIAGNOSIS BREAKDOWN ###################################
  #################################################################
  ################################################################
  
  
  ######################## EXCLUDE RX IN DIAG CRITERIA ###################################
  #################################################################
if(rx==F){
  if(me=="sbp"){
  
    df[get(me)<cutoff  &  dbp<variant_cutoff, diseased:=0]
    
    
    
    
    ##USERNAME:hypertensive from sbp only
    df[get(me)>=cutoff  &  dbp<variant_cutoff,    diseased:=1 ]
    
    ##USERNAME: from dbp only
    df[dbp>=variant_cutoff   &  get(me)<cutoff,   diseased:=2 ]
    
    ##USERNAME:both dbp and sbp
    df[get(me)>=cutoff   &   dbp>=variant_cutoff,   diseased:=3 ]
    
    ##USERNAME: missing
    df[is.na(diseased)  |  is.nan(diseased), diseased:=8]
    
    
    df[, type:=factor(diseased, levels=c(8, 0, 2, 3, 1), labels=c("Missing", "Not hypertensive", "SBP+DBP", "DBP", "SBP"), ordered=T),]
    
  
  }
  if(me=="chl"){
    
    
    df[get(me)<cutoff, diseased:=0]
  
    
    ##USERNAME:hypertensive from sbp only
    df[get(me)>=cutoff,    diseased:=1 ]
    
    
    ##USERNAME: missing
    df[is.na(diseased)  |  is.nan(diseased), diseased:=8]
    
    
    df[, type:=factor(diseased, levels=c(8, 0, 1), labels=c( "Missing", "Not hyperchol", "Total Chl"), ordered=T),]
    
    
    
  }
}
  
  
  
  ######################## INCLUDE RX IN DIAG CRITERIA ###################################
  #################################################################
  
if(rx==T){
  if(me=="sbp"){
    
    #diseased<-df[get(me)>=me_cutoff  |  dbp>=90  |  hypertension_drug==1,]
    
    
    ##USERNAME:set hypertension categories
    ##USERNAME:not hypertensive 
    df[get(me)<cutoff  &  dbp<variant_cutoff  &  (sbp_rx==0  |  is.nan(sbp_rx)), diseased:=0]
    
  
    
    
    ##USERNAME:hypertensive from sbp only
    df[get(me)>=cutoff  &  dbp<variant_cutoff  &  (sbp_rx==0  |  is.na(sbp_rx)),    diseased:=1 ]
    
    
    
    ##USERNAME:hypertensive from dbp only
    df[dbp>=variant_cutoff   &  get(me)<cutoff  &  (sbp_rx!=1  |  is.na(sbp_rx)),   diseased:=2 ]
    
    ##USERNAME:hypertensive from drug only
    df[sbp_rx==1   &   get(me)<cutoff  &  dbp<variant_cutoff,   diseased:=3 ]
    
    ##USERNAME: combos
    df[get(me)>=cutoff   &   dbp>=variant_cutoff  &  (sbp_rx!=1  |  is.na(sbp_rx)),   diseased:=4 ]  ##USERNAME; sbp and dbp
    df[get(me)>=cutoff   &   sbp_rx==1   &  dbp<variant_cutoff,   diseased:=5 ]  ##USERNAME: sbp and drug
    df[dbp>=variant_cutoff   &   sbp_rx==1  &  get(me)<cutoff,    diseased:=6 ]             ##USERNAME: dbp and drug
    df[get(me)>=cutoff  &  dbp>=variant_cutoff  &  sbp_rx==1,     diseased:=7 ]            ##USERNAME: all three
    df[is.na(diseased)  |  is.nan(diseased), diseased:=8]
    
    
    ##USERNAME:this is for graphing
    df[, type:=factor(diseased, levels=c(8, 0, 3 , 6 , 2, 7, 5, 4, 1), labels=c("Missing", "Not hypertensive", "Meds",  "DBP+Meds", "DBP", "SBP+DBP+Meds", "SBP+Meds", "SBP+DBP", "SBP"), ordered=T),]
    
  }
  
  
  if(me=="chl"){
    
    df[get(me)<cutoff  &  (chl_rx==0  |  is.nan(chl_rx)),   diseased:=0]  ##USERNAME: not hyperchol
    df[get(me)>=cutoff   &    (chl_rx==0  |  is.nan(chl_rx)),                        diseased:=1]  ##USERNAME: hyperchol from total chl only
    df[get(me)<cutoff   &  chl_rx==1,                        diseased:=2]                          ##USERNAME: hyperchol from meds only
    df[get(me)>=cutoff   &  chl_rx==1,                        diseased:=3]                         ##USERNAME: hyperchol from total chl and meds
    df[is.na(diseased)  |  is.nan(diseased), diseased:=8]
    
    ##USERNAME:this is for graphing
    df[, type:=factor(diseased, levels=c(8, 0, 2 ,3 , 1), labels=c( "Missing", "Not hyperchol", "Meds", "Total Chl+Meds", "Total Chl"), ordered=T),]
    
    
  }
}
  
  
  ######################## GET PREV BASED ON SELF REPORT ###################################
  #################################################################
  
  if(rx=="self_report"){
    if(me=="sbp"){
      
      df[hypertension==0, diseased:=0]
      df[hypertension==1, diseased:=1]
      df[is.na(diseased)  |  is.nan(diseased), diseased:=8]
      
      
      
    }
    
    if(me=="chl"){
      
      df[hyperchol==0, diseased:=0]
      df[hyperchol==1, diseased:=1]
      df[is.na(diseased)  |  is.nan(diseased), diseased:=8]
      
    }
    
    df[, type:=factor(diseased, levels=c(8, 0, 1), labels=c( "Missing", "No diagnosis", "Self-reported diagnosis"), ordered=T),]
    
  }
  
  
  
  
  
  df[diseased!=0, status:="Diseased"]
  df[diseased==0, status:="Not diseased"]
  df[diseased==8, status:="Missing"]
  
  
  ################### PLOT DIAGNOSTIC BREAKDOWN FOR EACH FILE #########################################
  ##############################################################
  

  p<-ggplot(data=df, aes(x=factor(status, levels=c("Not diseased", "Diseased", "Missing"))))+
    geom_bar(aes(y=(..count..)/sum(..count..), fill=factor(type)))+
    scale_x_discrete(breaks=c("Not diseased", "Diseased", "Missing"))+
    scale_y_continuous(labels = scales::percent)+
    ggtitle(paste(me, "diagnostic breakdown in NHANES:", unique(df$year_start)))+
    xlab(NULL)+
    ylab("Percent")+
    #ylim(0, 1)+
    guides(fill=guide_legend(title="Category"))+
    scale_fill_brewer(palette = "Paired")+
    theme_classic()
  print(p)
 # dev.off()

  
  ################### CALCULATE PREVS #########################################
  ######################################################
  ##USERNAME:calculating prevs based on non-missing stuff, different than 'status' which visualizes missings
  ## Set rx to 0 if measured but no response
  df <- df[!is.na(get(me)) & is.na(get(paste0(me, "_rx"))), (paste0(me, "_rx")) := 0]
  
  ## Create binary if condition is met in the df  ##USERNAME: old system was more flexible, I should revert back to it eventually
  #for (i in 1:nrow(diag)) df[, (diag$dc_short[i]) := as.numeric(eval(parse(text=diag$dc_condition[i])))]
  
  ##USERNAME: only keep rows with non-missing values
  print(paste("# of missing", me, ":", nrow(df[is.na(get(me)),])))
  
  ##USERNAME: this creates a binary based on diagnostic criteria settings  
  if(rx==T){
    if(me=="sbp"){
      df[sbp>cutoff  | dbp>variant_cutoff  |  sbp_rx==1   , hyper:=1]
      df[is.na(hyper), hyper:=0]
    }
    
    if(me=="chl"){   ##USERNAME: don't  usually see ldl used in chl diagnostic criteria, so not writing it in for now
      df[chl>cutoff  | chl_rx==1   , hyper:=1]
      df[is.na(hyper), hyper:=0]
    }
  }
  
  
  
  ##USERNAME: take out rx of diagnostic criteria
   if(rx==F){
    if(me=="sbp"){
      df[sbp>cutoff  | dbp>variant_cutoff   , hyper:=1]
      df[is.na(hyper), hyper:=0]
    }
    
    if(me=="chl"){
      df[chl>cutoff , hyper:=1]
      df[is.na(hyper), hyper:=0]
    }
   }
  
  
  
  ##USERNAME: self-reported diagnosis is only criteria
  if(rx=="self_report"){
    if(me=="sbp"){
      
      df[hypertension==1, hyper:=1]
      df[is.na(hyper), hyper:=0]
    
      
    }
    
    if(me=="chl"){
      
      df[hyperchol==1, hyper:=1]
      df[is.na(hyper), hyper:=0]
      
    }
    
  }
  
  
  
  
  
  
  
  ######################## GET PREVALENCE ###################################
  #################################################################
  
  ##USERNAME:collapse the hyper col (get prev)
  prevs<-df[, mean(hyper), by=c("ihme_loc_id", "year_id", "age_group_id", "sex_id")]
  setnames(prevs, "V1", "prev")
  
  
  ##USERNAME:calculate sample size to bind back on
  df<-df[, count:=1]
  sample_sizes<-df[, sum(count, na.rm=T), by=c("age_group_id", "sex_id", "ihme_loc_id", "year_id")]
  setnames(sample_sizes, "V1", "sample_size_prev")
  
  
  prevs<-merge(prevs, sample_sizes, by=c("ihme_loc_id", "year_id", "age_group_id", "sex_id"))
  prevs[, age_group_id:=as.integer(age_group_id)]

    ##USERNAME:merge w/microdata
  full<-merge(prevs, micro, by=c("ihme_loc_id", "year_id", "age_group_id", "sex_id"), all.x=T)
  full<-unique(full)
    
    prev_data <- rbind(prev_data, full)
    
    
    
    
    
    ################### SCATTER MEAN VS PREV FOR EACH FILE #########################################
    ######################################################
    
    p<-ggplot(data=full, aes(x=prev, y=mean, color=factor(age_group_id)))+
      geom_point()+
      facet_wrap(~sex_id)+
      ggtitle(paste0("Prev v Mean in NHANES:", unique(full$year_id)), paste("by sex, rx=", rx))+
      scale_color_discrete(name="Age group id")+
      theme_classic()
    print(p)
    
    
    
    

    
    
    
    
    
    
    
    
  
} ##USERNAME: end loop#################################################









######################## SCATTER ACROSS ALL NHANES ###################################
#################################################################


p<-ggplot(data=prev_data, aes(x=prev, y=mean, color=factor(age_group_id)))+
  geom_point()+
  facet_wrap(~sex_id)+
  ggtitle(paste0("Prev v mean across all NHANES"), paste("by sex, rx=", rx))+
  scale_color_discrete(name="Age group id")+
  theme_classic()
print(p)





######################## CROSSWALK ###################################
#################################################################

##USERNAME: run model by sex
for(sex in unique(prev_data$sex_id)){
  
#  sex<-1

  
x<-prev_data[sex_id==sex,]


##USERNAME:create a weight based on sample size to be used in model, not sure if this is a good idea. makes rmse go up slightly
x[, wt:=sample_size/sum(sample_size)]

form<-paste("mean ~ prev")




mod<-lm(form, data=x, weights=1/x$standard_error)   #, weights=wt)



prediction<-predict(mod, interval="confidence", level=0.95)

x<-cbind(x, prediction)

##USERNAME:calculate rmse
x[, sqr_err:=(mean-fit)^2]

rmse<-round(sqrt(mean(x$sqr_err)), digits=2)



######################## OOS.RMSE FUNCTION ###################################
#################################################################



## RMSE Function  ##USERNAME: randomly selects holdouts
run.oos.rmse <- function(df, prop_train, model, reps) {
  
  lapply(1:reps, function(x) {  
    ## Split
    set.seed(80)  
    train_index <- sample(seq_len(nrow(df)), size = floor(prop_train * nrow(df)))
    
    
    train <- df[train_index]  
    test <- df[-train_index]
    ## Model
    test_mod <- lm(as.formula(model), data=train)  ##USERNAME: changed lmer to lm
    ## Predict
    prediction <- predict(test_mod, newdata=test, allow.new.levels=TRUE) #%>% exp  
    ## Detect variable of interest by parsing on "~"
    var <- strsplit(model, "~")[[1]][1] %>% str_trim  ##USERNAME: pulls out name of measure type from the formula stored in the object 'model'
    ## Strip the log
    #var <- strsplit(var, "log[(]|[)]")[[1]][2]        ##USERNAME: pulls the measure type out of 'log' function
    ## RMSE
    rmse <- sqrt(mean((prediction-test[[var]])^2, na.rm=T))  ##USERNAME: calculate rmse: root of the mean of the square of the difference between the predicted value and the observed value
    return(rmse)
  }) %>% unlist %>% mean
  
}




oos.rmse<-round(run.oos.rmse(df=x[!is.na(mean)], prop_train=0.8, model=form, reps=10), digits=2)








######################## PLOT LM ###################################
#################################################################

p<-ggplot(data=x, aes(x=prev, y=mean))+
  geom_point(aes(color=factor(age_group_id)))+
  geom_line(aes(x=prev, y=fit))+
  geom_ribbon(aes(x=prev, ymin=lwr, ymax=upr), fill="blue", alpha=0.1)+
  ggtitle(paste(me, "model result for sex id:", sex, "\ni.RMSE:", rmse, units, "\no.RMSE:", oos.rmse, units))+
  scale_color_discrete(name="Age group id")+
  guides(alpha=F)+
  theme_classic()
print(p)




p<-ggplot(data=x, aes(x=prev, y=(mean-fit)))+
  geom_point(aes(color=factor(age_group_id)))+
  ggtitle(paste("Residuals for sex id:", sex, "\ni.RMSE:", rmse, units, "\no.RMSE:", oos.rmse, units))+
  scale_color_discrete(name="Age group id")+
  guides(alpha=F)+
  theme_classic()
print(p)

}

######################## BRING IN BRFSS AND PREDICT ###################################
#################################################################

brfss<-read.csv(brfss_in)
brfss<-as.data.table(brfss)
setnames(brfss, "mean", "prev")

##USERNAME: start prediction loop
full<-list()
for(sex in unique(brfss$sex_id)){
  x<-brfss[sex_id==sex,]
  #pred<-exp(predict.lm(mod, x, interval="prediction", level=0.95, weights=1/(x$standard_error)))  ##USERNAME: if model is in log space
  pred<-predict.lm(mod, x, interval="prediction", level=0.95, weights=1/(x$standard_error))
  
  pred<-as.data.table(pred)
  
  x<-cbind(x, pred)
  
  
  
  
  ######################## PLOT PREDICTION ###################################
  #################################################################
  
  ##USERNAME:this really only givesa sense of CI
  p<-ggplot(data=x, aes(x=prev, y=fit))+
    geom_point(aes(color=as.factor(age_group_id)))+
    geom_ribbon(aes(ymin=lwr, ymax=upr), alpha=0.2)+
    ggtitle("Predicted vals")+
    theme_minimal()
  print(p)
  
  ##USERNAME:histograms in preds
  p<-ggplot(data=x, aes(x=fit))+
    geom_histogram(fill="black")+
    facet_wrap(~age_group_id)+
    ggtitle(paste0("Distribution of predicted means by age grp in sex id:", sex))+
    theme_classic()
  print(p)
  
  ##USERNAME: histograms in nhanes(training dataset)
  p<-ggplot(data=prev_data[sex_id==sex,], aes(x=mean))+
    geom_histogram(fill="black")+
    facet_wrap(~age_group_id)+
    ggtitle(paste0("Distribution of NHANES means by age grp in sex id:", sex))+
    theme_classic()
  print(p)
  
  
  
  full[[sex]]<-x
}



dev.off()


full<-rbindlist(full)

##USERNAME: recalculate se, not sur eif this equation is right
full[, standard_error:=(upr-lwr)/3.92]
full[, me_name:=NULL]  ##USERNAME:need to do this because it was a factor
full[, me_name:=me]


setnames(full, "fit", "mean")

full<-full[, .(nid, ihme_loc_id, age_group_id, sex_id, year_id, me_name, mean, standard_error, age_start, age_end, sample_size)]


write.csv(full, file=output, row.names=F)







######################## JUNKIN! ###################################
################################################################# 


if(F){
###USERNAME: check out these histograms in order to see what's getting crosswalked here



hist(prev_data.t$sbp, breaks="FD")
hist(prev_data.t$sbp_140_dbp_90_rx, breaks="FD")
hist(prev_data.t$sbp_140_dbp_90, breaks="FD")



}