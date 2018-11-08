###########################################################
### Author: USERNAME
### Date: 4/5/2017
### Purpose: Create adjustment factors for regression to the mean for SBP
### Notes: -Currently re-creating analysis as outlined in JAMA bp appendix-redoing w/ newly extracted longitudinal datasets
## updating to compare to a fixed effects method
###########################################################



rm(list=ls())
os <- .Platform$OS.type

date<-gsub("-", "_", Sys.Date())

library(data.table)
library(lme4)
library(ggplot2)
library(arm)
library(haven)
library(readstata13)
library(utils)


################### ARGS AND PATHS #########################################
######################################################
me<-"sbp"

long_folder<-paste0("FILEPATH")

ave_meas<-F  ##USERNAME: average over measurement @ each time point?


old_ratios_path<-paste0("FILEPATH.dta")

output<-paste0("FILEPATH/sual_bp_adj.pdf")

path<-paste0("FILEPATH")



################### GET OLD RATIOS #########################################
######################################################
old_ratios<-read_stata(old_ratios_path)
old_ratios<-as.data.table(old_ratios)

##USERNAME: only keep one of oldest age groups (they were all copy-pasted)
old_ratios<-old_ratios[age_group_id %in% c(10:30)]

old_ratios[age_group_id==30, age_group_id:=21]







################### COMPILE DATA #########################################
######################################################
################################################


files<-list.files(long_folder, full.names=F)


##USERNAME: this is for matching studies up, so that they can be formatted together
files<-data.table(files, id=unlist(lapply(strsplit(files, "_"), "[", 1)))






meta<-list()
full<-list()
for(i in 1:length(unique(files$id))){
  if(F){
    i<-3
  }
  
  study<-unique(files$id)[i]
  message(paste("Getting", study))
  
    ##USERNAME: check class of line_id and convert to numeric
    study_dt<-list()
    for(u in 1:length(unique(files[id==study, files]))){
      if(F){
        u<-2
      }
      
      x<-unique(files[id==study, files])[u]
      
      tbl<-read_stata(paste0(long_folder, x))
      tbl<-as.data.table(tbl)
      
      
      if("chl" %in% names(tbl)){
        message(x, "has chl")
      }
      ##USERNAME: for looking at where line_id will get coerced to NA

      ##USERNAME: reformat name for nhanes followup
      if(i==6 & u==2){ tbl[, survey_name:="USA/NHANES"]}
      
      
      if(x %in% c("CHRLS_2012_resample.dta", "IDN_FLS_2000.dta", "IDN_FLS_2007.dta", "USA_NHANES_1982_84.dta")){
        message(paste("For survey", x, ", line_id is", class(tbl$line_id), ", attempting to convert to numeric--"))
        
        pre<-nrow(tbl[is.na(line_id)])
        tbl$line_id<-as.numeric(tbl$line_id)
        post<-nrow(tbl[is.na(line_id)])
        message(paste("  ", post-pre, "line_id values coerced to NA!!"))
      }
      if(!is.numeric(tbl$line_id)){stop(paste("line_id is not numeric for ", x))}
      
      ##USERNAME: designate followup period
      tbl[, followup:=u]
      
      ##USERNAME: if only 1 measurement and I ddin't extract as sbp_1
      if(!"sbp_1" %in% names(tbl)){
        tbl[, sbp_1:=sbp]
      }
      
      study_dt[[u]]<-tbl
    #read.dta13(paste0(long_folder, x))
    }
    study_dt<-rbindlist(study_dt, fill=T)
  
    
    
    ##USERNAME: check that id's match up  
    if(study %in% c("MEX")){
      message("more unique line_ids than measurement periods, pasting w/ hh_id")
      study_dt[, id:=paste0(hh_id, line_id)]
    }else{
      study_dt[, id:=line_id]
    }
  
  if(length(unique(study_dt$id))<nrow(study_dt)/length(unique(files[id==study, files]))){stop("Not enough unique line_ids, try binding w/ hh_id")}

    
    
    ##USERNAME: get number of sbp measurements
    sbp_cols<-paste0("sbp_", 1:3)
    col<-c()
    for(p in 1:length(sbp_cols)){
      if(sbp_cols[p] %in% names(study_dt)){
        col[p]<-sbp_cols[p]
      }
    }
    sbp_cols<-col
    

    
  ##USERNAME: only keep relevant rows
  study_dt<-study_dt[, c(me, sbp_cols, "age_year", "id", "sex_id", "year_start", "survey_name", "followup"), with=F]

  
  if(ave_meas==T){
    study_dt[, c(sbp_cols):=NULL]
    
  }else{
    
    
    if(!is.null(sbp_cols)){##USERNAME: make sure there are multiple measurements
      study_dt[, sbp:=NULL]
    }else{
      
      sbp_cols<-"sbp_1"
    }
    ##USERNAME: reshape so each indiv gets a
    study_dt<-melt(study_dt, id.vars=c("id", "followup", "survey_name", "sex_id", "age_year", "year_start"), measure.vars=sbp_cols, variable.name="k", value.name="sbp", variable.factor = F)
    
  }
  
  
  
  ##USERNAME: drop NAs
  study_dt<-study_dt[!is.na(sbp)]
  
  
  ##USERNAME: get number of measurements for each survey year
  study_meta<-list()
  for(p in 1:length(unique(study_dt$year_start))){
    fol<-unique(study_dt$year_start)[p]
    x<-study_dt[year_start==fol, ]
    numks<-length(unique(x$k))
    loss<-sum(!unique(study_dt[followup==1, id]) %in% unique(study_dt[year_start==fol, id]))
    s_size<-sum(unique(study_dt[followup==1, id]) %in% unique(study_dt[year_start==fol, id]))
    
    study_meta[[p]]<-data.table(fol, numks, loss, s_size)
  }
  study_meta<-data.table(surv=unique(study_dt$survey_name), rbindlist(study_meta), sample_size=length(unique(study_dt[followup==1, id])))
  
  
  meta[[i]]<-study_meta
  full[[i]]<-study_dt
}


full<-rbindlist(full, fill=T)





################### DIAGNOSTIC PLOTS OF EACH DATASET #########################################
######################################################
if(F){
pdf(file=paste0(FILEPATH, "long_data_diagnostics.pdf"))
for(i in 1:length(unique(full$survey_name))){  ##USERNAME: skipping over mex family life for now, and nhanes since it isn't extracted
  if(F){
    i<-6
  }
  
  surv<-unique(full$survey_name)[i]
  sub<-full[survey_name==surv  &  !is.na(age_year) & !is.na(sbp) & !is.na(sex_id) & !is.na(id) & sbp>80 & sbp<270,]
  
  message(paste("Creating plots for", surv))
  
  
  ##USERNAME: create categorical age groups
  sub[age_year<=79, age_group_id := round((age_year + 23)/5)]
  sub[age_year>=80, age_group_id:=21]
  sub[age_year>=85, age_group_id:=22]
  sub[age_year>=90, age_group_id:=23]
  sub[age_year>=95, age_group_id:=24]  ##USERNAME: aggregating all oldest age groups to id 21
  
  
  
  ##USERNAME: set to 12 oldest age groups
  sub<-sub[age_group_id>=10]
  
  base<-sub[followup==1, ]
  if(length(unique(base$k))>1){
    base<-base[, .(sbp=mean(sbp)), by="id"]
  }
  
  
  
  
  for(u in 1:length(unique(sub$followup))){
    fol<-unique(sub$followup)[u]
    cross<-sub[followup==u,]
    
    ##USERNAME:only get ppl present at baseline
    cross.t<-cross[id %in% base$id]
    
    if(length(unique(cross$k))>1){
      cross.t<-cross.t[, .(sbp=mean(sbp)), by="id"]
    }
    

    p<-ggplot()+
      geom_freqpoly(data=cross.t, aes(x=sbp), color="red", binwidth=5)+
      geom_freqpoly(data=base, aes(x=sbp), color="black", binwidth=5)+
      ggtitle(paste(surv, ", measurement period:", unique(cross.t$year_start)))+
      theme_classic()
    print(p)
    
  }
  
}


dev.off()
}





################### CONSTRUCT REGRESSION #########################################
######################################################
####################################################

pdf(file=output, width=9.5)
stack<-list()
stack2<-list()
stack3<-list()
for(i in  c(1,3, 6, 7)){    #1:length(unique(full$survey_name))){  ##USERNAME: skipping over mex family life for now, and nhanes since it isn't extracted
  if(F){
    i<-7
  }
  surv<-unique(full$survey_name)[i]
  sub<-full[survey_name==surv  &  !is.na(age_year) & !is.na(sbp) & !is.na(sex_id) & !is.na(id) & sbp>80 & sbp<270,]
  
  message(paste("Computing for", surv))
  counts<-unique(sub[, .(id, followup)])
  counts<-counts[, .(count=.N), by=c("id")]
  length(unique(counts[count>1, id]))
  
  ##USERNAME: create categorical age groups
  sub[age_year<=79, age_group_id := round((age_year + 23)/5)]
  sub[age_year>=80, age_group_id:=21]
  sub[age_year>=85, age_group_id:=22]
  sub[age_year>=90, age_group_id:=23]
  sub[age_year>=95, age_group_id:=24]  ##USERNAME: aggregating all oldest age groups to id 21
  
  
  
  ##USERNAME: set to 12 oldest age groups
  sub<-sub[age_group_id>=10]

  
  ################### CONSTRUCT REGRESSION 2/24/2018 #########################################
  ######################################################
  
  form<-"sbp~factor(sex_id)+age_year+age_year+(1|id)"
  form2<-"sbp~age_year+factor(sex_id)+as.factor(id)"
  
  ag_stack<-list()
  pred_stack<-list()
  for(z in 1:length(unique(sub$age_group_id))){
    if(F){
      z<-1
    }
    ag<-sort(unique(sub$age_group_id))[z]
    baseids<-as.numeric(sub[age_group_id==ag & followup==1, id])
    if(length(unique(baseids))>100){
      ##USERNAME:grab only people who are in the age group at baseline
      ag_sub<-sub[id %in% baseids,]
      
      mod1<-lmer(form, ag_sub)
      mod2<-lm(form2, ag_sub)
      #mod<-lm(form, data.t)
      base<-ag_sub[followup==1]
      
      preds<-predict(mod1, newdata=base)
      preds2<-predict(mod2, newdata=base)
      base<-cbind(base, preds)
      base<-cbind(base, preds2)
      base[, diff:=preds-sbp]
      
      ##USERNAME:get rmse
      rmse<-sqrt(mean(base$diff^2))
      
      predvar<-var(base$preds)
      predvar2<-var(base$preds2)
      
      realvar<-var(base$sbp)
      #realvar2<-var(base)
      
      ag_stack[[z]]<-data.table(ratio=predvar/realvar, ratio2=predvar2/realvar, age_group_id=ag, sample_size=nrow(ag_sub))
      pred_stack[[z]]<-base
    }
  }
ag_stack<-rbindlist(ag_stack)
pred_stack<-rbindlist(pred_stack)
    
    ##USERNAME: plot density plots of pre-and post-modeled sbp
    p<-ggplot()+
      geom_density(data=pred_stack, aes(x=sbp), color="red", size=1.5)+
      geom_density(data=pred_stack, aes(x=preds), color="blue", size=1.5)+
      #geom_density(data=pred_stack, aes(x=preds2), color="orange", size=1.5)+
      ggtitle(paste(surv," random int"))+
      ylim(0, .025)+
      #facet_wrap(~age_group_id)+
      theme_classic()+
      theme(text=element_text(size=18))
    print(p)
    
    p<-ggplot()+
      geom_density(data=pred_stack, aes(x=sbp), color="red", size=1.5)+
      geom_density(data=pred_stack, aes(x=preds2), color="blue", size=1.5)+
      ggtitle(paste(surv, " fixed effect"))+
      ylim(0, .025)+
      #facet_wrap(~age_group_id)+
      theme_classic()+
      theme(text=element_text(size=18))
    print(p)
    

  stack[[i]]<-cbind(ag_stack, survey=surv)
}
################### COLLAPSE AND DIAGNOSTIC PLOTS #########################################
#####################################################################


final<-rbindlist(stack)
# final2<-rbindlist(stack2)
# final3<-rbindlist(stack3)

collapsed<-final[, .(ratio=mean(ratio), ratio2=mean(ratio2)), by=.(age_group_id)]
collapsed[, age_start:=age_group_id*5-25]
setnames(collapsed, c("ratio", "ratio2"), c("Random effect method", "Fixed effect method"))
collapsed<-melt(collapsed, id.vars=c("age_group_id", "age_start"))
#collapsed<-merge(collapsed, old_ratios, by="age_group_id")

# collapsed2<-final2[, .(new_ratio=mean(newratio)), by=.(age_group_id)]
# collapsed2<-merge(collapsed2, old_ratios, by="age_group_id")
# 
# 
# collapsed3<-final2[, .(new_ratio=mean(newratio)), by=.(age_group_id)]
# collapsed3<-merge(collapsed2, old_ratios, by="age_group_id")
# 

##USERNAME: plot age-specific ratios by age
p<-ggplot(data=collapsed, aes(x=age_start, y=value, color=variable))+
  geom_line(size=2)+
  xlab("Age")+
  ylab("Adjustment factor")+
  ylim(0,1)+
  theme_classic()+
  theme(text=element_text(size=20))
print(p)
dev.off()
# 
