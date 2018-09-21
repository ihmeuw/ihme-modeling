###########################################################
### Author: USERNAME
### Date: 4/5/2017
### Purpose: Create adjustment factors for regression to the mean for SBP
### Notes: -Currently re-creating Merdhad's analysis as outlined in JAMA bp appendix-redoing w/ newly extracted longitudinal datasets
###########################################################



rm(list=ls())
os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
} else {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
}

date<-gsub("-", "_", USERNAMEs.Date())

library(data.table, lib.loc=lib_path)
library(lme4, lib.loc=lib_path)
library(ggplot2, lib.loc=lib_path)
library(arm, lib.loc=lib_path)
library(haven, lib.loc=lib_path)
library(readstata13, lib.loc=lib_path)
library(utils, lib.loc=lib_path)
library(formattable, lib.loc=lib_path)
library(htmltools, lib.loc=lib_path)
library(webshot, lib.loc=lib_path)

################### ARGS AND PATHS #########################################
######################################################
me<-"sbp"

long_folder<-paste0(j, "FILEPATH")

ave_meas<-F  ##USERNAME: average over measurement @ each time point


old_ratios_path<-paste0(j, "FILEPATH")

output<-paste0(j, "FILEPATH")

path<-paste0(j, "FILEPATH")



################### UTILITY FUNCTIONS #########################################
######################################################


export_formattable <- function(f, file, width = "100%", height = NULL, 
                               background = "white", delay = 0.2)
{
  w <- as.htmlwidget(f, width = width, height = height)
  path <- html_print(w, background = background, viewer = NULL)
  url <- paste0("file:///", gsub("\\\\", "/", normalizePath(path)))
  webshot(url,
          file = file,
          selector = ".formattable_widget",
          delay = delay)
}

gm_mean = function(x, na.rm=TRUE){
  exp(sum(log(x[x > 0]), na.rm=na.rm) / length(x))
}



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
    i<-6
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
      #tbl[, test:=as.numeric(line_id)]
      #View(tbl[is.na(test)])
      

      ##USERNAME: reformat name for nhanes followup
      if(i==6 & u==2){ tbl[, survey_name:="USA/NHANES"]}
      
      
      if(x %in% c("CHRLS_2012_resample.dta", "IDN_FLS_2000.dta", "IDN_FLS_2007.dta", "USA_NHANES_1982_84.dta")){
        message(paste("For survey", x, ", line_id is", class(tblUSER_id), ", attempting to convert to numeric--"))
        
        pre<-nrow(tbl[is.na(line_id)])
        tblUSER_id<-as.numeric(tblUSER_id)
        post<-nrow(tbl[is.na(line_id)])
        message(paste("  ", post-pre, "line_id values coerced to NA!!"))
      }
      if(!is.numeric(tblUSER_id)){stop(paste("line_id is not numeric for ", x))}
      
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
pdf(file=paste0(j, "FILEPATH"))
for(i in 1:length(unique(full$survey_name))){  
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





  ################### CONSTRUCT REGRESSION 4/25 #########################################
  ######################################################
  
  ##USERNAME: set columns for random intercepts
  sub$id_k <- paste(sub$id, sub$k, sep="_")
  sub$id_followup <-paste(sub$id, sub$followup, sep="_")
  
  form<-"sbp~factor(sex_id)+age_year+(1|id)"
  
  
  
  ################### RUN REGRESSION FOR EACH AGE GROUP #########################################
  #####################################################################
  sub[, age_year:=as.numeric(age_year)]
  
  ag_stack<-list()
  for(z in 1:length(unique(sub$age_group_id))){
    if(F){
      z<-1
    }
    ag<-sort(unique(sub$age_group_id))[z]
    baseids<-as.numeric(sub[age_group_id==ag & followup==1, id])
   if(length(unique(baseids))>100){
    ##USERNAME:grab only people who are in the age group at baseline
    ag_sub<-sub[id %in% baseids,]
     
    mod<-lmer(form, ag_sub)
    #mod<-lm(form, data.t)
    preds<-predict(mod)
    ag_sub<-cbind(ag_sub, preds)
    ag_sub[, diff:=preds-sbp]
    
    ##USERNAME:get rmse
    rmse<-sqrt(mean(ag_sub$diff^2))
    
    
    ##USERNAME: get predicted var from predictions
    #predvar<-var(ag_sub$preds)
    
    tempsub2<-ag_sub[ , .(mean_pred=mean(preds)), by=.(id)]
    predvar<-var(tempsub2$mean_pred)
    
    ##USERNAME:average over measurements for realvar
    tempsub<-ag_sub[ , .(mean_me=mean(sbp)), by=.(id)]
    
    realvar<-var(tempsub$mean_me)
    
    ag_stack[[z]]<-data.table(ratio=predvar/realvar, age_group_id=ag, sample_size=nrow(ag_sub))
    
    
    ##USERNAME: plot density plots of pre-and post-modeled sbp
    p<-ggplot()+
      geom_density(data=tempsub, aes(x=mean_me), color="red")+
      geom_density(data=tempsub2, aes(x=mean_pred), color="blue")+
      ggtitle(paste("Diet method distributions in", surv), "red is raw, blue is predictions")+
      #facet_wrap(~age_group_id)+
      theme_classic()
    print(p)
    
    p<-ggplot()+
      geom_density(data=tempsub, aes(x=mean_me), color="red")+
      geom_density(data=tempsub2, aes(x=mean_pred), color="blue")+
      ggtitle(paste("Diet method distributions in", surv), "red is raw, blue is predictions")+
      #facet_wrap(~age_group_id)+
      theme_classic()
    print(p)
    
    
    
    
    if(ag==16){
      
      gal<-319456
      
      gal_df2<-ag_sub[id==gal,]
      
      aver<-mean(gal_df2[followup==1, sbp])
      aver<-data.table(followup=1, aver=aver)
      basepred<-mean(gal_df2[followup==1, preds])
      basepred<-data.table(followup=1, aver=basepred)
      
      
      pdf(file=paste0(j, "FILEPATH"))
      p<-ggplot(data=gal_df2, aes(x=followup, y=sbp))+
        geom_point(size=3)+
        ggtitle("Observed blood pressure values for one person in ZAF_NIDS")+
        xlab("Measurement period")+
        theme_classic()+
        theme(axis.ticks.x = element_blank())
      print(p)
      
      p<-ggplot(data=gal_df2, aes(x=followup, y=sbp))+
        geom_point(size=3)+
        geom_line(aes(x=followup, y=preds), color="orange", size=2)+
        geom_point(aes(x=followup, y=preds), shape=4, color="orange", size=6)+
        geom_point(data=basepred, aes(x=followup, y=aver), shape=4, color="blue", size=6)+
        geom_point(data=aver, aes(x=followup, y=aver), color="red", size=6)+
        ggtitle("All relevant values for one person in ZAF_NIDS")+
        xlab("Measurement period")+
        theme_classic()+
        theme(axis.ticks.x = element_blank())
      print(p)
      dev.off()
      
    }
    
    
    
    
   }
    
  }
  stack[[i]]<-cbind(rbindlist(ag_stack), survey=surv)
  
  if(F){
  ################### CALCULATE RATIOS FOR MOD2 #########################################
  #####################################################################
  var<-copy(sub2)
  
  
  ##USERNAME: drop loss to followup or new obs
  var.t<-var[duplicated(id, k), ]
  var<-var.t
  #var<-var[followup==1,]
  
  ##USERNAME: drop age groups w/ small sample size
  ags<-sort(unique(var$age_group_id))
  for(l in 1:length(ags)){
    ag<-ags[l]
    #message(paste(i, ag))
    if(nrow(var[age_group_id==ag])<10){
      message(paste0("   Dropping age_group_id ", ag, ", only ", nrow(var[age_group_id==ag]), " observations" ))
      
      var<-var[age_group_id!=ag,]
    }
  }
  
  if(F){##USERNAME: get predicted var from person-specific intercepts (instead of predictions)
    person_var<-as.data.table(cbind(id=rownames(coef(mod)$id), int=coef(mod)$id [, "(Intercept)"]))  ##USERNAME: int is the intercept from the person-specific temporal 
    person_var[, int:=as.numeric(int)]
    person_var[, id:=as.numeric(id)]
    predvar<-person_var[, (pvar=var(int))]
  }
  
  if(T){ ##USERNAME: get predicted var from predictions
    predvar<-var[, .(pvar=var(preds)), by=.(age_group_id)]
  }
  
  realvar<-var[, .(rvar=var(sbp)), by=.(age_group_id)]
  
  vars<-merge(predvar, realvar, by="age_group_id")
  
  vars[, newratio:=pvar/rvar]
  vars[, survey:=surv]
  
  
  ##USERNAME: plot density plots of pre-and post-modeled sbp
  p<-ggplot(data=var)+
    geom_histogram(aes(x=sbp), alpha=0.2, color="red")+
    geom_histogram(aes(x=preds2), alpha=0.2, color="blue")+
    ggtitle(paste("New method distributions in", surv), "red is raw, blue is predictions")+
    facet_wrap(~age_group_id)+
    theme_classic()
  print(p)
  
  
  
  stack2[[i]]<-vars
  
  }
#################################end survey loop




################### COLLAPSE AND DIAGNOSTIC PLOTS #########################################
#####################################################################


final<-rbindlist(stack)


collapsed<-final[, .(new_ratio=mean(ratio)), by=.(age_group_id)]
collapsed<-merge(collapsed, old_ratios, by="age_group_id")


##USERNAME: set up dt for plotting
collapsed[, survey:="Collapsed"]
final<-rbindlist(list(final, collapsed), use.names=T, fill=T)
surv_char<-unique(final$survey)
surv_labs<-c("CHRLS", "IFLS", "NHANES", "NIDS", "Collapsed")
final[, survey:=factor(survey, levels=surv_char, labels=surv_labs)]
pdf(file=paste0(j, "FILEPATH"))
p<-ggplot(data=final, aes(x=age_group_id, y=ratio, color=survey))+
  geom_point()+
  geom_line()+
  #geom_point(data=collapsed, aes(x=age_group_id, y=new_ratio), color="black")+
  #geom_line(data=collapsed, aes(x=age_group_id, y=new_ratio), color="black")+
  ylim(0, 1)+
  ggtitle("Correction factors by survey")+
  xlab("Age group id")+
  ylab("variance of predictions/observed variance")+
  #ylab("Temporal/unexplained variance ratio")+
  #guides(color="Survey series")+
  theme_classic()+
  guides(color=guide_legend(nrow=2, byrow=T))+
  theme(legend.position="bottom", legend.title=element_blank())
print(p)
dev.off()
#




p<-ggplot(data=collapsed, aes(x=ratio, y=new_ratio, color=factor(age_group_id)))+
  geom_point()+
  geom_abline(slope=1, intercept=0)+
  ylim(0, 1)+
  xlim(0, 1)+
  xlab("GBD 2015 correction factors")+
  ylab("Recreated correction factors, random intercept")+
  theme_classic()
print(p)

tab<-formattable(collapsed)


dev.off()



export_formattable(tab, file=paste0(j, "FIELPATH"))




##save final ratios

collapsed[, ratio:=NULL]
setnames(collapsed, "new_ratio", "ratio")

##sy:add rows for oldest age groups
final<-rbind(collapsed, data.table(age_group_id=c(30, 31, 32, 235), ratio=collapsed[age_group_id==21, ratio]))



write.csv(final, file=paste0(j, "FILEPATH"), row.names=F)


