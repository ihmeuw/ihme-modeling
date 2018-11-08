#--------------------------------------------------------------
# Date: 2018-06-07
# Project: Nonfatal CKD Estimation: stage-specific etiology proportions
# Purpose: Create function library for use in stage-specific etiology 
# proportions 
#--------------------------------------------------------------

# setup -------------------------------------------------------
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH' 
  h_root <- 'FILEPATH'
} else { 
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}

source(paste0(h_root,"FILEPATH"))

require(pacman)
p_load(data.table,openxlsx,MASS)

# functions ---------------------------------------------------------------

source_shared_functions(c("get_demographics","get_draws"))

center_age<-function(age_start,age_end){
  age_midpoint<-(((age_start+age_end)/2)-60)/10
  print("centered age values")
  return(age_midpoint)
}

pred<-function(pred_df, coef_df, stage_name, etio_name, cov_dt){
  # Create matrix of variables 
  predmat<-as.matrix(copy(pred_df[stage==stage_name,.(age,age2,sex_id,const)]))
  print("beginning predictions")
  
  # Create a matrix of the coefficient for given stage/etiology
  coefmat<-as.matrix(copy(coef_df[stage==stage_name&etio==etio_name,.(rrr)]))

  # Subset variance-covariance matrix to the square matrix for given stage/etiology
  keepcols<-names(cov_dt)[grep(etio_name,names(cov_dt))]
  covmat<-as.matrix(copy(cov_dt[stage==stage_name&etio==etio_name,keepcols,with=F]))
  
  # Create 1,000 draws of regression coefficient using the betas + vcov matrix
  cov_draws<-t(mvrnorm(n = 1000, mu = coefmat, Sigma = covmat))
  print("created 1,000 draws of regression coefficients from variance-covariance matrix")
  
  # Predict by multiplying  predictor matrix by the draws of regression coefs
  etio_preds<-as.data.table(predmat%*%cov_draws)
  print(paste("generated predictions for",etio_name, stage_name))
  
  # Name  draws
  draws<-paste0("draw_",0:999)
  setnames(etio_preds,paste0("V",1:1000),draws)
  
  # Bind  predcitor matrix to  predictions
  etio_preds<-cbind(pred_df[stage==stage_name,],etio_preds)

  # Add on etiology and stage
  etio_preds[,etio:=etio_name]
  etio_preds[,stage:=stage_name]
  
  return(etio_preds)
}

pred_etio<-function(pred_df, coef_df, stage_name, cov_dt){
  print(paste("looping over etiologies for", stage_name))
  etios<-unique(coef_df[,etio])
  etio_preds<-lapply(etios, function(etio) pred(pred_df, coef_df, stage_name, etio, cov_dt))
  etio_preds<-rbindlist(etio_preds,use.names = T)
  return(etio_preds)
}

pred_stage<-function(pred_df, coef_df, cov_dt){
  print("looping over stages")
  stages<-unique(coef_df[,stage])
  stage_preds<-lapply(stages, function(stage) pred_etio(pred_df, coef_df, stage, cov_dt))
  stage_preds<-rbindlist(stage_preds)
  return(stage_preds)
}

process_draws<-function(pred_dt){
  print("processing draws")
  
  # Exponentiate
  draws<-paste0("draw_",0:999)
  pred_dt[,(draws):=lapply(.SD,exp),.SDcols=draws]
  print("exponentiated draws")
  
  # Generate the denominator of the OR
  denoms<-paste0("denom_",0:999)
  pred_dt[,(denoms):=lapply(1:1000,function(x) 1+sum(get(draws[x]),na.rm =T)),by=c("stage","sex_id","age_group_id")]
  print("generated denominator for predictions")
  
  # Fill in RRR as 1 for the base category
  pred_dt[etio=="unknown",(draws):=1]
  
  # Calculate probability
  props<-paste0("prop_",0:999)
  pred_dt[,(props):=lapply(1:1000,function(x) get(draws[x])/get(denoms[x]))]
  print("converted predictions to probability space")
  
  # Drop original draws and denoms 
  pred_dt[,c(draws,denoms):=NULL]
  setnames(pred_dt,props,draws)
  
  # Aggregate other and unknown
  print("aggregating proportions for CKD due to other causes and CKD due to unknown causes")
  # Separate other and unknown from dm1/dm2/htn/gn
  pred_dt_other<-pred_dt[etio%in%c("oth","unknown")]
  pred_dt<-pred_dt[!(etio%in%c("oth","unknown"))]
  # Melt long
  pred_dt_other<-melt(pred_dt_other,id.vars = c("age_group_id","sex_id","stage","age_group_name","age_start","age_end","age","age2","const","etio"))
  # Add the draws
  pred_dt_other<-pred_dt_other[,sum(value),by=c("age_group_id","sex_id","stage","age_group_name","age_start","age_end","age","age2","const","variable")]
  # Label as "other"
  pred_dt_other[,etio:="oth"]
  # Cast wide
  pred_dt_other<-dcast(pred_dt_other,formula = age_group_id+sex_id+stage+age_group_name+age_start+age_end+age+age2+const+etio~variable,value.var = "V1")
  # Tack other back onto dm1/dm2/htn/gn
  pred_dt<-rbindlist(list(pred_dt,pred_dt_other),use.names=T)
  
  print("done processing draws")
  return(pred_dt)
}

run_process_preds<-function(coef_filepath, covmat_filepath, extrapolate_under_20=F){
  # (1) Source regression cofficient + vcov matrix & create prediction matrix 
  # (2) Run functions to generate draws of your predictions 
  # (3) Run function to process those draws so they're in proportion space + combine other/unknown
  
  #(1) 
    print("pulling coefficient values & variance-covariance matrix. creating predictor matrix.")
    # Get age map
    ages<-get_age_map(5)
    
    # Source coefficient values 
    coef_df<-as.data.table(read.xlsx(coef_filepath))
    coef_df[,rrr:=log(rrr)]
    
    # Source variance covariance matrix 
    cov_dt<-fread(covmat_filepath)
    cov_dt[,beta:=NULL]
    
    # Create dt to predict out for 
    pred_df<-as.data.table(expand.grid(sex_id=c(0,1), age_group_id=c(ages[,age_group_id]),stage=unique(coef_df[,stage])))
    pred_df<-merge(pred_df,ages,by="age_group_id")
    if (extrapolate_under_20==F){
      pred_df[age_start>=20,age:=center_age(age_start,age_end)]
      pred_df[age_start<20,age:=center_age(20,24)]
      pred_df[,age2:=age^2]
      pred_df[,const:=1]
    }else{
      pred_df[,age:=center_age(age_start,age_end)]
      pred_df[,age2:=age^2]
      pred_df[,const:=1]
    }
  
  # (2)
    # Run stage predictions 
    preds<-pred_stage(pred_df, coef_df, cov_dt)
    
    if (extrapolate_under_20==F){
      preds[,c("age_start","age_end","age_group_name"):=NULL]
      preds<-merge(preds,ages,by="age_group_id",all=T)
    }
    
  # (3)
    # Process draws 
    preds<-process_draws(preds)
  
  return(preds)
}

format_draws<-function(pred_dt, dm_correction, dm_1_me, dm_2_me, loc_id=NULL, proportion_dir){
  pred_dt[,sex_id:=ifelse(sex_id==0,1,2)]
  pred_dt[,measure_id:=18]
  
  print("getting demographics to save for")
  demographics<-get_demographics("epi")
  demographics[["location_id"]]<-NULL
  template<-as.data.table(expand.grid(demographics))
  pred_dt<-merge(pred_dt,template,by=c("age_group_id","sex_id"),allow.cartesian = T)
  
  if (dm_correction==T){
    # Diabetes prevalence correction 
    if (loc_id!=102){
      print("beginning diabetes correction process")
      # Set draw names
      draws<-paste0("draw_",0:999)
      
      # Get draws for given location and the U.S.
      print(paste("getting dm1/2 draws for u.s. and loc id",loc_id))
      dm_draws<-get_draws(gbd_id_type = "modelable_entity_id", gbd_id = c(dm_1_me, dm_2_me), source = 'epi', measure_id = 5, location_id = c(102,loc_id),
                            sex_id = c(1,2), status = "best")
      # Melt draws 
      print("processing dm draws")
      id_vars<-c("age_group_id","sex_id","year_id","location_id","modelable_entity_id")
      dm_draws_long<-melt(dm_draws[,-c("model_version_id","measure_id","metric_id"),with=F],id.vars = id_vars,variable.name = "draw",
                          value.name = "prev")
      dm_draws_long[,etio:=ifelse(modelable_entity_id==dm_1_me,"dm1","dm2")]
      dm_draws_long[,modelable_entity_id:=NULL]
      # Cast wide by DM type 
      dm_draws_long<-dcast(data=dm_draws_long,formula=year_id+age_group_id+sex_id+draw+location_id~etio,value.var=("prev"))
      # Sum prev of DM1 and DM2 by location, year, age, sex, and draw
      dm_draws_long[,total:=sum(dm1,dm2),by=c("location_id","year_id","age_group_id", "sex_id","draw")]
      # Calculate the ratios of t1 and t2 to total dm 
      etios<-c("dm1","dm2")
      dm_draws_long[,(etios):=lapply(1:length(etios),function(x) get(etios[x])/total),by=c("location_id","year_id","age_group_id", "sex_id","draw")]
      # Drop the total column 
      dm_draws_long[,total:=NULL]
      # Melt t1 and t2 long 
      dm_draws_long<-melt(data = dm_draws_long, id.vars= c("year_id","age_group_id","sex_id","draw","location_id"),variable.name = "etio",
                          value.name = "prev")
      # Rename location_id before casting 
      dm_draws_long[,location_id:=paste0("loc_",location_id)]
      # Cast location_id wide 
      dm_draws_long<-dcast(data = dm_draws_long, formula = year_id+age_group_id+sex_id+draw+etio~location_id,value.var = "prev")
      # Calc ratio of loc x to US for dm1 and dm2 
      loc_var<-paste0("loc_",loc_id)
      loc_us<-"loc_102"
      dm_draws_long[get(loc_var)==get(loc_us),ratio:=1]
      dm_draws_long[is.na(ratio)&get(loc_var)==0,(paste(loc_var)):=1e-6]
      dm_draws_long[is.na(ratio)&get(loc_us)==0,(paste(loc_us)):=1e-6]
      dm_draws_long[is.na(ratio),ratio:=get(loc_var)/get(loc_us)]
      # Drop loc-specific proportions 
      dm_draws_long[,c(loc_var,loc_us):=NULL]
      # Cast draws wide 
      dm_draws<-dcast(dm_draws_long, formula= year_id+age_group_id+sex_id+etio~draw, value.var="ratio")
      ratios<-paste0("ratio_",0:999)
      setnames(dm_draws,draws,ratios)
      
      # Merge dm correction values on pred_dt
      print("merging dm draws with predictions. correcting precitions")
      pred_dt<-merge(pred_dt,dm_draws,by=c("age_group_id","year_id","sex_id","etio"),all.x=T)
      # Correct prop dm1/2 
      pred_dt[etio%in%c("dm1","dm2"),(draws):=lapply(1:1000, function(x) get(draws[x])*get(ratios[x])),by=c("sex_id","age_group_id","year_id","sex_id",
                                                                                                            "etio")]
      # Drop ratios
      pred_dt[,c(ratios):=NULL]
      print("done with diabetes correction process")
    }
  }
    
  # Add location_id
  pred_dt[,location_id:=loc_id]
  
  # Write files for each stage/etio/location
  print(paste("writing files to", proportion_dir))
  keep<-c(names(pred_dt)[grepl("draw_",names(pred_dt))],names(pred_dt)[grepl("_id",names(pred_dt))])
  for (s in unique(pred_dt[,stage])){
    for (e in unique(pred_dt[,etio])){
      dt<-copy(pred_dt[stage==s&etio==e,])
      dt<-dt[,keep,with=F]
      dir<-paste0(proportion_dir,s,"/",e,"/")
      write.csv(dt,paste0(dir,loc_id,".csv"),row.names = F)
      print(paste("wrote", dir,loc_id,".csv"))
    }
  }
  print("done!")

}

plot_predictions<-function(dir, pred_dt, plot_version, cohort){
  draws<-paste0("draw_",0:999)
  p<-copy(pred_dt)
  # Collapse draws for plotting
  print("collapsing draws")
  p[,mean:=apply(.SD,1,mean),.SDcols=draws]
  p[,upper:=apply(.SD,1,quantile,c(0.975),na.rm=T),.SDcols=draws]
  p[,lower:=apply(.SD,1,quantile,c(0.025),na.rm=T),.SDcols=draws]
  p[,(draws):=NULL]
  
  ages<-get_age_map(5)
  
  # plot all ages
  p[,sex:=ifelse(sex_id=="0","Male","Female")]
  pdf(file = paste0(dir,"/",cohort,"_etiologies_by_age_sex_stage_v",plot_version,".pdf"),width = 11,height = 11)
  gg<-ggplot(data=p)+
    geom_bar(aes(x=factor(age_group_name,levels=c(unique(ages[,age_group_name]))),y=mean,fill=etio),stat="identity")+
    ggtitle(paste0("CKD etiologies by age, sex, and stage"))+
    scale_fill_manual(values=c("#D7263D","#F46036","#9BC53D","#2E294E","#1B998B"))+
    theme_bw()+
    facet_wrap(stage~sex,nrow = 5,ncol = 2)+
    theme(axis.text.x = element_text(angle = 90, hjust = 1))+
    xlab("Age")+
    ylab("Proportion")
  print(gg)
  dev.off()
  print("plotted all ages")
  
  # plot uncertaintiy 
  pdf(file=paste0(dir,"/",cohort,"_uncertainty_by_stage_etiology_v",plot_version,".pdf"),width=11,height=11)
  for (e in unique(p[,etio])){
    for (s in unique(p[,stage])){
      plot_dat<-copy(p[stage==s&etio==e,])
      gg<-ggplot(data=plot_dat,aes(x=factor(age_group_name,levels=c(unique(ages[,age_group_name]))),y=mean))+
        geom_point()+
        geom_errorbar(aes(ymin=lower,max=upper),width=0.1)+
        scale_y_continuous(limits=c(0,1))+
        facet_wrap(~sex)+
        xlab("Age")+
        ylab("Proportion")+
        theme_bw()+
        ggtitle(paste(s,e,"uncertainty"))
      print(gg)
    }
  }
  dev.off()
  print("plotted uncertainty")
  
  # plot over 20 
  pdf(file=paste0(dir,"/",cohort,"_etiologies_by_age_sex_stage_v",plot_version,"_over20.pdf"),width=11,height=11)
  p20<-copy(p[age_start>=20])
  gg<-ggplot(data=p20)+
    geom_bar(aes(x=factor(age_group_name,levels=c(unique(ages[,age_group_name])[8:23])),y=mean,fill=etio),stat="identity")+
    ggtitle(paste0("CKD etiologies by age, sex, and stage"))+
    scale_fill_manual(values=c("#D7263D","#F46036","#9BC53D","#2E294E","#1B998B"))+
    theme_bw()+
    facet_wrap(stage~sex,nrow = 5,ncol = 2)+
    theme(axis.text.x = element_text(angle = 90, hjust = 1))+
    xlab("Age")+
    ylab("Proportion")
  print(gg)
  dev.off()
  print("done plotting.")

}

