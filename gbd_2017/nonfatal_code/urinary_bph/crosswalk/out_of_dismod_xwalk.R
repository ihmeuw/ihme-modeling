# 1/12/2018

oodm_xwalk<-function(acause, bundle, datasheet){
  # ---SETUP-----------------------------------------------------------------------------------------------
  # set directories
  doc_dir<-paste0(j_root,"FILEPATH")
  dir.create(doc_dir,showWarnings = F,recursive = T)
  
  # load packages
  require(ggplot2)
  
  # source functions  
  source_shared_functions(c("get_envelope","get_population"))
  #--------------------------------------------------------------------------------------------------------
  
  # only keep necessary cols #
  cv_cols<-grep(x = names(datasheet),pattern = "cv_",value = T)
  keep_cols<-c("location_id","location_name","sex","year_start","year_end","age_start","age_end","mean","cases","sample_size","standard_error")
  xwalk_data<-copy(datasheet[,c(cv_cols,keep_cols),with=F])
  
  # set terminal age group age_end to 99
  xwalk_data[age_start==95,age_end:=99]
  
  # create one df with only hospital data and one df with only claims 2012 data, drop cv+ cols 
  hospital<-copy(xwalk_data[cv_hospital==1])
  hospital[,grep(x=names(hospital),pattern = "cv_"):=NULL]
  
  ms<-copy(xwalk_data[cv_marketscan==1&cv_marketscan_inp_2000==0&cv_marketscan_all_2000==0,])
  ms[,grep(x=names(ms),pattern = "cv_"):=NULL]
  
  # only keep locs in both data sets
  keep_locs<-intersect(unique(ms[,location_id]),unique(hospital[,location_id]))
  ms<-ms[location_id%in%keep_locs]
  hospital<-hospital[location_id%in%keep_locs]
  
  # create diagnostic plots 
  h<-copy(hospital)
  setnames(h,c("mean","standard_error"),c("mean_hosp","standard_error_hosp"))
  m<-copy(ms)
  setnames(m,c("mean","standard_error"),c("mean_ms","standard_error_ms"))
  h[,c("cases","sample_size"):=NULL]
  m[,c("cases","sample_size"):=NULL]
  compare<-merge(h,m,by=c("location_id","location_name","sex","age_start","age_end"),allow.cartesian = T)
  compare[year_start.y==2012,ratio:=round(mean_ms/mean_hosp,2)]
  max_val<-round(max(compare[,c("mean_ms","mean_hosp")],na.rm = T),2)
  
  date<-gsub("-","_",Sys.Date())
  pdf(paste0(doc_dir,"plot_ms_hospital_comparison_",date,".pdf"),width=11)
  for(loc in unique(compare[,location_name])){
    for (year in unique(compare[,year_start.x])){
      plot_dat<-copy(compare[location_name==loc&year_start.x==year,])
      if(nrow(plot_dat)>=1){
        gg<-ggplot(data = plot_dat,aes(x=mean_hosp,y=mean_ms,color=as.factor(year_start.y),shape=sex))+
          geom_abline()+
          geom_point()+
          scale_y_continuous(limits=c(0,max_val))+
          geom_text(aes(label=ratio),hjust=0.05,vjust=0.05,check_overlap = T)+
          ggtitle(paste(loc,": MarketScan vs. HCUP", year))
        print(gg) 
      }
    }
  }
  dev.off()
  
  # process MS: sum cases and sample size by age over location to get prevalence for US  
  ms[,c("cases_total","sample_size_total"):=lapply(.SD,sum),.SDcols=c("cases","sample_size"),by=c("age_start","age_end","sex")]
  # calculate the average prevalence/incidence across years
  ms[,avg:=cases_total/sample_size_total]
  # calculate new standard error 
  ms[,standard_error:=sqrt(avg*(1-avg)/sample_size_total)]
  # drop loc_id, loc name, cases, and sample_size now 
  ms[,c("location_id","location_name","cases","sample_size","cases_total","sample_size_total","year_start","year_end","mean"):=NULL]  
  # subset to unique rows by year and sex 
  ms<-unique(ms)
  # rename cols for comparison 
  setnames(ms,"avg","ms_mean")
  setnames(ms,"standard_error","ms_standard_error")
  
  # process hospital: pull age table to merge on age_group_id
  age_table<-get_age_map(5,add_under_1 = T)
  hospital<-merge(hospital,age_table,by=c("age_start","age_end"),all.x=T)
  # pull mid-year populations # 
  locs<-unique(hospital[,location_id])
  hospital[,year_id:=year_start+(year_end-year_start)]
  years<-unique(hospital[,year_id])
  ages<-unique(hospital[,age_group_id])
  pop<-get_population(age_group_id = ages,
                      location_id = locs,
                      year_id = years,
                      sex_id = c(1,2))
  pop[,sex:=ifelse(sex_id==1,"Male","Female")]
  
  # merge pop onto hospital data
  hospital<-merge(hospital,pop[,.(age_group_id,location_id,year_id,sex,population)],by=c("year_id","sex","age_group_id","location_id"))
  # calculate implied cases from mean and population  
  hospital[,cases:=mean*population]
  # sum cases and population across all locations 
  hospital[,c("cases_total","sample_size_total"):=lapply(.SD,sum),.SDcols=c("cases","population"),by=c("age_start","age_end","sex")]
  # calculate mean and standard error from cases and sample size
  hospital[,avg:=cases_total/sample_size_total]
  hospital[,standard_error:=sqrt(avg*(1-avg)/sample_size_total)]
  # drop extra cols
  hospital[,c("year_id","age_group_id","location_id","location_name","mean","cases","sample_size","population","year_start","year_end","cases_total","sample_size_total"):=NULL]
  # subset to unique rows by year and sex 
  hospital<-unique(hospital)
  setnames(hospital,"avg","hosp_mean")
  setnames(hospital,"standard_error","hosp_standard_error")
  
  # compare ms to hospital # 
  compare<-merge(ms,hospital,by=c("sex","age_start","age_end"), all.x = T)
  compare[,ratio:=ms_mean/hosp_mean] 
  compare[,ratio_se:=sqrt((ms_mean/hosp_mean)*((ms_standard_error^2/ms_mean^2)+(hosp_standard_error^2/hosp_mean^2)))]
  
  # save ratios for documentation 
  date<-gsub("-","_",Sys.Date())
  write.csv(compare,paste0(doc_dir,"ms_hosp_xwalk_",date,".csv"),row.names = F,na="")
  
  # add cv_hospital to compare to merge on 
  compare[,cv_hospital:=1]
  
  # adjust input data
  datasheet<-merge(datasheet,compare[,.(age_start,ratio,ratio_se,sex,cv_hospital)],by=c("age_start","sex","cv_hospital"),all.x=T)
  datasheet[is.na(ratio)==F,mean:=mean*ratio]
  datasheet[is.na(ratio)==F,standard_error:=sqrt(standard_error^2*ratio_se^2+ratio^2*standard_error^2+mean*ratio_se^2)]
  datasheet[is.na(ratio)==F,c("lower","upper","uncertainty_type_value"):=NA]
  datasheet[standard_error<1,c("cases","sample_size"):=NA]
  datasheet[standard_error>=1 & is.na(cases)==F & is.na(sample_size)==F,c("mean","standard_error"):=NA]
  datasheet<-datasheet[is.na(standard_error)==T|standard_error<1]
  datasheet[mean>1,mean:=1]
  datasheet[,c("ratio","ratio_se"):=NULL]
  
  return(datasheet)
}
