#code to calculate weighted values.


### Data cleaning code
library (data.table)
library (rJava)
library(xlsx)
library (ggplot2)
library (compare)
library (car)

keep<-c("nid","year_start",'year_end',"age_start","age_end","measure","lower","upper","cd","site_memo",
        "ihme_loc_id","sample_size","error","mean","unit_type","uncert_type","cases","sex","me_name","unit")

#####################
#####################
#Work with individual datasets
#####################
#####################

####################
#FPG literature
####################

bm_lit<-read.csv(filepath , stringsAsFactors = FALSE, na.strings=c("","NA"))
bm_lit<-as.data.frame(bm_lit) #convert biomarker file to datatable
fpg_lit<-bm_lit[bm_lit$me_name=='2hbg' | bm_lit$me_name=='diabetes' | bm_lit$me_name=='fpg' | 
                  bm_lit$me_name=='hba1c',]

mean_est<-fpg_lit[is.na(fpg_lit$mean),]
mean_est_wt<-mean_est[!is.na(mean_est$mean1),]

#calculate weighted mean
mean_est_wt$mean1<-as.numeric(mean_est_wt$mean1)
mean_est_wt$sample_size1<-as.numeric(mean_est_wt$sample_size1)
mean_est_wt$mean2<-as.numeric(mean_est_wt$mean2)
mean_est_wt$sample_size2<-as.numeric(mean_est_wt$sample_size2)

mean_est_wt$wtd_mean_1<-mean_est_wt$mean1*mean_est_wt$sample_size1
mean_est_wt$wtd_mean_2<-mean_est_wt$mean2*mean_est_wt$sample_size2
mean_est_wt$wtd_mean_3<-mean_est_wt$mean3*mean_est_wt$sample_size3
mean_est_wt$wtd_mean_4<-mean_est_wt$mean4*mean_est_wt$sample_size4

mean_est_wt$mean_calc<-rowSums(cbind(mean_est_wt$wtd_mean_1, mean_est_wt$wtd_mean_2,mean_est_wt$wtd_mean_3,mean_est_wt$wtd_mean_4),na.rm=T)
mean_est_wt$tot<-rowSums(cbind(mean_est_wt$sample_size1, mean_est_wt$sample_size2,mean_est_wt$sample_size3,mean_est_wt$sample_size4),na.rm=T)

mean_est_wt$mean<-mean_est_wt$mean_calc/mean_est_wt$tot

#calculate weighted error
mean_est_wt$error1<-as.numeric(mean_est_wt$error1)
mean_est_wt$sample_size1<-as.numeric(mean_est_wt$sample_size1)
mean_est_wt$error2<-as.numeric(mean_est_wt$error2)
mean_est_wt$sample_size2<-as.numeric(mean_est_wt$sample_size2)
mean_est_wt$wtd_error_1<-mean_est_wt$error1*mean_est_wt$sample_size1
mean_est_wt$wtd_error_2<-mean_est_wt$error2*mean_est_wt$sample_size2
mean_est_wt$wtd_error_3<-mean_est_wt$error3*mean_est_wt$sample_size3
mean_est_wt$wtd_error_4<-mean_est_wt$error4*mean_est_wt$sample_size4

mean_est_wt$error_calc<-rowSums(cbind(mean_est_wt$wtd_error_1, mean_est_wt$wtd_error_2,mean_est_wt$wtd_error_3,mean_est_wt$wtd_error_4),na.rm=T)
mean_est_wt$tot<-rowSums(cbind(mean_est_wt$sample_size1, mean_est_wt$sample_size2,mean_est_wt$sample_size3,mean_est_wt$sample_size4),na.rm=T)
mean_est_wt$error<-mean_est_wt$error_calc/mean_est_wt$tot

#calculate weighted lower
mean_est_wt$lower1<-as.numeric(mean_est_wt$lower1)
mean_est_wt$sample_size1<-as.numeric(mean_est_wt$sample_size1)
mean_est_wt$lower2<-as.numeric(mean_est_wt$lower2)
mean_est_wt$sample_size2<-as.numeric(mean_est_wt$sample_size2)
mean_est_wt$wtd_lower_1<-mean_est_wt$lower1*mean_est_wt$sample_size1
mean_est_wt$wtd_lower_2<-mean_est_wt$lower2*mean_est_wt$sample_size2
mean_est_wt$wtd_lower_3<-mean_est_wt$lower3*mean_est_wt$sample_size3
mean_est_wt$wtd_lower_4<-mean_est_wt$lower4*mean_est_wt$sample_size4

mean_est_wt$lower_calc<-rowSums(cbind(mean_est_wt$wtd_lower_1, mean_est_wt$wtd_lower_2,mean_est_wt$wtd_lower_3,mean_est_wt$wtd_lower_4),na.rm=T)
mean_est_wt$tot<-rowSums(cbind(mean_est_wt$sample_size1, mean_est_wt$sample_size2,mean_est_wt$sample_size3,mean_est_wt$sample_size4),na.rm=T)
mean_est_wt$lower<-mean_est_wt$lower_calc/mean_est_wt$tot

#calculate weighted upper
mean_est_wt$upper1<-as.numeric(mean_est_wt$upper1)
mean_est_wt$sample_size1<-as.numeric(mean_est_wt$sample_size1)
mean_est_wt$upper2<-as.numeric(mean_est_wt$upper2)
mean_est_wt$sample_size2<-as.numeric(mean_est_wt$sample_size2)
mean_est_wt$wtd_upper_1<-mean_est_wt$upper1*mean_est_wt$sample_size1
mean_est_wt$wtd_upper_2<-mean_est_wt$upper2*mean_est_wt$sample_size2
mean_est_wt$wtd_upper_3<-mean_est_wt$upper3*mean_est_wt$sample_size3
mean_est_wt$wtd_upper_4<-mean_est_wt$upper4*mean_est_wt$sample_size4

mean_est_wt$upper_calc<-rowSums(cbind(mean_est_wt$wtd_upper_1, mean_est_wt$wtd_upper_2,mean_est_wt$wtd_upper_3,mean_est_wt$wtd_upper_4),na.rm=T)
mean_est_wt$tot<-rowSums(cbind(mean_est_wt$sample_size1, mean_est_wt$sample_size2,mean_est_wt$sample_size3,mean_est_wt$sample_size4),na.rm=T)
mean_est_wt$upper<-mean_est_wt$upper_calc/mean_est_wt$tot

#recode lower/upper/error to NA if not reported in initial extraction
mean_est_wt$lower[mean_est_wt$lower==0] <- NA
mean_est_wt$upper[mean_est_wt$upper==0] <- NA
mean_est_wt$error[mean_est_wt$error==0] <- NA


#recombine files
mean_fpg<-fpg_lit[!is.na(fpg_lit$mean),] #with reported mean
no_mean_fpg<-mean_est[is.na(mean_est$mean1),] #with no mean
mean_est_wt #with weighted mean

mean_fpg<-mean_fpg[keep]
no_mean_fpg<-no_mean_fpg[keep]
mean_est_wt<-mean_est_wt[keep]
fpg_lit_fix<-rbind(mean_fpg, no_mean_fpg, mean_est_wt)

