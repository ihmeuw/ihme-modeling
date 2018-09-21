

#test4
#merge case definition info
cd<-read.csv(filepath)
test4<-read.csv(filepath)

cw<-merge(cd,test4, by=c('nid'), all.x=T)

cw_prev<-cw[cw$measure=='prevalence',]

summary(cw_prev$lower)
cw_prev$lower_1<-round(cw_prev$lower, digits = 2)
cw_prev$upper_1<-round(cw_prev$upper, digits = 2)
cw_prev$mean_1<-round(cw_prev$mean, digits = 2)
cw_prev$age1<-cw_prev$age_start
cw_prev$is_outlier<-0
cw_prev$sex_id<-ifelse(cw_prev$sex=='Female',2,1)
cw1<-cw_prev[cw_prev$cd !='FPG ? 126 mg/dl',]
cw1$type<-substr(cw1$cd, 7, 9)
table(cw1$age1)

#within biomarker crosswalk

tf<-read.csv(filepath)

library(ggplot2)
pdf(filepath, width=16, height = 9)
for (i in unique (tf_mean$cd)){
  temp<-tf_mean[(tf_mean$cd==i),]
  g<-ggplot(temp, aes(x =prev, y = prev_mean,colour=as.factor(age))) + xlab("reported prevalance using different fpg cutpoint") + ylab("est prev at fpg >126 mg/dl") +
    xlim(0,1) + ylim(0,1) + geom_point(size=3) + facet_wrap(~sex)+
    ggtitle(paste0("scatter between reported prevalence at FPG cutpoint and prevalence at 126 for:", i)) 
  print(g)
}
dev.off()


tf$lower<-round(tf$prev_lower, digits = 1)
tf$upper<-round(tf$prev_upper, digits = 1)
tf$prev<-round(tf$prev_mean, digits = 1)
tf$mean_1<-round(tf$mean, digits = 1)
tf$sex_id<-tf$sex

colnames(tf)

aggdata<-aggregate(cbind(prev_110,prev_120,prev_121,prev_125,prev_126,prev_139,prev_140, 
                         prev_lower_110,prev_lower_120,prev_lower_121,prev_lower_125,
                         prev_lower_126,prev_lower_139,prev_lower_140, prev_upper_110,prev_upper_120,prev_upper_121,
                         prev_upper_125,prev_upper_126,prev_upper_139,prev_upper_140, prev_HbA1C_6_5) ~ age + sex , data = tf, mean)

colnames(tf)
tf_mean<-reshape(aggdata,
                 varying=c("prev_110","prev_120", "prev_121"  ,    "prev_125",    "prev_139", "prev_140" ,'prev_HbA1C_6_5'
                                                ),
                 timevar="cd" , v.names="prev", 
                 times=c("prev_110","prev_120", "prev_121"  ,    "prev_125",     "prev_139", "prev_140" ,'prev_HbA1C_6_5'
                     )  ,    
                 direction="long")
tf_mean$type<-substr(tf_mean$cd, 6, 8)
str(tf_mean)
tf_mean$ratio<-tf_mean$prev_126/tf_mean$prev
tf_mean$sex_id<-tf_mean$sex
tf_mean$age1<-tf_mean$age

mround <- function(x,base){ 
  base*round(x/base) 
} 
cw1$age1<-mround(cw1$age,5)
table(cw1$age1)
hba1c_cw$age1<-mround(hba1c_cw$age,5)
table(hba1c_cw$sex_id)

aggdata1<-aggregate (ratio ~ age + sex + type, data = tf_mean, mean)
aggdata1$age1<-aggdata1$age
aggdata1$sex_id<-aggdata1$sex


merge_1<-merge(aggdata1,cw1, by=c('age1','sex_id','type'), all.x=T)
merge_1$mean<-merge_1$ratio*merge_1$mean
merge_1$lower<-merge_1$ratio*merge_1$lower
merge_1$upper<-merge_1$ratio*merge_1$upper

#######
#addupdated file to original file

cw_prev_keep<-cw[cw$measure!='prevalence',]
cw_prev_keep<-as.data.table(cw_prev_keep)
cw1_keep<-cw_prev[cw_prev$CD =='HbA1C ? 6.5%' & cw_prev$CD =='HbA1C ? 6.5% OR Treatment',]
cw1_keep<-as.data.table(cw1_keep)

table(test4$sex)
table(merge_1$sex.y)
merge_1$sex<-merge_1$sex.y
test4_1<-merge(test4,merge_1, by=c('urbanicity_type','nid','age_start','age_end', 'year_start','year_end','location_id','measure','sex'), all.x=T)
write.csv(filepath)
  