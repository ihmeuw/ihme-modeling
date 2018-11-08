#create age pattern for dm_type 1 for older age groups

source(filepath)
df<-get_epi_data(bundle_id = bundle_id)
df<-as.data.frame(df)

#create file with nids and age_start
keep<-c('nid','location_name','measure','age_start','age_end','is_outlier','group_review')
age<-df[keep]
age<-unique(age)
age<-age[age$measure=='incidence' & age$is_outlier==0 & (is.na(age$group_review) | age$group_review ==0),]
age_sheet<-table(age$nid, age$age_start)
write.csv(age_sheet, filepath)

# post formatting file
age_format<-read.xlsx(filepath)
keep<-'nid'
child<-age_format[keep]


###Read in files for calculation
calc<-read.xlsx(filepath)

########Create file to collapse
keep<-c('age_start','sex','year_start','year_end','is_outlier','group_review','measure','age_end','nid','location_name','location_id','standard_error','cases','mean','lower','upper')

df.1<-df[keep]
df.2<-merge(df.1,child, by='nid',all.y=T)
df.2<-df.2[df.2$measure=='incidence' & df.2$is_outlier==0 & (is.na(df.2$group_review) | df.2$group_review ==0),]

table(df.2$nid[is.na(df.2$cases)])

#back calculate sample_size for pyrs

#hard code
df.2$cases<-ifelse(df.2$nid==225240, 278, df.2$cases)
df.2$cases<-ifelse(df.2$nid==227946, 245, df.2$cases)
df.2$cases<-ifelse(df.2$nid==336208 & df.2$sex=='Female' , 42, df.2$cases)
df.2$cases<-ifelse(df.2$nid==336208 & df.2$sex=='Male' , 30, df.2$cases)
df.2$cases<-ifelse(df.2$nid==336444 & df.2$sex=='Male' & df.2$age_start==0, 53.69432314, df.2$cases)
df.2$cases<-ifelse(df.2$nid==336444 & df.2$sex=='Male' & df.2$age_start==5, 99.98253275, df.2$cases)
df.2$cases<-ifelse(df.2$nid==336444 & df.2$sex=='Male' & df.2$age_start==10,173.1179039, df.2$cases)
df.2$cases<-ifelse(df.2$nid==336444 & df.2$sex=='Female' & df.2$age_start==0, 62.30567686, df.2$cases)
df.2$cases<-ifelse(df.2$nid==336444 & df.2$sex=='Female' & df.2$age_start==5, 116.0174672, df.2$cases)
df.2$cases<-ifelse(df.2$nid==336444 & df.2$sex=='Female' & df.2$age_start==10,200.8820961, df.2$cases)

df.2$sample_size<-df.2$cases/df.2$mean

#collapse to 0-15 age groups. calculate mean and se
df.3.cases<-aggregate(cases~nid+sex+location_id+year_start+year_end, df.2, FUN=sum)
df.3.sample_size<-aggregate(sample_size~nid+sex+location_id+year_start+year_end, df.2, FUN=sum)
df.3<-merge(df.3.cases,df.3.sample_size, by=c('nid','year_start','sex','location_id','year_end'),all=T)
df.3$mean<-df.3$cases/df.3$sample_size
df.3$lower<-df.3$cases*(1-1/(9*df.3$cases)-1.96/(3*sqrt(df.3$cases)))^3/df.3$sample_size
df.3$upper<-(df.3$cases+1)*(1-1/(9*df.3$cases+1)+1.96/(3*sqrt(df.3$cases+1)))^3/df.3$sample_size
df.3$standard_error<-(df.3$upper-df.3$lower)/(2*1.96)

####Create new age groups
create<-merge(df.3, calc, by='sex', all=T)
create$mean_new<-create$ratio*create$mean
create$standarad_error_new<-sqrt(create$standard_error.x^2*create$ratio_se^2+create$standard_error.x^2*create$ratio^2+create$ratio_se^2*create$mean^2)

#####
##add to original file to see all full pattern
#####
create.test<-as.data.table(create)
df.test<-as.data.table(df.2)
df.test$mean_new<-df.test$mean
test<-rbind(create.test, df.test, fill=T)

library(ggplot2)
ggplot(test,aes(x=age_start, y=mean_new, color(sex)))+facet_wrap(~nid)+geom_point()


###Create data upload sheet
all<-create
all$standard_error<-all$standard_error_new
all$mean<-all$mean_new
all$age_end<-all$age_start+4
all$bundle_id <-NA
all$bundle_name <- NA
all$nid <-356533
all$seq<-NA
all$bundle_id <-NA
all$bundle_name <- NA
all$field_citation_value <- NA
all$source_type <- 'Unidentifiable'
all$smaller_site_unit <- NA
all$age_demographer <- NA
all$age_issue <- NA
all$sex_issue <- NA
all$year_issue <- NA
all$unit_type <- 'Person'
all$unit_value_as_published <-1
all$measure_adjustment <- NA
all$measure_issue <- NA
all$measure <- 'incidence'
all$case_definition <- NA
all$note_modeler <- NA
all$extractor <-'ongl'
all$is_outlier <- 0
all$underlying_nid <- NA
all$sampling_type <- NA
all$representative_name <-'Unknown'
all$urbanicity_type <- 'Unknown'
all$recall_type <-'Not Set'
all$uncertainty_type <- 'Standard error'
all$input_type <- NA
all$standard_error <- NA
all$effective_sample_size <- NA
all$design_effect <- NA
all$site_memo <- NA
all$case_name <- NA
all$case_diagnostics <- NA
all$response_rate <- NA
all$note_SR <- 'Calculated Dm Type 1 age_pattern'
all$uncertainty_type_value <-95
all$seq_parent <- NA
all$recall_type_value <- NA


all[is.na(all)]<-''
all<-all[,-which(names(all) %in% c('standard_error.y','standarad_error_new','mean_new','ratio','ratio_se', 'standard_error.x','sex_id'))]
write.xlsx(all,'FILEPATH/type1_age_pattern_071518.xlsx',sheetName='extraction')

#upload data
source('FILEPATH/upload_epi_data.R')
upload_epi_data(bundle_id = 3218,'/FILEPATH/type1_age_pattern_071518.xlsx')

#######
pdf('FILEPATH/t1.pdf')
for (i in unique(create.test$location_id)){
  temp<-create.test[create.test$location_id==i]
p<-  ggplot(temp,aes(x=age_start, y=mean_new, color(nid)))+facet_wrap(~sex)+geom_point()
p<-p+ggtitle (paste0(('type 1 in '),i))
  print(p)

}
dev.off()

loc<-read.csv('FILEPATH/ihme_loc_metadata_gbd2017.csv')
keep<-c('super_region_name','region_name','location_id')
loc<-loc[keep]

loc.create<-merge(loc,create.test, by=c('location_id'), all.y=T)

loc.df<-merge(loc,df.test, by=c('location_id'), all.y=T)

table(loc.df$region_name,exclude=NULL)
table(loc.create$region_name,exclude=NULL)
