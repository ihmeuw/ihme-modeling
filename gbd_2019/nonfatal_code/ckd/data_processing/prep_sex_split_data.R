#--------------------------------------------------------------
# Project: Non-fatal CKD estimation 
# Purpose: Make sex-matched datasets for each CKD bundle
#--------------------------------------------------------------


# setup -------------------------------------------------------------------
user <- Sys.info()["user"]
share_path <- "FILEPATH"
code_general <- "FILEPATH"

# source packages and functions
require(data.table)
require(ggplot2)
require(logitnorm)
source(paste0(code_general, "/function_lib.R"))
source(paste0(share_path,"FILEPATH/data_processing_functions.R"))
source_shared_functions("get_bundle_version")

args<-commandArgs(trailingOnly = T)
thrds<-as.numeric(args[1])
setDTthreads(thrds)
bvid<-args[2]
print(bvid)
ds<-args[3]
apply_offset<-args[4]
offset_pct<-as.numeric(args[5])
acause<-args[6]
mod_type<-args[7]
bid<-args[8]

# set output paths
epi_path<-paste0("FILEPATH")
if(!dir.exists(epi_path)){dir.create(epi_path,recursive=T)}

plot_dir<-paste0("FILEPATH")
if(!dir.exists(plot_dir)){dir.create(plot_dir,recursive = T)}

#   -----------------------------------------------------------------------


# pull and fill data ------------------------------------------------------

# pull in bundle data
data<-get_bundle_version(bundle_version_id = bvid,export = F,transform = T)

# deal with weird slashes in albuminuria cv_cols
if (any(grepl("/",names(data)))){
  fix_col_names<-grep("/",names(data),value=T)
  new_col_names<-sub(pattern = "/",replacement = "_",fix_col_names)
  setnames(data,fix_col_names,new_col_names)
}

# drop any rows that remission data - there is no input remission data for CKD
# it's all created from CKD models so it shouldn't be used to determine sex ratios
message("dropping remission 'data'")
data<-data[!measure%in%c("remission")]

# make sure every row has mean 
if (nrow(data[is.na(mean)])>0){
  # give informative message
  missing_mean<-nrow(data[is.na(mean)])
  message(paste(missing_mean,"rows of data are missing a mean value. filling in mean as cases/sample size"))
  
  # fill in missing means as cases/ss
  data[is.na(mean)&!(is.na(cases))&!(is.na(sample_size)),paste0("mean"):=cases/sample_size]
  
  # check if rows are still missing mean
  if(nrow(data[is.na(mean)])>0){
    mids<-paste(data[is.na(mean),seq],collapse = ",")
    stop(paste("some rows of data are missing mean and cases or sample size. fix before proceeding:"),mids)
  }
}

# make sure every row has standard error
if (nrow(data[is.na(standard_error)])>0){
  # give informative message
  missing_se<-nrow(data[is.na(standard_error)&measure%in%c("prevalence","proportion")])
  message(paste(missing_se,"rows of prev/proportion data are missing a standard error value. 
                filling in with wilson score se"))
  
  # fill in missing standard error vals
  data[is.na(standard_error)&!(is.na(mean))&!(is.na(sample_size))&measure%in%c("prevalence","proportion"),
       standard_error:=wilson_se(prop=mean,n=sample_size)]
  
  # check if rows are still missing se
  if(nrow(data[is.na(standard_error)&measure%in%c("prevalence","proportion")])>0){
    missing_se<-nrow(data[is.na(standard_error)&measure%in%c("prevalence","proportion")])
    message(paste(missing_se,"rows of prev/prop data are still standard error value. 
                  approximating with mean, lower, upper"))
    
    # fill in missing standard error vals
    data[is.na(standard_error)&!(is.na(mean))&!(is.na(upper))&!(is.na(lower)),standard_error:=(upper-lower)/3.92]
    
    # check if rows are still missing se
    if(nrow(data[is.na(standard_error)&measure%in%c("prevalence","proportion")])>0){
      mids<-paste(data[is.na(standard_error)&measure%in%c("prevalence","proportion"),seq],collapse = ",")
      stop(paste("the follwing prev/prop data seqs are missing standard error and mean, sample size, lower, and upper. 
                 this should be impossible. fix before proceeding:"),mids)
    }
  }
  
  # now check incidence 
  if (nrow(data[is.na(standard_error)&measure%in%c("incidence")])>0){
    missing_se<-nrow(data[is.na(standard_error)&measure%in%c("incidence")])
    message(paste(missing_se,"rows of inc data are missing a standard error value. filling in based on mean and ss"))
    
    #back fill cases if necessary
    data[is.na(standard_error)&measure%in%c("incidence")&is.na(cases)&!is.na(sample_size)&!is.na(mean), 
         cases := mean*sample_size]
    
    data[is.na(standard_error)&measure%in%c("incidence")&!is.na(cases)&!is.na(sample_size)&!is.na(mean)&cases<5, 
         standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    data[is.na(standard_error)&measure%in%c("incidence")&!is.na(cases)&!is.na(sample_size)&!is.na(mean)&cases>=5, 
         standard_error :=sqrt(mean/sample_size)]
    
    # check if rows are still missing se
    if(nrow(data[is.na(standard_error)&measure%in%c("incidence")])>0){
      mids<-paste(data[is.na(standard_error)&measure%in%c("incidence"),seq],collapse = ",")
      stop(paste("the follwing inc seqs are missing standard error and mean, sample size, or cases. 
                 this should be impossible. fix before proceeding:"),mids)
    } 
  }
}

# Only keep sex-specific data 
message("dropping `Both` sex data")
data<-data[sex%in%c("Male","Female")]

#   -----------------------------------------------------------------------


# age aggregate -----------------------------------------------------------

# implement age aggregation - for any study with multiple age groups per sex should 
# be aggregated to represent data for the entire age range captured in the study:

# calculate the age span for each row of data
data[,age_span:=age_end-age_start]

# pull out study-level covariate columns
cv_cols<-grep("cv_",names(data),value=T)

# indicate the minimum age span for the given age_start
data[,min_age_span:=min(age_span),by=c(cv_cols,"nid","location_id","year_start",
                                       "year_end","sex","measure","age_start")]

# drop rows where the age span is greater than the minimum age span for a given
# age start
data<-data[age_span==min_age_span]

# count the number of age rows and make a vector of all age_start values for a 
# study-sex-location_id combo
data_age_agg<-data[,.(age_starts=paste(sort(unique(age_start)),collapse = ","),
                      age_ends=paste(sort(unique(age_end)),collapse = ","),
                      age_count=.N,age_start=age_start,age_end=age_end,
                      mean=mean,standard_error=standard_error,cases=cases,
                      sample_size=sample_size,seq=seq),
                   by=c("nid","underlying_nid","location_id","location_name",
                        "year_start","year_end","sex","field_citation_value","measure",cv_cols)]

# figure out what the next age group should in sequential order
data_age_agg[,next_age_start:=age_end+1]
data_age_agg[,prev_age_end:=age_start-1]

# mark which rows to aggregate across - basically ensures that for studies that
# skip age groups, the extra ages on the ends aren't included in aggregation 
# e.g. for some of the ISN data, there is a row for 15-19 and then the next row
# is 25-29. Since there is no information 
data_age_agg[,search_age_start:=paste0("^",next_age_start,",|,",next_age_start,",|,",next_age_start,"$")]
data_age_agg[,search_age_end:=paste0("^",prev_age_end,",|,",prev_age_end,",|,",prev_age_end,"$")]
data_age_agg[,aggregate_age_start:=grepl(search_age_start,age_starts),by="seq"]
data_age_agg[,aggregate_age_end:=grepl(search_age_end,age_ends),by="seq"]

# don't pay attention to this where age_count == 1 - there are no rows on either side
# of the age range for these studies
data_age_agg[age_count==1,c("aggregate_age_start","aggregate_age_end"):=T]

# remove rows where both agg_age_start and agg_age_end are FALSE - this means that data
# containing age_start and/or age_end above the the given row are not present so this 
# row shouldn't be aggregated 
data_age_agg<-data_age_agg[!(aggregate_age_start==F&aggregate_age_end==F)]

# there are a few cases where there's data from a larger age span for one sex
# than the other (e.g. we have males 15-49 but females 20-44). in these cases
# only keep the ages that overlap - the following algorith should do that...
data_age_agg[,both_sex_age_min:=min(age_start),
             by=c("nid","underlying_nid","location_id","location_name","year_start",
                  "year_end","measure",cv_cols,"field_citation_value")]
data_age_agg[,by_sex_age_min:=min(age_start),
             by=c("nid","underlying_nid","location_id","location_name","year_start",
                  "year_end","measure",cv_cols,"field_citation_value","sex")]
data_age_agg[,min_sex_count:=length(unique(by_sex_age_min)),
             by=c("nid","underlying_nid","location_id","location_name","year_start",
                  "year_end","measure",cv_cols,"field_citation_value")]
data_age_agg[min_sex_count>1&age_start==by_sex_age_min,
             drop_extra_ages_younger:=age_start<max(by_sex_age_min),
             by=c("nid","underlying_nid","location_id","location_name","year_start",
                  "year_end","measure",cv_cols,"field_citation_value")]

data_age_agg[,by_sex_age_max:=max(age_end),
             by=c("nid","underlying_nid","location_id","location_name","year_start",
                  "year_end","measure",cv_cols,"field_citation_value","sex")]
data_age_agg[,both_sex_age_max:=min(by_sex_age_max),
             by=c("nid","underlying_nid","location_id","location_name","year_start",
                  "year_end","measure",cv_cols,"field_citation_value")]
data_age_agg[,max_sex_count:=length(unique(by_sex_age_max)),
             by=c("nid","underlying_nid","location_id","location_name","year_start",
                  "year_end","measure",cv_cols,"field_citation_value")]
data_age_agg[max_sex_count>1&age_end>=both_sex_age_max,
             drop_extra_ages_older:=age_end>both_sex_age_max,
             by=c("nid","underlying_nid","location_id","location_name","year_start",
                  "year_end","measure",cv_cols,"field_citation_value")]

extra_age_groups<-nrow(data_age_agg[(drop_extra_ages_older==T|drop_extra_ages_younger==T)])
message(paste("dropping rows where age range for one sex expands beyond that of the other sex. 
               affects",extra_age_groups,"rows"))
data_age_agg<-data_age_agg[!(drop_extra_ages_older%in%(T)|drop_extra_ages_younger%in%(T))]

# drop all the extra columns that were just created
drop_cols<-c("next_age_start","prev_age_end","search_age_start","search_age_end",
             "aggregate_age_start","aggregate_age_end","age_starts","age_ends",
             "age_count","both_sex_age_min","by_sex_age_min","min_sex_count",
             "drop_extra_ages_younger","by_sex_age_max","both_sex_age_max",
             "max_sex_count","drop_extra_ages_older")
data_age_agg[,(drop_cols):=NULL]

# can't aggregate rows without cases and sample size - shouldn't be a large proportion
# of data - drop these for now
missing_cases_ss<-nrow(data_age_agg[is.na(cases)|is.na(sample_size)])
message(paste("dropping rows that are missing cases or sample size - dropped",
              missing_cases_ss,"of",nrow(data_age_agg),"rows"))
data_age_agg<-data_age_agg[!(is.na(cases)|is.na(sample_size))]

# aggregate cases and sample size for each study-sex-year-measure-covaraite combo
data_age_agg[,c("aggregate_cases","aggregate_sample_size"):=lapply(.SD,sum),
             .SDcols=c("cases","sample_size"),
             by=c("nid","underlying_nid","location_id","location_name",
                  "year_start","year_end","sex","measure",cv_cols,
                  "field_citation_value")]
data_age_agg[,aggregate_age_start:=min(age_start),by=c("nid","underlying_nid","location_id","location_name",
                                                       "year_start","year_end","sex","measure",cv_cols,
                                                       "field_citation_value")]
data_age_agg[,aggregate_age_end:=max(age_end),by=c("nid","underlying_nid","location_id","location_name",
                                                "year_start","year_end","sex","measure",cv_cols,
                                                "field_citation_value")]
data_age_agg[,aggregate_mean:=aggregate_cases/aggregate_sample_size]
data_age_agg[,aggregate_standard_error:=wilson_se(prop=aggregate_mean,n=aggregate_sample_size)]

#   -----------------------------------------------------------------------


# plot age aggregates  ----------------------------------------------------

# Plot each NID with sex-specific and aggregated data for vetting purposes
plot_dt<-copy(data_age_agg)
plot_dt[,age_midpoint:=(age_start+age_end)/2]
plot_dt<-melt(plot_dt,id.vars=names(plot_dt)[!(names(plot_dt)%in%c("mean","cases","sample_size","standard_error",
                                                                   "lower","upper","age_start","age_end","aggregate_mean",
                                                                   "aggregate_cases","aggregate_sample_size",
                                                                   "aggregate_age_end","aggregate_standard_error",
                                                                   "aggregate_age_start"))])
plot_dt[variable%like%"aggregate",aggregated:="age_aggregated"]
plot_dt[is.na(aggregated),aggregated:="age_specific"]
plot_dt[,variable:=gsub("aggregate_","",variable)]
lhs<-paste(names(plot_dt)[!names(plot_dt)%in%c("variable","value")],collapse = "+")
plot_dt<-dcast(plot_dt,as.formula(paste(lhs,"variable",sep = "~")),value.var = "value")
max_prev<-max(plot_dt[,mean])
# add function to wrap title text
wrapper <- function(x, ...)
{
  paste(strwrap(x, ...), collapse = "\n")
}
message(paste("writing plots to"),plot_dir)

pdf(file = "FILEPATH.pdf"),width=10,height=8)
for(id in unique(plot_dt[,nid])){
  plot_dt_nid<-copy(plot_dt[nid==id])
  for (lid in unique(plot_dt_nid[,location_id])){
    plot_dt_nid_lid<-copy(plot_dt_nid[location_id==lid])
    for (year_sid in unique(plot_dt_nid_lid[,year_start])){
      plot_dt_nid_lid_yrs<-copy(plot_dt_nid_lid[year_start==year_sid])
      gg<-ggplot(data=plot_dt_nid_lid_yrs,aes(x=age_midpoint,y=mean))+
        geom_errorbarh(aes(xmax=age_start,xmin=age_end,color=sex),height=0,alpha=0.75)+
        geom_point(aes(color=sex),alpha=0.5,size=2)+
        facet_wrap(aggregated+location_name~measure,scales = "free")+
        theme_bw()+
        ggtitle(wrapper(paste(unique(plot_dt_nid_lid_yrs[,field_citation_value]),
                              unique(plot_dt_nid_lid_yrs[,year_start])), width = 150))+
        theme(plot.title = element_text(size=8))+
        scale_x_continuous(limits=c(0,100),expand=c(0,0))+
        scale_y_continuous(limits=c(0,max_prev),expand = c(0,0))
      print(gg)
    }   
  }
}
dev.off()

#   -----------------------------------------------------------------------


# make sex matches --------------------------------------------------------

# subset to useful columns
match_cols<-append(c("underlying_nid","nid","location_name","location_id","sex","year_start","year_end",
                     "aggregate_age_start","aggregate_age_end","measure","aggregate_mean",
                     "aggregate_standard_error","aggregate_cases","aggregate_sample_size"),cv_cols)
data_age_agg<-unique(data_age_agg[,..match_cols])

# seperate into male and female datasets
data_m<-data_age_agg[sex=="Male"]
data_f<-data_age_agg[sex=="Female"]

# set names of measure cols for male and female data then drop sex
measure_cols<-c("mean","standard_error","cases","sample_size")
aggregate_measure_cols<-paste0("aggregate_",measure_cols)
lapply(list(data_m,data_f),function(x){
  sex_lab<-unique(tolower(x[,sex]))
  setnames(x,aggregate_measure_cols,paste0(measure_cols,"_",sex_lab))
  x[,sex:=NULL]
})

# merge back together
data<-merge(data_m,data_f,by=intersect(names(data_m),names(data_f)))

# If offset is TRUE, add a %age of the median of all non-zero datapoints
if (apply_offset==F){
  message(paste("applying offset to data where both num and denom are 0. offset is calculated to be", 
                offset_pct,"% of the median of all non-zero datapoints."))
  offset_val<-median(c(data[mean_male!=0,mean_male],
                       data[mean_female!=0,mean_female]),
                     na.rm = T)*offset_pct
  data[mean_male==0|mean_female==0,offset_flag:=1]
  data[mean_male==0,mean_male:=offset_val]
  data[mean_female==0,mean_female:=offset_val]
  # Recalculate SE after applying the offset
  data[offset_flag==1&mean_male==offset_val,standard_error_male:=wilson_se(prop=mean_male,n = sample_size_male)]
  data[offset_flag==1&mean_female==offset_val,standard_error_female:=wilson_se(prop=mean_female,n = sample_size_female)]
}

# Transform male and female data into appropriate space
if (mod_type=="log_diff"){
  message("Calculating log ratio and log ratio se")
  data[,ratio:=mean_female/mean_male]
  
  # Log transform ratio
  data[,log_ratio:=log(ratio)]
  
  # Calculate SE of the ratio with the formula presented in crosswalking guide
  data[,ratio_se:=prop_se_div(m1 = mean_female,
                              se1 = standard_error_female, 
                              m2 = mean_male, 
                              se2 = standard_error_male)]
  
  if (apply_offset==T){
    message("dropping rows where ratio and ratio SE are NA or inf b/c offsetting was not implemented")
    # If offset if FALSE drop any place where mean or SE is NA 
    drop_count<-nrow(data[(is.na(ratio)|is.na(ratio_se)|is.infinite(ratio)|is.na(ratio_se))])
    tot_count<-nrow(data)
    data<-data[!(is.na(ratio)|is.na(ratio_se)|is.infinite(ratio)|is.na(ratio_se))]
    message(paste0("dropped ",drop_count," of ", tot_count," rows"))
  }
  
  # Calculate SE of the xformed ratio using the delta method
  message("applying the delta method to derive standard error of the log-transformed ratio")
  data[,log_ratio_se:=sapply(1:nrow(data), function(i){
    ratio_i <- data[i,ratio]
    ratio_se_i <- data[i,ratio_se]
    deltamethod(~log(x1),ratio_i,ratio_se_i^2)
  })]
  
  setnames(data,c("ratio","ratio_se","log_ratio","log_ratio_se"),c("val","val_se","xform_val","xform_val_se"))
}

if (mod_type=="logit_diff"){
  message("Calculating logit difference and logit difference se")
  # Logit the study means
  data[,logit_female:=logit(mean_female)]
  data[,logit_male:=logit(mean_male)]
  
  # Calcualte to difference
  data[,logit_difference:=logit_female-logit_male]
  
  # Calculate SE both logit means
  message("applying the delta method to derive standard error of the logit-transformed means")
  data[,logit_female_se:=sapply(1:nrow(data), function(i){
    mean_i <- data[i,mean_female]
    mean_se_i <- data[i,standard_error_female]
    deltamethod(~log(x1/(1-x1)),mean_i,mean_se_i^2)
  })]
  data[,logit_male_se:=sapply(1:nrow(data), function(i){
    mean_i <- data[i,mean_male]
    mean_se_i <- data[i,standard_error_male]
    deltamethod(~log(x1/(1-x1)),mean_i,mean_se_i^2)
  })]
  
  # Assume additive variance to calculate SE of logit diff
  data[,logit_difference_se:=sqrt(logit_female_se^2+logit_male_se^2)]
  
  if (apply_offset==F){
    message("dropping rows where ratio and ratio SE are NA or inf b/c offsetting was not implemented")
    # If offset is FALSE drop any place where mean or SE is NA 
    drop_count<-nrow(data[(is.na(logit_difference)|is.na(logit_difference_se)|is.infinite(logit_difference)|is.infinite(logit_difference_se))])
    tot_count<-nrow(data)
    data<-data[!(is.na(logit_difference)|is.na(logit_difference_se)|is.infinite(logit_difference)|is.infinite(logit_difference_se))]
    message(paste0("dropped ",drop_count," of ", tot_count," rows"))
  }
  
  setnames(data,c("logit_difference","logit_difference_se"),c("xform_val","xform_val_se"))
  
}

# Write outputs
offset_lab<-ifelse(apply_offset,paste0("_offset_",offset_pct),"_no_offset")
write.csv(x = data, file = "FILENAME.csv"),row.names = F)

#   -----------------------------------------------------------------------


