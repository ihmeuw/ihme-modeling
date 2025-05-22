################################################################################
#Purpose: Date prep - Claims data crosswalk - POLAND
#Date: 
#Description:
#       1. LOAD CROSSWALK VERSION
#       2. CREATE MATCHES
#       3. DELTA TRANSFORM
#       4. PREDICT COEFFICIENTS
#       5. CROSSWALK ALTERNATIVE CASE DEFINITIONS
#NOTE:  this script drops 0 values from the alternative defintion microdata file
################################################################################


rm(list=ls())
date <- gsub("-", "_", Sys.Date())


#Load crosswalking package
library(reticulate)
reticulate::use_python('FILEPATH')
cw <- reticulate::import("crosswalk")

#Source central functions
invisible(sapply(list.files('FILEPATH', full.names = T), source))

#load other packages
library(dplyr)
library(readr)
library(data.table)
library(tidyr)
library(ggplot2)
library(stringr)

#load shared functions
invisible(sapply(list.files("'FILEPATH'", full.names = TRUE), source)) 

#load unique functions
output_path<- 'FILEPATH'

#get bundle version to get the claims data
bundle<- get_bundle_version(44538)
new<-get_crosswalk_version(42970) 

#location information
locs<-get_location_metadata(location_set_id=9, release_id=16)
locs_min<-locs %>% dplyr::select(location_id, location_name)

#get age information
age<-get_age_metadata(age_group_set_id = 32, release_id=16)
age<-age %>% dplyr::select(age_group_id, age_group_years_start, age_group_years_end)

#POLAND
pol<- locs %>% filter(parent_id==51) %>% dplyr::select(location_id, location_name)
poland_id<- unique(pol$location_id)
poland_id<- c(poland_id, 51)


#-------------------------------------------------------------------------------
# POLAND CROSSWALKING TO LIPIDOGRAM
#-------------------------------------------------------------------------------
  
  lipid<- new %>% filter(study_name %like% "LIPIDOGRAM" & is_outlier==0)
  #given we only have 1 reference survey covering years 2015 and 2016, will only use claims 2015 and 2016 to created match pairs for the beta
  claims<- bundle %>% filter(clinical_data_type!="" & location_id %in% poland_id & age_start>=15 & year_start %in% c(2015,2016)) 
  
  #aggregate to national level using cases and sample size for matched pairs
  claims_national<- claims %>% group_by(nid, sex, year_start, year_end, age_start, age_end) %>% 
                         summarise(cases=sum(cases), sample_size=sum(sample_size))
  #calculate mean, se, lower ,and upper from aggregated national values 
  claims_national<- claims_national %>% mutate(mean=cases/sample_size, 
                                                       standard_error=sqrt(mean*(1-mean)/sample_size),
                                                       lower=mean -1.96*standard_error,
                                                       upper=mean+1.96*standard_error,
                                                       location_name="Poland",
                                                       location_id=102)
  claims<-copy(claims_national)
  unique(lipid$measure)
  unique(claims$location_id)  
  unique(claims$year_start)

  
  #-------------------------------------------------------------------------------
  # SET UP FOR MATCHED PAIRS
  #-------------------------------------------------------------------------------
  # prepare matching year
  lipid<-as.data.table(lipid)
  lipid<-lipid[,.(nid, sex, year_start, year_end, age_start, age_end, mean, standard_error, lower, upper, location_name, location_id)]
  lipid$source<-"lipidogram"
  #nhanes<-nhanes[,source:=paste0("nhanes_", year_start)]
  
  claims<-as.data.table(claims)

  #source will the the alt definition col
  claims<-claims[,source:="claims"]
  #claims$source<-"claims"
  df<-rbind(claims, lipid, fill=TRUE)
  
  #create id variable 
  #matching on age_start (since all 5 year age bins), sex, no need to match on year here since only have 1 data source and both claims years will be matched to this single survey
  df<- df %>% mutate(
    id= paste(age_start, sex, sep="|"), #id var
    orig_row=1:nrow(.))  
  
  n_distinct(df$id)
  
  #-------------------------------------------------------------------------------
  # Finding matched pairs 
  #-------------------------------------------------------------------------------
      
      #set up definitions
      method_var<- "source"
      gold_def<- "lipidogram"
      keep_vars<- c("orig_row", "id", "mean", "standard_error")  
      
      #run match for 1999/2000--------------------------------------------------------
      df_1<-copy(df)
      df_1<-data.frame(df_1)
      
      df_matched_1<- do.call("rbind", lapply(unique(df_1$id), function(i) {
        dat_i<- filter(df_1, id==i) %>% mutate(dorm=get(method_var))
        keep_vars<- c("dorm", keep_vars)
        row_ids<- expand.grid(idx1=1:nrow(dat_i), idx2=1:nrow(dat_i))
        if ( nrow(row_ids)>1 ) {
          do.call("rbind", lapply(1:nrow(row_ids), function(j){
            dat_j <- row_ids[j, ]
            dat_j[, paste0(keep_vars, "_alt")] <- dat_i[dat_j$idx1, keep_vars]
            dat_j[, paste0(keep_vars, "_ref")] <- dat_i[dat_j$idx2, keep_vars]
            filter(dat_j, dorm_alt != gold_def & dorm_alt != dorm_ref)
          })) %>% mutate(id=i) %>% dplyr::select(-idx1, -idx2)
        }
      }))
      #since we only want direct compairson, remove dorm ref thats claims
      df_matched_1<-df_matched_1 %>% filter(dorm_ref=="lipidogram") # should have 34 rows
      colnames(df_matched_1)

      write.csv(df_matched_1, paste0(output_path,date,"/all_matched_rows_",date,".csv"), row.names = F)  
      
  #-------------------------------------------------------------------------------
  # Delta transform: Linear to logit
  #-------------------------------------------------------------------------------
  
    #read in matched rows
    df_tologit<- read_csv(paste0(output_path,date,"/all_matched_rows_",date,".csv"))
    df_tologit<-as.data.table(df_tologit)
      
    df_tologit<-df_tologit[,c("age_start", "sex") := tstrsplit(id, "|", fixed = TRUE)]
    df_tologit<-df_tologit[,sex_id:=ifelse(sex=="Female",1,0)] #male is 0 female is 1 
    # drop 0 | 1
    df_tologit<-df_tologit[!(mean_ref %in% c(0,1) | mean_alt %in%c(0,1)),] #drops youngest and oldest age groups basically
    
    xwalk<-data.frame()

      #-------------------------------------------------------------------------------
      # Convert to logit space
      #-------------------------------------------------------------------------------
    
      #transform linear to logit for reference and alternative values
        #alternative 
        df_tologit[, c("alt_mean2", "alt_se2")]<-as.data.frame(
          cw$utils$linear_to_logit(
            mean = array(df_tologit$mean_alt),
            sd = array(df_tologit$standard_error_alt))
        )
      
      #reference
        df_tologit[, c("ref_mean2", "ref_se2")] <- as.data.frame(
          cw$utils$linear_to_logit(
            mean = array(df_tologit$mean_ref),
            sd = array(df_tologit$standard_error_ref)))
        
      #get the logit different 
        df_tologit<-  df_tologit %>%
          mutate(
            ydiff_log_mean = alt_mean2 - ref_mean2,
            ydiff_log_se = sqrt(alt_se2^2 + ref_se2^2)
          )
      
                                           
        #-------------------------------------------------------------------------------
        # Run crosswalk model
        #-------------------------------------------------------------------------------
  
            # CWData(), CovModel() and CWModel() functions
            df_tologit$id <- as.integer(as.factor(df_tologit$id)) # ...housekeeping
            colnames(df_tologit)
            
            
            # format data for meta-regression; pass in data.frame and variable names
            dat1 <- cw$CWData(
              df = df_tologit,
              obs = "ydiff_log_mean",   # matched differences in log space
              obs_se = "ydiff_log_se",  # SE of matched differences in log space
              alt_dorms = "dorm_alt",   # var for the alternative def/method
              ref_dorms = "dorm_ref",   # var for the reference def/method
              covs = list("sex_id"),            # list of (potential) covariate column names
              study_id = "id"          # var for random intercepts; i.e. (1|study_id)
            )
            
            fit1 <- cw$CWModel(
              cwdata = dat1,           # result of CWData() function call
              obs_type = "diff_logit",   # must be "diff_logit" or "diff_log"
              cov_models = list(       # specify covariate details
                cw$CovModel("intercept"),
                cw$CovModel("sex_id")),
              gold_dorm = "lipidogram"  # level of 'ref_dorms' that's the gold standard
            )
            
            fit1$fit(inlier_pct = .9)  #trimmed given some unstable high values from lipidogram
            
            df_tmp<-as.data.table(fit1$create_result_df())
          
          write_csv(df_tmp, paste0(output_path,date,"/betas_",date, ".csv"))
          
          #-------------------------------------------------------------------------------
          # Funnel plots 
          #------------------------------------------------------------------------------- 
  
          plots <- import("crosswalk.plots")

          plots$funnel_plot(
            cwmodel = fit1,
            cwdata = dat1,
            continuous_variables = list(),
            obs_method = "claims",
            plot_note = paste0("POLAND LIPIDOGRAM vs claims (2015 + 2016)"),
            plots_dir = paste0(output_path,date,"/"),
            file_name = paste0("funnel_plot_",date),
            write_file = T
          )

          print(plots)
              
  
              
#-------------------------------------------------------------------------------
# apply crosswalks
#------------------------------------------------------------------------------- 
          
  # first convert nation levels for vetting--------------------------------------
    
    claims_all<- bundle %>% filter(clinical_data_type!="" & location_id %in% poland_id & age_start>=15) # now include all years 
  
  #aggregate to national level using cases and sample size for matched pairs
  claims_national_all<- claims_all %>% group_by(nid, sex, year_start, year_end, age_start, age_end) %>% 
    summarise(cases=sum(cases), sample_size=sum(sample_size))
  #calculate mean, se, lower ,and upper from aggregated national values 
  claims_national_all<- claims_national_all %>% mutate(mean=cases/sample_size, 
                                               standard_error=sqrt(mean*(1-mean)/sample_size),
                                               lower=mean -1.96*standard_error,
                                               upper=mean+1.96*standard_error,
                                               location_name="Poland",
                                     location_id=102)
  df2<-as.data.table(copy(claims_national_all))
  df2<-df2[,source:="claims"]
  df2<-df2[,sex_id:=ifelse(sex=="Female",1,0)]

          nrow(df2)
          
          pred_tmp <- fit1$adjust_orig_vals(
            df = df2 ,
            orig_dorms = "source",
            orig_vals_mean = "mean", # mean in un-transformed space
            orig_vals_se = "standard_error"      # SE in un-transformed space
          )
          
          head(pred_tmp)
          
          #add the adjusted values back to the original dataset
          df2[, c(
            "meanvar_adjusted", "sdvar_adjusted", "pred_logit", 
            "pred_se_logit", "data_id")] <- pred_tmp
          
    #}    
        
        nrow(df2)

        #write out post crosswalked rows for the specific definition
        write_csv(df2, paste0(output_path,date,"/claims_xwalked_all_national.csv"))
              

