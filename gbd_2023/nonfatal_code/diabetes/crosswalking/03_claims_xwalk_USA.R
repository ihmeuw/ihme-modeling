################################################################################
#Purpose: Date prep - Claims data crosswalk - USA
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
invisible(sapply(list.files('FILEPATH', full.names = TRUE), source)) 

#load unique functions
output_path<-'FILEPATH' 

#get bundle version to get the claims data
bundle<- get_bundle_version(44538)
new<-get_crosswalk_version(42970) # nhanes need post crosswalked claims data

#location information
locs<-get_location_metadata(location_set_id=9, release_id=16)
locs_min<-locs %>% dplyr::select(location_id, location_name)

#get age information
age<-get_age_metadata(age_group_set_id = 32, release_id=16)
age<-age %>% dplyr::select(age_group_id, age_group_years_start, age_group_years_end)

#USA
states<- locs %>% filter(parent_id==102) %>% dplyr::select(location_id, location_name)
states_id<- unique(states$location_id)
states_id<- c(states_id, 102)

#-------------------------------------------------------------------------------
# USA crosswalking - MARKETSCAN ONLY
#-------------------------------------------------------------------------------
  
  nhanes<- new %>% filter(study_name %like% "NHANES" & year_start %in% c(1999, 2009,2011,2013, 2015, 2017))
  
  claims<- bundle %>% filter(clinical_data_type!="" & location_id %in% states_id & age_start>=15 & year_start<2020 & !(field_citation_value %like% "CMS"))
  
  #aggregate to national level using cases and sample size for matched pairs
  claims_us<- claims %>% group_by(nid, sex, year_start, year_end, age_start, age_end) %>% 
                         summarise(cases=sum(cases), sample_size=sum(sample_size))
  #calculate mean, se, lower ,and upper from aggregated national values 
  claims_us<- claims_us %>% mutate(mean=cases/sample_size, 
                                                       standard_error=sqrt(mean*(1-mean)/sample_size),
                                                       lower=mean -1.96*standard_error,
                                                       upper=mean+1.96*standard_error,
                                                       location_name="United States of America",
                                                       location_id=102)
  claims<-copy(claims_us)
  unique(nhanes$measure)
  unique(claims$location_id)
  
  #-------------------------------------------------------------------------------
  # SET UP FOR MATCHED PAIRS
  #-------------------------------------------------------------------------------
  # prepare matching year
  nhanes$match_year<-nhanes$year_start
  nhanes<-as.data.table(nhanes)
  nhanes<-nhanes[,.(nid, sex, match_year, year_start, year_end, age_start, age_end, mean, standard_error, lower, upper, location_name, location_id)]
  nhanes$source<-"nhanes"
  #nhanes<-nhanes[,source:=paste0("nhanes_", year_start)]
  
  claims<-as.data.table(claims)
  #Creating matche years for the claims data
  claims<-claims[year_start==2000, match_year:=1999]
  claims<-claims[year_start==2010, match_year:=2009]
  claims<-claims[year_start %in% c(2011,2012), match_year:=2011]
  claims<-claims[year_start %in% c(2013,2014), match_year:=2013]
  claims<-claims[year_start %in% c(2015,2016), match_year:=2015]
  claims<-claims[year_start %in% c(2017,2018,2019), match_year:=2017]
  #source will the the alt definition col
  claims<-claims[,source:=paste0("claims_",year_start)]
  #claims$source<-"claims"
  df<-rbind(claims, nhanes, fill=TRUE)
  
  #create id variable 
  #matching on age_start (since all 5 year age bins), match_year, sex
  df<- df %>% mutate(
    id= paste(age_start, match_year, sex, sep="|"), #id var
    orig_row=1:nrow(.))  
  
  n_distinct(df$id)
  
  #-------------------------------------------------------------------------------
  # Finding matched pairs 
  #-------------------------------------------------------------------------------
      
      #set up definitions
      method_var<- "source"
      gold_def<- "nhanes"
      keep_vars<- c("orig_row", "id", "mean", "standard_error")  
      
      #run match for 1999/2000--------------------------------------------------------
      df_1<-copy(df)
      df_1<-data.frame(df_1)
      
      df_matched_1<- do.call("rbind", lapply(unique(df_1$id), function(i) {
        dat_i<- filter(df_1, id==i) %>% mutate(dorm=get(method_var))
        keep_vars<- c("dorm", keep_vars)
        row_ids<- expand.grid(idx1=1:nrow(dat_i), idx2=1:nrow(dat_i))
        do.call("rbind", lapply(1:nrow(row_ids), function(j){
          dat_j <- row_ids[j, ]
          dat_j[, paste0(keep_vars, "_alt")] <- dat_i[dat_j$idx1, keep_vars]
          dat_j[, paste0(keep_vars, "_ref")] <- dat_i[dat_j$idx2, keep_vars]
          filter(dat_j, dorm_alt != gold_def & dorm_alt != dorm_ref)
        })) %>% mutate(id=i) %>% dplyr::select(-idx1, -idx2)
      }))
      
      colnames(df_matched_1)
      #since we only want direct comparison
      df_matched_1<-df_matched_1 %>% filter(dorm_ref=="nhanes")
      write.csv(df_matched_1, paste0(output_path,"all_matched_rows_",date,".csv"), row.names = F)  
      
  #-------------------------------------------------------------------------------
  # Delta transform: Linear to logit
  #-------------------------------------------------------------------------------
  
    #read in matched rows
    df_tologit<- read_csv(paste0(output_path,date,"/all_matched_rows_",date,".csv"))
    df_tologit<-as.data.table(df_tologit)
      
    df_tologit<-df_tologit[,c("age_start", "match_year", "sex") := tstrsplit(id, "|", fixed = TRUE)]
      
    # drop 0 | 1
    df_tologit<-df_tologit[!(mean_ref %in% c(0,1) | mean_alt %in%c(0,1)),]
    
    df_tologit_all<-df_tologit
    alt<-unique(df_tologit$dorm_alt)
    
    cols<-c("dorms", "cov_names", "beta", "beta_sd", "gamma")
    beta_all <- data.frame(matrix(ncol = length(cols), nrow = 0))
    xwalk<-data.frame()
    #loops through each unqieu claims year data
    
    for (i in alt) {
      
      #get the alternative defs matched rows
        df_tologit<-df_tologit_all[dorm_alt==i,]
        
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
              covs = list(),            # list of (potential) covariate column names
              study_id = "id"          # var for random intercepts; i.e. (1|study_id)
            )
            
            fit1 <- cw$CWModel(
              cwdata = dat1,           # result of CWData() function call
              obs_type = "diff_logit",   # must be "diff_logit" or "diff_log"
              cov_models = list(       # specify covariate details
                cw$CovModel("intercept") ),
              gold_dorm = "nhanes"  # level of 'ref_dorms' that's the gold standard
            )
            
            fit1$fit()  
            
            df_tmp<-as.data.table(fit1$create_result_df())
            df_tmp2<-df_tmp %>% dplyr::dplyr::select(cols) %>% filter(dorms==i)
            df_tmp2$gamma<- unique(df_tmp[!is.na(gamma),]$gamma)
            beta_all<-rbind(beta_all,df_tmp2)
    }   
          
          write_csv(beta_all, paste0(output_path,"betas_year_to_year_",date, ".csv"))
             
  
              
          #-------------------------------------------------------------------------------
          # apply crosswalks
          #------------------------------------------------------------------------------- 
          
          # #first convert nation levels for vetting
          #   df2<-as.data.table(copy(claims_us))
          #   df2<df2[,source:=paste0("claims_",year_start)]
          #   df2<-df2[source==i,]
          #get subnational claims data that needs to be crosswalked
          df2<- bundle %>% filter(clinical_data_type!="" & location_id %in% states_id & age_start>=15 & year_start<2020 &year_start>=2000 & !(field_citation_value %like% "CMS"))
          df2<-as.data.table(df2)
          df2<df2[,source:=paste0("claims_",year_start)]
          df2<-df2[source==i,]
          #count the rows of data that needs to be crosswalked
          points<- nrow(df2)
          
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
          
          xwalk<-bind_rows(xwalk,df2)
    #}    
        
        nrow(xwalk)
        check<- bundle %>% filter(clinical_data_type!="" & location_id %in% states_id & age_start>=15 & year_start<2020 &year_start>=2000 & !(field_citation_value %like% "CMS"))
        nrow(check)
        #write out post crosswalked rows for the specific definition
        write_csv(xwalk, paste0(output_path,date,"/claims_xwalked_all_subnat.csv"))
              
  #---------------------------crosswalk completed------------------------------          
  