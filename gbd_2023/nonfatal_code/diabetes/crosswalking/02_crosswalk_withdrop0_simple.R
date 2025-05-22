################################################################################
#Purpose: Date prep - Crosswalking Alternative Defintions - SIMPLE CROSSWALK
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

#Load crosswalking package
library(reticulate)
reticulate::use_python('FILEPATH')
cw <- reticulate::import("crosswalk")
date <- gsub("-", "_", Sys.Date())

#load other packages
library(dplyr)
library(readr)
library(data.table)
library(tidyr)
library(openxlsx)
library(ggplot2)
#load shared functions
invisible(sapply(list.files("/share/cc_resources/libraries/current/r/", full.names = TRUE), source)) 

#load unique functions
output_path<-'FILEPATH'

#-------------------------------------------------------------------------------
# get collapsed micro data for crosswalking
#-------------------------------------------------------------------------------
    df<-read.csv('FILEPATH')
    unique(df$measure)
    df<- as.data.table(df)
    
    
    #calculate standard error
    z <- qnorm(0.975)
    df[is.na(effective_sample_size), effective_sample_size:=sample_size] #some missing effective sample size, need to back to microdata to check
    df[,standard_error:=sqrt(mean*(1-mean)/effective_sample_size + z^2/(4*effective_sample_size^2))] 

#-------------------------------------------------------------------------------
# SET UP FOR MATCHED PAIRS
#-------------------------------------------------------------------------------
        #summarry prior to data drop
        sum1<-df[,total_obs:=.N]
        sum1<-sum1[,obs_by_sr:=.N, by="super_region_name"]
        sum1<-sum1[,perc:=obs_by_sr/total_obs]
        sum1<-distinct(sum1[,.(standardized_case_definition, super_region_name, perc)])
        
        ggplot(sum1, aes(x=super_region_name, y=perc, fill=super_region_name))+
          geom_col()+
          facet_wrap(~standardized_case_definition) +
          labs(title="Percent of data points from each super region - prior to dropping data",
               x="",
               y="percentage")+
          theme_bw() +
          theme(axis.text.x = element_blank(),
                legend.position="bottom") 

    #remove sample_size<10 obs 
        df_0<-df[sample_size<10,] 
    
        #vet data points dropped due to small sample size----------------------------
        ggplot(df_0) +
          geom_linerange(aes(xmin = age_start, xmax = age_end,
                             y = mean, linetype = "longdash")) +
          labs(title="Alt definition SS<10 by super region",
               x = "Age", 
               y = "Mean") +
          lims(x = c(0, 99), y = c(0, 1))+
          facet_wrap(~super_region_name)
    
        ggplot(df_0) +
          geom_linerange(aes(xmin = age_start, xmax = age_end,
                             y = mean, color=super_region_name, linetype = "longdash")) +
          labs(title="Alt definition SS<10 by definition",
               x = "Age", 
               y = "Mean") +
          lims(x = c(0, 99), y = c(0, 1)) +
          facet_wrap(~standardized_case_definition)
    
    n_distinct(df_0$nid)
    check<-unique(df_0[,c("nid", "age_start", "age_end")])
    
    #drop sample size <10
    df<-df[sample_size>10]
    
    n_distinct(df$standardized_case_definition) #check to see still has 42 definitions
    
    #remove mean = 0 or mean =1 <-!!! need to review if remain like this
    df_mean0<-df[mean==0] #4755
    df_mean1<-df[mean==1] #214
    
        #vet data being dropped
        ggplot(df_mean0) +
          geom_linerange(aes(xmin = age_start, xmax = age_end,
                             y = sample_size, color=super_region_name, linetype = "longdash")) +
          labs(title = "Alt definition mean =0",
               x = "Age", 
               y = "sample_size") +
          facet_wrap(~standardized_case_definition)
        
        ggplot(df_mean1) +
          geom_linerange(aes(xmin = age_start, xmax = age_end,
                             y = sample_size, color=super_region_name, linetype = "longdash")) +
          labs(title="Alt definition mean =1",
               x = "Age", 
               y = "Sample_size") +
          facet_wrap(~standardized_case_definition)
    
    
    check<-unique(df_mean1[,c("nid", "age_start", "age_end")])
    df<-df[mean!=0 & mean!=1]
    
          #summary post drop
          sum2<-df[,total_obs:=.N]
          sum2<-sum2[,obs_by_sr:=.N, by="super_region_name"]
          sum2<-sum2[,perc:=obs_by_sr/total_obs]
          sum2<-distinct(sum2[,.(standardized_case_definition, super_region_name, perc)])
          ggplot(sum2, aes(x=super_region_name, y=perc, fill=super_region_name))+
            geom_col()+
            facet_wrap(~standardized_case_definition) +
            labs(title="Percent of data points from each super region - after to dropping data",
                 x="",
                 y="percentage")+
            theme_bw() +
            theme(axis.text.x = element_blank(),
                  legend.position="bottom") 
    
    #create id variable  
    df<- df %>% mutate(
      midage=(age_end+age_start)/2,
      age_cat=cut(midage, breaks = seq(0, 100, by=5), right = FALSE),
      #midyear=(year_start+year_end)/2, #commented out since if studyname and nid match the year would match
      #year_cat=cut(midyear, breaks = seq(1900, 2030, by=3), right=FALSE),
      id= paste(nid, age_cat, year_start, sex, location_id, study_name,site_memo, measure, sep="-"), #id var
      orig_row=1:nrow(.))  
    
    n_distinct(df$id)

#-------------------------------------------------------------------------------
# Finding matched pairs 
#-------------------------------------------------------------------------------
      #get all unique case defs
      defs<-unique(df[standardized_case_definition!="fpg_7_tx"]$standardized_case_definition)
      a1c<- unique(df[standardized_case_definition %like% "hba1c",]$standardized_case_definition)
       defs<-a1c
       not_use_a1c_nids<-c(94199, 112653, 112655, 112656, 135720, 8618, 81748 ) #first 5 are KNHANEs, last 2 are ENSANUT MEXICO
       no_nhanes<-c(29943,30187,81030,250035,451673,826,327780,112185,126397,140031,58421,492463)
       mym<-c(250035) #myanmar only
       
#the loop below write out all the matched rows for each unique alt case definition in their own folder      

    for (i in defs) {
      
      message(paste0("Creating matched rows for: ", i))
      
        #Test 1: 1 alternative to 1 reference
      
        df_1<- df[(standardized_case_definition=="fpg_7_tx" |
                    standardized_case_definition==i) & !(nid %in% not_use_a1c_nids),] # this is where you subset for matched pairs
        #set up definitions
        method_var<- "standardized_case_definition"
        gold_def<- "fpg_7_tx"
        keep_vars<- c("orig_row", "id", "mean", "standard_error")  
        
        #run match
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
        
        colnames(df_matched_1)
        
        dir.create(paste0(output_path,"simple_xwalk/a1c_test/less_kor_mex_nids/",i)) #create folder for each definition

        write.csv(df_matched_1, paste0(output_path,"simple_xwalk/a1c_test/less_kor_mex_nids/",i,"/matched_rows.csv"))
        
        message(paste0("Completed matching for: ", i))
        
}
      
   
##################################################################################     
#  START HERE WHEN IF MATCHED PAIRS ALREADY DONE
##################################################################################  
    
#-------------------------------------------------------------------------------
# Delta transform: Linear to logit
#-------------------------------------------------------------------------------
   df<-read.csv('FILEPATH')

    df<-as.data.table(df) #134 rows
    unique(df$standardized_case_definition)
    unique(df$measure)
    ogtt<- unique(df[standardized_case_definition %like% "ogtt" & !(standardized_case_definition %like% "hba1c")]$standardized_case_definition)
    a1c<- unique(df[standardized_case_definition %like% "hba1c",]$standardized_case_definition)
    
    #keep only the alt case definitions we want and save all other data type to the side 
    df_other<- df %>% filter(standardized_case_definition %in% c("fpg_7_tx" , "claims" , "nzl_vdr") | data_origin=="dm1" |  !(measure %in% c("prevalence", "incidence"))) #41952 rows
    df_new<-df %>% filter(standardized_case_definition!="fpg_7_tx" & standardized_case_definition!="claims" & standardized_case_definition!="nzl_vdr" & (is.na(data_origin) | data_origin=="") & measure %in% c("prevalence", "incidence")) #6275 rows
    unique(df_other$standardized_case_definition)
    unique(df_new$standardized_case_definition)
    
    unique(df_new$measure)
    
    #now redefine defs as we only need to crosswalk one's we currently have in the actual data
    defs_toxwalk<-unique(df_new$standardized_case_definition)

    #read in matched df
      cols<-c("dorms", "cov_names", "beta", "beta_sd", "gamma")
      beta_all <- data.frame(matrix(ncol = length(cols), nrow = 0))
      
      dir.create(paste0(output_path,"simple_xwalk/",date)) 
      dir.create(paste0(output_path,"simple_xwalk/",date,"/betas")) 
      dir.create(paste0(output_path,"simple_xwalk/",date,"/rows_to_xwalk")) 
      dir.create(paste0(output_path,"simple_xwalk/",date,"/rows_to_xwalk/mean_zero")) 
      dir.create(paste0(output_path,"simple_xwalk/",date,"/rows_to_xwalk/mean_one")) 
      dir.create(paste0(output_path,"simple_xwalk/",date,"/rows_xwalked")) 
      

#the loop below pulls all the matched rows, runs mrbrt, finds beta, applies to data that needs to be xwalked
        
for ( i in defs_toxwalk){
  
      message(paste0(i, " : delta transform matched rows"))
            
            #read in matched rows 
              #if single ogtt definition use myanmar steps data only
                if (i %in% c("ogtt_11.1_tx", "ogtt_11.1", "ogtt_10_tx", "ogtt_10")) {
                  message("definition is ogtt only, using myanmar matched data only")
                    df_tologit<- read.csv(paste0('FILEPATH'))
                  }
              #if ogtt combination defintiions use nonhanes data only
                if (( (i %in% ogtt) & !(i %in% c("ogtt_11.1_tx", "ogtt_11.1", "ogtt_10_tx", "ogtt_10")))) {
                  message("definition is fpg+ogtt, using non-nhanes matched data only")
                    df_tologit<- read.csv(paste0('FILEPATH'))
                }
              #if a1c related definition use all but a select few nids from KNHANES and ENSANUT mexico
                if (i %in% a1c) {
                  message("definition is a1c related, use all but a select few KNHANEs and ENSANUT nids")
                  df_tologit<- read.csv(paste0('FILEPATH'))
                }
              #if not an ogtt related definition use all data 
              if (!(i %in% c(ogtt, a1c))) {
                message("definition is non-ogtt related, use all data")
                df_tologit<- read.csv(paste0('FILEPATH'))
              }
            
            df_tologit<-as.data.table(df_tologit)
            
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
      message(paste0(i, " : run MRBRT to get crosswalk coefficient"))
            
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
              gold_dorm = "fpg_7_tx"  # level of 'ref_dorms' that's the gold standard
            )
            
            fit1$fit()  
            
      message(paste0(i, " : write out crosswalk efficient"))
            
            df_tmp<-as.data.table(fit1$create_result_df())
            df_tmp2<-df_tmp %>% dplyr::select(cols) %>% filter(dorms==i)
            df_tmp2$gamma<- unique(df_tmp[!is.na(gamma),]$gamma)
            beta_all<-rbind(beta_all,df_tmp2)
            write.csv(df_tmp, paste0(output_path,"simple_xwalk/",date,"/betas/",i,"_beta.csv"))
            
    #-------------------------------------------------------------------------------
    # Funnel plots 
    #------------------------------------------------------------------------------- 
      message(paste0(i, " : produce funnel plot"))

            plots <- import("crosswalk.plots")

            plots$funnel_plot(
              cwmodel = fit1,
              cwdata = dat1,
              continuous_variables = list(),
              obs_method = i,
              plot_note = paste0("Single def:fpg_7_tx_vs",i),
              plots_dir = paste0(output_path,"simple_xwalk/",date,"/betas/"),
              file_name = paste0(i,"_funnel_plot"),
              write_file = TRUE
            )

            print(plots)
    
    
         
            
    #-------------------------------------------------------------------------------
    # apply crosswalks
    #------------------------------------------------------------------------------- 

      message(paste0(i, " : apply crosswalk coefficient to relevant data"))

          #get data that needs to be crosswalked
          df2<-df_new[standardized_case_definition==i]
          write.csv(df2, paste0(output_path,"simple_xwalk/",date,"/rows_to_xwalk/",i,"_pre_xwalk.csv"))

          #count the rows of data that needs to be crosswalked
          points<- nrow(df2)

      message(paste0(i, " : total of ", points, " rows of data to crosswalk"))

          #deal with 0 and 1 in original data
          message(paste0(i, " : total of ", nrow(df2), " rows"))
          points<-nrow(df2[mean==0])
          message(paste0(i, " : total of ", points, " rows with mean = 0"))
          points<-nrow(df2[mean==1])
          message(paste0(i, " : total of ", points, " rows with mean = 1"))

          df2_0<- df2 %>% filter(mean==0) # 996 data points
          write.csv(df2_0, paste0(output_path,"simple_xwalk/",date,"/rows_to_xwalk/mean_zero/",i,".csv"))

          df2_1<- df2 %>% filter(mean==1) #none
          write.csv(df2_1, paste0(output_path,"simple_xwalk/",date,"/rows_to_xwalk/mean_one/",i,".csv"))

          #DEALING WITH MEAN=ZEROES
            # there are two options: half of the smallest value (or keep zero and inflate uncertainty)
            # used below: choose half of the smallest value

            #find smallest non zero mean
            minmean<- min(df2[mean>0]$mean)

            #take half of the smalled non-zero mean
            df2[mean==0, crosswalk_note:="orig mean is 0"]
            df2[mean==0, mean:=minmean/2]

        #DEALING WITH MEAN=ONES
            df2[mean==1, crosswalk_note:="orig mean is 1"]
            df2[mean==1, mean:=0.999]

          pred_tmp <- fit1$adjust_orig_vals(
            df = df2 ,
            orig_dorms = "standardized_case_definition",
            orig_vals_mean = "mean", # mean in un-transformed space
            orig_vals_se = "standard_error"      # SE in un-transformed space
          )

          head(pred_tmp)

          #add the adjusted values back to the original dataset
          df2[, c(
            "meanvar_adjusted", "sdvar_adjusted", "pred_logit",
            "pred_se_logit", "data_id")] <- pred_tmp

          #write out post crosswalked rows for the specific definition
          write.csv(df2, paste0(output_path,"simple_xwalk/",date,"/rows_xwalked/",i,"_xwalked.csv"))
}

      
      #write out all the betas
      write.csv(beta_all, paste0(output_path,"simple_xwalk/",date,"/final_betas.csv"))
      