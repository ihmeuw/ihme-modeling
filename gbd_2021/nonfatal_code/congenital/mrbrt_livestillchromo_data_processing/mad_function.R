mads_function <- function(df){

calc_mad <- function(age,df){
  
  #Pulling mean/median/mad from age 0 calculation that comes first 
  mean_0<-mean_0
  median_0<-median
  mad_0<-mad
  
  #Calculating mean/median for a given age on only MS data 
  mean_age<-df[age_start==age & clinical_data_type=="claims" & mean!=0, mean(mean)]
  median_age<-df[age_start==age & clinical_data_type=="claims"& mean!=0, median(mean)]
  #Mean for age 0 on only MS data: 
  mean_0_ms<-mean(df[age_start==0 & clinical_data_type=="claims" & mean!=0, (mean) ] )
  
  # Prevalence_age_5 = Prevalence_age_zero + (Beta_age_5 * Prevalence_age_zero)
  beta_age = (mean_age + mean_0_ms) / mean_0_ms
  
  mad_age_specific = mad_0 + (beta_age * mad_0)
  mad_age = median_age + (3*mad_age_specific)
  mad_age_lower = 0.5*median_age
  
  return(c(mad_age_lower, mad_age))
}

# subset out outliers for this entire process and bring them back at the end
outliers <- all_data_for_modeling[is_outlier==1,]
df <- all_data_for_modeling[is_outlier==0,]
df <- as.data.table(df)

df <- df[, c("mad", "mad3", "mad_n1", "median"):=NULL]
# this is only for prev data: 
non_prev_mad <- df[measure!="prevalence",]
utlas <- df[underlying_nid==159942,]
df <- df[measure=="prevalence",]
df <- df[!(underlying_nid %like% 159942)]

b <- bun_id
rm(table)


# FOR PREVALENCE ONLY: 

  ############
  #non-chromo#
  ############
  
  #total msk, limb reduction, and urogen
  if  (b!=439 & b!=2975 & b!=2972 & b!=2978 & b!=3029 & b!= 436 & b!=638 & b!=437 & b!=438) {

    #want to calculate MAD on all under 1 if it is NTDs
    if (b==608 | b==612 | b==626 | b==614 | b==620){
      median<- median( df[age_start==0, (mean) ] )
      mean_0<- mean( df[age_start==0 , (mean) ] )
      mad<- mad( df[age_start==0, (mean) ] )
      mad3<- ((3*mad)+median)
      mad_n1<- (0.5*median)
      
      
      df<-df[(mean>mad3), is_outlier:=1]
      df<-df[(age_start==0 & mean<mad_n1), is_outlier:=1]
      df<- df[(age_start==0 & mean<mad_n1),outlier_note:="outliering based on MAD restrictions"]
      df<- df[(age_start==0 & mean>mad3),outlier_note:="outliering based on MAD restrictions"]
      
      loc_yr<-df[(age_start==0 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid, short_registry_name)]
      loc_yr[, flag:=1]
      loc_yr <- unique(loc_yr)
      df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid", "short_registry_name"), all.x=TRUE)
      df[flag==1, is_outlier:=1]
      df<-df[, -c("flag")]
      
      #For older ages: 
      ages<-unique(df[age_start!=0]$age_start)
      mads<-lapply(ages, calc_mad, df)
      mads <-do.call('rbind',mads)
      table<-cbind(ages, mads)
      print(table)

      for(i in 1:(nrow(table))){
        age=table[i, 1]
        mad_age_lower=table[i, 2]
        mad_age=table[i, 3]
        mad_age<-as.numeric(mad_age)
        mad_age_lower<-as.numeric(mad_age_lower)
        df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
        df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
        df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]

      }
     }
    
    if (b==610){
      median<- median( df[age_start==0 & source_type=="Registry - congenital" & !(is.na(mean)), (mean) ] )
      mean_0<- mean( df[age_start==0 & source_type=="Registry - congenital" & !(is.na(mean)), (mean) ] )
      mad<- mad( df[age_start==0 & source_type=="Registry - congenital" & !(is.na(mean)), (mean) ] )
      mad3<- ((3*mad)+median)
      mad_n1<- (0.5*median)
      
      
      df<-df[(mean>mad3), is_outlier:=1]
      df<-df[(mean<mad_n1), is_outlier:=1]
      df<- df[(mean<mad_n1),outlier_note:="outliering based on MAD restrictions"]
      df<- df[(mean>mad3),outlier_note:="outliering based on MAD restrictions"]
    }
    
    if (b==616 | b==618){
      median<- median( df[age_start==0 & (nid == 128835 | (underlying_nid ==128835) | nid %in% nids | clinical_data_type=="claims"), mean ] )
      mean_0<- mean( df[age_start==0 & (nid ==128835 | (underlying_nid ==128835) | nid %in% nids | clinical_data_type=="claims"), (mean) ] )
      mad<- mad( df[age_start==0 & ( nid ==128835 | (underlying_nid ==128835) | nid %in% nids | clinical_data_type=="claims"), mean ] )
      mad3<- ((3*mad)+median)
      mad_n1<- (0.5*median)
      
      df<-df[(mean>mad3), is_outlier:=1]
      df<-df[(age_start==0 & mean<mad_n1), is_outlier:=1]
      df<- df[(age_start==0 & mean<mad_n1), outlier_note:="outliering based on MAD restrictions"]
      df<- df[(age_start==0 & mean>mad3), outlier_note:="outliering based on MAD restrictions"]
      
      loc_yr<-df[(age_start==0 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid, short_registry_name)]
      loc_yr[, flag:=1]
      loc_yr <- unique(loc_yr)
      df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid", "short_registry_name"), all.x=TRUE)
      df[flag==1 & age_start!=0, is_outlier:=1]
      df<-df[, -c("flag")]
      
      #For older ages: 
      ages<-unique(df[age_start!=0]$age_start)
      mads<-lapply(ages, calc_mad, df)
      mads <-do.call('rbind',mads)
      table<-cbind(ages, mads)
      print(table)

      for(i in 1:(nrow(table))){
        age=table[i, 1]
        mad_age_lower=table[i, 2]
        mad_age=table[i, 3]
        mad_age<-as.numeric(mad_age)
        mad_age_lower<-as.numeric(mad_age_lower)
        df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
        df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
        df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
      }

     }
    
    if (b==604){
      median<- median( df[age_start==0 & source_type=="Registry - congenital", (mean) ] )
      mean_0<- mean( df[age_start==0 & source_type=="Registry - congenital", (mean) ] )
      mad<- mad( df[age_start==0 & source_type=="Registry - congenital", (mean) ] )
      mad3<- ((3*mad)+median)
      mad_n1<- (0.5*median)
      
      df<-df[(mean>mad3), is_outlier:=1]
      df<-df[(age_start==0 & mean<mad_n1), is_outlier:=1]
      df<- df[(age_start==0 & mean<mad_n1), outlier_note:="outliering based on MAD restrictions"]
      df<- df[(age_start==0 & mean>mad3), outlier_note:="outliering based on MAD restrictions"]
      
      loc_yr<-df[(age_start==0 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid, short_registry_name)]
      loc_yr[, flag:=1]
      loc_yr <- unique(loc_yr)
      df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid", "short_registry_name"), all.x=TRUE)
      df[flag==1 & age_start!=0, is_outlier:=1]
      df<-df[, -c("flag")]
      
      #For older ages: 
      ages<-unique(df[age_start!=0]$age_start)
      mads<-lapply(ages, calc_mad, df)
      mads <-do.call('rbind',mads)
      table<-cbind(ages, mads)
      print(table)

      for(i in 1:(nrow(table))){
        age=table[i, 1]
        mad_age_lower=table[i, 2]
        mad_age=table[i, 3]
        mad_age<-as.numeric(mad_age)
        mad_age_lower<-as.numeric(mad_age_lower)
        df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
        df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
        df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
      }

     }
    
    if (b==602){
      median<- median( df[age_start==0 & clinical_data_type=="claims", mean ] )
      mean_0<- mean( df[age_start==0 & clinical_data_type=="claims", (mean) ] )
      mad<- mad( df[ age_start==0 & clinical_data_type=="claims", mean ] )
      mad3<- ((3*mad)+median)
      mad_n1<- (0.5*median)
      
      df<-df[(mean>mad3), is_outlier:=1]
      df<-df[(age_start==0 & mean<mad_n1), is_outlier:=1]
      df<- df[(age_start==0 & mean<mad_n1), outlier_note:="outliering based on MAD restrictions"]
      df<- df[(age_start==0 & mean>mad3), outlier_note:="outliering based on MAD restrictions"]
      
      loc_yr<-df[(age_start==0 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid, short_registry_name)]
      loc_yr[, flag:=1]
      loc_yr <- unique(loc_yr)
      df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid", "short_registry_name"), all.x=TRUE)
      df[flag==1 & age_start>=1, is_outlier:=1]
      df<-df[, -c("flag")]
      
      #For older ages: 
      ages<-unique(df[age_start!=0]$age_start)
      mads<-lapply(ages, calc_mad, df)
      mads <-do.call('rbind',mads)
      table<-cbind(ages, mads)
      print(table)

      for(i in 1:(nrow(table))){
        age=table[i, 1]
        mad_age_lower=table[i, 2]
        mad_age=table[i, 3]
        mad_age<-as.numeric(mad_age)
        mad_age_lower<-as.numeric(mad_age_lower)
        df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
        df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
        df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
      }

     }
    
    if ((b!=608 & b!=610 & b!=612 & b!=614 & b!=602 & b!=604 & b!=616 & b!=618 & b!=626 & b!=620)) {
      median<- median(df[(age_start==0 & (nid ==128835 | (underlying_nid ==128835)| nid %in% nids)), (mean)])
      mean_0<- mean( df[ age_start==0 & (nid ==128835 | (underlying_nid ==128835)| nid %in% nids), mean ] )
      mad<- mad( df[age_start==0 & (nid ==128835 | (underlying_nid ==128835)| nid %in% nids), mean ] )
      mad3<- ((3*mad)+median)
      mad_n1<- (0.5*median)
      
      df<-df[(mean>mad3), is_outlier:=1]
      df<-df[(age_start==0 & mean<mad_n1), is_outlier:=1]
      df<- df[(age_start==0 & mean<mad_n1), outlier_note:="outliering based on MAD restrictions"]
      df<- df[(age_start==0 & mean>mad3), outlier_note:="outliering based on MAD restrictions"]
      
      loc_yr<-df[(age_start==0 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid, short_registry_name)]
      loc_yr[, flag:=1]
      loc_yr <- unique(loc_yr)
      df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid", "short_registry_name"), all.x=TRUE)
      df[flag==1, is_outlier:=1]
      df<-df[, -c("flag")]
      
      #For older ages: 
      ages<-unique(df[age_start!=0]$age_start)
      mads<-lapply(ages, calc_mad, df)
      mads <-do.call('rbind',mads)
      table<-cbind(ages, mads)
      print(table)

      for(i in 1:(nrow(table))){
        age=table[i, 1]
        mad_age_lower=table[i, 2]
        mad_age=table[i, 3]
        mad_age<-as.numeric(mad_age)
        mad_age_lower<-as.numeric(mad_age_lower)
        df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
        df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
        df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
      }

     }
    print("end of non chromo loop")}
  
  
  ########
  #chromo#
  ########
  
  if (b==436){
    median<- median( df[age_start==0  & (nid==128835 | (underlying_nid ==128835) | nid %in% nids | clinical_data_type=="claims"), (mean) ] )
    mean_0<- mean( df[age_start==0  & (nid ==128835 | (underlying_nid ==128835)| nid %in% nids | clinical_data_type=="claims"), (mean) ] )
    mad<- mad( df[age_start==0 & (nid ==128835 | (underlying_nid ==128835) | nid %in% nids | clinical_data_type=="claims"), (mean) ] )
    mad3<- ((3*mad)+median)
    mad_n1<- (0.5*median)
    
    
    df<-df[(mean>mad3), is_outlier:=1]
    df<-df[(age_start==0 & mean<mad_n1), is_outlier:=1]
    df<- df[(age_start==0 & mean<mad_n1),outlier_note:="outliering based on MAD restrictions"]
    df<- df[(age_start==0 & mean>mad3),outlier_note:="outliering based on MAD restrictions"]
    
    loc_yr<-df[(age_start==0 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid, short_registry_name)]
    loc_yr[, flag:=1]
    loc_yr <- unique(loc_yr)
    df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid", "short_registry_name"), all.x=TRUE)
    df[flag==1, is_outlier:=1]
    df<-df[, -c("flag")]
    print("downs outliers applied")
    
    
    #For older ages: 
    ages<-unique(df[age_start!=0]$age_start)
    mads<-lapply(ages, calc_mad, df)
    mads <-do.call('rbind',mads)
    table<-cbind(ages, mads)
    print(table)

    for(i in 1:(nrow(table))){
      age=table[i, 1]
      mad_age_lower=table[i, 2]
      mad_age=table[i, 3]
      mad_age<-as.numeric(mad_age)
      mad_age_lower<-as.numeric(mad_age_lower)
      df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
      df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
      df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
    }

   }
  
  
  if  (b==638) {
    
    median<- median( df[(age_start==0 & (nid ==128835 | underlying_nid ==128835 | nid %in% nids) & mean!=0), mean ] )
    mean_0<- mean( df[(age_start==0 & (nid ==128835 | (underlying_nid ==128835) | nid %in% nids) & mean!=0), mean ] )
    mad<- mad( df[(age_start==0 & (nid ==128835 | (underlying_nid ==128835) | nid %in% nids) & mean!=0), mean ] )
    mad3<- ((3*mad)+median)
    mad_n1<- (0.5*median)
    
    df<-df[(mean>mad3), is_outlier:=1]
    df<-df[(age_start==0 & mean<mad_n1), is_outlier:=1]
    df<- df[(age_start==0 & mean<mad_n1), outlier_note:="outliering based on MAD restrictions"]
    df<- df[(age_start==0 & mean>mad3), outlier_note:="outliering based on MAD restrictions"]
    
    loc_yr<-df[(age_start==0 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid, short_registry_name)]
    loc_yr[, flag:=1]
    loc_yr <- unique(loc_yr)
    df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid", "short_registry_name"), all.x=TRUE)
    df[flag==1, is_outlier:=1]
    df<-df[, -c("flag")]
    
    #For older ages: 
    ages<-unique(df[age_start!=0]$age_start)
    mads<-lapply(ages, calc_mad, df)
    mads <-do.call('rbind',mads)
    table<-cbind(ages, mads)
    print(table)

    for(i in 1:(nrow(table))){
      age=table[i, 1]
      mad_age_lower=table[i, 2]
      mad_age=table[i, 3]
      mad_age<-as.numeric(mad_age)
      mad_age_lower<-as.numeric(mad_age_lower)
      df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
      df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
      df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
    }

   }
   
  #########################
  #turner and klinefelter##
  #########################
  ### TURNER ###
  if (b==437) {
    
    median<- median(df[(age_start==0), (mean)])
    mean_0<- mean( df[ (age_start==0), mean ] )
    mad<- mad( df[(age_start==0), mean ] )
    mad3<- ((3*mad)+median)
    mad_n1<- (0.5*median)
    
    
    df<-df[(mean>mad3), is_outlier:=1]
    df<-df[(age_start==0 & mean<mad_n1), is_outlier:=1]
    df<- df[(age_start==0 & mean<mad_n1), outlier_note:="outliering based on MAD restrictions"]
    df<- df[(age_start==0 & mean>mad3), outlier_note:="outliering based on MAD restrictions"]
    
    loc_yr<-df[(age_start==0 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid, short_registry_name)]
    loc_yr[, flag:=1]
    loc_yr <- unique(loc_yr)
    df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid", "short_registry_name"), all.x=TRUE)
    df[flag==1, is_outlier:=1]
    df<-df[, -c("flag")]
    
    #For older ages: 
    ages<-unique(df[age_start!=0]$age_start)
    mads<-lapply(ages, calc_mad, df)
    mads <-do.call('rbind',mads)
    table<-cbind(ages, mads)
    print(table)

    for(i in 1:(nrow(table))){
      age=table[i, 1]
      mad_age_lower=table[i, 2]
      mad_age=table[i, 3]
      mad_age<-as.numeric(mad_age)
      mad_age_lower<-as.numeric(mad_age_lower)
      df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
      df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
      df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
    }
  }
  
  if (b==438) {
    ###########################################################################
    ###########################################################################
    median<- median(df[(age_start==0  & (nid ==128835 | (underlying_nid ==128835)| nid %in% nids)), (mean)])
    mean_0<- mean( df[ age_start==0  & (nid ==128835 | (underlying_nid ==128835)| nid %in% nids), mean ] )
    mad<- mad( df[age_start==0  & (nid ==128835 | (underlying_nid ==128835)| nid %in% nids), mean ] )
    mad3<- ((3*mad)+median)
    mad_n1<- (0.5*median)


    
    df<-df[(mean>mad3), is_outlier:=1]
    df<-df[(age_start==0 & mean<mad_n1), is_outlier:=1]
    df<- df[(age_start==0 & mean<mad_n1), outlier_note:="outliering based on MAD restrictions"]
    df<- df[(age_start==0 & mean>mad3), outlier_note:="outliering based on MAD restrictions"]
    
    loc_yr<-df[(age_start==0 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid, short_registry_name)]
    loc_yr[, flag:=1]
    loc_yr <- unique(loc_yr)
    df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid", "short_registry_name"), all.x=TRUE)
    df[flag==1, is_outlier:=1]
    df<-df[, -c("flag")]
    
    #For older ages: 
    ages<-unique(df[age_start!=0]$age_start)
    mads<-lapply(ages, calc_mad, df)
    mads <-do.call('rbind',mads)
    table<-cbind(ages, mads)
    print(table)  
    
    for(i in 1:(nrow(table))){
      age=table[i, 1]
      mad_age_lower=table[i, 2]
      mad_age=table[i, 3]
      mad_age<-as.numeric(mad_age)
      mad_age_lower<-as.numeric(mad_age_lower)
      df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
      df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
      df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
    }        
    
    
  }
  
  ##################
  ## total chromo ##
  ##################
  if (b==3029) {
    ###########################################################################
    ### TOTAL CHROMO ###
    
    ##################################  PROCESS  ################################################## 
    ###############################################################################################
    # 1. pull eurocat data from the 4 sub chromo causes 
    # 2. append all 4 eurocat datasets onto each other 
    # 3. add the proportion of other chromo to the prevalence numbers for each of these 
    # 4. Calculate MAD and median and apply those values to the total chromo bundle 
    # 5. Upload the new data
    ###############################################################################################
    ###############################################################################################
    
    ###############################################################################################
    # 1. pull eurocat data from the 4 sub chromo causes 
    ###############################################################################################
    keepvars<-c("bundle_id", "nid", "clinical_data_type", "underlying_nid", "source_type", "location_id", "location_name", "sex", "year_start", "year_end", "age_start", "age_end", "measure", "mean", "upper", "lower", "is_outlier")
    
    save_b_v <- save_bundle_version(436, step, include_clinical=c('inpatient', 'claims'), gbd_round_id=7)
    b_v <- save_b_v$bundle_version_id
    print(b_v)
    down_temp <- get_bundle_version(b_v, export=FALSE, transform=TRUE, fetch="all")
    down_temp<-as.data.table(down_temp)
    down_small<-down_temp[down_temp$measure=="prevalence",]
    down_small<-as.data.table(down_small)
    down_small<-down_small[,..keepvars]
    down_eurocat<-down_small[(nid ==128835 | underlying_nid ==128835 | clinical_data_type=="claims")]
    
    save_b_v <- save_bundle_version(437, step, include_clinical=c('inpatient', 'claims'), gbd_round_id=7)
    b_v <- save_b_v$bundle_version_id
    print(b_v)
    turner_temp <- get_bundle_version(b_v, export=FALSE, transform=TRUE, fetch="all")
    turner_temp<-as.data.table(turner_temp)
    turner_small<-turner_temp[turner_temp$measure=="prevalence",]
    turner_small<-turner_small[,..keepvars]
    turner_small<-as.data.table(turner_small)
    turner_eurocat<-turner_small[(nid ==128835 | underlying_nid ==128835 | clinical_data_type=="claims")]
    
    
    save_b_v <- save_bundle_version(438, step, include_clinical=c('inpatient', 'claims'), gbd_round_id=7)
    b_v <- save_b_v$bundle_version_id
    print(b_v)
    klinefelter_temp <- get_bundle_version(b_v, export=FALSE, transform=TRUE, fetch="all")
    klinefelter_temp<-as.data.table(klinefelter_temp)
    klinefelter_small<-klinefelter_temp[klinefelter_temp$measure=="prevalence",]
    klinefelter_small<-klinefelter_small[,..keepvars]
    klinefelter_small<-as.data.table(klinefelter_small)
    klinefelter_eurocat<-klinefelter_small[(nid ==128835 | underlying_nid ==128835 | clinical_data_type=="claims")]
    
    
    save_b_v <- save_bundle_version(638, step, include_clinical=c('inpatient', 'claims'), gbd_round_id=7)
    b_v <- save_b_v$bundle_version_id
    print(b_v)
    edpatau_temp <- get_bundle_version(b_v, export=FALSE, transform=TRUE, fetch="all")
    edpatau_temp<-as.data.table(edpatau_temp)
    edpatau_small<-edpatau_temp[edpatau_temp$measure=="prevalence",]
    edpatau_small<-edpatau_small[,..keepvars]
    edpatau_small<-as.data.table(edpatau_small)
    edpatau_eurocat<-edpatau_small[(nid ==128835 | underlying_nid ==128835 | clinical_data_type=="claims")]
    
    
    ###############################################################################################
    # 2. append all 4 eurocat datasets onto each other 
    ###############################################################################################
    all_chromo_eurocat<-rbind(down_eurocat, turner_eurocat, klinefelter_eurocat, edpatau_eurocat)
    
    ###############################################################################################
    # 3. add the proportion of other chromo to the prevalence numbers for each of these 
    ###############################################################################################
    
    
    all_chromo_eurocat<-as.data.table(all_chromo_eurocat)
    all_chromo_eurocat<-all_chromo_eurocat[sex == "Female", mean_total := (mean/(1-.874898823))]
    all_chromo_eurocat<-all_chromo_eurocat[sex == "Male", mean_total := (mean/(1-.900157585))]
    all_chromo_eurocat<-all_chromo_eurocat[sex == "Both", mean_total := (mean/(1-.887528204))]
    
    
    ###############################################################################################
    # 4. Calculate MAD and median and apply those values to the total chromo bundle 
    ###############################################################################################
    #Calculate MAD bounds
    all_chromo_eurocat_nonzero<-all_chromo_eurocat[mean!=0]
    median<- median( all_chromo_eurocat_nonzero[ age_start==0 & (nid ==128835| underlying_nid ==128835 | nid %in% nids), mean_total ] )
    mean<- mean( all_chromo_eurocat_nonzero[ age_start==0 & (nid ==128835| underlying_nid ==128835 | nid %in% nids), mean_total ] )
    mad<- mad( all_chromo_eurocat_nonzero[age_start==0 & (nid ==128835| underlying_nid ==128835 | nid %in% nids), mean_total ] )
    mad3<- ((3*mad)+median)
    mad_n1<- (0.5*median)
    mean_0<-mean(df$mean[df$age_start==0])
    
    df<-df[(mean>mad3), is_outlier:=1]
    df<-df[(age_start==0 & mean<mad_n1), is_outlier:=1]
    df<- df[(age_start==0 & mean<mad_n1), outlier_note:="outliering based on MAD restrictions"]
    df<- df[(age_start==0 & mean>mad3), outlier_note:="outliering based on MAD restrictions"]
    
    loc_yr<-df[(age_start==0 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid, short_registry_name)]
    loc_yr[, flag:=1]
    loc_yr <- unique(loc_yr)
    df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid", "short_registry_name"), all.x=TRUE)
    df[flag==1, is_outlier:=1]
    df<-df[, -c("flag")]
    
    #For older ages: 
    ages<-unique(df[age_start!=0]$age_start)
    mads<-lapply(ages, calc_mad, df)
    mads <-do.call('rbind',mads)
    table<-cbind(ages, mads)
    print(table)

    for(i in 1:(nrow(table))){
      age_start=table[i, 1]
      mad_age_lower=table[i, 2]
      mad_age=table[i, 3]
      mad_age<-as.numeric(mad_age)
      mad_age_lower<-as.numeric(mad_age_lower)
      df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
      df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
      df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
    }
  }

# Bring back non prev data
df <- rbind(df, non_prev_mad, outliers, utlas)
df <- df[measure=="mtwith" & location_id %in% c(6, 354, 361, 491:521), is_outlier:=1]

write.csv(table, paste0("FILEPATH/gbd2020_", b, "_mad_table.csv"))
result <- list()
result$df <- df
result$mad3 <- mad3
result$mad_n1 <- mad_n1
birth_mads <- data.frame(age_start=0, age_end=0, mad3=mad3, mad_n1=mad_n1)
write.csv(birth_mads, paste0("FILEPATH/", b, "_mad_table_birth_no_utlas.csv"))
return(result)
}
