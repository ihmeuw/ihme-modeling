
##Purpose: Prep data for age-sex splitting


################### SETUP DATASET #########################################

prep_split_data<-function(data, decomp_step){
  message("Prepping Data...")
  data<-copy(data)
  
  source(paste0(FILEPATH, "/expand_pops.R")) ##USERNAME: function to return expanded populations
  
  temp_dir<- FILEPATH
  
  ################### DATA CHECKS #########################################
  ######################################################
  
  data <- readRDS(paste0(FILEPATH, mod_name, "_data.rds"))
  nec_cols<-c("sex_id", "age_start", "age_end", "to_split", "to_train", "location_id", "year_id", "nid", "data")
  
  
  ##USERNAME: check that columns exist
  invisible(
    lapply(nec_cols, function(x){
      if(!x %in% names(data)){stop("Missing column '", x, "' from input data!")}
    })
  )
  
  ##USERNAME: check that columns are integers
  int_cols<-c("sex_id", "age_start", "age_end", "to_split", "to_train", "location_id", "year_id", "nid")
  invisible(
    lapply(int_cols, function(x){
      if(!is.integer(data[[x]])){
        message("Column '", x, "' is not an integer, coercing.")
        data[, (x):=as.integer(get(x))]
      }})
  )
  
  
  ################### SETUP TRAIN AND TEST DATA #########################################
  ######################################################
  message(" Prepping training dataset...")
  if("split_id" %in% names(data)){data[, split_id:=NULL]}
  data[, split_id:=1:.N]
  cols <- c("location_id", "year_id", "age_start", "age_end", "sex_id", "data", "sample_size", "data_type", "nid", "standard_error", "to_train", "to_split")
  meta.cols <- setdiff(names(data), cols)  ##USERNAME: setdiff takes all the cols not in the 'cols' vector
  metadata <- data[, meta.cols, with=F]
  data<-data[, c("split_id", cols), with=F]
  
  training<-data[to_train==1,]
  
  ################### FORMAT TRAINING DATA #########################################
  ################################################################
  
  #setnames(training, "data", "mu")
  
  ##USERNAME: make nid unique by year/loc
  training[, src_loc_yr:=paste0(nid, "_", location_id, "_", year_id)]
  ##USERNAME: add sex in this identifier for now
  training[, src_loc_yr_sex:=paste0(nid, "_", location_id, "_", year_id, "_", sex_id)]
  
  ##USERNAME: if se==0, it is usually because sample size is too small, so I'm setting se for these rows to max se
  large_se<-quantile(training$standard_error, probs=0.99)
  small_se<-quantile(training$standard_error, probs=0.025)
  training[standard_error<small_se, standard_error:=large_se]
  
  ##USERNAME: need full age pattern for splitting
  ##training<-training[age_start>=25]  ##USERNAME: for now, subset the data instead of the model
  
  ##USERNAME: to dedupe
  dupes<-training[duplicated(training[,.(src_loc_yr, sex_id, age_start, age_end)])]
  if(nrow(dupes)>0){message("  There are duplicated points for the same survey-year-sex-location-age group!  Estimating as unique source")}
  
  ##USERNAME: create a new functional nid for the duplicates
  training[duplicated(training[,.(src_loc_yr, sex_id, age_start, age_end)]), src_loc_yr:=paste0(src_loc_yr, "_2")]
  ##USERNAME: drop any remaining duplicates
  training<-training[!duplicated(training[,.(src_loc_yr, sex_id, age_start, age_end)])]
  
  ##USERNAME: setting the order is necessary for the ragged array fix in Stan
  setorder(training, src_loc_yr, sex_id, age_start)
  
  
  ##USERNAME: recode age_start to start at 1
  training[, new_age_start:=as.numeric(as.factor(age_start))] ##need a data point for each age group
  
  
  
  ################### DEFINE REFERENCE PATTERNS AND MEANS #########################################
  ################################################################
  
  
  ##USERNAME: create global pattern the old way
  g_mus<-training[, .(g_mu=mean(data)), by=.(age_start, sex_id)]
  wtd_g_mus<-training[, .(wtd_g_mu=weighted.mean(data, 1/standard_error)), by=.(age_start, sex_id)]
  
  setorder(g_mus, sex_id, age_start)
  setorder(wtd_g_mus, sex_id, age_start)
  
  ##USERNAME: this is the mean for each source, hoping to use as a covariate eventually
  collapsed_means<-training[, .(cv_pop_mean=mean(data)), by=.(src_loc_yr)]
  training[, cv_pop_mean:=mean(data), by=.(src_loc_yr)]
  
  #src_link<-training[, .(src_loc_yr=unique(src_loc_yr)), by="super_region_name"] ##USERNAME: get super region of each source
  
  
  age_start_link<-data.table(new_age_start=unique(training$new_age_start), age_start=unique(training$age_start))
  setorder(age_start_link, age_start)
  
  ##USERNAME: the age_end_link is actually for age_starts, but it is the last age_start for a point to be split
  age_end_link<-copy(age_start_link)
  setnames(age_end_link, c("new_age_start", "age_start"), c("new_age_end", "age_end"))
  age_end_link[, age_end:=age_end+4]
  
  
  
  ###################  FORMAT DATA TO SPLIT  #########################################
  ################################################################
  ###############################################################
  
  message(" Prepping data to split...")
  
  split <- data[to_split==1,]
  
  split_copy<-expand_pops(split, expand_sex=T, decomp_step=paste0("step", decomp_step))
  
  
  data[is.na(to_split), to_split:=0]
  data[is.na(to_train), to_train:=0]
  no_split<-data[to_split==0 & to_train==0]
  
  return(list(training=training, split_copy=split_copy, no_split=no_split,
              metadata=metadata, global_means=g_mus))
  message("Done prepping data")
}

