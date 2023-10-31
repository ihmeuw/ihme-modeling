# The intention for this function is to create a dataframe that is the collapsed
# and merged data that will be used in crosswalks. It subsets out the reference
# data in a bundle and the non-reference data. The user has the choice of how to merge
# the data depending on if there is within study information or not.
# It cuts these data depending on where the user wants them by age and year. The function
# returns a dataframe that is prepped for calculating the mean effect for the ratio
# in a crosswalk that can be used either in rma() or in MR-BRT.
# Written by Chris Troeger, March 2019

# df is the dataframe (i.e. bundle data)
# covariate_name is the NAMES of the binary COLUMNS for the network matching
# reference_name is the name of the binary column for the reference definition
# merge_type is how you want the data to be merged by study. The options are
# "within" means an exact match within a single study (merges by NID, age_start, age_end, year_start, year_end, sex)
# "between" means a close match by location, year (binned by year_cut argument), and age (binned by age_cut argument)
# Currently, if "within" is chosen as the merge_type, it merges data for exact matches so age_cut and year_cut only apply for "between" option
# location_match is at what level do you want your data merged by location, meaning how close geographically is a match?
# "exact" means it merges by location_id
# "country" means it merges by country (collapses subnationals into single value)
# "region" means it merges at GBD region (collapses all regional data into single value)
# "super" means it merges at GBD super region (collapses all super regional data into single value)
# age_cut is where you want the data to be merged by age (bins data at those values and aggregates). If you don't want this option, set to c(0,100)
# year_cut is where you want the data to be merged by year (bins data at thos values and aggregates). If you don't want this option, set to c(1980,2019)


bundle_crosswalk_collapse_network <- function(df, 
                                      gbd_round_id,
                                      decomp_step,
                                      covariate_names, 
                                      reference_name, 
                                      age_cut=c(0,100), 
                                      year_cut=c(1980,2019), 
                                      merge_type="within", 
                                      location_match="exact", 
                                      include_logit = T){
  library(plyr)
  library(metafor, lib.loc="/home/j/temp/ctroeger/r_packages/")
  library(msm, lib.loc="/home/j/temp/ctroeger/r_packages/")
  
  locs <- get_location_metadata(location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
  
  # Chris T at least has some places where cases/sample_size doesn't equal mean. For the
  # purposes of this work, it doesn't matter what the magnitude is so just set cases (used
  # in aggregation) to be the product of mean and sample_size.
  
  # get missing sample size before doing this
  source(paste0(h, "00_repos/adhoc/get_row_populationV2.R"))
  df <- add_population_cols(df, gbd_round = gbd_round_id, decomp_step = ds)
  
  df$cases <- df$mean * df$sample_size
  
  # Subset outliers
  # df <- subset(df, year_end > min(year_cut) & is_outlier == 0)
  
  # Create a working dataframe
  wdf <- df[,c("location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
               reference_name,covariate_names), with = F]
  wdf <- join(wdf, locs[,c("location_id","ihme_loc_id","super_region_name","region_name")], by="location_id")
  
  # Create indicators for merging
  if(location_match=="exact"){
    wdf$location_match <- wdf$location_id
  } else if(location_match=="country"){
    wdf$location_match <- substr(wdf$ihme_loc_id,1,3)
  } else if(location_match=="region"){
    wdf$location_match <- wdf$region_name
  } else if(location_match=="super"){
    wdf$location_match <- wdf$super_region_name
  } else{
    print("The location_match argument must be [exact, country, region, super]")
  }
  
  wdf$age_median <- (wdf$age_end + wdf$age_start) / 2
  wdf$age_bin <- cut(wdf$age_median, age_cut)
  
  wdf$year_median <- (wdf$year_end + wdf$year_start) / 2
  wdf$year_bin <- cut(wdf$year_median, year_cut)
  
  matches <- list()
  
  ## All combinations of alternate + reference definitions.
  ## If there's only one combination it'll make sure it's still a matrix
  comb <- combn(c(reference_name, covariate_names), 2)
  if (is.null(ncol(comb))) {
    comb <- as.matrix(comb)
  }
  
  ### Checking to see if some rows have multiple definitions that are not mutually exclusive (i.e. you are nonfatal & blood-test.)
  ### If so, we have to do a slightly more complicated match. We can tell that's the case when more than one definition are marked in a row.
  defs <- c(reference_name, covariate_names)
  ## set the definition = 0 if it is NA
  for (col in covariate_names) wdf[is.na(get(col)), (col) := 0]
  wdf$num_defs <- rowSums(wdf[, ..defs])
  multiple_defs <- ifelse(max(wdf$num_defs) > 1, T, F)
  
  ## If you have non-mutually-exclusive data points, we have to do some tagging.
  ## First we're figuring out what the multiple-definition-combinations are
  ## Then we're tagging those combinations, making sure not to tag anything incorrectly
  ## We also add those new combination-definitions to covariate_names and comb
  
  
  if (multiple_defs) {
    
    mult_combs <- unique(wdf[num_defs>1, ..defs]) ## What are the rows with more than 1 def?
    new_defs <- vector()
    for (row in 1:nrow(mult_combs)) { ## What columns are tagged 1? I.e. what are the combinations in each of these problematic rows?
      def <- mult_combs[row]
      name <- vector()
      for (n in names(def)) {
        if (sum(def[, get(n)]) > 0) name <- c(name, n) ## Here we're pulling out columns that are actually marked 1
      }
      def <- paste(name, collapse = "AND")
      new_defs <- c(new_defs, def) ## Now we have all of the new combination-definitions
    }
    for (def in new_defs) { ## Now we're looking at all the combination-definitions. We're trying to tag our full dataset now.
      
      def_s <- strsplit(def, split = "AND") ## Splitting them into separate combinations again
      defs_sub <- paste0(def_s[[1]], collapse = " & ") ## We want these to be true
      non_defs <- setdiff(defs, def_s[[1]]) ## and these to be false
      non_defs <- paste0(" & !(", paste0(non_defs, collapse= " | "), ")") ## Putting the string together
      command <- paste0(defs_sub, non_defs)
      wdf[eval(parse(text = command)), paste0(def) := 1] ## Evaluating it. When all defs are true and nothing else is true mark that new definition as 1
      
    }
    # DROP cv_suspected & bacterial_viral from comb
    # because we are only interested in cv_surveillanceAND... for those two
    matrix_names <- c("cv_surveillance", new_defs)
    comb <- combn(c(reference_name, matrix_names), 2)
  }
  
  


  for (n in 1:ncol(comb)){
    ## Naming the combination
    reference_name <- comb[1, n]
    covariate_name <- comb[2, n]
    
    comp <- paste0(reference_name, "-", covariate_name)

    ## Collapse to the desired merging ##  ## Create reference and non-reference data frames ##
    # Account for mutually-exclusive subdefinitions - when you are looking at surveillance, 
    # you want to match ONLY the "gold std" within surveillance.
    if (reference_name == "cv_surveillance"){
      other_covariate_names <- c("broadly_defined", "partial_bacterial")
      ref <- subset(wdf, (get(reference_name) == 1 & get(other_covariate_names) == 0 ))
    } else {
      other_covariate_names <- NULL
      ref <- subset(wdf, (get(reference_name) == 1))
    }
    # Account for mutually-exclusive subdefinitions
    if (covariate_name == "cv_surveillance"){
      other_covariate_names <- c("broadly_defined", "partial_bacterial")
      nref <- subset(wdf, (get(covariate_name) == 1 & get(other_covariate_names) == 0 ))
    } else {
      other_covariate_names <- NULL
      nref <- subset(wdf, (get(covariate_name) == 1))
    }
    setnames(nref, c("mean","standard_error","cases","sample_size"), c("n_mean","n_standard_error","n_cases","n_sample_size"))
    if (nrow(nref) == 0){
      message (paste(covariate_name, "is never uniquely defined so no matches can be found for it alone"))
      next
    } 
    if (nrow(ref) == 0){
      message (paste(reference_name, "is never uniquely defined so no matches can be found for it alone"))
      next
    }
    if(merge_type=="within"){
      # This might allow more matching if reference/non-reference don't share the exact age ranges...
      # aref <- aggregate(cbind(sample_size, cases) ~ age_bin + year_bin + nid + location_match, data=ref, FUN=sum)
      # anref <- aggregate(cbind(n_sample_size, n_cases) ~ age_bin + year_bin + nid + location_match, data=nref, FUN=sum)
      # wmean <- merge(aref, anref, by=c("nid","age_bin","location_match","year_bin"))
      
      # Use this to get exact demographic matches
      wmean <- merge(ref[,c("nid","cases","sample_size","age_start","age_end","year_start","year_end","age_bin","year_bin","location_match","sex")],
                     nref[,c("nid","n_cases","n_sample_size","age_start","age_end","year_start","year_end","age_bin","year_bin","location_match","sex")],
                     by=c("nid","age_start","age_end","year_start","year_end","age_bin","year_bin","location_match","sex"))
      wmean <- unique(wmean) # Why are there duplicate rows?
      
    } else if(merge_type=="between") {
      ## Aggregate by NID (study)? ##
      nref$n_nid <- nref$nid
      aref <- as.data.table(aggregate(cbind(sample_size, cases) ~ age_bin + year_bin + location_match + nid, data=ref, FUN=sum))
      anref <- as.data.table(aggregate(cbind(n_sample_size, n_cases) ~ age_bin + year_bin + location_match + n_nid, data=nref, FUN=sum))
      
      wmean <- merge(aref, anref, by=c("age_bin","location_match","year_bin"))
      wmean$nid_combo <- paste(wmean$nid, wmean$n_nid)
      
      # ## Also add the option to aggregate across ALL ages and years of a study, in case age ranges are too wide
      # big_aref <- as.data.table(aggregate(cbind(sample_size, cases) ~ location_match + nid + year_median, data=ref, FUN=sum))
      # big_anref <- as.data.table(aggregate(cbind(n_sample_size, n_cases) ~ location_match + n_nid + year_median, data=nref, FUN=sum))
      # 
      # # find the age min and max for each of those NIDs
      # for (nidval in unique(big_aref$nid)){
      #   age_minimum <- min(ref[nid == nidval]$age_start)
      #   age_maximum <- max(ref[nid == nidval]$age_end)
      #   big_aref[nid == nidval, `:=`(age_min = age_minimum, age_max = age_maximum)]
      # }
      # for (nidval in unique(big_anref$n_nid)){
      #   age_minimum <- min(nref[nid == nidval]$age_start)
      #   age_maximum <- max(nref[nid == nidval]$age_end)
      #   big_anref[n_nid == nidval, `:=`(age_min = age_minimum, age_max = age_maximum)]
      # }
      # 
      # big_wmean <- merge(big_aref, big_anref, by = c("location_match"))
      # big_wmean$nid_combo <- paste(big_wmean$nid, big_wmean$n_nid)
      # # drop rows that already have nid combos in the more granular
      # big_wmean <- big_wmean[!nid_combo %in% wmean$nid_combo]
      # 
      # # drop those which are too far apart in median year
      # big_wmean$year_diff <- big_wmean$year_median.x - big_wmean$year_median.y
      # big_wmean <- big_wmean[abs(year_diff) <= 6]
      # # this is really meant for those all-age matches, so filter by that
      # big_wmean <- big_wmean[(age_min.x < 1 & age_max.x > 50) & (age_min.y < 1 & age_max.y > 50)]
      # 
    } else {
      print("The merge_type argument must be either 'within' (within a single NID) or 'between' (matched on location, year, age)")
    }
    
    if (nrow(wmean) > 0){
      # Get the ratio!
      wmean$mean <- wmean$cases/wmean$sample_size
      wmean$n_mean <- wmean$n_cases/wmean$n_sample_size
      wmean$standard_error <- sqrt(wmean$mean * (1-wmean$mean)/wmean$sample_size)
      wmean$n_standard_error <- sqrt(wmean$n_mean * (1-wmean$n_mean)/wmean$n_sample_size)
      
      # if zeros still exist after aggregation, drop them
      wmean <- as.data.table(wmean)
      wmean <- wmean[mean > 0 & n_mean > 0 & standard_error > 0 & n_standard_error > 0] 
      
      # calculate the ratio with ALTERNATIVE as the NUMERATOR
      wmean$ratio <- (wmean$n_mean) / (wmean$mean)
      
      # Gives really high estimates of SE but is what is in guidance document
      wmean$se <- sqrt((wmean$mean^2 / wmean$n_mean^2) * (wmean$standard_error^2/wmean$mean^2 + wmean$n_standard_error^2/wmean$n_mean^2))
      
      # Lower, more plausible in my opinion
      #wmean$se <- sqrt(wmean$mean / wmean$n_mean * (wmean$standard_error^2/wmean$mean + wmean$n_standard_error^2/wmean$n_mean))
      
      # Use new crosswalk package to calculate ratios
      wmean[, c("logit_mean_alt", "se_logit_mean_alt") := data.table(delta_transform(mean = n_mean, sd = n_standard_error, transformation = "linear_to_logit"))]
      wmean[, c("logit_mean_ref", "se_logit_mean_ref") := data.table(delta_transform(mean = mean, sd = standard_error, transformation = "linear_to_logit"))]
      
      wmean[, logit_diff := logit_mean_alt - logit_mean_ref]
      wmean[, logit_diff_se := sqrt(se_logit_mean_alt^2 + se_logit_mean_ref^2)] # se for logit difference
      
      wmean[, c("log_mean_alt", "se_log_mean_alt") := data.table(delta_transform(mean = n_mean, sd = n_standard_error, transformation = "linear_to_log"))]
      wmean[, c("log_mean_ref", "se_log_mean_ref") := data.table(delta_transform(mean = mean, sd = standard_error, transformation = "linear_to_log"))]
      
      wmean[, c("log_diff", "log_diff_se")] <- calculate_diff(
        df = wmean, 
        alt_mean = "log_mean_alt", alt_sd = "se_log_mean_alt",
        ref_mean = "log_mean_ref", ref_sd = "se_log_mean_ref" )
      
      # Pull out age_start, age_end, year_start, year_end
      # Using substr now, not sure if this is most efficient way to do it.
      if(merge_type == "between"){
        age_list <- data.frame(matrix(unlist(strsplit(as.character(wmean$age_bin),",")), ncol=2, byrow=TRUE))
        year_list <- data.frame(matrix(unlist(strsplit(as.character(wmean$year_bin),",")), ncol=2, byrow=TRUE))
        
        wmean$year_start <- as.numeric(substr(year_list$X1,2,5))
        wmean$year_end <- as.numeric(substr(year_list$X2,1,4))
        
        wmean$age_start <- as.numeric(substr(age_list$X1,2,4))
        wmean$age_end <- as.numeric(gsub(pattern = "]",replacement = "", age_list$X2))
      }
      
      # prepare formatting for code to follow
      if (location_match == "exact"){
        setnames(wmean, "location_match", "location_id")
        wmean <- merge(wmean, locs, by = "location_id")
        wmean[, ihme_loc_abv := substr(ihme_loc_id,1,3)]
      }
      wmean$alt_dorms <- covariate_name
      wmean$ref_dorms <- reference_name
      
      # Add this dataframe to the list and repeat
      matches[[comp]] <- wmean
      
    } else if (nrow(wmean) == 0){
      message (paste("no matches found for", comp))
      next
    }

  }
  
  # Return the dataframe for further analysis!
  matches_df <- rbindlist(matches)
  return(matches_df)
}


#########################################################################################################################################
## Example usage ##
# library(metafor, lib.loc="/home/j/temp/ctroeger/r_packages/")
## Set up the needed values in the dataset ##
#   df <- read.csv(paste0("/home/j/WORK/12_bundle/diarrhea_rotavirus/12/00_documentation/Rotaviral enteritis_gbd2017_data_final.csv"))
# # The function needs an "is_reference" column, I am creating that here because I know that
# # if the data are not the non-reference group, they are the reference group
#   df$is_reference <- ifelse(df$cv_inpatient==1,0,1)
#
# test <- bundle_crosswalk_collapse(df, covariate_name="cv_inpatient", age_cut=c(0,1,5,20,40,60,80,100), year_cut=c(seq(1980,2015,5),2019), merge_type="within", location_match="exact")

# mod <- rma(yi=log_ratio, sei=delta_log_se, data=test, measure="RR")
# summary(mod)
# results <- data.frame(variable = rownames(mod$b), mean=exp(mod$b), lower=exp(mod$ci.lb), upper=exp(mod$ci.ub))
# results

## Link that with launching and loading an MR-BRT model ##
# write.csv(test, "/home/j/temp/ctroeger/Diagnostics/rota_inpatient_xwalk.csv")
# # Including NID as a study id
# fit1 <- run_mr_brt(
#   output_dir = "/home/j/temp/ctroeger/test_folder/",
#   model_label = "cv_inpatient",
#   data = "/home/j/temp/ctroeger/Diagnostics/rota_inpatient_xwalk.csv",
#   mean_var = "log_ratio",
#   se_var = "delta_log_se",
#   overwrite_previous = TRUE,
#   trim_pct = 0.05,
#   method = "trim_maxL",
#   study_id = "nid"
# )
#
# check_for_outputs(fit1)
# df_pred <- data.frame(intercept = 1)
# pred1 <- predict_mr_brt(fit1, newdata = df_pred)
# check_for_preds(pred1)
# pred_object <- load_mr_brt_preds(pred1)
# preds <- pred_object$model_summaries