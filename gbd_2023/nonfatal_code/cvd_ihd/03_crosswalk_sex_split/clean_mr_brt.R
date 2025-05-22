
##
## Author:USERNAME
## Date: DATE
## Purpose: Cleaning & helper functions for MR-BRT modeling.
##          Create matches, count matches, log-transform the ratios, sex-match, etc.
##

library(msm)
library(data.table)
library(openxlsx)
library(gtools)
library(stringr)
library(reticulate)

reticulate::use_python("FILEPATH/python")
xwalk <- import("crosswalk")

source("FILEPATH/data_tests.R") ## helper functions 
source("FILEPATHs/functions_utility.R") ## convenience functions in the R xwalk package that don't exist in the underlying python

source("FILEPATH/get_location_metadata.R")

#############################################################################################
############################# MAIN FUNCS ####################################################
#############################################################################################

match_mr_brt <- function(data,
                         release_id,
                         reference_def=NULL,
                         alternate_defs,
                         year_overlap=5,
                         age_overlap=5,
                         age_range=10,
                         year_range=10,
                         subnat_to_nat=F,
                         forced_denominator=NULL,
                         quiet=F,
                         allow_wide_age_bin_match=T) {
  
  age_overlap <- age_overlap
  year_overlap <- year_overlap
  
  ## Cleaning reference and alternate definitions in case they have dashes.
  if (!(is.null(reference_def) | missing(reference_def))) reference_def <- gsub(reference_def, pattern = "-", replacement = "_")
  alternate_defs <- gsub(alternate_defs, pattern = "-", replacement = "_")
  names(data) <- gsub(names(data), pattern = "-", replacement = "_")
  
  ## Checking the existence of key columns.
  nec_cols <- c("age_start", "age_end", "year_start", "year_end", "location_id", "sex", "measure", "location_name",
                "field_citation_value", "mean", "lower", "upper", "standard_error")
  invisible(lapply(nec_cols, check_exists, df=data))
  
  bundle_prep <- copy(data)
  
  defs <- c(reference_def, alternate_defs)
  
  ## If toggled, this will match subnational data points to national data points and vice versa. Pulls in location metadata
  ## from a central function
  if (subnat_to_nat) {
    locs <- get_location_metadata(release_id = release_id, location_set_id = 35)
    cols <- c("ihme_loc_id", "level", "parent_id", "location_type", "ihme_loc_id")
    lapply(X = cols, FUN = function(x) if (x %in% names(bundle_prep)) bundle_prep[, paste0(x) := NULL])
    bundle_prep <- merge(bundle_prep, locs[, .(location_id, level, parent_id, location_type, ihme_loc_id)], by="location_id")
    bundle_prep[, loc_match := location_id]
    bundle_prep[level >= 4, loc_match := parent_id]
    bundle_prep[level == 6, loc_match := 4749] ## PAIR UTLAS TO ENGLAND
    bundle_prep[level == 5 & grepl("GBR", ihme_loc_id), loc_match := 4749] ## PAIR ENGLAND REGIONS TO ENGLAND
    bundle_prep[level == 5 & location_type %in% c("urbanicity", "admin2"), loc_match := 163] ## PAIR INDIA URBAN/RURAL TO INDIA
    bundle_prep[level == 5 & grepl("CHN", ihme_loc_id), loc_match := 6]
    bundle_prep[level == 5 & grepl("KEN", ihme_loc_id), loc_match := 180]
  }
  
  ## Generating midpoints of years and ages. Depending on overlap, we'll take mean +/- some number of years.
  bundle_prep[, age_mid := (age_start + age_end)/2]
  bundle_prep[, year_mid := (year_start + year_end)/2]
  
  ## Generating age ranges. If greater than age_range/year_range argument, we'll do exact matches rather than mean +/-. For age, this is only done if allow_wide_age_bin_match=T. If it is set to F, age ranges above the age_range argument are not used.
  bundle_prep[, age_range := (age_end - age_start)]
  bundle_prep[, year_range := (year_end - year_start)]
  
  ## All combinations of alternate + reference definitions.
  ## If there's only one combination it'll make sure it's still a matrix
  comb <- combn(c(reference_def, alternate_defs), 2)
  if (is.null(ncol(comb))) {
    comb <- as.matrix(comb)
  }
  
  ### STARTING TO CREATE MATCHES ###
  if (!quiet) message("Pairing matches and calculating SE.")
  paired_data <- data.table()
  matched_counts <- data.table()
  
  ### Checking to see if some rows have multiple definitions that are not mutually exclusive 
  ### If so, we have to do a slightly more complicated match. We can tell that's the case when more than one definition are marked in a row.
  bundle_prep$num_defs <- rowSums(bundle_prep[, ..defs])
  multiple_defs <- ifelse(max(bundle_prep$num_defs) > 1, T, F)
  
  ## If you have non-mutually-exclusive data points, we have to do some tagging.
  ## First we're figuring out what the multiple-definition-combinations are
  ## Then we're tagging those combinations, making sure not to tag anything incorrectly
  ## We also add those new combination-definitions to alternate_defs and comb
  if (multiple_defs) {
    
    mult_combs <- unique(bundle_prep[num_defs>1, ..defs]) ## What are the rows with more than 1 def?
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
      bundle_prep[eval(parse(text = command)), paste0(def) := 1] ## Evaluating it. When all defs are true and nothing else is true mark that new definition as 1
      
    }
    bundle_prep[is.na(bundle_prep)] <- 0
    alternate_defs <- c(alternate_defs, new_defs)
    comb <- combn(c(reference_def, alternate_defs), 2)
  }
  
  ##### RUNNING THROUGH MATCHES FOR EACH COMBINATION OF DEFS #######
  
  for (n in 1:ncol(comb)) {
    
    ## Naming the combination
    pair_1 <- comb[1, n]
    pair_2 <- comb[2, n]
    
    if (!(is.null(forced_denominator) | missing(forced_denominator))) {
      if ((forced_denominator %in% c(pair_1, pair_2)) & pair_1 == forced_denominator) {
        pair_1 <- pair_2
        pair_2 <- forced_denominator
      }
    }
    ## If reference definition is the numerator, it should be the denominator
    if ((reference_def %in% c(pair_1, pair_2)) & pair_1 == reference_def) {
      pair_1 <- pair_2
      pair_2 <- reference_def
    }
    
    ## Comparison
    comp <- paste0(pair_1, "-", pair_2)
    
    ##### Splitting out dfs with each definition.
    
    if (multiple_defs) {
      
      if (length(setdiff(new_defs, pair_1)) > 0) {
        null_1 <- paste0(pair_1, " & !(", paste(setdiff(new_defs, pair_1), collapse = " | "), ")")
      } else {
        null_1 <- paste0(pair_1,"==1")
      }
      if (length(setdiff(new_defs, pair_2)) > 0) {
        null_2 <- paste0(pair_2, " & !(", paste(setdiff(new_defs, pair_2), collapse = " | "), ")")
      } else {
        null_2 <- paste0(pair_2,"==1")
      }
      
      a <- bundle_prep[eval(parse(text = null_1)),]
      b <- bundle_prep[eval(parse(text = null_2)),]
      
    } else {
      a <- bundle_prep[get(pair_1) == 1,]
      b <- bundle_prep[get(pair_2) == 1,]
    }
    
    a[,paste0(c(reference_def, alternate_defs)) := NULL]
    b[,paste0(c(reference_def, alternate_defs)) := NULL]
    
    ## Merging them back together to create all permutations of alt/ref
    if (subnat_to_nat) {
      df <- merge(a, b, by=c("sex", "measure", "loc_match"), allow.cartesian=T)
    } else {
      df <- merge(a, b, by=c("sex", "measure", "location_id", "location_name"), allow.cartesian=T)
    }
    
    if (nrow(df) > 0) {
      df[, comp_1 := paste0(pair_1)]
      df[, comp_2 := paste0(pair_2)]
      
      df[, age_diff := abs(age_mid.x - age_mid.y)]
      df[, year_diff := abs(year_mid.x - year_mid.y)]
      
      ## Potential matches:
      df <- df[age_diff <= age_overlap & year_diff <= year_overlap,]
      
      
      ## matches on mean +/- overlap if the range is not large, exact matches if it's a large range.
      ## So 15-20 (midpoint 17.5) would get matched to 17.5 +/- 5, but 15-80 would only be matched to 15-80.
      
      ## True matches:
      df[, is_match := ifelse(age_range.x <= age_range & age_range.y <= age_range & year_range.x <= year_range & year_range.y <= year_range, 1,
                              ifelse((year_range.x > year_range | year_range.y> year_range) & year_start.x == year_start.y & year_end.x == year_end.y & age_range.x <= age_range & age_range.y <= age_range, 1,
                                     ifelse((age_range.x > age_range | age_range.y > age_range) & age_start.x == age_start.y & age_end.x == age_end.y & year_range.x <= year_range & year_range.y <= year_range, 1,
                                            ifelse((year_range.x > year_range | year_range.y> year_range) & (age_range.x > age_range | age_range.y > age_range) & year_start.x == year_start.y & year_end.x == year_end.y & age_start.x == age_start.y & age_end.x == age_end.y, 1, 0))))]
      
      # drop large age ranges (which were exactly matched) if allow_wide_age_bin_match is set to F
      if(allow_wide_age_bin_match==F){
        df[(age_range.x > age_range | age_range.y > age_range), is_match := 0]
      }
      
      df <- df[is_match == 1,]
      
      ## Counting matches
      if (nrow(df) > 0) {
        if(subnat_to_nat){
          num_data_matches <- nrow(unique(df[, .(location_id.x, age_start.x, age_end.x, year_start.x, year_end.x, sex, nid.x, measure)])) +
            nrow(unique(df[, .(location_id.y, age_start.y, age_end.y, year_start.y, year_end.y, sex, nid.y, measure)]))
        } else {
          num_data_matches <- nrow(unique(df[, .(location_id, age_start.x, age_end.x, year_start.x, year_end.x, sex, nid.x, measure)])) +
            nrow(unique(df[, .(location_id, age_start.y, age_end.y, year_start.y, year_end.y, sex, nid.y, measure)]))
        }
        num_nids <- length(unique(c(df[, nid.x], df[, nid.y])))
        report_matches <- data.table(measure=unique(df$measure), comparison=comp, comp_1=paste0(pair_1), comp_2=paste0(pair_2), data_points=num_data_matches, nids=num_nids)
        matched_counts <- rbind(matched_counts, report_matches, fill=T)
        
      } else {
        report_matches <- data.table(measure=unique(data$measure), comparison=comp, comp_1=paste0(pair_1), comp_2=paste0(pair_2), data_points=0, nids=0)
        matched_counts <- rbind(matched_counts, report_matches, fill=T)
      }
      
      if (nrow(df) > 0) {
        
        df[, comparison := paste0(pair_1, "-", pair_2)]
        
        if (multiple_defs) {
          if (pair_1 %in% new_defs) {
            cvs <- strsplit(pair_1, "AND")[[1]]
            for (cv in cvs) {
              if (cv != reference_def) df[, paste0(cv) := 1]
            }
          } else {
            df[, paste0(pair_1) := 1]
          }
          if (pair_2 %in% new_defs) {
            cvs <- strsplit(pair_2, "AND")[[1]]
            for (cv in cvs) {
              if (!(cv %in% names(df))) df[, paste0(cv) := 0]
              if (cv != reference_def) df[, paste0(cv) := get(cv) - 1]
            }
          } else if (pair_2 != reference_def) df[, paste0(pair_2) := -1]
          
        } else {
          df[, paste0(pair_1) := 1]
          if (pair_2 != reference_def) df[, paste0(pair_2) := -1]
        }
        
        setnames(df, 'standard_error.x', 'alt_se')
        setnames(df, 'standard_error.y', 'ref_se')
        names(df) <- gsub(pattern = "\\.x", replacement = "_alt", x = names(df))
        names(df) <- gsub(pattern = "\\.y", replacement = "_ref", x = names(df))
        
        paired_data <- rbind(paired_data, df, fill=T)
        
      }
    }
    
  }## Done with for loop ##
  
  if (nrow(paired_data)==0) stop("You have no matches.")
  paired_data <- as.data.table(paired_data)
  paired_data[is.na(paired_data)] <- 0
  
  if (!quiet) message("Done cleaning")
  return(list(data=paired_data, original_data=bundle_prep, defs=defs, counts=matched_counts))
  
}

### TAKES DATA OUTPUT ###
log_transform_mrbrt <- function(df) {
  
  nec_cols <- c("mean_alt", "mean_ref", "alt_se", "ref_se")
  invisible(lapply(X = nec_cols, FUN = check_exists, df=df, warn=F))
  
  df[, ratio := mean_alt/mean_ref]
  df[, ratio_se := sqrt((mean_alt^2/mean_ref^2) * (alt_se^2/mean_alt^2 + ref_se^2/mean_ref^2))]
  
  df[, c("ratio_log", "ratio_se_log") := data.table(delta_transform(mean = ratio, sd = ratio_se, transformation = "linear_to_log"))]
  
  return(df)
  
}

logit_transform_mrbrt <- function(df) {
  
  nec_cols <- c("mean_alt", "mean_ref", "alt_se", "ref_se")
  invisible(lapply(X = nec_cols, FUN = check_exists, df=df, warn=F))
  
  df[, c("temp_logit_alt", "se_logit_mean_alt") := data.table(delta_transform(mean = mean_alt, sd = alt_se, transformation = "linear_to_logit"))]
  df[, c("temp_logit_ref", "se_logit_mean_ref") := data.table(delta_transform(mean = mean_ref, sd = ref_se, transformation = "linear_to_logit"))]
  
  df[, c("logit_diff", "logit_diff_se") := data.table(calculate_diff(df = df, alt_mean = "temp_logit_alt", alt_sd = "se_logit_mean_alt", ref_mean = "temp_logit_ref", ref_sd = "se_logit_mean_ref"))]
  
  return(df)
  
}

adj_mr_brt <- function(data, original_data=F,
                       sex = T,
                       age = T,
                       year = T,
                       nid = T,
                       fix_zeros=T,
                       age_mean=NULL,
                       age_sd=NULL,
                       xw_covs) {
  
  if (original_data) {
    if (!("age_mid" %in% names(data))) data[, age_mid := (age_start + age_end)/2]
  }
  
  if (fix_zeros) {
    if("ratio_log" %in% names(data)) {
      data[ratio_log == 0, ratio_log := .01*(median(data[ratio_log != 0, ratio_log]))]
      data[ratio_se_log == 0, ratio_se_log := .01*(median(data[ratio_se_log != 0, ratio_se_log]))]
    }
    if("logit_diff" %in% names(data)) {
      data[logit_diff == 0, logit_diff := .01*(median(data[logit_diff != 0 & logit_diff != 1, logit_diff]))]
      data[logit_diff_se == 0, logit_diff_se := .01*(median(data[logit_diff_se != 0 & logit_diff_se != 1, logit_diff_se]))]
    }
  }
  
  matches <- copy(data)
  if (sex) {
    if (nrow(matches[sex=="Both"]) > 0) stop("You need to sex-split your data.")
    matches[, male := ifelse(sex == "Male", 1, 0)]
  }
  if (nid) {
    if (original_data==F) matches[, id_var := .GRP, by=.(nid_ref, nid_alt)]
    if (original_data==T) matches[, id_var := nid]
  }
  if (age) {
    if (original_data==T) matches[, age_scaled := (age_mid - age_mean)/age_sd]
    if (original_data==F) matches[, age_scaled := (age_mid_ref - mean(age_mid_ref))/sd(age_mid_ref)]
  }
  if (year) {
    if (original_data==T) matches[, year_scaled := (year_mid - mean(year_mid))/sd(year_mid)]
    if (original_data==F) matches[, year_scaled := (year_mid_ref - mean(year_mid_ref))/sd(year_mid_ref)]
  }
  if (sex) matches[is.na(sex), sex :="Both"]
  
  # for any non-standard xwalk covariates (not age_scaled or male), create a single value by taking the mean of the alt and reference cov if necessary
  merge_covs <- xw_covs[xw_covs %ni% c("age_scaled", "male")]
  if(length(merge_covs)>0){
    for(cov in merge_covs){
      if(cov %ni% names(matches) & !original_data){
        matches[, paste0(cov) := (get(paste0(cov, '_alt'))+get(paste0(cov, '_ref')))/2]
      }
    }
  }
  
  return(matches)
  
}

sex_match <- function(data,
                      reference_def=NULL,
                      alternate_defs=NULL,
                      age_overlap=0,
                      year_overlap=0,
                      age_range=10,
                      year_range=10,
                      nid_match=T,
                      exact_match=F,
                      quiet=F) {
  
  age_overlap <- age_overlap
  year_overlap <- year_overlap
  
  ## If columns have dashes change them to underscores
  if (!(is.null(reference_def) | missing(reference_def))) reference_def <- gsub(reference_def, pattern = "-", replacement = "_")
  if (!(is.null(alternate_defs) | missing(alternate_defs))) alternate_defs <- gsub(alternate_defs, pattern = "-", replacement = "_")
  names(data) <- gsub(names(data), pattern = "-", replacement = "_")
  
  ## Make sure necessary columns exist
  nec_cols <- c("age_start", "age_end", "year_start", "year_end", "location_id", "sex", "measure", "location_name",
                "field_citation_value", "mean", "lower", "upper", "nid")
  invisible(lapply(nec_cols, check_exists, df=data))
  
  bundle_prep <- copy(data)
  
  ## If no reference definition inputted, this will create it.
  if (is.null(reference_def) | missing(reference_def)) {
    if (!(is.null(alternate_defs) | missing(alternate_defs))) {
      if (length(alternate_defs) > 1) bundle_prep[rowSums(bundle_prep[, ..alternate_defs])==0, reference := 1]
      if (length(alternate_defs) == 1) bundle_prep[get(alternate_defs) == 0, reference := 1]
      bundle_prep[is.na(reference), reference := 0]
      reference_def <- "reference"
    } else {
      bundle_prep[, reference := 1]
      reference_def <- "reference"
    }
  }
  
  ## Make 'defs' column so that sex matching only happens within the same case definitions
  if (!(is.null(alternate_defs) | missing(alternate_defs))) {
    defs <- c(reference_def, alternate_defs)
  } else {
    defs <- reference_def
  }
  
  ## See if there are multiple non-exclusive definitions
  bundle_prep$num_defs <- rowSums(bundle_prep[, ..defs])
  multiple_defs <- ifelse(max(bundle_prep$num_defs, na.rm = T) > 1, T, F)
  
  ## Tagging multiple definitions
  if (multiple_defs) {
    
    mult_combs <- unique(bundle_prep[num_defs>1, ..defs]) ## What are the rows with more than 1 def?
    new_defs <- vector()
    for (row in 1:nrow(mult_combs)) { 
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
      bundle_prep[eval(parse(text = command)), paste0(def) := 1] ## Evaluating it. When all defs are true and nothing else is true mark that new definition as 1
      bundle_prep[eval(parse(text = command)), combined_definition := 1]
      
    }
    bundle_prep[is.na(bundle_prep)] <- 0
    alternate_defs <- c(alternate_defs, new_defs)
  }
  
  defs <- c(reference_def, alternate_defs)
  invisible(lapply(defs, check_class, df=bundle_prep, class="numeric", coerce=T))
  
  ## Get a "Definition" column to merge onto
  if ("V1" %in% names(bundle_prep)) bundle_prep[, V1 := NULL]
  if (length(defs) > 1) {
    if (multiple_defs) {
      
      mults <- defs[grepl("AND", defs)]
      non_mults <- defs[!grepl("AND", defs)]
      for (mult in mults) bundle_prep[get(mult) == 1, Definition := paste0(mult)]
      for (non_mult in non_mults) bundle_prep[get(non_mult) == 1 & combined_definition == 0, Definition := paste0(non_mult)]
      
    } else {
      
      for (def in defs) bundle_prep[get(def) == 1, Definition := paste0(def)]
      
    }
  } else {
    bundle_prep[, Definition := paste0(reference_def)]
  }
  
  ## Generating midpoints of years and ages. Depending on overlap, we'll take mean +/- some number of years.
  bundle_prep[, age_mid := (age_start + age_end)/2]
  bundle_prep[, year_mid := (year_start + year_end)/2]
  
  ## Generating age ranges. If greater than age_range/year_range argument, we'll do exact matches rather than mean +/-.
  bundle_prep[, age_range := (age_end - age_start)]
  bundle_prep[, year_range := (year_end - year_start)]
  
  ## ID variable of location, sex, measure
  if (nid_match) {
    bundle_prep[, id_var := .GRP, by=.(nid, location_id, measure, Definition)]
  } else {
    bundle_prep[, id_var := .GRP, by=.(location_id, measure, Definition)]
  }
  
  ### MAIN CLEANING ###
  columns <- c("nid", "sex", "measure", "location_id", "location_name", "year_start", "year_end", "age_start", "age_end", "standard_error",
               "field_citation_value", "mean", "lower", "upper", "age_mid", "age_range", "year_mid", "year_range", "Definition", "id_var")
  bundle_prep <- bundle_prep[, ..columns]
  
  if (!quiet) message("Pairing matches and calculating SE.")
  
  a <- bundle_prep[sex=="Female",]
  b <- bundle_prep[sex=="Male",]
  
  ## Merging them back together to create all permutations of alt/ref
  df <- merge(a, b, by="id_var", allow.cartesian=T)
  
  df[, age_diff := abs(age_mid.x - age_mid.y)]
  df[, year_diff := abs(year_mid.x - year_mid.y)]
  
  ## Potential matches:
  df <- df[age_diff <= age_overlap & year_diff <= year_overlap,]
  
  ## True matches:
  df[, is_match := ifelse(age_range.x <= age_range & age_range.y <= age_range & year_range.x <= year_range & year_range.y <= year_range, 1,
                          ifelse((year_range.x > year_range | year_range.y> year_range) & year_start.x == year_start.y & year_end.x == year_end.y & age_range.x <= age_range & age_range.y <= age_range, 1,
                                 ifelse((age_range.x > age_range | age_range.y > age_range) & age_start.x == age_start.y & age_end.x == age_end.y & year_range.x <= year_range & year_range.y <= year_range, 1,
                                        ifelse((year_range.x > year_range | year_range.y> year_range) & (age_range.x > age_range | age_range.y > age_range) & year_start.x == year_start.y & year_end.x == year_end.y & age_start.x == age_start.y & age_end.x == age_end.y, 1, 0))))]
  df <- df[is_match == 1,]
  
  if (exact_match) df <- df[age_start.x == age_start.y & age_end.x == age_end.y & year_start.x == year_start.y &  year_end.x == year_end.y,]
  
  if (nrow(df) > 0) {
    num_data_matches <- nrow(unique(df[, .(location_id.x, age_start.x, age_end.x, year_start.x, year_end.x, nid.x)])) +
      nrow(unique(df[, .(location_id.y, age_start.y, age_end.y, year_start.y, year_end.y, nid.y)]))
    num_nids <- length(unique(c(df[, nid.x], df[, nid.y])))
    matched_counts <- data.table(comparison="Female-Male", data_points=num_data_matches, nids=num_nids)
    
  } else {
    matched_counts <- data.table(comparison="Female-Male", data_points=0, nids=0)
  }
  
  if (nrow(df) == 0) stop("You have no matches.")
  if (nrow(df) > 0) {
    df[, comparison := "Female-Male"]
    setnames(df, 'standard_error.x', 'alt_se')
    setnames(df, 'standard_error.y', 'ref_se')
    names(df) <- gsub(pattern = "\\.x", replacement = "_alt", x = names(df))
    names(df) <- gsub(pattern = "\\.y", replacement = "_ref", x = names(df))
    paired_data <- copy(df)
  }
  
  return(list(data=paired_data, matches=matched_counts))
  
}
