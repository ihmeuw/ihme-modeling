
##
## Author: USERNAME
## Date: 3/8/19
## Purpose: Cleaning & helper functions for MR-BRT modeling.
##          Create matches, count matches, log-transform the ratios, sex-match, etc.
##

os <- .Platform$OS.type
if (os=="windows") {
  j <- "J:/"
  h <-"H:/"
} else {
  j <- "/home/j/"
  h <-paste0("/homes/", Sys.info()["user"], "/")
}


date<-gsub("-", "_", Sys.Date())

suppressPackageStartupMessages(library(combinat, lib='FILEPATH'))
library(msm)
pacman::p_load(ggplot2, data.table, openxlsx, gtools, stringr)


source("FILEPATH") ## helper functions from Simon

source("FILEPATH")


#############################################################################################
############################# MAIN FUNCS ####################################################
#############################################################################################

match_mr_brt <- function(data,
                         reference_def=NULL,
                         alternate_defs,
                         year_overlap=5,
                         age_overlap=5,
                         age_range=10,
                         year_range=10,
                         fix_zeros=T,
                         offset=F,
                         subnat_to_nat=F,
                         forced_denominator=NULL,
                         quiet=F) {

  age_overlap <- age_overlap
  year_overlap <- year_overlap

  ## Cleaning reference and alternate definitions in case they have dashes.
  if (!(is.null(reference_def) | missing(reference_def))) reference_def <- gsub(reference_def, pattern = "-", replacement = "_")
  alternate_defs <- gsub(alternate_defs, pattern = "-", replacement = "_")
  names(data) <- gsub(names(data), pattern = "-", replacement = "_")

  bundle_prep <- copy(data)

  ## Checking the existance of key columns.
  nec_cols <- c("age_start", "age_end", "year_start", "year_end", "location_id", "sex", "measure", "location_name",
                "field_citation_value", "mean", "lower", "upper", "standard_error")
  lapply(nec_cols, check_exists, df=data)

  ## If no reference definition inputted, this will create it.
  if (is.null(reference_def) | missing(reference_def)) {
    if (length(alternate_defs) > 1) bundle_prep[rowSums(bundle_prep[, ..alternate_defs])==0, reference := 1]
    if (length(alternate_defs) == 1) bundle_prep[get(alternate_defs) == 0, reference := 1]
    bundle_prep[is.na(reference), reference := 0]
    reference_def <- "reference"
  }
  defs <- c(reference_def, alternate_defs)
  for (def in defs) {
    bundle_prep[is.na(get(def)),paste0(def) := 0]
  }

  ## If toggled, this will match subnational data points to national data points and vice versa. Pulls in location metadata
  ## from a central function so might throw a Python warning
  if (subnat_to_nat) {
    locs <- get_location_metadata(gbd_round_id = 6, location_set_id = 35)
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

  ## Generating age ranges. If greater than age_range/year_range argument, we'll do exact matches rather than mean +/-.
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

  ## Offsetting instances where mean = 0 by 1% of median of non-zero values (like Dismod). Or just dropping zero data points.
  if (offset) {
    if (nrow(bundle_prep[mean==0])>0) {
      off <- .01*median(bundle_prep[mean != 0, mean])
      bundle_prep[mean==0, mean := off]
      if (!quiet) message("You have zeros in your dataset that are being offset by 1% of the median of all non-zero values. To avoid this, set offset=F and set fix_zeros to T.")
    }
  }
  if (fix_zeros) {
    bundle_prep <- bundle_prep[mean != 0,]
    if (!quiet) message("There are some data points with mean=0, which makes the ratios difficult. Dropping these data points from the list of possible matches. Alternatively, you could set offset=T to offset zero-values instead of dropping them.")
  }

  ### Checking to see if some rows have multiple definitions that are not mutually exclusive (i.e. you are nonfatal & blood-test.)
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
    ## If multiple combinations exist (A-B-C, for example) we have to be more careful. We don't want to say "match anything with A to anything with B"
    ## because A-B-C would be matched to itself, and would generally be matched many times. At the moment, we do this:
    ##    - Match needs to have A-B-C
    ##    - We check if there are any other new multiple-combinations (like A-B-C-D) that would also be caught. It has to NOT have those.
    ##    - null_1 and null_2 execute that: it says HAVE your def, and DON'T HAVE any other new definition except the one you want.
    ## if (length(setdiff...)) is a necessary but confusing check.
    ## SEB - it's likely we update the way we do this matching.

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


      ## -- matches on mean +/- overlap if the range is not large(r than specified), exact matches if it's a large range.
      ## So 15-20 (midpoint 17.5) would get matched to 17.5 +/- 5, but 15-80 would only be matched to 15-80.

      ## True matches:
      df[, is_match := ifelse(age_range.x <= age_range & age_range.y <= age_range & year_range.x <= year_range & year_range.y <= year_range, 1,
                              ifelse((year_range.x > year_range | year_range.y> year_range) & year_start.x == year_start.y & year_end.x == year_end.y & age_range.x <= age_range & age_range.y <= age_range, 1,
                                     ifelse((age_range.x > age_range | age_range.y > age_range) & age_start.x == age_start.y & age_end.x == age_end.y & year_range.x <= year_range & year_range.y <= year_range, 1,
                                            ifelse((year_range.x > year_range | year_range.y> year_range) & (age_range.x > age_range | age_range.y > age_range) & year_start.x == year_start.y & year_end.x == year_end.y & age_start.x == age_start.y & age_end.x == age_end.y, 1, 0))))]

      df <- df[is_match == 1,]

      ## Counting matches
      if (nrow(df) > 0) {
        num_data_matches <- as.data.table(table(unique(df[, .(age_start.x, age_end.x, year_start.x, year_end.x, nid.x, measure)])$measure) +
                                            table(unique(df[, .(age_start.y, age_end.y, year_start.y, year_end.y, nid.y, measure)])$measure))
        num_nids <- as.data.table(table(unique(df[, nid.x, measure])$measure) + table(unique(df[, nid.y, measure])$measure))
        report_matches <- data.table(measure=num_nids$V1, comparison=comp, comp_1=paste0(pair_1), comp_2=paste0(pair_2), data_points=num_data_matches$N, nids=num_nids$N)
        matched_counts <- rbind(matched_counts, report_matches, fill=T)

      } else {
        num_data_matches <- data.table(V1="NA", N="0")
        num_nids <- data.table(V1="NA", N="0")
        report_matches <- data.table(measure=num_nids$V1, comparison=comp, comp_1=paste0(pair_1), comp_2=paste0(pair_2), data_points=num_data_matches$N, nids=num_nids$N)
        matched_counts <- rbind(matched_counts, report_matches, fill=T)
      }


      ## Create ratios if there is anything to compare
      if (nrow(df) > 0) {

        df[, ratio := mean.x/mean.y]
        df[, alt_se := standard_error.x]
        df[, ref_se := standard_error.y]
        df[, ratio_se := sqrt((mean.x^2/mean.y^2) * (alt_se^2/mean.x^2 + ref_se^2/mean.y^2))]
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

        names(df) <- gsub(pattern = "\\.x", replacement = "_alt", x = names(df))
        names(df) <- gsub(pattern = "\\.y", replacement = "_ref", x = names(df))

        paired_data <- rbind(paired_data, df, fill=T)

      }
    }

  }## Done with for loop ##

  if (nrow(paired_data)==0) stop("You have no matches.")
  paired_data <- as.data.table(paired_data)
  paired_data[is.na(paired_data)] <- 0

  if (!quiet) message("Done cleaning!")
  return(list(data=paired_data, original_data=bundle_prep, defs=defs, counts=matched_counts))

}

### TAKES OUTPUT OF MATCH_MR_BRT ###
count_matches <- function(df) {

  paired_data <- df$data
  defs <- df$defs
  matched_counts <- df$counts
  bundle_prep <- df$original_data

  ## Grab demographic information
  dems <- list()
  for (comp in unique(paired_data$comparison)) {
    for (meas in unique(paired_data$measure)) {
      if (nrow(paired_data[comparison==comp&measure==meas,])>0) {
        locs <- paired_data[comparison == comp & measure==meas, unique(location_name)]
        ages <- paired_data[comparison == comp & measure==meas, paste(range(age_start_alt, age_start_ref, age_end_alt, age_end_ref), collapse="-")]
        years <- paired_data[comparison == comp & measure==meas, paste(range(year_start_alt, year_start_ref, year_end_alt, year_end_ref), collapse="-")]
        sexes <- paired_data[comparison == comp & measure==meas, unique(sex)]
        dem <- list(measure = paste0(meas), locs = locs, ages = ages, years = years, sexes = sexes)
        dems[[paste0(comp)]] <- dem
      }
    }
  }

  ## This object will be passed out as "counts"
  matches <- copy(matched_counts)

  ## Grabbing the diagonal info
  for (each in defs) {
    for (meas in unique(paired_data$measure)) {
      num <- nrow(bundle_prep[measure==meas & get(each)==1,]) - nrow(unique(rbind(unique(paired_data[measure == meas & comp_1==each, .(year_start_alt, year_end_alt, age_start_alt, age_end_alt, mean_alt)]), unique(paired_data[measure == meas & comp_2==each, .(year_start_ref, year_end_ref, age_start_ref, age_end_ref, mean_ref)]), fill=T)))
      nids <- length(bundle_prep[measure == meas & get(each)==1,unique(nid)]) - length(unique(c(paired_data[measure == meas & comp_1==each, nid_alt], paired_data[measure == meas & comp_2==each,nid_ref])))
      add <- data.table(measure = meas, comparison=paste0(each, "-", each), data_points=num, nids=nids, comp_1=paste0(each), comp_2=paste0(each))
      matched_counts <- rbind(matched_counts, add)
    }
  }

  matched_counts[, nids := as.numeric(nids)]
  matched_counts[, data_points := as.numeric(data_points)]

  data_points <- list()
  nids <- list()
  ## Make the matrices, label the cols & rows, fill it in the for loops below.
  for (meas in unique(paired_data$measure)) {
    mat <- matrix(nrow=length(defs), ncol=length(defs) + 1)
    colnames(mat) <- c(defs, 'total')
    rownames(mat) <- defs

    nid <- matrix(nrow=length(defs), ncol=length(defs) + 1)
    colnames(nid) <- c(defs, 'total')
    rownames(nid) <- defs

    for (each in 1:length(defs)) {
      def <- defs[each]
      col <- ncol(mat)
      mat[each, col] <- nrow(bundle_prep[measure == meas & get(def) == 1,])
      for (other in 1:length(defs)) {
        if (!(identical(matched_counts[measure == meas & comp_1==defs[each] & comp_2 == defs[other], data_points], numeric(0)))) {
          mat[each, other] <- matched_counts[measure == meas & comp_1==defs[each] & comp_2 == defs[other], data_points]
        }
        if (is.na(mat[each, other])) mat[each, other] <- mat[other, each]
        if (is.na(mat[other, each])) mat[other, each] <- mat[each, other]
      }
    }
    mat[is.na(mat)] <- 0

    for (each in 1:length(defs)) {
      def <- defs[each]
      col <- ncol(nid)
      nid[each, col] <- length(unique(bundle_prep[measure == meas & get(def)==1,nid]))
      for (other in 1:length(defs)) {
        if (!(identical(matched_counts[measure == meas & comp_1==defs[each] & comp_2 == defs[other], nids], numeric(0)))) {
          nid[each, other] <- matched_counts[measure == meas & comp_1==defs[each] & comp_2 == defs[other], nids]
        }
        if (is.na(nid[each, other])) nid[each, other] <- nid[other, each]
        if (is.na(nid[other, each])) nid[other, each] <- nid[each, other]
      }
    }
    nid[is.na(nid)] <- 0
    data_points[[paste0(meas)]] <- mat
    nids[[paste0(meas)]] <- nid
  }


  return(list(data_points=data_points, nids=nids, data=paired_data, original_data=bundle_prep, demographics=dems))

}
#
# ### TAKES OUTPUT OF MATCH_MR_BRT ####
# find_missing <- function(df) {
#
#   matrix <- as.data.table(df$data_points$prevalence)
#   data <- df$original_data
#   defs <- colnames(matrix)[!(colnames(matrix) %in% c("sum", "missing", "names", "total"))]
#
#   matrix[, sum := rowSums(matrix)]
#
#   for (row in 1:nrow(matrix)) {
#     if (matrix[row, sum] == max(matrix[row,..defs])) matrix[row, missing := 1]
#   }
#
#   matrix[is.na(matrix)] <- 0
#   matrix[, def := defs]
#
#   missings <- matrix[missing==1,def]
#   rep <- list()
#   if (!is.null(missings)) {
#     for (each in unique(missings)) {
#       citations <- data[get(each) == 1, unique(field_citation_value)]
#       rep[paste0("Missing definition: ", each)] <- citations
#     }
#   }
#
#   return(list(missing_defs=missings, missing_citations=rep))
#
# }

### TAKES DATA OUTPUT ###
log_transform_mrbrt <- function(df) {

  nec_cols <- c("ratio", "ratio_se")
  lapply(X = nec_cols, FUN = check_exists, df=df, warn=F)

  df[, ratio_log := log(ratio)]
  df$ratio_se_log <- sapply(1:nrow(df), function(i) {
    ratio_i <- df[i, "ratio"]
    ratio_se_i <- df[i, "ratio_se"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })

  return(df)

}

logit_transform_mrbrt <- function(df) {

  # df[, logit_diff := logit(mean_alt) - logit(mean_ref)] # logit difference: "ratio"

  df[, temp_logit_alt := log(mean_alt / (1-mean_alt))]
  df[, temp_logit_ref := log(mean_ref / (1-mean_ref))]
  df[, logit_diff := temp_logit_alt - temp_logit_ref]

  df$se_logit_mean_alt <- sapply(1:nrow(df), function(i) {
    alt_i <- df[i, mean_alt]
    alt_se_i <- df[i, alt_se]
    deltamethod(~log(x1/(1-x1)), alt_i, alt_se_i^2)
  })

  df$se_logit_mean_ref <- sapply(1:nrow(df), function(i) {
    ref_i <- df[i, mean_ref]
    ref_se_i <- df[i, ref_se]
    deltamethod(~log(x1/(1-x1)), ref_i, ref_se_i^2)
  })

  df[, logit_diff_se := sqrt(se_logit_mean_alt^2 + se_logit_mean_ref^2)] # se for logit difference

  return(df)

}

adj_mr_brt <- function(data, original_data=F,
                       sex = T,
                       age = T,
                       year = T,
                       nid = T,
                       fix_zeros=T,
                       age_mean=NULL,
                       age_sd=NULL) {

  if (original_data) {
    if (!("age_mid" %in% names(data))) data[, age_mid := (age_start + age_end)/2]
    data[, mean_log := log(mean)]
    data[, mean_logit := log(mean / (1-mean))]
  }

  if (fix_zeros) {
    if("ratio_log" %in% names(data)) {
      data[ratio_log == 0, ratio_log := .01*(median(data[ratio_log != 0, ratio_log]))]
      data[ratio_se_log == 0, ratio_se_log := .01*(median(data[ratio_se_log != 0, ratio_se_log]))]
    }
    if("logit_diff" %in% names(data)) {
      data[logit_diff == 0 || logit_diff == 1, logit_diff := .01*(median(data[logit_diff != 0, logit_diff]))]
      data[logit_diff_se == 0 || logit_diff == 1, logit_diff_se := .01*(median(data[logit_diff_se != 0, logit_diff_se]))]
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
  #matches[is.na(matches)] <- 0

  #for (col in c()) matches[is.na(get(col)), paste0(col) := 0]

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
                     fix_zeros=T,
                     offset=F,
                     quiet=F) {

  age_overlap <- age_overlap
  year_overlap <- year_overlap

  if (!(is.null(reference_def) | missing(reference_def))) reference_def <- gsub(reference_def, pattern = "-", replacement = "_")

  #num_cols <- c(reference_def, alternate_defs)
  #lapply(X = num_cols, FUN = check_class, df=data, class="numeric")

  if (!(is.null(alternate_defs) | missing(alternate_defs))) alternate_defs <- gsub(alternate_defs, pattern = "-", replacement = "_")

  names(data) <- gsub(names(data), pattern = "-", replacement = "_")

  bundle_prep <- copy(data)

  nec_cols <- c("age_start", "age_end", "year_start", "year_end", "location_id", "sex", "measure", "location_name",
               "field_citation_value", "mean", "lower", "upper", "nid")
  lapply(nec_cols, check_exists, df=data)

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

  if (!(is.null(alternate_defs) | missing(alternate_defs))) {
    defs <- c(reference_def, alternate_defs)
  } else {
    defs <- reference_def
  }

  if ("V1" %in% names(bundle_prep)) bundle_prep[, V1 := NULL]
  if (length(defs) > 1) {
    lapply(defs, check_class, df=bundle_prep, class="numeric", coerce=T)
    bundle_prep <- melt(bundle_prep, measure.vars = defs)
    bundle_prep <- bundle_prep[value==1,]
    setnames(bundle_prep, "variable", "Definition")
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

  paired_data <- data.table()
  matched_counts <- data.table()

  ## Offsetting instances where mean = 0 by 1% of median of non-zero values (like Dismod).
  if (offset) {
    if (nrow(bundle_prep[mean==0])>0) {
      off <- .01*median(bundle_prep[mean != 0, mean])
      bundle_prep[mean==0, mean := off]
      if (!quiet) message("You have zeros in your dataset that are being offset by 1% of the median of all non-zero values. To avoid this, set offset=F and remove such 0 values before inputting into MR software.")
    }
  }

  if (fix_zeros) {
    bundle_prep <- bundle_prep[mean != 0,]
    if (!quiet) message("Dirty fix for mean=0, just dropping those data points. Working on a better fix.")
  }

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

  if (nrow(df) > 0) {
    num_data_matches <- as.data.table(nrow(unique(df[, .(age_start.x, age_end.x, year_start.x, year_end.x, nid.x)])) +
                                        nrow(unique(df[, .(age_start.y, age_end.y, year_start.y, year_end.y, nid.y)])))
    num_nids <- as.data.table(length(unique(df[, nid.x])))
    report_matches <- data.table(comparison="Female-Male", data_points=num_data_matches$V1, nids=num_nids$V1)
    matched_counts <- rbind(matched_counts, report_matches, fill=T)

  } else {
    num_data_matches <- data.table(V1="NA", N="0")
    num_nids <- data.table(V1="NA", N="0")
    report_matches <- data.table(comparison="Female-Male", data_points=num_data_matches$N, nids=num_nids$N)
    matched_counts <- rbind(matched_counts, report_matches, fill=T)
  }

  if (exact_match) df <- df[age_start.x == age_start.y & age_end.x == age_end.y & year_start.x == year_start.y &  year_end.x == year_end.y,]

 ## Create ratios if there is anything to compare

  if (nrow(df) == 0) stop("You have no matches.")
  if (nrow(df) > 0) {
   df[, ratio := mean.x/mean.y]
   #df[, alt_se := (upper.x - lower.x)/3.92]
   #df[, ref_se := (upper.y - lower.y)/3.92]
   df[, alt_se := standard_error.x]
   df[, ref_se := standard_error.y]
   df[, ratio_se := sqrt((mean.x^2/mean.y^2) * (alt_se^2/mean.x^2 + ref_se^2/mean.y^2))]
   df[, comparison := "Female-Male"]
   names(df) <- gsub(pattern = "\\.x", replacement = "_alt", x = names(df))
   names(df) <- gsub(pattern = "\\.y", replacement = "_ref", x = names(df))
   paired_data <- rbind(paired_data, df, fill=T)
  }

 return(list(data=paired_data, matches=matched_counts))

  }


## USERNAME's function
##USERNAME: identify rows where mean is contained inside adjusted SE
find_mrbrt_overlaps<-function(model_obj, adjust_zcovs=T){

  ##sy: pull in data
  trimmed_data<-copy(model_obj$data_objects$trimmed_data)
  cov_meta<-copy(model_obj$data_objects$cov_meta)
  model_coefs<-copy(model_obj$model_coefs)

  counter_preds<-copy(trimmed_data)
  counter_preds<-predict_mr_brt(model_obj, counter_preds, temp_folder=temp_folder)
  counter_preds<-counter_preds$model_summaries

  trimmed_data[, adjusted_data:=get(model_obj$data_objects$values[1])]

  trimmed_data[, adjusted_se:=get(model_obj$data_objects$values[2])]
  #trimmed_data[, adjusted_data:=adjusted_data + (counter_preds$Y_mean)] ##sy: don't adjust data


  ##sy: adjust z covs
  for(i in 1:nrow(cov_meta)){
    var<-cov_meta[i, covariate]
    if(cov_meta[i, design_matrix]=="Z" & adjust_zcovs==T & var!="intercept"){
      trimmed_data[, adjusted_se:=exp(log(adjusted_se) - (get(var) * model_coefs[z_cov==var, gamma_soln]))]
    }
  }

  ##sy: calculate upper/lower
  trimmed_data[, `:=` (adj_lower=adjusted_data - qnorm(.975)*adjusted_se, adj_upper=adjusted_data + qnorm(.975)*adjusted_se) ]

  ##sy: find if predicted mean for a row is between data interval
  mean_cov<-ifelse(counter_preds$Y_mean %between% list(trimmed_data$adj_lower, trimmed_data$adj_upper), "Data within funnel", "Data outside funnel")
  trimmed_data[, mean_covered:=mean_cov]
  return(trimmed_data)
}

## USERNAMES's functions
summaries <- function(dt, draw_vars){
  sum <- copy(dt)
  sum[, mean := rowMeans(.SD), .SDcols = draw_vars]
  sum[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draw_vars]
  sum[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draw_vars]
  sum[, c(draw_vars) := NULL]
  return(sum)
}

get_con <- function(dbname, host) {
  if (.Platform$OS.type == "windows") {
    j <- "FILEPATH"
  } else {
    j <- "FILEPATH"
  }
  credentials <- fread(paste0(j, "FILEPATH"))
  con <- suppressWarnings(src_mysql(dbname = dbname, host = host, user = credentials$user, password = credentials$pw))
  return(con)
}
run_query <- function(dbname, host, query) {
  con <- get_con(dbname, host)
  return(con %>% tbl(sql(query)) %>% collect(n=Inf) %>% data.table)
  disconnect()
}
disconnect <- function() {
  lapply( dbListConnections(MySQL()), function(x) dbDisconnect(x) )
}
get_bundle_version_ids<-function(bids){

  dbname <- "ADDRESS"
  host <- "ADDRESS"

  # Query tables

  ### epi.model_version : model run specs ######################################################
  string<-c()
  for (bid in bids){
    if (is.null(string)==T){
      string<-paste0(bid)
    }else{
      string<-paste0(string," OR bundle_id =",bid)
    }
  }
  query<- paste0("SELECT
                      bundle_version.bundle_version.bundle_version_id,
                      bundle_version.bundle_version.bundle_id,
                      bundle_version.bundle_version.gbd_round_id,
                      bundle_version.bundle_version.date_inserted,
                      bundle_version.bundle_version.last_updated,
                      bundle_version.bundle_version_decomp_step.decomp_step_id,
                      shared.cause.acause
                  FROM
                      bundle_version.bundle_version
                          LEFT JOIN
                      (bundle_version.bundle_version_decomp_step) ON (bundle_version.bundle_version.bundle_version_id = bundle_version.bundle_version_decomp_step.bundle_version_id)
                          LEFT JOIN
                      (bundle.bundle) ON (bundle_version.bundle_version.bundle_id = bundle.bundle.bundle_id)
                          LEFT JOIN
                      (shared.cause) ON (bundle.bundle.cause_id = shared.cause.cause_id)
                  WHERE
                      bundle_version.bundle_version.bundle_id =",string)
  query<-gsub("\n","",query)
  bdt<-run_query(dbname,host,query)
  return(bdt)
}
get_xwalk_version_ids<-function(bid,version_dt){

  dbname <- "ADDRESS"
  host <- "ADDRESS"
  bvids <- version_dt$bundle_version_id
  if (length(bvids) > 1){
    bvid_list <- paste0(bvids[1], paste0(", ", bvids[-1], collapse = ""), collapse = ",")
  } else {
    bvid_list <- bvids
  }

  # Query tables

  ### epi.model_version : model run specs ######################################################
  query<- paste0("SELECT crosswalk_version_id,bundle_version_id,crosswalk_description,date_inserted,last_updated
                 FROM crosswalk_version.crosswalk_version WHERE bundle_version_id IN (",bvid_list, ")")
  bdt<-run_query(dbname,host,query)
  bdt <- merge(bdt, version_dt[, .(bundle_id, bundle_version_id, decomp_step_id, acause)], by = "bundle_version_id")
  return(bdt)
}

get_most_recent_bundle_vid <- function(bid) {

  bundle_versions <- get_bundle_version_ids(bid)
  id <- bundle_versions[date_inserted == max(bundle_versions$date_inserted), bundle_version_id]

  return(id)
}


get_model_version_ids <- function(meid) {

  dbname <- "ADDRESS"
  host <- "ADDRESS"

  # Query tables

  ### epi.model_version : model run specs ######################################################
  string<-c()
  for (bid in meid){
    if (is.null(string)==T){
      string<-paste0(bid)
    }else{
      string<-paste0(string," OR epi.model_version.modelable_entity_id =",bid)
    }
  }
  query<- paste0("SELECT
                      epi.model_version.model_version_id,
                      epi.model_version.modelable_entity_id,
                      epi.model_version.gbd_round_id,
                      epi.model_version.decomp_step_id,
                      epi.model_version.description,
                      epi.model_version.date_inserted,
                      epi.model_version.last_updated,
                      epi.model_version.best_description
                  FROM
                      epi.model_version
                  WHERE
                      epi.model_version.modelable_entity_id =",string)
  query<-gsub("\n","",query)
  bdt<-run_query(dbname,host,query)
  return(bdt)

}


get_most_recent_model_vid <- function(meid) {

  versions <- get_model_version_ids(meid)
  id <- versions[date_inserted==max(versions$date_inserted), model_version_id]

  id

}
