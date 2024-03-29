##########################################################################
### Project: GBD Nonfatal Estimation
### Purpose: CROSSWALKING GBD 2020 - functions
##########################################################################


get_dem_data <- function(topic, sub_topic = NULL, recall_expand = T, crosswalk, covariates, keep_more_cols){
  
  if(topic == "alc_dep"){
    
    if (F){
    df <- read.csv("FILEPATH")
    df$issue_sum <- df$any_adjustment + df$duplicate_extract + df$error_extract + df$gr0_poor_quality + df$reextraction_needed
    
    df_usable <- df[df$issue_sum == 0,]
    bundle <- as.data.table(df_usable)
        
    bundle[,age_middle:= (age_start + age_end) / 2]
    
    # Get counts (0 is ref, 1 is alternate definition)
    bundle$new_recall <- ifelse(bundle$recall_type == "Not Set", "Not Set",
                         ifelse(bundle$recall_type == "Point", "Point",
                         ifelse(bundle$recall_type == "Period: days"   & bundle$recall_type_value == 30, "1 month",
                         ifelse(bundle$recall_type == "Period: months" & bundle$recall_type_value == 1,  "1 month",
                         ifelse(bundle$recall_type == "Period: months" & bundle$recall_type_value == 12, "1 year",
                         ifelse(bundle$recall_type == "Period: years"  & bundle$recall_type_value == 1,  "1 year", 
                         ifelse(bundle$recall_type == "Period: weeks"  & bundle$recall_type_value == 2,  "2 weeks",
                         ifelse(bundle$recall_type == "Period: months" & bundle$recall_type_value == 6,  "6 months",
                                                                                        ,"000")))))))))
    
    if(recall_expand == T){
      bundle$new_recall <- ifelse(bundle$recall_type == "Period: days"   & bundle$recall_type_value == 30, "Point",
                           ifelse(bundle$recall_type == "Period: months" & bundle$recall_type_value == 1,  "Point",
                           ifelse(bundle$recall_type == "Period: weeks"  & bundle$recall_type_value == 2,  "Point",
                                                bundle$new_recall)))
    }
    
    bundle$cv_recall_1year   <- ifelse(bundle$new_recall == "1 year", 1, 0)
    }
    bundle <- fread("FILEPATH")
    
    
    
    bundle <- bundle[,audit_saq := cv_audit_saq]
    bundle <- bundle[,abuse_dependence := cv_abuse_dependence]
    bundle <- bundle[,recall_1year := cv_recall_1year]
    
    if (sub_topic == "recall"){
    bundle <- bundle[bundle$new_recall == "Point" | bundle$new_recall == "1 year",]
    }
    
    bundle <- bundle[measure == "prevalence"]
    dt <- as.data.table(bundle)
    
    } else if (topic == "fetal alcohol syndrome"){
      
      df <- fread("FILEPATH")
      
    df <- df[group_review %in% c(1, NA)]
    df <- df[measure == "prevalence"]
    
    dt <- as.data.table(df)
    
    
    } 
  
  #  #subset bundle to useful columns
  columns <- dt[,c("nid","seq", "location_id", "location_name", "sex"
                   ,"year_start", "year_end", "age_start", "age_end", "measure"
                   , "mean", "lower", "upper", "standard_error", "cases", "sample_size"
                   , "urbanicity_type"
                   , eval(crosswalk), eval(covariates), eval(keep_more_cols))]
  dt <- dt[,..columns]
  dt <- unique(dt)
  print(paste0("Deleting zero prevalence rows: ", nrow(dt[mean <= 0, ])))
  dt <- dt[mean > 0]
  
  if (topic == "alc_dep" & sub_topic == "audit"){
        
    
    df <- fread("FILEPATH")
    
    df[,c("Title/Abst Incl/Excl", "Full Text Incl/Excl", "Reason for Exclude", "PMID Link", "Language", 
          "Article Title", "Authors", "Article Type", "Citation Medium", "Got PDF?")] <- NULL
    
    df <- rename(df, "gold_standard" = `Gold Standard`)
    df <- rename(df, "screening_tool" = `Screening Tool`)
    df <- rename(df, "screening_score" = `Screening Score`)
    df <- rename(df, "gold_cases" = `Gold Standard Prevalence`)
    df <- rename(df, "screening_cases" = `Screening Tool Prevalence`)
    
    df$`Sample Size`<- ifelse(df$`Sample Size` == "13,067", "13067", df$`Sample Size`)
    df$`Sample Size`<- ifelse(df$`Sample Size` == "13,879", "13879", df$`Sample Size`)
    df$sample_size <- as.numeric(df$`Sample Size`)
    df$`Sample Size` <- NULL
    
    df <- df[gold_standard != "MINI"]
    df <- df[screening_tool != "brief AUDIT"]
    df <- df[case_definition != "lifetime dependence + abuse"]
    df <- df[case_definition != "lifetime dependence"]
    
    df$case_definition <- ifelse(df$case_definition == "past-year dependence", "dependence", df$case_definition)
    df$case_definition <- ifelse(df$case_definition == "past-year dependence + abuse", "dependence + abuse", df$case_definition)
    df$case_definition <- ifelse(df$case_definition == "depencence + abuse", "dependence + abuse", df$case_definition)
    
    
    df$screening_cases <- (df$Sensitivity*df$gold_cases) + (df$sample_size- df$gold_cases - ((df$sample_size-df$gold_cases)*df$Specificity))
    
    df$gold_prevalence <- df$gold_cases/df$sample_size
    df$screen_prevalence <- df$screening_cases/ df$sample_size
        
    dep <- df[case_definition == "dependence"]

    if (F){
    
    ggplot(dep[screening_tool == "AUDIT"]) +
      geom_point(aes(x= screening_score, y = screen_prevalence, color = drinking_status, shape = as.factor(diff_cut))) +
      geom_point(data = dep[screening_tool == "AUDIT-C"], aes(x= screening_score, y = screen_prevalence, color = drinking_status)) +
      xlab("Screening Score") +
      ylab("Prevalence (%)") +
      geom_vline(xintercept = 8, alpha = 1/10, color = "red") +
      geom_vline(xintercept = 3, alpha = 1/5, color = "green") +
      geom_vline(xintercept = 4, alpha = 1/10, color = "purple")
    
    table(dep$screening_tool)
    }
    
    test1 <- dep[(screening_tool == "AUDIT"   & screening_score == 8) |
                   (screening_tool == "AUDIT-C" & sex == "Male" & screening_score == 4) |
                   (screening_tool == "AUDIT-C" & sex == "Female" & screening_score == 3)|
                   (screening_tool == "AUDIT-C" & sex == "Both" & screening_score == 4)]
    
    test2 <- dep[(screening_tool == "AUDIT"   & screening_score == 9  & NID == 410474)]
    test3 <- dep[(screening_tool == "AUDIT"   & screening_score == 13 & NID == 410497)]
    
    dep <- rbind(test1, test2)
    dep <- rbind(dep, test3)
    
    if (F){
    ggplot(dep[screening_tool == "AUDIT"]) +
      geom_point(aes(x= screening_score, y = screen_prevalence, color = screening_tool, shape = as.factor(diff_cut))) +
      geom_point(data = dep[screening_tool == "AUDIT-C"], aes(x= screening_score, y = screen_prevalence, color = sex)) +
      xlab("Screening Score") +
      ylab("Prevalence (%)") +
      geom_vline(xintercept = 8, alpha = 1/10, color = "red") +
      geom_vline(xintercept = 3, alpha = 1/5, color = "green") +
      geom_vline(xintercept = 4, alpha = 1/10, color = "purple")
    
    ggplot(dep, aes(x = screening_score, fill = as.factor(location_name))) + geom_histogram(bins = 50)
    
    ggplot(dep) +
      geom_point(aes(x = gold_prevalence, y = st_prevalence, color = screening_tool)) +
      geom_abline(slope = 1, intercept = 0) + 
      xlab("Gold Standard Prevalence") +
      ylab("Screening Score Prevalence") 
    
    mean(dep$screening_cases/ dep$gold_cases)
    valid_nids <- unique(test$NID)
    ughhhh <- unique(dep$NID)
    setdiff(ughhhh, valid_nids)
    }
    
    dep <- rename(dep, nid = NID)
    
  
    
    dep$standard_error  <- NA
    dep$lower           <- NA
    dep$upper           <- NA
    
    gold <- copy(dep)
    
    gold$mean            <- gold$gold_prevalence
    gold$cases           <- gold$gold_cases 
    gold$cv_audit        <- 0
    
    dep$mean             <- dep$screen_prevalence
    dep$cases            <- dep$screening_cases
    dep$cv_audit         <- 1
    
    dep <- rbind(gold, dep)
        
    dep <- dep[,c("nid", "location_id", "location_name", "sex"
                     ,"year_start", "year_end", "age_start", "age_end"
                     , "mean", "lower", "upper", "standard_error", "cases", "sample_size"
                     , "cv_audit", "screening_tool", "drinking_status")]
    dep$measure <- "prevalence"
    dep$standard_error <- as.numeric(dep$standard_error)
    dep$age_start <- ifelse(is.na(dep$age_start), 0, dep$age_start)
    
    dt <- as.data.table(dep)

  }
  
  
  return(dt)
  
}

## FILL OUT MEAN/CASES/SAMPLE SIZE
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

## GET DEFINITIONS (ALL BASED ON CV'S - WHERE ALL 0'S IS REFERENCE)
get_definitions <- function(ref_dt){
  dt <- copy(ref_dt)
  cvs <- names(dt)[grepl("^cv_", names(dt)) & !names(dt) %in% cv_drop]
  cvs <- cvs[!cvs %in% cv_drop] 
  dt[, definition := ""]
  for (cv in cvs){
    dt[get(cv) == 1, definition := paste0(definition, "_", cv)]
  }
  dt[definition == "", definition := "reference"]
  return(list(dt, cvs))
}


subnat_to_nat <- function(subnat_dt){
  dt <- copy(subnat_dt)
  dt[, ihme_loc_id := NULL]
  dt <- merge(dt, loc_dt[, .(location_id, level, parent_id, location_type, ihme_loc_id)], by = "location_id")
  dt[, loc_match := location_id]
  dt[level >= 4, loc_match := parent_id]
  dt[level == 5 & grepl("CHN", ihme_loc_id), loc_match := 6]
  dt[level == 5 & grepl("KEN", ihme_loc_id), loc_match := 180]
  return(dt)
}

calc_year <- function(year_dt){
  dt <- copy(year_dt)
  dt[, year_match := (year_start+year_end)/2]
  return(dt)
}

## GET UNIQUE "AGE SERIES" FOR THE PURPOSE OF AGGREGATING
get_age_combos <- function(agematch_dt){
  dt <- copy(agematch_dt)
  by_vars <- c("nid", "location_id","year_match", "sex", "measure", names(dem_dt)[grepl("^cv_", names(dem_dt)) & !names(dem_dt) %in% cv_drop])
  dt[, age_n := .GRP, by = by_vars]
  small_dt <- copy(dt[, c(by_vars, "age_start", "age_end","age_n"), with = F])
  return(list(dt, small_dt))
}


## HELPER FUNCTION TO ROUND TO NEAREST AGE START/AGE END IN GBD-LAND
get_closest_age <- function(i, var, start = T, dt){
  age <- dt[i, get(var)]
  if (start == T){
    age_dt[, age_group_years_start][which.min(abs(age_dt[, age_group_years_start] - age))]
  } else if (start == F){
    age_dt[, age_group_years_end][which.min(abs(age_dt[, age_group_years_end] - age))]
  }
}


## AGGREGATE
aggregation <- function(start_match, end_match, dt = pull_dt){
  agg <- nrow(dt[start_match == age_start & end_match == age_end]) == 0 ## FLAGS TO AGGREGATE EACH SIDE OF THE RATIO
  agg1 <- nrow(dt[start_match == age_start2 & end_match == age_end2]) == 0
  z <- qnorm(0.975)
  if (agg == T){ ## AGGREGATE FIRST SIDE
    row_dt <- unique(dt[age_start >= start_match & age_end <= end_match], by = c("age_start", "age_end"))
    row_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size))]
    row_dt[, `:=` (mean = cases/sample_size, age_start = min(age_start), age_end = max(age_end))]
    row_dt[measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    row_dt[measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    row_dt[measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
    row_dt <- dplyr::select(row_dt, names(row_dt)[!grepl("2$", names(row_dt))])
    row_dt <- unique(row_dt, by = "nid")
  } else { ## OR LEAVE THE SAME
    row_dt <- unique(dt[age_start == start_match & age_end == end_match], by = c("age_start", "age_end"))
    row_dt <- dplyr::select(row_dt, names(row_dt)[!grepl("2$", names(row_dt))])
  }
  if (agg1 == T){ ## AGGREGATE SECOND SIDE
    row_dt2 <- unique(dt[age_start2 >= start_match & age_end2 <= end_match], by = c("age_start2", "age_end2"))
    row_dt2[, `:=` (cases2 = sum(cases2), sample_size2 = sum(sample_size2))]
    row_dt2[, `:=` (mean2 = cases2/sample_size2, age_start2 = min(age_start2), age_end2 = max(age_end2))]
    row_dt2[measure == "prevalence", standard_error2 := sqrt(mean2*(1-mean2)/sample_size2 + z^2/(4*sample_size2^2))]
    row_dt2[measure == "incidence" & cases2 < 5, standard_error2 := ((5-mean2*sample_size2)/sample_size2+mean2*sample_size2*sqrt(5/sample_size2^2))/5]
    row_dt2[measure == "incidence" & cases2 >= 5, standard_error2 := sqrt(mean2/sample_size2)]
    row_dt2 <- dplyr::select(row_dt2, names(row_dt2)[grepl("2$", names(row_dt2))])
    row_dt2 <- unique(row_dt2, by = "nid2")
  } else { ## OR LEAVE THE SAME
    row_dt2 <- unique(dt[age_start2 == start_match & age_end2 == end_match], by = c("age_start2", "age_end2"))
    row_dt2 <- dplyr::select(row_dt2, names(row_dt2)[grepl("2$", names(row_dt2))])
  }
  new_row <- cbind(row_dt, row_dt2) ## BUT THE SIDES BACK TOGETHER
  return(new_row)
}


## SET UP AGGREGATION
aggregate_tomatch <- function(match_dt, id){
  print(id)
  pull_dt <- copy(match_dt[agg_id == id])
  start_matches <- pull_dt[age_start_match == T, age_start] ## GET ALL START MATCHES 
  allend_matches <- pull_dt[age_end_match == T, unique(age_end)] ## GET ALL END MATCHES
  end_matches <- c() ## MAKE SURE ACTUAL END MATCHES MAP 1:1 WITH START MATCHES
  for (x in 1:length(start_matches)){
    match <- allend_matches[allend_matches > start_matches[x]]
    match <- match[which.min(match-start_matches[x])]
    end_matches <- c(end_matches, match)
  }
  aggregated <- rbindlist(lapply(1:length(start_matches), function(x) aggregation(start_match = start_matches[x],  end_match = end_matches[x], dt = pull_dt)))
  return(aggregated)
}

## FULL FUNCTION TO GET MATCHES
get_matches <- function(n, pair_dt, year_span = 10, age_span = 5, covariates, keep_more_cols){
  
  dt <- copy(pair_dt)
  pair <- pairs[,n]
  print(pair)
  keep_vars <- c("nid", "location_id", "sex", "year_start", "year_end", "measure", "age_start", "age_end",
                 "mean", "standard_error", "cases", "sample_size", "loc_match", "year_match", "age_n",
                 "seq", "urbanicity_type", covariates, keep_more_cols)
  
  dt$loc_match <- dt$location_id 
  
  dt1 <- copy(dt[definition == pair[1]])
  dt1 <- dplyr::select(dt1, keep_vars)
  dt2 <- copy(dt[definition == pair[2]])
  dt2 <- dplyr::select(dt2, keep_vars)
  
  setnames(dt2, keep_vars[!keep_vars  == "loc_match"], paste0(keep_vars[!keep_vars  == "loc_match"], "2"))
  matched <- merge(dt1, dt2, by = "loc_match", allow.cartesian = T) ## INITIAL CARTISIAN MERGE ONLY MATCHING ON LOCATION
  matched <- matched[sex == sex2 & measure == measure2 &
                       data.table::between(year_match, year_match2 - year_span/2, year_match2 + year_span/2)] ## FILETER OUT SEX, MEASURE, YEAR
  
  
  if (pair[1] == "_cv_marketscan" | pair[2] == "_cv_marketscan"){ ## EXACT LOCS FOR MARKETSCAN BUT CURRENTLY ALL ARE EXACT LOCS
    matched <- matched[location_id == location_id2]
  }
  matched[, age_match := (data.table::between(age_start, age_start2 - age_span/2, age_start2 + age_span/2) & data.table::between(age_end, age_end2 - age_span/2, age_end2 + age_span/2))]
  unmatched <- copy(matched[age_match == F])
  ## AGE ROUNDING
  unmatched$age_start <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_start", dt = unmatched))
  unmatched$age_end <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_end", start = F, dt = unmatched))
  unmatched$age_start2 <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_start2", dt = unmatched))
  unmatched$age_end2 <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_end2", start = F, dt = unmatched))
  
  ## FIND WHERE IT IS POSSIBLE TO AGGREGATE
  unmatched[, age_start_match := age_start == age_start2]
  unmatched[, age_end_match := age_end == age_end2]
  unmatched[, sum_start := sum(age_start_match), by = c("age_n", "age_n2")]
  unmatched[, sum_end := sum(age_end_match), by = c("age_n", "age_n2")]
  agg_matches <- copy(unmatched[sum_start > 0 & sum_end > 0 & !is.na(cases) & !is.na(sample_size) & !is.na(cases2) & !is.na(sample_size2)])
  agg_matches[, agg_id := .GRP, by = c("age_n", "age_n2")]
  
  ## AGGREGATE IF THERE ARE PLACES TO AGGREGATE OTHERWISE DO NOTHING
  if (nrow(agg_matches) > 0){
    aggregated <- rbindlist(lapply(1:agg_matches[, max(agg_id)], function(x) aggregate_tomatch(match_dt = agg_matches, id = x)))
    aggregated[, c("age_start_match", "age_end_match", "sum_start", "sum_end", "agg_id") := NULL]
    final_match <- rbind(matched[age_match == T], aggregated)
  } else {
    final_match <- copy(matched[age_match == T])
  }
  
  final_match[, c("age_match") := NULL]
  final_match[, `:=` (def = pair[1], def2 = pair[2])] ## LABEL WITH DEFINITIONS
  return(final_match)
}

create_ratios <- function(ratio_dt, x_walk_measure){
  
  if (x_walk_measure == "odds_ratio"){
  
    dt <- copy(ratio_dt)
    dt[, ratio := mean2/mean]
    dt[, ratio_se := sqrt((mean2^2 / mean^2) * (standard_error2^2/mean2^2 + standard_error^2/mean^2))]
    dt[, log_ratio := log(ratio)]
    dt$log_rse <- sapply(1:nrow(dt), function(i) {
      mean_i <- dt[i, "ratio"]
      se_i <- dt[i, "ratio_se"]
      deltamethod(~log(x1), mean_i, se_i^2)
  })
  
  } else if (x_walk_measure == "logit_difference"){
   
    dt <- copy(ratio_dt)
    dt[, `:=` (logit_mean = qlogis(mean), logit_mean2 = qlogis(mean2))]
    dt$logit_se <- sapply(1:nrow(dt), function(i){
      mean_i <- as.numeric(dt[i, "mean"])
      se_i <- as.numeric(dt[i, "standard_error"])
      deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
    })
    dt$logit_se2 <- sapply(1:nrow(dt), function(i){
      mean_i <- as.numeric(dt[i, "mean2"])
      se_i <- as.numeric(dt[i, "standard_error2"])
      deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
    })
    dt[, ldiff := logit_mean2 - logit_mean]
    dt[,se_diff_logit := sqrt(logit_se^2 + logit_se2^2)]
    
    }
  
  return(dt)
}

create_mrbrtdt <- function(match_dt, vars = cvs, xcovs, x_walk_measure){
  dt <- copy(match_dt)
  vars <- gsub("^cv_", "", vars)
  for (var in vars){
    dt[, (var) := as.numeric(grepl(var, def2)) - as.numeric(grepl(var, def))]
    if (nrow(dt[!get(var) == 0]) == 0){
      dt[, c(var) := NULL]
      vars <- vars[!vars == var]
    }
  }
  loc_dt <- as.data.table(loc_dt)
  loc_map <- copy(loc_dt[, .(location_ascii_name, location_id)])
  dt <- merge(dt, loc_map, by = "location_id")
  setnames(loc_map, c("location_id", "location_ascii_name"), c("location_id2", "location_ascii_name2"))
  dt <- merge(dt, loc_map, by = "location_id2")
  dt[, id := paste0(nid2, " (", location_ascii_name2, ": ",year_start2,"-",year_end2,") - ", nid, " (", location_ascii_name, ": ",year_start,"-",year_end,")")]
  
  
  if (x_walk_measure == "odds_ratio"){
    dt <- dt[, c("id", "log_ratio", "log_rse", "def", "def2", xcovs, vars), with = F]
  } else {
    dt <- dt[, c("id", "ldiff", "se_diff_logit", "def", "def2", xcovs, vars), with = F]
  }
    vars <- c(vars, xcovs)
  return(list(dt, vars))
}

explore_data <- function(data, covariate_name){
  data <- data[,covariate := data[,..covariate_name]]
  
  data$covariate <- as.factor(data$covariate)
  data <- merge(data, loc_dt, by = "location_id", all.x = T)
  data <- data[,age_mid := (age_start + age_end)/2]
  data <- data[,year_mid := (year_start + year_end)/2]
  
  if (covariate_name == "cv_abuse_dependence"){
    data$within_study <- as.numeric(data$within_study)
  }
  
  
  for (i in unique(data$sex)){
    p <- ggplot(data[year_end >= 1990 & sex == i], aes(x = year_mid, y = age_mid, color = covariate, alpha = 1/100)) +
      geom_point(size = 0.8) +
      geom_errorbar(aes(ymin=age_start, ymax=age_end, color = covariate), width=.1) +
      geom_segment(aes(x = year_start, xend= year_end, yend = age_mid)) +
      ggtitle(paste0("Potential Matches:", i)) +
      labs(color = parse(text=covariate_name)) +
      facet_wrap(facets = "super_region_name", nrow = 3, ncol = 3)
    
    print(p)
  }
  
  
  for (i in unique(data$sex)){
    p <- ggplot(data[year_end >= 1990 & sex == i], aes(x = year_mid, y = mean, color = covariate, alpha = 1/100)) +
      geom_point(size = 0.8) +
      geom_errorbar(aes(ymin=lower, ymax=upper, color = covariate), width=.1) +
      geom_segment(aes(x = year_start, xend= year_end, yend = mean)) +
      ggtitle(paste0("Prevalence of different groups:", i)) +
      labs(color = parse(text=covariate_name)) +
      facet_wrap(facets = "super_region_name", nrow = 3, ncol = 3)
    print(p)
    
  }
}

get_preds <- function(model, x_covs, z_covs){
  x_preds <- unique(mrbrt_dt[, c(x_covs), with = F])
  z_preds <- unique(mrbrt_dt[, c(z_covs), with = F])
  predicts <- predict_mr_brt(model, newdata = x_preds, z_newdata = z_preds, write_draws = T)
  return(predicts)
}


evaluate_trimming <- function(){
  
  files <- as.data.table(c("abuse_dependence"))
  
  names(files) <- "z_cov"
  for (i in seq(5,95,5)){
    
    path <- paste0(dem_dir, "FAS_zcov_trim")
    
    file <- fread(paste0(path,i,"%/model_coefs.csv"))
    file[,paste0("beta_",i) := exp(file$beta_soln)]
    gamma <- file$gamma_soln[1]
    file[,paste0("beta_lower2_",i) := exp(file$beta_soln - 1.96*sqrt(file$beta_var + gamma))]
    file[,paste0("beta_upper2_",i) := exp(file$beta_soln + 1.96*sqrt(file$beta_var + gamma))]
    file <- file[,c(1,14:16)]
    files <- merge(files, file, by = "x_cov")
  }
  
  files <- melt(files, id.vars = "x_cov")
  files$var <- ifelse(grepl(files$variable, pattern="upper"), "upper", NA)
  files$var <- ifelse(grepl(files$variable, pattern="lower"), "lower", files$var)
  files$var <- ifelse(is.na(files$var), "beta", files$var)
  
  files$trim <- NA
  for (i in seq(5,95,5)){
    files$trim <- ifelse(grepl(files$variable, pattern=i), i, files$trim)
  }
  
  files <- dcast(files, x_cov + trim ~ var, value.var = "value")
  
  
  files$trim <- as.numeric(as.character(files$trim))
  pd <- position_dodge(0.1) 
  
  p <- ggplot(files, aes(x=trim, y=beta, group = x_cov, color = x_cov)) + 
    geom_errorbar(aes(ymin=lower, ymax=upper, color = x_cov), width=.1, position=pd) +
    geom_line(position=pd) +
    geom_point(position=pd) +
    scale_x_continuous(breaks = seq(5,90,5)) +
    geom_line(aes(y = 1)) + 
    ggtitle(paste0("Within Study"))
  print(p)
  #}
  
  for (i in seq(5,90,5)){
    
    path <- "FILEPATH"
    
    file <- fread(paste0(path,i,"%/train_data.csv"))
    print(paste0("trimming = ",i,"%; " ,sum(file$w)," observations included"))
    
  }
}


summaries <- function(dt, draw_vars){
  sum <- copy(dt)
  sum[, mean := rowMeans(.SD), .SDcols = draw_vars]
  sum[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draw_vars]
  sum[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draw_vars]
  sum[, c(draw_vars) := NULL]
  return(sum)
}

graph_combos <- function(model, predictions){
  
  model = dem_model
  predictions = dem_predicts
  
  data <- as.data.table(model$train_data)
  data[, ratio_name := paste0(def2, " / ", def)]
  if (length(names(data)[grep("^X", names(data))]) > 0){
    name_change <- names(data)[grepl("^X", names(data))]
    setnames(data, name_change, gsub("^X", "", name_change))
  }
  data[, `:=` (log_ratio_l = log_ratio - 1.96*log_rse, log_ratio_u = log_ratio + 1.96*log_rse)]
  preds <- as.data.table(predictions$model_draws)
  preds <- summaries(preds, draws)
  xcov_names <- names(preds)[grepl("^X", names(preds))]
  setnames(preds, xcov_names, gsub("^X_", "", xcov_names))
  preds <- merge(preds, unique(data[, c("ratio_name", mrbrt_vars), with = F]), by = mrbrt_vars)
  fit_graph <- function(n){
    ratio <- preds[, ratio_name][n]
    ratio_mean <- preds[ratio_name == ratio, mean]
    ratio_lower <- preds[ratio_name == ratio, lower]
    ratio_upper <- preds[ratio_name == ratio, upper]
    graph_dt <- copy(data[ratio_name == ratio])
    gg <- ggplot() +
      geom_rect(data = graph_dt[1,], xmin = ratio_lower, xmax = ratio_upper, ymin = -Inf, ymax = Inf, alpha = 0.2, fill = "darkorchid") +
      geom_point(data = graph_dt, aes(x = log_ratio, y = id, color = as.factor(w))) +
      geom_errorbarh(data = graph_dt, aes(x = log_ratio, y = id, xmin = log_ratio_l, xmax = log_ratio_u, color = as.factor(w))) +
      geom_vline(xintercept = ratio_mean, linetype = "dashed", color = "darkorchid") +
      geom_vline(xintercept = 0) +
      labs(x = "Log Effect Size", y = "") +
      xlim(-3, 5.5) +
      scale_color_manual(name = "", values = c("1" = "midnightblue", "0" = "red"), labels = c("0" = "Trimmed", "1" = "Included")) +
      ggtitle(paste0("Model fit for ratio ", ratio)) +
      theme_classic() +
      theme(axis.title.y = element_blank(), axis.ticks.y = element_blank(), axis.line.y = element_blank(),
            axis.text.y = element_text(size = 5))
    return(gg)
  }
  fit_graphs <- lapply(1:length(preds[, unique(ratio_name)]), fit_graph)
  return(fit_graphs)
}

test = F
if (test == T){
p <- ggplot(matches, aes(x = age_middle, y = age_middle2, alpha = 1/100, color = "red")) +
  geom_point(size = 0.8) +
  geom_errorbar(aes(ymin=age_start2, ymax=age_end2 ), width=.1) +
  geom_segment(aes(x = age_start, xend= age_end, yend = age_middle2)) +
  ggtitle(paste0("Matches")) +
  xlab("Reference Age") +
  ylab("Alternative Age")
print(p)

matches$lower <- matches$mean - 1.96*matches$standard_error
matches$lower2 <- matches$mean2 - 1.96*matches$standard_error2
matches$upper <- matches$mean + 1.96*matches$standard_error
matches$upper2 <- matches$mean2 + 1.96*matches$standard_error2


p <- ggplot(matches, aes(x = mean, y = mean2, alpha = 1/100, color = age_middle)) +
  geom_point(size = 0.8) +
  ggtitle(paste0("Matches")) +
  xlab("Reference Prevalence") +
  ylab("Alternative Prevalence") +
  geom_abline(slope = 1, alpha = 1/5)
print(p)
}
