########### functions to assist with RR work ###############

impute_std <- function(raw, method = "high"){
  #' @description For rows with missing standard deviation, try to impute the standard error
  #' for us in the variance weighted meta-regression. 
  #' @raw dataset
  #' @param method: Decides how to impute the STD, if there is no other way to impute. 
  #' linear: Fits a linear regression for log(std) ~ log(mean). Predicts the missing upper and lower
  #' based on the found relationship. Outliers all points where the mean is below 1 for current smokers, 
  #' and where mean is above 1 for former smokers. 
  #' high: Finds the 95th percentile of standard errors. This downweights the study, but doesn't take 
  #' into account that certain size RR's have certain size upper lowers. 
  raw[,log_mean := log(mean)]
  data <- copy(raw)
  data[,meas_stdev := (log(upper) - log(lower))/3.92]
  data2 <- data[!is.na(upper) & mean > 1]
  
  if (method == "high"){
    meas <- quantile(data[,meas_stdev], .95, na.rm = T)
    data[is.na(upper), meas_stdev := meas]
    data[is.na(upper), `:=`(upper = exp(log_mean + 1.96 * meas_stdev),
                            lower = exp(log_mean - 1.96 * meas_stdev))]
  } else if (method == "regress") {
    gg <- ggplot(data2, aes(log_mean, meas_stdev)) + 
      geom_point(alpha=0.5) + 
      geom_smooth(method = "lm") + 
      xlab("log(mean)") + 
      ylab("log(stdev)")
    
    lm.test <- lm(meas_stdev ~ log_mean, data = data2)
    x <- raw[is.na(upper),.(log_mean)]
    meas <- predict(lm.test, x)
    data[is.na(upper), meas_stdev := meas]
    data[is.na(upper), `:=`(upper = exp(log_mean + 1.96 * meas_stdev),
                            lower = exp(log_mean - 1.96 * meas_stdev))]
  }
  
  data
}

log_transform_mrbrt_effect <- function(df) {
  
  df[, effect_size_log := log(effect_size)]
  df$se_effect_log <- sapply(1:nrow(df), function(i) {
    effect_size_i <- df[i, "effect_size"]
    se_effect_i <- df[i, "se_effect"]
    deltamethod(~log(x1), effect_size_i, se_effect_i^2)
  })
  
  df
}

log_transform_mrbrt_exposure <- function(df) {
  
  df[, mean_exp_log := log(mean_exp)]
  df$se_exp_log <- sapply(1:nrow(df), function(i) {
    mean_exp_i <- df[i, "mean_exp"]
    se_exp_i <- df[i, "se_exp"]
    deltamethod(~log(x1), mean_exp_i, se_exp_i^2)
  })
  
  df
}


cbind.fill <- function(...){
  nm <- list(...) 
  nm <- lapply(nm, as.matrix)
  n <- max(sapply(nm, nrow)) 
  do.call(cbind, lapply(nm, function (x) 
    rbind(x, matrix(, n-nrow(x), ncol(x))))) 
}


create_bias_covs <- function(df){
  libraries = c("msm","readxl","data.table")
  invisible(lapply(libraries,library,character.only=TRUE))
  
  df<-as.data.table(df)
  # 1) Subpopulation (cv_subpopulation)
  
  df<-df[,cv_subpopulation:=ifelse(rep_geography==1 & rep_prevalent_disease==0,0,1)]
  
  # 2) Exposure measurement (cv_exposure_population, cv_exposure_selfreport, cv_exposure_study)
  # For tobacco, we are omitting cv_exposure_population, since we only extract for individual exposure.
  # We consider self-reported exposure to be a higher-quality indicator, so cv_exposure_selfreport
  # is reversed compared to other risks. 
  df<-df[, cv_exposure_selfreport:=ifelse(exp_method_1=="Self-report (human/environment)",0,1)] # reversed relationship, as Vinnie suggests
  
  df<-df[, cv_exposure_study:=ifelse(exp_type=="Baseline", 1, 0)] # Baseline not as good as multiple measurements
  
  # 3) Outcome measurement (cv_outcome_selfreport, cv_outcome_unblinded)
  # For tobacco, we are omitting cv_outcome_unblinded, based on the assumption that
  # the physicians who diagnose the causes are always aware of the subjects' smoking status.
  # We are assigning a 0 to cv_outcome_selfreport if any outcome assessment is not self-reported.
  
  # make sure that this does what we expect with the NA values
  df<-df[,cv_outcome_selfreport:=ifelse((outcome_assess_1!="Self-report" & !is.na(outcome_assess_1))|(outcome_assess_2!="Self-report" & !is.na(outcome_assess_2))|(outcome_assess_3!="Self-report" & !is.na(outcome_assess_3)),0,1)]
  
  # 4) Reverse Causation (cv_reverse_causation)
  # For smoking, we are omitting cv_reverse_causation:
  # - Pack years should be stable against the effect of chronicly sick quitters. 
  # - Cigs/day shouldn't be affected by acute stroke
  
  # 5) Confounding (cv_confounding_nonrandom, cv_confounding_uncontrolled)
  # For smoking, we are omitting cv_confounding_nonrandom, as we use observational studies.
  # We will seek guidance on important confounders from cause-specific teams.
  
  
  # Confounding
  if (T){
    df<-df[confounders_income==1 & confounders_education==1, cv_confounding_uncontrolled:=0]
    df<-df[is.na(cv_confounding_uncontrolled) & (design %like%"Case" | (confounders_age == 1 & (confounders_sex== 1 | percent_male==1 | percent_male==0))) & confounders_bmi==1 & (confounders_physical_activity==1), cv_confounding_uncontrolled:=1]
    df<-df[is.na(cv_confounding_uncontrolled) & (design %like%"Case" | (confounders_age == 1 & (confounders_sex== 1 | percent_male==1 | percent_male==0))), cv_confounding_uncontrolled:=2]
    df<-df[is.na(cv_confounding_uncontrolled), cv_confounding_uncontrolled:=3]
  } else{
    confounders = c("confounders_age","confounders_sex")
    
    all_confounders = df[,confounders,with=FALSE]
    df<-df[,cv_confounding_uncontrolled := ifelse(apply(all_confounders,1,function(x)(all(x == "1"))),0,1)]
  }
  
  return(df)
  
  
  # 6) Selection bias (cv_selection_bias)
  # For smoking, studies almost always pre-emptively exclude subjects without smoking information
  # Even if the number of exclusions is given, there is no standard way to record it in the extraction sheet.
}

other_confounders_reshape <- function(df, map){
  if (F){
    map <- fread(FILEPATH)
  }
  # this function is based off of the work that Jason Anderson did to split the other confounders
  # we then mapped these confounders, and we will use this map to count the number of other confounders that each row has
  
  # CREATE A DATA TABLE WITH CONFOUNDER COLUMNS AS THE FOCUS.
  ################################################################################
  
  confounder_col_names <- grep("^extract|confounder|^nid|^outcome$", 
                               colnames(df), 
                               value = TRUE
  )
  confounders <- df[, ..confounder_col_names]
  
  # ELIMINATE PARENTHETICAL STATEMENTS AND PERIODS AT THE END OF STRINGS.
  ################################################################################
  
  confounders[, confounders_other := gsub("(?<!log)\\([^\\)]+(\\)|\\s)",
                                          "", 
                                          confounders_other, 
                                          perl = TRUE
  )
  ][, confounders_other := gsub("\\.$", "", confounders_other, perl = TRUE)
  ]
  
  # USE THE DATA TABLE METHOD FOR STRING SPLITTING AND TRANSPOSING.
  ################################################################################
  
  # SPLIT THE OTHER CONFOUNDER STRINGS USING THE tstrsplit FUNCTION.
  other_confounders <- confounders[, tstrsplit(confounders_other, 
                                               split = ",( and)?|;", 
                                               perl  = TRUE
  )
  ]
  # SPLIT THE SPECIAL CASES OF TWO CONFOUNDERS CONTAING THE WORD "and".
  if("V2" %in% names(other_confounders)){
    other_confounders <- other_confounders[grep("^Race and study center$|^Caffeine intake and gender$|^Use of NSAIDs and aspirin$|^Social class and parity$", 
                                                V1, 
                                                perl = TRUE), 
                                           c("V1", "V2") := tstrsplit(V1, split = " and ", perl = TRUE)
    ]
  }
  
  # CONVERT TO A MATRIX, TRIM WHITE SPACE, AND REVERT TO A DATA TABLE.
  other_confounders <- as.matrix(other_confounders) %>% trimws() %>% as.data.table()
  
  # COLUMN BIND temp DATA TABLE WITH MAIN confounders DATA TABLE.
  df <- cbind(df, other_confounders)
  df[, confounders_other := NULL]
  
  max_cols <- length(names(other_confounders))
  cols_melt <- paste0("V",c(1:max_cols))
  df[, ID := .I]
  
  df_melt <- melt(df, measure.vars=cols_melt,value.name="confounder")
  df_melt$variable <- NULL
  df_melt <- df_melt[!is.na(confounder)]
  
  nrow(df_melt)
  # merge on the map:
  map$V1 <- NULL
  df_melt_mapped <- merge(df_melt,map,by=c("outcome","confounder"))
  nrow(df_melt_mapped)
  num_not_mapped <- nrow(df_melt) - nrow(df_melt_mapped)
  
  if (num_not_mapped != 0){
    message(paste0(num_not_mapped," rows not mapped!"))
    # write the confounders that did not map to the file
    confound_to_map <- unique(anti_join(df_melt[,.(outcome,confounder)],map[,.(outcome,confounder)]))
    new_data <- rbind(confound_to_map,map,fill=T)
    message(paste0("There are ",nrow(new_data[is.na(map_value)])," confounders that have NA values in the reference sheet"))
    write.csv(new_data, paste0(FILEPATH, "/other_confounders_map_rr.csv"))
    stop(paste0(num_not_mapped," rows not mapped!"))
  }
  
  # count how many "other confounders" get mapped to each of these more general mappings
  df_melt_mapped[!is.na(map_value)]
  df_melt_mapped[, count := .N, by = c("map_value", "ID")]
  
  ##### IN THE FUTURE ONCE WE FIGURE OUT WHAT EXACTLY WE WANT TO INCLUDE
  df_melt_mapped[,c("mediator","confounder") := list(NULL,NULL)]
  df_melt_mapped <- unique(df_melt_mapped)
  # then change the confounder names slighlty to help with notation
  df_melt_mapped$map_value <- gsub("^","confounders_oth_",df_melt_mapped$map_value)
  # create a new count of the counts to figure out which model is most adjusted:
  df_melt_mapped[,adjustment := sum(count), by = "ID"]
  
  # widen the data so that each confounder gets its own column
  id_cols <- names(df_melt_mapped)
  id_cols <- id_cols[id_cols != "map_value" & id_cols != "count"]
  f <- as.formula(paste(paste(c(id_cols), collapse = " + "), "~ map_value"))
  df_wide_mapped <- dcast.data.table(df_melt_mapped,f,value.var = "count")
  
  # convert NA values to 0s in these new confounders columns:
  new_confounder_cols <- unique(df_melt_mapped$map_value)
  new_confounder_cols <- new_confounder_cols[!is.na(new_confounder_cols)]
  df_wide_mapped[, (new_confounder_cols) := lapply(.SD, function(x){x[is.na(x)] <- 0; x}), .SDcols = new_confounder_cols]
  
  # data cleaning before returning the data table:
  df_wide_mapped$ID <- NULL
  
  return(df_wide_mapped)
  
}


create_reference <- function(ref_path,data){
  ## merge on reference file separately for cases and controls
  data_cc <- data[design == "Case-control" | design == "Nested case-control"]
  data_cohort <- data[design == "Prospective cohort" | design == "Case-cohort"]
  ref_data <- as.data.table(read.csv(ref_path))
  ref_data$X <- NULL
  ref_data[,exposure_units := as.character(exposure_units)]
  ref_data[,exposure_defns := as.character(exposure_defns)]
  
  
  # also want to take out all of the rows that have NA for cohort_exp_unit_rr
  if(!risk_reduction){
    data_cohort <- data_cohort[!(outcome != "Fractures" & is.na(cohort_exp_unit_rr))]
  }
  
  # trim the white space from all of the variables
  data_cohort[, cohort_exp_unit_rr:=str_trim(cohort_exp_unit_rr),by=cohort_exp_unit_rr]
  data_cohort[, cohort_exposed_def:=str_trim(cohort_exposed_def),by=cohort_exposed_def]
  
  data_cc[, cc_exp_unit_rr:=str_trim(cc_exp_unit_rr),by=cc_exp_unit_rr]
  data_cc[, cc_exposed_def:=str_trim(cc_exposed_def),by=cc_exposed_def]
  
  ref_data[, exposure_units:=str_trim(exposure_units),by=exposure_units]
  ref_data[, exposure_defns:=str_trim(exposure_defns),by=exposure_defns]
  
  # merge on the reference data to map the variables
  # first for the case control studies
  data_cohort <- merge(data_cohort,unique(ref_data[exposure_units != "",.(exposure_units,map)]),by.x=c("cohort_exp_unit_rr"),by.y=c("exposure_units"),all.x=T)
  data_cohort <- merge(data_cohort,unique(ref_data[exposure_defns != "",.(exposure_defns,map_defn)]),by.x=c("cohort_exposed_def"),by.y=c("exposure_defns"),all.x=T)
  
  # and then for cohorts
  data_cc <- merge(data_cc,unique(ref_data[exposure_units != "",.(exposure_units,map)]),by.x=c("cc_exp_unit_rr"),by.y=c("exposure_units"),all.x=T)
  data_cc <- merge(data_cc,unique(ref_data[exposure_defns != "",.(exposure_defns,map_defn)]),by.x=c("cc_exposed_def"),by.y=c("exposure_defns"),all.x=T)
  
  # bind back together:
  data <- rbind(data_cc,data_cohort)
  
  # return the data
  data
  
}

transform_former_new <- function(raw, new_max = 10, new_min = 1, bound = "keep", seed = 124, transform = "rrr"){
  #' @description Takes a dataset with former smoking data - years since quitting, and transforms RR data into a form that sets new mins and maxes
  #' @param raw   The dataset, with just Former vs. Never years since quitting data
  #' @param bound Either [reflect, truncate, keep]. This is how to handle transformed draws above 1 and below 0.
  if(F){
    raw <- copy(data)
    new_max = 10
    new_min = 1
    bound = "keep"
    seed = 124
    transform = "minmax"
  }
  if(!(transform %in% c("rrr", "minmax"))){stop("Transformation parameter must either be 'rrr' or 'minmax'")}
  set.seed(seed)
  test <- copy(raw)
  id.vars <- c("nid", "custom_exp_level_lower", "custom_exp_level_upper", "percent_male", "study_name","outcome_def") 

  #' Expand row by 1000 (for 1000 draws), and add a new column with 1000 draws by study-exposure range, sampling from log mean and SD - assumes
  #' uncorrelated exposure ranges
  #' this gives you a log distribution of draws, which causes some of the non-YSQ-0 effect sizes to be larger than the effect size for YSQ 0
  # try just creating distributions per row instead of having them by combos of variables, because some of the outcomes are not defined precisely enough
  # but, we also want to alert the user if there are duplicates by id.vars
  test[,count := .N, by = id.vars]
  dup_nids <- unique(test[count > 1, nid])
  message(paste0("The following NIDs have duplicates by, ",paste(id.vars,sep="",collapse=", "),":\n",paste(dup_nids,sep="",collapse=", ")))
  
  # set former_smok_match to a unique combination of id.vars if it is empty
  test[,identity := paste(nid,study_name,percent_male,outcome_def)]
  test <- test %>% group_by(identity) %>% mutate(md5=digest(identity)) %>% as.data.table()
  test$identity <- NULL
  test[former_smok_match == "", former_smok_match := md5]
  
  test[,line_num := .I]
  test <- test[rep(1:.N, each=1000)][, `:=`(draw  = 1:.N, value = rlnorm(1000, effect_size_log, se_effect_log)), by = line_num] # get a distribution for each row
  # this may need to be edited in the future if we change our modeling approach
  short <- copy(test)[custom_exp_level_lower == 0 & custom_exp_level_upper == 0]
  
  message(paste0("The following NIDs do not have YSQ = 0 values: ",paste(setdiff(test$nid,short$nid),sep="",collapse="\n")))
  
  short[, old_max := value] # isolates the distribution of RR values for the exposure value of 0 for each study
  # this merge may not always work because some rows are also age-specific, but not all ages were extracted exactly
  # so, we will need to figure out what we want to do in these cases!
  test <- merge(test, short[,.(draw, old_max,former_smok_match)], by = c("draw","former_smok_match"))
  
  #' Perform max-min transformation by DRAW-study-exposure_range, with the new max and min
  if(transform == "minmax"){
    test[, old_min := ifelse(old_max==1, 0, 1)] 
    if(F){
      ggplot()+geom_histogram(data=test[nid==336216 & effect_size == 1.03],aes(value,fill="value"),alpha=0.3)+
        geom_histogram(data=test[nid==336216 & effect_size == 1.03],aes(old_max,fill="max"),alpha=0.3)
    }
    
    
    # some of these values may be larger than 10 - this could be evidence of sick quitter
    test[, value := ((value - old_min) / (old_max - old_min) * (new_max - new_min)) + new_min]
  } else if (transform == "rrr"){
    #' Perform new RRR transformation
    test[, value := 1 - (old_max - value) / old_max]
  }
  #' Collapse max-min draws
  test[, mean_new    := mean(value), by = id.vars]
  test[, lower_new   := quantile(value, 0.025, na.rm = T), by = id.vars]
  test[, upper_new   := quantile(value, 0.975, na.rm = T), by = id.vars]
  #' Get rid of unnecessary columns
  cols <- c("value", "draw", "old_max", "old_min", "effect_size_log")
  test[, (cols) := NULL]
  test <- unique(test)
  setnames(test, c("mean_new", "lower_new", "upper_new"), c("effect_size_log", "lower_effect_log_new", "upper_effect_log_new"))
  
  # tell the user which NIDs that have duplicates did not get mapped:
  message(paste0("The following duplicate NIDs did not get mapped: ",paste(setdiff(dup_nids,test$nid),sep="",collapse=", ")))
  
  test
}

read_in_data <- function(data_path,path_metadata,cause_choice_formal,cause_choice_c,cancer,risk_reduction){
  
  data_path <- path_metadata[outcome %in% cause_choice_formal, file_path]
  data_path <- data_path[data_path %like% "xlsx" | data_path %like% "xlsm"]
  if (cause_choice_c == "lri"){
    data_path <- FILEPATH
  }
  
  data_2017 <- as.data.table(openxlsx::read.xlsx(FILEPATH)) # evidence score extractions completed during 2017
  # make some changes because the epi uploader did not like the equal signs
  data_2017[custom_exp_level_lower_sign == 0, custom_exp_level_lower_sign := "="]
  data_2017[custom_exp_level_upper_sign == 0, custom_exp_level_upper_sign := "="]
  
  # subset to the cause
  data_2017_cause <- data_2017[outcome %in% cause_choice_formal & risk == risk_choice]
  t <- 0
  if (length(data_path) != 0){
    for (i in data_path){
      t <- t+1
      data_2019 <- as.data.table(readxl::read_xlsx(i,sheet="extraction"))
      if (t > 1){
        message(paste0("Binding on additional 2019 data: ", i))
        data <- rbind(data_2019,data,fill=T)
      }else{
        message(paste0("Binding on new 2019 data: ",i))
        data <- rbind(data_2019,data_2017_cause,fill=T)
      }
      
    }
    
  } else{
    data <- copy(data_2017_cause)
  }
  
  if (cancer){
    message("Binding on cancer-specific data")
    data_cancer <- as.data.table(readxl::read_xlsx(FILEPATH))
    data_cancer <- data_cancer[outcome %in% cause_choice_formal]
    data <- rbind(data,data_cancer,fill=T)
  }
  
  # add on Thun data
  thun_data <- as.data.table(readxl::read_xlsx(FILEPATH),sheet="extraction")
  thun_data <- thun_data[outcome %in% cause_choice_formal]
  data <- rbind(data,thun_data,fill=T)
  
  
  # add on British Doctors' data
  brit_data <- as.data.table(readxl::read_xlsx(FILEPATH),sheet="extraction")
  brit_data <- brit_data[outcome %in% cause_choice_formal]
  data <- rbind(data,brit_data,fill=T)
  
  # if you want to run a risk reduction curve, then read in the YSQ = 0 data:
  if(risk_reduction == T){
    ysq_0_extrac <- as.data.table(readxl::read_xlsx(FILEPATH,sheet = "extraction"))
    ysq_0_extrac <- ysq_0_extrac[outcome == cause_choice_formal]
    data <- rbind(data,ysq_0_extrac,fill=T)
  }
  
  
  # get rid of NA NID values
  data <- data[!is.na(nid)]
  
  data
}


select_units_and_defs <- function(data,risk_reduction){
  if (risk_reduction){
    # also include ever smokers as a potential comparison group
    data[((is.na(custom_exp_level_lower) & is.na(custom_exp_level_upper)) | 
            (custom_exp_level_lower == 0 & custom_exp_level_upper == 0)) & map_defn %in% c("current","ever") & is.na(map),
         c("map","custom_exp_level_lower","custom_exp_level_upper") := list("ysq",0,0)]
  } else{
    # take out data where the effect size is 1, and there is no uncertainty for current smokers - these must be data extraction errors
    # only true for current smoking curves because you could have a RR for current smokers versus former smokers that is equal to 1
    data <- data[!(effect_size == 1 & lower == 1 & upper == 1 & map_defn == "current") | (is.na(lower) | is.na(upper))]
  }
  
  
  
  # then divide units as necessary so that all of the units are in the correct space (packs/year, cigs/day, years since quitting)
  data[,og_map := map]
  data[,og_map_def := map_defn]
  non_standard_units <- c("pd","ysq (month to years)", "dsq", "cy (thousands)", "gd", "msq", "cd (young)", "cy", "cw")
  standard_units <- c("py","cd","ysq")
  changeCols <- c("custom_exp_level_lower","custom_exp_level_upper","cc_exp_level_rr","cohort_exp_level_rr","upper","lower","effect_size")
  data[,(changeCols):= lapply(.SD, as.numeric), .SDcols = changeCols]
  # the values could be in the following columns: custom_exp_level_lower, custom_exp_level_upper, cc_exp_level_rr, cohort_exp_level_rr
  data[map == "ysq (month to years)", custom_exp_level_lower := custom_exp_level_lower/12]
  data[map == "ysq (month to years)",map := "ysq"]
  
  data[map == "pd", c("custom_exp_level_lower","custom_exp_level_upper","cc_exp_level_rr","cohort_exp_level_rr") := list(custom_exp_level_lower*20,custom_exp_level_upper*20,cc_exp_level_rr*20,cohort_exp_level_rr*20)]
  data[map == "pd",map := "cd"]
  
  data[map == "dsq", c("custom_exp_level_lower","custom_exp_level_upper","cc_exp_level_rr","cohort_exp_level_rr") := list(custom_exp_level_lower/365,custom_exp_level_upper/365,cc_exp_level_rr/365,cohort_exp_level_rr/365)]
  data[map == "dsq",map := "ysq"]
  
  data[map == "gd", map := "cd"]
  
  data[map == "msq",c("custom_exp_level_lower","custom_exp_level_upper","cc_exp_level_rr","cohort_exp_level_rr") := list(custom_exp_level_lower/12,custom_exp_level_upper/12,cc_exp_level_rr/12,cohort_exp_level_rr/12)]
  data[map == "msq",map:="ysq"]
  
  data[map == "cy", c("custom_exp_level_lower","custom_exp_level_upper","cc_exp_level_rr","cohort_exp_level_rr") := list(custom_exp_level_lower/20,custom_exp_level_upper/20,cc_exp_level_rr/20,cohort_exp_level_rr/20)]
  data[map == "cy",map := "py"]
  
  data[map == "cw", c("custom_exp_level_lower","custom_exp_level_upper","cc_exp_level_rr","cohort_exp_level_rr") := list(custom_exp_level_lower/7,custom_exp_level_upper/7,cc_exp_level_rr/7,cohort_exp_level_rr/7)]
  data[map == "cw",map := "cd"]
  
  # some maps have still not been fixed becaues I am not sure what to do with them
  data <- data[map %in% standard_units]
  
  # subset data to where unexposed group is only never/non-smokers
  # this process will get rid of rows where both the unexposed case control and unexposed cohort variables are NA
  
  if(!risk_reduction){
    unexposed_def <- paste0("never|non")
    data_cohort_ok <-  data %>% filter(grepl(unexposed_def, cohort_unexp_def,ignore.case = T)) %>% as.data.table()
    data_cc_ok <-  data %>% filter(grepl(unexposed_def, cc_unexposed_def,ignore.case = T)) %>% as.data.table()
    data <- rbind(data_cohort_ok,data_cc_ok) 
    data <- data[map_defn %in% c("ever","current")]
  } else{
    message("Risk reduction analysis will keep both current and never smokers")

    # merge on the unexposed definition maps:
    data_cc <- data[design == "Case-control" | design == "Nested case-control"]
    data_cohort <- data[design == "Prospective cohort" | design == "Case-cohort"]
    
    data_cohort[, cohort_unexp_def:=str_trim(cohort_unexp_def),by=cohort_unexp_def]
    data_cc[, cc_unexposed_def:=str_trim(cc_unexposed_def),by=cc_unexposed_def]
    
    unexp_map <- unique(unexp_map)
    
    data_cohort <- merge(data_cohort,unexp_map,by.x="cohort_unexp_def",by.y="unexposed")
    data_cc <- merge(data_cc,unexp_map,by.x="cc_unexposed_def",by.y="unexposed")
    
    data <- rbind(data_cohort,data_cc)
    
    data <- data[map_unexp != "error"]
    
  }
  
  
  # subset to the exposure metric that should be used
  if (!risk_reduction){
    if (cause_choice_formal %like% "cancer" | cause_choice_formal %like% "obstructive pulmonary" | cause_choice_formal == "Leukemia"){
      metric <- "py"
      metric_name = "Pack-Year"
    } else{
      metric <- "cd"
      metric_name = "Cig/Day"
    }
    
  } else{
    metric <- "ysq"
    metric_name = "Years Since Quitting"
  }
  
  if(cause_choice_c == "diabetes"){
    metric <- "cd"
    metric_name = "Cig/Day"
  }
  
  
  data <- data[map == metric]
  # convert columns to correct type:
  data[,c("custom_unexp_level_lower","custom_unexp_level_upper") := list(as.numeric(custom_unexp_level_lower),as.numeric(custom_unexp_level_upper))]
  
  data
}


clean_up_time <- function(data,risk_reduction){
  # create standard errors for: the mean effect and the exposure
  data[!is.na(lower) & !is.na(upper),se_effect := (upper-lower)/(2*1.96)]
  impute_se_effect <- quantile(x=data$se_effect,probs=0.95,na.rm=TRUE)[[1]] # take a conservative measure of SE of the effect in order to fill in rows that have no uncertainty
  data[is.na(se_effect), se_effect := impute_se_effect]
  
  # and now for the exposure metrics
  data[nid == 334460 & is.na(custom_exp_level_upper) & custom_exp_level_lower == 5, 
       c("custom_exp_level_lower","custom_exp_level_upper","custom_exp_level_lower_sign","custom_exp_level_upper_sign") := list(1,5,NA,"<")]
  data[nid == 334460 & custom_exp_level_upper == 25 & is.na(custom_exp_level_lower),  
       c("custom_exp_level_lower","custom_exp_level_upper","custom_exp_level_lower_sign","custom_exp_level_upper_sign") := list(25,NA,">",NA)]
  
  # first, assume that if the upper exposure is 0, and no lower bound is given, then this is only measuring 0 exposure
  data[custom_exp_level_upper == 0 & is.na(custom_exp_level_lower), c("custom_exp_level_lower") := list(0)]
  
  # fix some extraction errors that don't make sense
  # if the following is the case, then really the lower value should be the upper value, and the lower value should be 0
  data[,lower_temp := custom_exp_level_lower]
  data[,upper_temp:=custom_exp_level_upper]
  data[is.na(custom_exp_level_upper) & (custom_exp_level_lower_sign == "<"),c("custom_exp_level_upper","custom_exp_level_lower"):=list(lower_temp-1,0)]
  data[is.na(custom_exp_level_upper) & (custom_exp_level_lower_sign == "<=" | custom_exp_level_lower_sign == "≤"),c("custom_exp_level_upper","custom_exp_level_lower"):=list(lower_temp,0)]
  
  # and same for unexposure
  data[,lower_temp_unexp := custom_unexp_level_lower]
  data[,upper_temp_unexp :=custom_unexp_level_upper]
  data[is.na(custom_unexp_level_upper) & (custom_unexp_level_lower_sign == "<"),c("custom_unexp_level_upper","custom_unexp_level_lower"):=list(lower_temp_unexp-1,0)]
  data[is.na(custom_unexp_level_upper) & (custom_unexp_level_lower_sign == "<=" | custom_unexp_level_lower_sign == "≤"),c("custom_unexp_level_upper","custom_unexp_level_lower"):=list(lower_temp_unexp,0)]
  # not going to do this for the unexposed values unless necessary
  
  # second case: there is an upper value, and the lower sign is going in the wrong direction, act like this is an extraction error and assume that it should be the other way
  # under this assumption, we would add 1 to the lower level
  data[is.na(custom_exp_level_upper) & (custom_exp_level_lower_sign == "<" | custom_exp_level_lower_sign == ">"), custom_exp_level_lower := custom_exp_level_lower + 1]
  # third case: lower value is not present, but there is a sign provided; very specific case:
  data[nid == 350745 & !is.na(custom_exp_level_lower_sign) & is.na(custom_exp_level_lower), 
       c("custom_exp_level_lower","custom_exp_level_lower_sign") := list(30,">=")]
  
  # if the lower bound sign is not specified and the lower bound is 0, add 1 because we can assume 0 is not included
  # no NOT want to do the same for the unexposed ranges, because 0 can be included (for example, current smokers including those who quit < 2 years before diagnosis)
  # for YSQ, we want to convert any "all current smoking" RRs into former smoking at YSQ = 0, so we need to keep the rows with lower and upper = 0
  if(!risk_reduction){
    data[(is.na(custom_exp_level_lower_sign) | custom_exp_level_lower_sign == 0) & custom_exp_level_lower == 0,
         custom_exp_level_lower := custom_exp_level_lower + 1 ]
  }
  
  
  # adjust the upper and lower limits based on the signs given
  # if the sign is NA or 0, then assume the upper and lower values are included, unless one of them is 0
  data[!is.na(custom_exp_level_upper) & custom_exp_level_upper_sign == "<" & custom_exp_level_upper > 1, custom_exp_level_upper :=custom_exp_level_upper - 1]
  data[!is.na(custom_exp_level_upper) & custom_exp_level_upper_sign == ">", custom_exp_level_upper :=custom_exp_level_upper + 1]
  
  # and same for unexposed categories:
  data[!is.na(custom_unexp_level_upper) & custom_unexp_level_upper_sign == "<" & custom_unexp_level_upper > 1, custom_unexp_level_upper :=custom_unexp_level_upper - 1]
  data[!is.na(custom_unexp_level_upper) & custom_unexp_level_upper_sign == ">", custom_unexp_level_upper :=custom_unexp_level_upper + 1]
  
  
  # then, for cases where there is no upper limit given, just assume the 95th percentile of upper limits? If it is smaller than the lower value, then set the upper value to be the max value
  # fill in the lower limit when it is NA, but the upper limit is given:
  data[!is.na(custom_exp_level_upper) & is.na(custom_exp_level_lower), custom_exp_level_lower := 0]
  data[!is.na(custom_unexp_level_upper) & is.na(custom_unexp_level_lower), custom_unexp_level_lower := 0]
  
  # make the same changes to the upper exposure limit that Thomas made, since using the 95th percentile of the standard error was leading to weird results
  # but, should we be using the same metric for YSQ? For example, if the unexposed definition is never smokers or former smokers who quit > 5 years ago,
  ## then the lower exposure level will be 5, but what should the upper be?
  ## depending on the study, this may also be considered the exposed group, and thus the below transformation may be an issue
  ## just going to keep for now and include the unexposed definition and values in all of the hashed datasets from now on
  data[custom_exp_level_upper >= 99|is.na(custom_exp_level_upper), custom_exp_level_upper := custom_exp_level_lower * 1.5]
  data[custom_exp_level_upper >100, custom_exp_level_upper := 100]
  
  # and same for unexposed values
  data[custom_unexp_level_upper >= 99|is.na(custom_unexp_level_upper), custom_unexp_level_upper := custom_unexp_level_lower * 1.5]
  data[custom_unexp_level_upper >100, custom_unexp_level_upper := 100]
  
  
  # calculate SE first so that we can then impute upper and lower values, and thus mean values
  data[!is.na(custom_exp_level_lower) & !is.na(custom_exp_level_upper), se_exp := (custom_exp_level_upper-custom_exp_level_lower)/(2*1.96)]
  data[!is.na(lower) & !is.na(upper),se_effect := (upper-lower)/(2*1.96)]
  
  # now we can create mean exposure now that we have imputed lower and upper bounds
  # the below cannot be used with the new Mr-Brt, but there aren't many, so I will just exclude in the google sheet
  data[is.na(custom_exp_level_lower) & is.na(custom_exp_level_upper) & !is.na(cohort_exp_level_rr), mean_exp := cohort_exp_level_rr]
  data[is.na(custom_exp_level_lower) & is.na(custom_exp_level_upper) & !is.na(cc_exp_level_rr), mean_exp := cc_exp_level_rr]
  # and then deal with the exposure ranges:
  data[!is.na(custom_exp_level_lower) & !is.na(custom_exp_level_upper), mean_exp := (custom_exp_level_upper+custom_exp_level_lower)/2]
  
  # get rid of data that still have no mean exposure
  # these studies are weird because they have an exposure metric but no value
  # for COPD the only NID is 336216, but it is a lot of data
  data <- data[!is.na(mean_exp)] # but need to make sure that this is now getting rid of any important studies (for example for YSQ?)
  
  # and get rid of relationships that don't make sense - need to figure out why this problem is happening
  # these are data extraction errors that we need to go back and fix
  
  # if both upper and lower exposed or unexposed ranges are NA, then fill them in with zeros:
  data[is.na(custom_exp_level_lower) & is.na(custom_exp_level_upper), c("custom_exp_level_lower","custom_exp_level_upper") := list(0,0)]
  data[is.na(custom_unexp_level_lower) & is.na(custom_unexp_level_upper), c("custom_unexp_level_lower","custom_unexp_level_upper") := list(0,0)]
  
  # change the mapped exposure definition for risk reduction if it is a current smoker with upper and lower exposure values equal to 0
  if(risk_reduction){
    # only keep the dichotmous RRs for current smokers if there is a row with a non-dichotomous RR
    data_non_0 <- data[custom_exp_level_lower != 0 | custom_exp_level_upper != 0]
    nids_to_delete <- setdiff(data$nid,data_non_0$nid) # these are NIDs that don't have any continuous exposure data at all
    data <- data[!(nid %in% nids_to_delete)]
    data[map_defn %in% c("current","ever") & custom_exp_level_lower == 0 & custom_exp_level_upper == 0, map_defn := "former"]
  }
  
  data
}


outcome_spec_confounders <- function(data,cause_choice_c,date){
  setwd(FILEPATH)
  confounders_ref <- drive_download(as_id(ID),
                                    type="csv", overwrite = T, 
                                    path = paste0(FILEPATH,
                                                  cause_choice_c,"_",date,"_confounder.csv"))
  confounders_ref <- fread(as.data.table(confounders_ref)$local_path)
  confounders_important <- confounders_ref[cause == cause_choice_c & in_use == 1, .(list_confounders,level)]
  
  
  # if there exists a list of the confounders we want to use, then change cv_controlled based on that
  if (nrow(confounders_important) > 0){
    # based on the level of importance, we give it different weight
    # if a study controls for all of them, assign cv_confounding_uncontrolled to be 0
    # if a study controls for all level 1 and 2 confounders (but not 3), assign cv_confounding_controlled = 1
    # if a study controls for just level 1 confounders, assign cv_confounding_controlled = 2
    # otherwise, assign cv_confounding_controlled = 3
    
    # start by assigning each study a value of 4. then subtract
    # make some columns to indicate presence/absence of particular levels of confounders
    # initially assume that none are present
    data[,level_1_confounders := 0]
    data[,level_2_confounders := 0]
    data[,level_3_confounders := 0]
    
    # note: there might be a more nuanced way to do this- for example, if some but not all of the variables are present for less important confounders, we might still want to indicate that some were included
    for(c in confounders_important[level == 1, list_confounders]){
      data[get(c) == 1, level_1_confounders := level_1_confounders + 1/length(confounders_important[level == 1, list_confounders])]
    }
    
    for(c in confounders_important[level == 2, list_confounders]){
      data[get(c) == 1, level_2_confounders := level_2_confounders + 1/length(confounders_important[level == 2, list_confounders])]
    }
    
    for(c in confounders_important[level == 3, list_confounders]){
      data[get(c) == 1, level_3_confounders := level_3_confounders + 1/length(confounders_important[level == 3, list_confounders])]
    }
    
    # use these level indicators to determine the amount of confounding
    data[,level_1_confounders := ifelse(level_1_confounders == 1, 1, 0)]
    data[,level_2_confounders := ifelse(level_2_confounders == 1, 1, 0)]
    data[,level_3_confounders := ifelse(level_3_confounders == 1, 1, 0)]
    
    data[, cv_confounding_uncontrolled := ifelse(level_1_confounders == 1 & level_2_confounders == 1 & level_3_confounders == 1, 0,
                                                 ifelse(level_2_confounders== 1 & level_3_confounders == 1, 2, ifelse(level_3_confounders == 1, 3, 4)))]
    

  } else{
    message("Using default confounders")
  }
  
  
}

compare_nids_gbd2017 <- function(data,metric_name){
  ### compare GBD 2017 data
  to_compare <- as.data.table(readxl::read_xlsx(paste0(FILEPATH,cause_choice,"/",cause_choice,"_data.xlsx")))
  
  if ("exclude" %in% names(to_compare)){
    to_compare <- to_compare[is.na(exclude) | exclude == 0]
  }
  
  to_compare <- to_compare[`exposure definition` == metric_name]
  nids_to_examine <- setdiff(to_compare$nid,data$nid)
  
  if(length(nids_to_examine) > 0){
    message("The following NIDs are missing, but were present in GBD 2017: ")
    message(nids_to_examine)
  } else{
    message("No missing GBD 2017 NIDs")
  }
}

download_ref_dir <- function(data_coded,date,cause_choice_c){
  setwd(FILEPATH)
  
  # and then upload to google drive
  # will want to make a reference file with all of these IDs at some point (and possibly keeo this on the drive as well)
  # set location to the working directory so that the authentification file can be saved
  
  hash_directory_reference <- drive_download(as_id(ID),
                                             type="csv", overwrite = T, 
                                             path = paste0(FILEPATH,"directory_hash_reference_",date,".csv"))
  hash_directory_reference <- as.data.table(hash_directory_reference)
  hash_directory_reference <- fread(hash_directory_reference$local_path)
  
  temp_cause <- cause_choice_c
  
  # read the correct directory hash in the from sheet
  drive_key <- hash_directory_reference[cause_choice_c == temp_cause, drive_key]
  
  drive_key
  
}

upload_new <- function(data_coded,cause_choice_c,date,drive_key){
  dir.create(paste0(FILEPATH,cause_choice_c), showWarnings = F)
  # and then make another for the google sheet data
  dir.create(paste0(FILEPATH,cause_choice_c,"/google_sheet_reference"), showWarnings = F)
  write.csv(data_coded, paste0(FILEPATH,cause_choice_c,"/",cause_choice_c,"_",date,".csv"))
  
  #upload a new reference sheet to the drive
  drive_upload(media=paste0(FILEPATH,cause_choice_c,"/",
                            cause_choice_c,"_",date,".csv"), path=as_id(drive_key),type="spreadsheet")
}

download_file_to_use <- function(cause_choice_c,date,risk_reduction){
  # read in from the google sheet reference file
  hash_file_reference <- drive_download(as_id(ID),
                                        type="csv", overwrite = T, 
                                        path = paste0(FILEPATH,"file_hash_reference_",date,".csv"))
  hash_file_reference <- as.data.table(hash_file_reference)
  # get a table of the hash keys for each of the best files
  hash_file_reference <- fread(hash_file_reference$local_path)
  
  temp_cause <- cause_choice_c
  # read the correct file reference hash in the from sheet
  ysq_bool <- ifelse(risk_reduction == T, "y", "n")
  cause_id <- hash_file_reference[cause_choice_c == temp_cause & ysq == ysq_bool, cause_id]
  
  # and finally download the correct reference sheet
  df_drive <- drive_download(as_id(cause_id),
                             type="csv", overwrite = T, 
                             path = paste0(FILEPATH,cause_choice_c,"/google_sheet_reference/",
                                           cause_choice_c,"_",date,"_GS.csv"))
  
  # then read in the downloaded data (what was downloaded above)
  df_drive_data <- fread(paste0(FILEPATH,cause_choice_c,"/google_sheet_reference/",
                                cause_choice_c,"_",date,"_GS.csv"))
  
  df_drive_data
}