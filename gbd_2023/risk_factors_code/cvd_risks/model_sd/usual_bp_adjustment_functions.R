# Author: USERNAME
# Date: DATE
# Description: Functions for usual blood pressure adjustment to allow for sourcing from multiple scripts

##############################################################################################################
################################# # Compile data #############################################################
##############################################################################################################

load_data <- function(files, ave_meas){
  meta <- list()
  full <- list()
  for(study in unique(files$id)){
    
    study_files <- unique(files[id==study, files])
    message(paste0("\nStudy ", study, ', ', paste0(length(study_files), ' file(s)\n')))
    
    study_dt <- list()
    for(u in 1:length(study_files)){
      
      x <- study_files[u]
      print(paste0("File: ", x))
      tbl <- as.data.table(read_dta(paste0(x)))
      
      if ('hypertension_drug'%in%names(tbl)){
        print('Medication info present')
      }
      
      # reformat survey name
      tbl[, survey_name:=study]
      
      if(!'line_id'%in%names(tbl)){
        stop('No column for line_id!')
      }
      
      if(unique(tbl$nid)==131333){
        # adjust the baseline IDs from the CHN CHRLS study to be consistent with the followup cycles
        tbl[, hh_id := paste0(hh_id, '0')]
        tbl[, line_id := paste0(hh_id, substr(line_id, (nchar(line_id)-1), nchar(line_id)))]
      }
      
      # convert IDs to numeric
      if(!class(tbl$line_id) %in% c("integer", "numeric")){
        print(paste0("For survey ", x, ", line_id is ", class(tbl$line_id), ". Attempting to convert to numeric..."))
        
        tbl[line_id=='', line_id := NA]
        
        if(unique(tbl$nid)==131352){
          # reformat IDs from CHN CHRLS-pilot second followup to be able to convert to numeric
          tbl[grep('\\+', line_id), line_id := gsub('\\+', '', line_id)]
        }
        
        pre <- nrow(tbl[is.na(line_id)])
        tbl[, line_id := as.numeric(line_id)]
        post <- nrow(tbl[is.na(line_id)])
        if(pre != post){
          print(paste0(post-pre, " line_id values coerced to NA!"))
        } else{
          print("All line_id values successfully converted to numeric!")
        }
      }
      if(!is.numeric(tbl$line_id)){
        stop(paste("line_id is not numeric for ", x))
      }
      
      # designate followup period
      tbl[, followup:=u]
      
      # if within-visit mean SBP was not calculated with extraction custom code
      if(!"sbp" %in% names(tbl)){
        tbl[, sbp := rowMeans(.SD, na.rm=T), .SDcols=intersect(names(tbl), paste0('sbp_', 1:3))]
      }
      
      # if only 1 measurement and not extracted as sbp_1
      if(!"sbp_1" %in% names(tbl)){
        tbl[, sbp_1 := sbp]
      }
      
      study_dt[[u]]<-tbl
    }
    study_dt<-rbindlist(study_dt, fill=T)
    
    # check that id's match up  
    study_dt[, id:=line_id]
    
    if(length(unique(study_dt$id))<nrow(study_dt)/length(unique(files[id==study, files]))){
      stop("Not enough unique line_ids, try binding w/ hh_id")
    }
    
    # get number of sbp measurements
    sbp_cols <- intersect(names(study_dt), paste0('sbp_', 1:3))
    
    # only keep relevant rows
    study_dt <- study_dt[, c("sbp", sbp_cols, "age_year", "id", "sex_id", "year_start", "year_end", "nid", "survey_name", "followup"), with=F]
    
    if(ave_meas==T){
      # drop separate within-visit BP readings, leaving only mean within-visit
      study_dt[, c(sbp_cols):=NULL]
      study_dt[, k := '0']
    } else{
      # drop mean within-visit BP reading, leaving only separate within-visit BP readings
      study_dt[, sbp:=NULL]
      
      # reshape
      study_dt <- data.table::melt(study_dt, id.vars=c("id", "followup", "survey_name", "sex_id", "age_year", "year_start", "year_end", "nid"), 
                       measure.vars=sbp_cols, variable.name="k", value.name="sbp", variable.factor = F)
    }
    
    # drop NAs
    study_dt <- study_dt[!is.na(sbp)]
    
    # get number of measurements and sample sizes for each survey year
    study_meta <- list()
    for(fol in unique(study_dt$followup)){
      yrs <- paste0(unique(study_dt[followup==fol, year_start]), '-', unique(study_dt[followup==fol, year_end]))
      nid <- unique(study_dt[followup==fol, nid])
      numks <- study_dt[followup==fol, length(unique(k))]
      sample_size <- length(unique(study_dt[followup==fol, id]))
      ss_from_baseline <- sum(unique(study_dt[followup==1, id]) %in% unique(study_dt[followup==fol, id]))
      loss_from_baseline <- sum(!unique(study_dt[followup==1, id]) %in% unique(study_dt[followup==fol, id]))
      additions_since_baseline <- sum(!unique(study_dt[followup==fol, id]) %in% unique(study_dt[followup==1, id]))
      
      study_meta[[fol]] <- data.table(fol, yrs, nid, numks, sample_size, ss_from_baseline, loss_from_baseline, additions_since_baseline)
    }
    study_meta <- data.table(surv=unique(study_dt$survey_name), rbindlist(study_meta))
    
    meta[[study]] <- study_meta
    full[[study]] <- study_dt
  }
  return(list(meta = meta, full = full))
}

##############################################################################################################
################################# # Validate data ############################################################
##############################################################################################################

validate_data <- function(full){
  
  message(paste0('\nStarting sample size of data: ', nrow(full), '\n'))
  
  # fill in missing demographic info with info from the same visit
  full[, sex_id := nafill(sex_id, "nocb"), by = c('survey_name', 'id', 'followup')]
  full[, age_year := nafill(age_year, "nocb"), by = c('survey_name', 'id', 'followup')]
  
  # drop rows with missing IDs, ages, and sexes
  message('\nDropping rows with missing IDs, ages, and sexes:\n')
  print(table(full[is.na(id) | is.na(sex_id) | is.na(age_year), survey_name], 
              full[is.na(id) | is.na(sex_id) | is.na(age_year), followup],
              useNA='ifany', dnn=c('survey_name', 'followup')))
  full <- full[!is.na(id) & !is.na(sex_id) & !is.na(age_year)]
  
  # drop rows with IDs where sex_id is not consistent across followups
  full[, sex_N := length(unique(sex_id)), by = c('survey_name', 'id')]
  message('\nDropping rows with IDs where sex_id is inconsistent across followups:\n')
  print(full[sex_N!=1, .N, by=c('survey_name', 'sex_N')])
  full <- full[sex_N == 1]
  full[, sex_N := NULL]
  
  # drop rows with IDs where age_year is not consistent within a followup period
  full[, age_N := length(unique(age_year)), by = c('survey_name', 'id', 'followup')]
  message('\nDropping rows with IDs where age_year is inconsistent within a followup period:\n')
  print(full[age_N!=1, .N, by=c('survey_name', 'followup', 'age_N')])
  full <- full[age_N == 1]
  full[, age_N := NULL]
  
  # drop rows where IDs have multiple sets of BP readings within a single followup period
  full[, n_reading_sets := .N, by = c('survey_name', 'id', 'followup', 'k')]
  message('\nDropping rows where IDs have multiple sets of BP readings within a single followup period:\n')
  print(full[n_reading_sets!=1, .N, by=c('survey_name', 'followup', 'n_reading_sets')])
  full <- full[n_reading_sets == 1]
  full[, n_reading_sets := NULL]
  
  # subset to only survey year and participant age for each followup period
  full_demo <- unique(full[,.(survey_name, id, followup, year_start, year_end, age_year)])
  full_demo[, age_year := floor(age_year)]
  full_demo[, n_followup := length(unique(followup)), by=c('survey_name', 'id')]
  full_demo <- data.table::dcast(full_demo, survey_name + id + n_followup ~ followup, value.var = c('year_start', 'year_end', 'age_year'))
  
  # for each combination of followup period comparisons, calculate the difference in participant age and survey year
  followup_comps <- apply(combn(sort(unique(full$followup)),2), 2, paste, collapse='_') 
  full_demo[, paste0('age_diff_', followup_comps) := 
              lapply(X = followup_comps, FUN = function(x) get(paste0('age_year_', unlist(strsplit(x, '_'))[2])) - get(paste0('age_year_', unlist(strsplit(x, '_'))[1])))]
  full_demo[, paste0('max_year_diff_', followup_comps) := 
              lapply(X = followup_comps, FUN = function(x) get(paste0('year_end_', unlist(strsplit(x, '_'))[2])) - get(paste0('year_start_', unlist(strsplit(x, '_'))[1])))]
  full_demo[, paste0('min_year_diff_', followup_comps) := 
              lapply(X = followup_comps, FUN = function(x) get(paste0('year_start_', unlist(strsplit(x, '_'))[2])) - get(paste0('year_end_', unlist(strsplit(x, '_'))[1])))]
  
  # flag issues where the change in age is too large, the change in age is too small, or there is a negative change in age over time
  tol <- 5
  full_demo[, paste0('issue_', followup_comps) := 
              lapply(X = followup_comps, FUN = function(x) 
                (get(paste0('age_diff_', x)) > (get(paste0('max_year_diff_', x)) + 1 + tol)) | 
                  (get(paste0('age_diff_', x)) < (get(paste0('min_year_diff_', x)) - 1 - tol)) | 
                  (get(paste0('age_diff_', x)) < 0))]
  
  # calculate the total number of survey followup period comparisons and issues for each ID
  full_demo_issues <- data.table::melt(full_demo, id.vars = c('survey_name', 'id', 'n_followup'), 
                                       measure.vars = paste0('issue_', followup_comps), variable.name = 'comp', value.name = 'issue')
  full_demo_issues <- full_demo_issues[!is.na(issue)]
  full_demo_issues[, comp1 := tstrsplit(comp, '_',  keep=2)]
  full_demo_issues[, comp2 := tstrsplit(comp, '_',  keep=3)]
  full_demo_issues[, n_comps := .N, by=c('survey_name', 'id')]
  full_demo_issues[, n_issues := sum(issue), by=c('survey_name', 'id')]
  
  message('\nExamining issues due to invalid changes in age by comparing combinations of followup periods:\n')
  print(full_demo_issues[,.N,by=c('n_followup', 'n_comps', 'n_issues')][order(n_followup, n_comps, n_issues)])

  full_demo_issues <- data.table::melt(full_demo_issues, id.vars = c('survey_name', 'id', 'issue', 'comp', 'n_followup', 'n_comps', 'n_issues'), 
                                       measure.vars = c('comp1', 'comp2'), value.name = 'followup')
  
  # remove IDs with no issues
  full_demo_issues <- full_demo_issues[n_issues > 0] 
  
  # where... 
  #     there are issues in every survey period for an ID (number of issues is equal to the number of comparisons) 
  #     or there are only a few issues but they are not restricted to a single followup period (the number of issues doesn't equal the number of followup periods - 1, which it should if all issues are coming from a single followup period)
  #     or issues not present across all comparisons for that followup period (there is no followup period that has issues present in every comparison to another followup period)
  # ... drop all data IDs
  full_demo_issues[, prop_issue_by_followup := sum(issue)/.N, by = c('survey_name', 'id', 'followup')]
  full_demo_issues[, any_followup_w_complete_issue := any(prop_issue_by_followup == 1), by = c('survey_name', 'id')]
  drop_all_ID <- full_demo_issues[n_issues == n_comps | (n_issues != (n_followup - 1)) | any_followup_w_complete_issue == F]
  drop_all_ID <- unique(drop_all_ID[,.(survey_name, id)])
  drop_all_ID[, drop := T]
  drop_stats <- merge(drop_all_ID[, .(drop = .N), by='survey_name'], full[, .(SS = length(unique(id))), by='survey_name'], by='survey_name')
  drop_stats[, drop_prop := drop/SS]
  message('\nDropping all data for IDs with invalid changes in age not limited to a single followup period:\n')
  print(drop_stats)
  full <- merge(full, drop_all_ID, by=c('survey_name', 'id'), all.x=T)
  full <- full[is.na(drop)]
  full[, drop := NULL]
  
  # for IDs with issues restricted to a single followup period with issues present across all comparisons for that followup period, drop only data from that followup period
  drop_select_followup <- full_demo_issues[n_issues == (n_followup - 1) & any_followup_w_complete_issue == T]
  drop_select_followup <- unique(drop_select_followup[prop_issue_by_followup == 1,.(survey_name, id, followup)])
  drop_select_followup[, drop := T]
  drop_select_followup[, followup := as.numeric(followup)]
  drop_stats <- merge(drop_select_followup[, .(drop = .N), by=c('survey_name', 'followup')], 
                      full[, .(SS = length(unique(id))), by=c('survey_name', 'followup', 'year_start', 'year_end')], by=c('survey_name', 'followup'))
  drop_stats[, drop_prop := drop/SS]
  message('\nDropping followup period-specific data for IDs with invalid changes in age for that specific followup period:\n')
  print(drop_stats)
  full <- merge(full, drop_select_followup, by=c('survey_name', 'id', 'followup'), all.x=T)
  full <- full[is.na(drop)]
  full[, drop := NULL]
  
  message(paste0('\nSample size of data after validation: ', nrow(full), '\n'))
  
  return(full)
}
