#### Suite of functions for use in do_dismod_split.R

expand_test_data <- function(agg.test){
  #' @description Expands the number of rows in the test/aggregated age group dataset to appropriate number of
  #' GBD age and sex groups
  #' @param agg.test data.table. The test data from the output of divide_data
  #' @return A data.table with additional rows. One row in the old test data will lead to 'n' number of rows in new test data
  #' where 'n' is the number of GBD age*sex groups within the test data interval
  test <- copy(agg.test)
  setnames(test, c("age_start", "age_end", "sex_id"), c("agg_age_start", "agg_age_end", "agg_sex_id"))
  test[, `:=`(split.id = 1:.N,
              n.sex = ifelse(agg_sex_id==3, 2, 1))]
  test[agg_age_start > 1, n.age := (agg_age_end + 1 - agg_age_start)/5]
  test[agg_age_start == 1, n.age := (agg_age_end + 1)/5]
  test[agg_age_start == 0.07671233 & agg_age_end != 0.999, n.age := ((agg_age_end + 1)/5) + 1]
  test[agg_age_start == 0.07671233 & agg_age_end == 0.999, n.age := 1]
  test[agg_age_start == 0.01917808 & agg_age_end != 0.999 & agg_age_end != 0.07671233, n.age := ((agg_age_end + 1)/5) + 2]
  test[agg_age_start == 0.01917808 & agg_age_end == 0.07671233, n.age := 1]
  test[agg_age_start == 0.01917808 & agg_age_end == 0.999, n.age := 2]
  test[agg_age_start == 0 & agg_age_end != 0.07671233 & agg_age_end != 0.999 & agg_age_end != 0.01917808, n.age := ((agg_age_end + 1)/5) + 3]
  test[agg_age_start == 0 & agg_age_end == 0, n.age := 1]
  test[agg_age_start == 0 & agg_age_end == 0.01917808, n.age := 1]
  test[agg_age_start == 0 & agg_age_end == 0.07671233, n.age := 2]
  test[agg_age_start == 0 & agg_age_end == 0.999, n.age := 3]
  
  ## Expand for age
  test[, age_start_floor := agg_age_start]
  expanded <- rep(test[,split.id], test[,n.age]) %>% data.table("split.id" = .)
  test <- merge(expanded, test, by="split.id", all=T)
  test[, age.rep := (1:.N) - 1, by=split.id]
  
  #test$agg_age_start <- as.double(test$agg_age_start)
  
  test[age_start_floor > 1, age_start:= agg_age_start + age.rep * 5 ]
  test[age_start_floor == 1 & age.rep == 0, age_start:= 1]
  test[age_start_floor == 1 & age.rep > 0, age_start:= age.rep * 5]
  test[age_start_floor == 0.07671233 & age.rep == 0, age_start:= 0.07671233]
  test[age_start_floor == 0.07671233 & age.rep == 1, age_start:= 1]
  test[age_start_floor == 0.07671233 & age.rep > 1, age_start:= (age.rep - 1) * 5]
  test[age_start_floor == 0.01917808 & age.rep == 1, age_start:= 0.07671233]
  test[age_start_floor == 0.01917808 & age.rep == 2, age_start:= 1]
  test[age_start_floor == 0.01917808 & age.rep > 2, age_start:= (age.rep - 2) * 5]
  test[age_start_floor == 0 & age.rep == 0, age_start:= 0.00]
  test[age_start_floor == 0 & age.rep == 1, age_start := 0.01917808]
  test[age_start_floor == 0 & age.rep == 2, age_start:= 0.07671233]
  test[age_start_floor == 0 & age.rep == 3, age_start:= 1]
  test[age_start_floor == 0 & age.rep > 3, age_start:= (age.rep - 3) * 5]
  
  test[age_start > 1, age_end :=  age_start + 4]
  test[age_start == 1, age_end := 4]
  test[age_start == 0.07671233, age_end := 0.999]
  test[age_start == 0.01917808, age_end := 0.07671233]
  test[age_start == 0, age_end := 0.01917808]
  test[agg_age_end == 0, age_end := 0]
  
  ## Expand for sex
  test[, sex_split_id := paste0(split.id, "_", age_start)]
  expanded <- rep(test[,sex_split_id], test[,n.sex]) %>% data.table("sex_split_id" = .)
  test <- merge(expanded, test, by="sex_split_id", all=T)
  test <- test[agg_sex_id==3, sex_id := 1:.N, by=sex_split_id]
  test$sex_id <- as.double(test$sex_id)
  test[is.na(sex_id), sex_id:=agg_sex_id]
  #colnames(test)[colnames(test)=="agg_sex_id"] <- "sex_id"
  
  # hard-code sex restrictions for Turner (437 - should only be females) and Kleinfelter (438 - should only be males)
  if (bun_id == 437) {
    test[sex_id == 1, cases := 0]
  }
  if (bun_id == 438) {
    test[sex_id == 2, cases := 0]
  }
  
  test
}

add_pops <- function(expanded){
  #convert year_start and year_end to year_id as the midpoint
  expanded[,year_id := floor((as.numeric(year_end)+as.numeric(year_start))/2)]
  
  #make this a condition for all congenital bundles
  if (bun_id == 614) {expanded <- expanded[year_id > 1984]}
  
  #pull populations for every year and location of data that need to be split
  pops <- get_population(year_id = unique(expanded$year_id), sex_id = c(1,2), 
                         location_id = unique(expanded$location_id), 
                         age_group_id = unique(age_map$age_group_id),
                         gbd_round_id = 6, decomp_step = 'step1')
  pops$run_id <- NULL
  
  #round age_start and age_end for neonatal age groups, then add age_start and age_end to pops
  age_map[age_group_id %in% c(2:4), `:=` (age_start = round(age_start, 3),
                                          age_end = round(age_end, 4))]
  age_map$age_end[2] <- 0.077
  
  #add age end and age start to pops
  pops <- merge(pops, age_map, by = 'age_group_id')
  
  #round age_start and age_end for neonatal in the aggregate dataset, in preparation for the merge
  #(when there are too many decimal places, the merge can't handle it)
  #merge the populations onto the aggregate dataset
  expanded[age_end > 0 & age_end < 1, `:=` (age_start = round(age_start, 3),
                                            age_end = round(age_end, 3))]
  expanded <- merge(expanded, pops, by = c("age_start", "age_end", "sex_id", "location_id", "year_id"),
                    all.x = TRUE)
  
  #' Create an expand ID for tracking the draws. 1000 draws for each expand ID
  expanded[,expand.id := 1:.N]
  
  # Split ID is related to each orignial aggregated test data point. So add up the population of each individual
  #' group within each split ID. That is the total population of the aggregated age/sex group
  expanded[, pop.sum := sum(population), by = split.id]
  
  return(expanded)
  
  print("Expanding done")
}





pull_model_weights <- function(model_id, measure_name){
  
  measure_id <- ifelse(measure_name == "prevalence", 5, 6)
  
  wt_data <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = model_id, source = 'epi',
                       measure_id = measure_id, 
                       location_id = unique(expanded$location_id),
                       sex_id = unique(expanded$sex_id), 
                       gbd_round_id = 6, decomp_step = "step1")
  
  if (nrow(wt_data)==0) { 
    print(paste("ME ID",model_id,"does not have DisMod results for",measure_name))
    stop(paste("ME ID",model_id,"does not have DisMod results for",measure_name))
  }
  
  wt_data[, c("measure_id", "metric_id", 'model_version_id', 'modelable_entity_id') := NULL]
  # wt_data2 <- interpolate(gbd_id_type = 'modelable_entity_id', gbd_id = model_id, source = 'epi',
  #                        measure_id = 5, location_id = unique(expanded$location_id),
  #                        sex_id = c(1,2), gbd_round_id = 6, decomp_step = "step1",
  #                        reporting_year_start = 1980, reporting_year_end = 2017)
  
  setnames(wt_data, 'year_id', 'est_year_id')
}


gen_weights <- function(model_id, age_map){
  ## Generate weights
  wt_data <- fread(paste0("FILEPATH"))
  wt_data[, c('measure_id', "V1", "model_version_id", "lower", "upper"):=NULL]
  wt_data <- wt_data[!age_group_id %in% c(22,27)]
  
  if(model_id %in% map[type == "maternal", me_id]){
    wt_data <- wt_data[sex_id == 2]
  }
  
  wt_data <- wt_data[measure %in% c('prevalence', 'incidence')]
  
  if (nrow(wt_data) == 0) {
    stop("model data does not contain prevalence or incidence.")
  }
  
  ### pull population, for converting from rate to count space
  pops <- get_population(age_group_id = unique(age_map$age_group_id), location_id = unique(wt_data$location_id), year_id = unique(wt_data$year_id), sex_id = c(1,2), gbd_round_id = 6, decomp_step = 'step1')
  pops$run_id <- NULL
  
  wt_data <- merge(wt_data, pops, by = c("location_id", "sex_id", "age_group_id", "year_id"), all.x = TRUE)
  wt_data <- wt_data[, ':='(mean = mean*population, population = NULL)]
  
  wt_data <- wt_data[, agesex_wt:=mean/sum(mean), by = .(location_id, year_id, measure)]
  # write.csv(wt_data, paste0(h, "GBD_2019/congenital_work/190308_Xwalks/dismod_splits/wt_data2.csv"), row.names = FALSE)
  
  wt_data <- wt_data[, mean := NULL]
  
  if(!164 %in% unique(wt_data$age_group_id)){
    at_birth_workaround <- copy(wt_data)
    at_birth_workaround <- at_birth_workaround[age_group_id == 2]
    at_birth_workaround <- at_birth_workaround[, age_group_id :=164]
    
    wt_data <- rbind(at_birth_workaround, wt_data)
  }
  
  wt_data <- merge(wt_data, age_map[, .(age_group_id, order)], by = "age_group_id", all.x = TRUE)
  wt_data <- wt_data[, age_group_id := NULL]
  
  ## wide on age_group_id
  wt_data <- dcast(wt_data, measure + location_id + year_id + sex_id ~ order, value.var = 'agesex_wt')
  
  return(wt_data)
}



pull_keep_cols <- function(bun_id, measure_name){
  me_id <- map[bundle_id == bun_id, me_id]
  study_label <- fread(paste0("FILEPATH"))
  measure_map <- fread('FILEPATH')
  
  cvs <- study_label[measure_id == measure_map[measure == measure_name, measure_id], study_covariate]
  if(!identical(cvs, character(0))){
    cvs <- paste0("cv_", cvs)
    cvs <- cvs[!cvs %in% c("cv_NA", 'cv_sex')]
  }
  
  ####
  
  keep_cols <- c("nid", 'measure', "location_id", "year_start", "year_end", "sex", "age_start", "age_end", "age_demographer", "cases", "sample_size", "mean", "standard_error", cvs)
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
  dt[is.na(standard_error) & measure == "prevalence", standard_error := (1/(1+z^2/sample_size)) * sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

get_upper_lower <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[!is.na(mean) & !is.na(standard_error) & is.na(lower), lower := mean - (1.96 * standard_error)]
  dt[!is.na(mean) & !is.na(standard_error) & is.na(upper), upper := mean + (1.96 * standard_error)]
  return(dt)
}


pull_bundle_data <- function(measure_name, bun_id, bun_data){
  bun_data <- bun_data[measure == measure_name]
  
  #do not process outliers
  #bun_data <- bun_data[group_review == 0, is_outlier := 1]
  #bun_data <- bun_data[is_outlier != 1]
  
  #### special cases -- change saudi subnationals to national level
  bun_data <- bun_data[location_id %in% c(44551, 44547, 44553, 44541, 44549, 44550, 44545, 44546, 44552, 44548, 44542, 44543, 44544), location_id := 152]
  
  ### Fill in any blank values in mean, sample size, or cases, based on the available values
  bun_data[!is.numeric(standard_error), standard_error := as.numeric(standard_error)]
  bun_data[!is.numeric(upper), upper := as.numeric(upper)]
  bun_data[!is.numeric(lower), lower := as.numeric(lower)]
  #bun_data[standard_error == "", standard_error := NA]
  #bun_data[upper == "", upper := NA]
  #bun_data[lower == "", lower := NA]
  
  bun_data <- get_cases_sample_size(raw_dt = bun_data)
  bun_data <- get_se(raw_dt = bun_data)
  bun_data <- calculate_cases_fromse(raw_dt = bun_data)
  bun_data <- get_upper_lower(raw_dt = bun_data)
  
  ### change location_id to parent_id
  #bun_data <- merge_parent_id(bun_data)
  
  ### subset to id columns and measure-specific cv's
  # keep_cols <- pull_keep_cols(bun_id, measure_name)
  # bun_data <- bun_data[, c(keep_cols), with = FALSE]
}


gen_age_range <- function(measure_i, data){
  bun_data <- data[[measure_i]]

  ### age split
  bun_data <- bun_data[, age_range:=age_end-age_start]
  
  #rounds age_start down to nearest smaller multiple of 5, and rounds age_end up to the nearest larger multiple of 5 minus 1
  #i.e. age_start of 12 rounds down to 10, and age_end of 13 rounds up to 14, so the data now matches GBD age group 10-14
  wide_age <- bun_data[age_end >= 5, ':='(age_start = (age_start - age_start%%5), age_end = (age_end - age_end%%5 + 4))]
  
  wide_age <- merge(wide_age, age_map[, .(order, age_start)], by = "age_start", all.x = TRUE)
  wide_age <- wide_age[, start_order := order]
  wide_age <- wide_age[, order := NULL]
  
  wide_age <- merge(wide_age, age_map[, .(order, age_end)], by = "age_end", all.x = TRUE)
  
  sex_names <- get_ids(table='sex')
  wide_age <- merge(wide_age, sex_names, by = "sex", all.x = TRUE)
  
  
  return(wide_age)
}

merge_parent_id <- function(data){
  data <- merge(data, loc_metadata, by = "location_id", all.x = TRUE)
  data <- data[is.na(parent_id), parent_id := location_id]
  data <- data[, ':='(location_id = parent_id, parent_id = NULL)]
}

do_sex_specific <- function(measure_i, data){
  age_split <- copy(data[[measure_i]])
  
  if(nrow(age_split) != 0){
    age_split <- age_split[sex %in% c("Male", "Female") & start_order == order, keep := 1]
    age_split <- age_split[is.na(keep), keep := 0]
    age_spec <- split(age_split, age_split$keep)
    
    ### formats and sets aside M/F & w/in gbd-age_groups bun_data
    mf_inrange <- age_spec[['1']]
    mf_inrange <- mf_inrange[, c("age_range", "start_order", "keep"):= NULL]
    mf_inrange <- merge(mf_inrange, age_map[, .(order, age_group_id)], by = "order", all.x = TRUE)
    mf_inrange$order <- NULL
    # mf_inrange <- merge(mf_inrange, loc_metadata, by = "location_id", all.x = TRUE)
    # mf_inrange <- mf_inrange[is.na(parent_id), parent_id := location_id]
    
    #########################
    ### formats and creates ratio to adjust M/F non-gbd-age_group bun_data 
    mf_outrange <- age_spec[['0']][sex %in% c("Male", "Female")]
    mf_outrange <- mf_outrange[,  ':='(year_mid =ceiling((year_start + year_end)/2))]
    mf_outrange <- mf_outrange[,  ':='(year_id = year_mid - year_mid%%5)]
    mf_outrange <- mf_outrange[year_id %in% c(1990:2017)]
    id_cols <- length(colnames(mf_outrange))
    
    
    mf_outrange <- merge(mf_outrange, wt_data[measure == unique(age_split$measure)], by = c('location_id', 'sex_id', 'year_id', 'measure'), all.x = TRUE)
    # mf_outrange <- mf_outrange[, variable:=NULL]
    mf_outrange <- mf_outrange[, id:=.I]
    
    final_data <- data.table()
    for(n in c(1:nrow(mf_outrange))){
      loop_data <- copy(mf_outrange[n])
      cols <- c(loop_data[,start_order]:loop_data[,order]) + id_cols
      loop_data <- loop_data[, denom := sum(.SD, na.rm=T), .SDcols=cols]
      loop_data <- loop_data[, .(id, denom)]
      final_data <- rbind(final_data, loop_data)
    }
    
    mf_outrange <- merge(mf_outrange, final_data, by = "id", all.x = TRUE)
    
    final_data <- data.table()
    for(n in c(1:nrow(mf_outrange))){
      loop_data <- copy(mf_outrange[n])
      for(x in c(0:(loop_data[,order] - loop_data[,start_order]))){
        col <- loop_data[,start_order] + x + id_cols + 1
        print(col)
        
        test <- data.table(id = loop_data[, id], denom = loop_data[,denom], num = loop_data[, .SD, .SDcols = col], order = col- id_cols - 1)
        test$ratio <- test[[3]] / test[[2]]
        
        test <- test[, .(id, ratio, order)]
        
        final_data <- rbind(final_data, test)
      }
    }
    
    keep_cols <- pull_keep_cols(bun_id, unique(age_split$measure))
    keep_cols <- keep_cols[!keep_cols %in% c("age_start", 'age_end')]
    
    mf_outrange <- merge(final_data, mf_outrange[, c(keep_cols, 'id', 'sex_id'), with=FALSE], by = 'id', all.x = TRUE)
    
    
    mf_outrange <- mf_outrange[, ':='(cases = cases*ratio, sample_size = sample_size*ratio)]
    mf_outrange <- mf_outrange[, mean:=cases/sample_size]
    mf_outrange <- merge(mf_outrange, age_map, by = "order", all.x = TRUE)
    mf_outrange <- mf_outrange[, c("id", "ratio", "order") := NULL]
    
    # mf_outrange <- merge(mf_outrange, loc_metadata, by = 'location_id', all.x = TRUE)
    #########################
    
    
    final <- rbind(mf_inrange, mf_outrange)
    return(final)
  } else{
    return(age_split)
  }
  
}


do_sex_agg <- function(measure_i, data){
  sex_split <- copy(data[[measure_i]])
  if(nrow(sex_split) != 0){
    sex_split <- sex_split[sex_id == 3 & start_order == order, keep := 1]
    sex_split <- sex_split[is.na(keep), keep := 0]
    
    age_spec <- split(sex_split, sex_split$keep)
    
    ##########################################
    both_sex_split <- age_spec[['1']]
    if(is.null(both_sex_split)){
      test <- setNames(data.table(matrix(nrow = 0, ncol = length(colnames(sex_split)))), colnames(sex_split))
      return(test)
    } else{
      both_sex_split <- both_sex_split[,  ':='(year_mid =ceiling((year_start + year_end)/2))]
      both_sex_split <- both_sex_split[,  ':='(year_id = year_mid - year_mid%%5)]
      both_sex_split <- both_sex_split[year_id %in% c(1990:2017)]
      both_sex_split <- both_sex_split[, id:=.I]
      
      both_sex_split_wt <- melt(wt_data, id.vars = c('measure', 'location_id', 'year_id', 'sex_id'), variable.name = 'order')
      both_sex_split_wt <- dcast(both_sex_split_wt, formula = measure + location_id + year_id + order ~ sex_id, value.var = 'value')
      both_sex_split_wt <- both_sex_split_wt[, order := as.integer(order)]
      
      both_sex_split <- merge(both_sex_split, both_sex_split_wt, by = c('measure', 'location_id', 'year_id', 'order'), all.x = TRUE)
      
      both_sex_split$Male <- both_sex_split$`1` / (both_sex_split$`1` + both_sex_split$`2`)
      both_sex_split$Female <- both_sex_split$`2` / (both_sex_split$`1` + both_sex_split$`2`)
      
      keep_cols <- pull_keep_cols(bun_id, unique(both_sex_split$measure))
      
      final_data <- data.table()
      for(n in c(1:nrow(both_sex_split))){
        loop_data <- copy(both_sex_split[n])
        
        for(x in c('Male', 'Female')){
          test <- copy(loop_data)
          test$cases <- test$cases * test[, x, with = FALSE]
          test$sample_size <- test$sample_size * test[, x, with = FALSE]
          test$mean <- test$cases / test$sample_size
          test$sex <- x
          
          test <- test[, keep_cols, with = FALSE]
          
          final_data <- rbind(final_data, test)
        }
      }
      final_data <- merge(final_data, age_map[, .(age_start, age_end, age_group_id)], by = c('age_start', 'age_end'), all.x = TRUE)
      
      final_data <- merge(final_data, sex_names, by = 'sex', all.x = TRUE)
      return(final_data)
    }
    
  } else{
    return(sex_split)
  }
}

compile_split_data <- function(measure_i, sex_spec_data, sex_agg_data){
  sex_spec <- copy(sex_spec_data[[measure_i]])
  sex_agg <- copy(sex_agg_data[[measure_i]])
  
  
  datalist <- list(sex_spec, sex_agg)
  all_data <- rbindlist(datalist, use.names = TRUE, fill = TRUE, idcol = FALSE)
  
}


do_matched_merge <- function(measure_i, data, match_type){
  # wb <- createWorkbook()
  match_data <- copy(data[[measure_i]])
  
  if(nrow(match_data) > 0){
    cv_cols <- grep('cv_', colnames(match_data), value = TRUE)
    match_data <- match_data[, year_id := round((year_start + year_end)/2)]
    
    matched_data_list <- lapply(c(1:length(cv_cols)), ref_to_alt, cv_cols = cv_cols, match_data = match_data, match_type = match_type)
    return(matched_data_list)
    
  } else{
    return(match_data)
  }
}

ref_to_alt <- function(cv_position, cv_cols, match_data, match_type){
  cv <- cv_cols[cv_position]
  
  
  match_data <- match_data[, c('nid', 'location_id', 'sex_id', 'age_group_id', 'year_id', 'mean', 'standard_error', 'measure', cv), with = FALSE]
  
  match_split <- split(match_data, match_data[, c(cv), with = FALSE])
  if(length(match_split) == 2){
    # matched_merge <- merge(match_split[['0']], match_split[['1']], by = c('location_id', 'age_group_id', 'sex_id', 'measure', 'nid'), all.x = TRUE)
    
    if(match_type == 'within_study'){
      matched_merge <- merge(match_split[['1']], match_split[['0']],  by = c('nid', 'location_id', 'age_group_id', 'sex_id', 'measure'), all.x = TRUE, allow.cartesian=TRUE)
    } else if(match_type == 'between_study'){
      matched_merge <- merge(match_split[['1']], match_split[['0']],  by = c('location_id', 'age_group_id', 'sex_id', 'measure'), all.x = TRUE, allow.cartesian=TRUE)
    }
    
    
    matched_merge <- matched_merge[!is.na(mean.y)]
    matched_merge <- matched_merge[!mean.x==0]
    matched_merge <- matched_merge[!mean.y==0]
    
    
    if(!all(unique(matched_merge$mean.x == 0) & unique(matched_merge$mean.y == 0))){
      print('means other than zero == good')
      
      matched_merge <- matched_merge[abs(year_id.x - year_id.y) < 6]
      
      matched_merge <- matched_merge[, mean:= mean.x / mean.y]
      matched_merge <- matched_merge[, standard_error:= sqrt( (mean.x^2 / mean.y^2) * ( (standard_error.x^2 / mean.x^2) + (standard_error.y^2 / mean.y^2) ) ) ]
      
      matched_merge <- matched_merge[, year_id := round((year_id.x + year_id.y)/2)] ### workaround -- average of year range
      
      if(match_type == 'within_study'){
        matched_merge <- matched_merge[, c('nid', 'location_id', 'age_group_id', 'sex_id', 'year_id', 'measure', 'mean', 'standard_error')]
      } else if(match_type == 'between_study'){
        matched_merge <- matched_merge[, c('nid.x', 'nid.y', 'location_id', 'age_group_id', 'sex_id', 'year_id', 'measure', 'mean', 'standard_error')]
      }
      
      
      if(nrow(matched_merge) == 0){
        matched_merge <- matched_merge[1L]
        matched_merge <- matched_merge[, ':='(measure = unique(match_data$measure), cv_name = cv)]
      } else{
        matched_merge <- matched_merge[, cv_name := cv]
      }
    } else{
      print('all means zero')
      matched_merge <- data.table(cv_name = cv, measure = unique(match_data$measure), status = 'all mean observations are zero')
      
    }
    
    
    
    
    
    return(matched_merge)
  } else{
    print('no ref alt matches')
    no_match <- data.table(cv_name = cv, measure = unique(match_data$measure), status = 'no reference to alternate matches')
    return(no_match)
  }
  
}

save_matched_merge_list <- function(measure_i, data, wb){
  save_data <- copy(data[[measure_i]])
  
  
  if(measure_i == 1){
    type <- "pr"
  } else{
    type <- 'inc'
  }
  
  if(any(lengths(save_data) > 0)){
    lapply(c(1:length(save_data)), do_save, data = save_data, wb = wb, type_name = type)
  }
  else{
    sheet_name <- paste0(type, " - no data present" )
    
    addWorksheet(wb, sheet_name)
    writeData(wb, sheet_name, save_data)
    
    saveWorkbook(wb, file = paste0("FILEPATH"), overwrite = TRUE)
  }
}


do_save <- function(cv_i, data, wb, type_name){
  saved_data <- copy(data[[cv_i]])
  
  sheet_name <- paste0(type_name, " ", saved_data[, unique(cv_name)])
  
  addWorksheet(wb, sheet_name)
  writeData(wb, sheet_name, saved_data)
  
  saveWorkbook(wb, file = paste0("FILEPATH"), overwrite = TRUE)
  
}
