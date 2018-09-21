#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Crosswalk function for learning data (Human Capital Index project)
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### set options
# set reference test(s) with regex
ref_test <- "TIMSS|PIRLS"
# create vector of test groups
test_groups <- c("IAEP", 
                 "PISA", "SACMEQ", "SACMEQ_both", "NAEP_TREND", "PASEC", "pII",  
                 "NAS", "BAS", "RLS_Reading", 
                 "NAEP_MAIN", "SERCE", "TERCE", "PERCE", "IEA")
# set the vars to collapse by
collapse_vars <- c("x_subject", "x_grade", "x_year", "sex_id") 
# map path
map_path <- "PATH" 
#***********************************************************************************************************************


#----FUNCTION-----------------------------------------------------------------------------------------------------------
### prep crosswalking function
crosswalk <- function(method=NULL) {
  
  ### break if missing method specification
  if (!method %in% c("ratio", "z_score", "regression") | is.null(method)) { stop("BREAK | must specify method to use (i.e. 'ratio', 'z_score', or 'regression')") }
  #***********************************************************************************************************************
  
  
  #----PREP CROSSWALK-----------------------------------------------------------------------------------------------------
  ### get map
  map <- readxl::read_excel(map_path) %>% as.data.table
  
  ### read in all prepped files
  prepped_extractions <- list.files("PATH", full.names=TRUE)
  xwalk <- rbindlist(lapply(prepped_extractions, fread), fill=TRUE)
  
  ### variables of interest
  vars <- c("mean", "standard_error", "standard_deviation", "sample_size")
  
  ### drop 
  # TIMSS advanced
  xwalk <- xwalk[!(test %in% c("TIMSS_Mathematics", "TIMSS_Science") & grade_level==12)]
  # missing sexes
  xwalk <- xwalk[sex_id %in% c(1, 2, 3)]
  # NAS 8th grade (very odd scores)
  NAS_tests <- xwalk[(grep("NAS_", test)), test] %>% unique
  xwalk <- xwalk[!(test %in% NAS_tests & grade_level==8)]
  # NAEP_TREND 2004 original assessment (we have data for both original and revised, I chose to keep revised because that's the test we have for 2008) # "Revised assessment test, test content different from original assessment, and allow accomodations for students"
  xwalk <- xwalk[!(test %in% c("NAEP_TREND_reading", "NAEP_TREND_math") & year_start==2004 & ihme_loc_id=="USA" & notes=="Original Assessment, physically disabled students might be excluded")]
    
  ### remove unnecessary columns
  xwalk <- xwalk[, c("notes", "nclust", "nstrata", "design_effect", "age_year", "location_id", "location_name", "var", "extractor", "test_name", "comments") := NULL]
  
  ### prep and fix columns
  # remove observations missing both mean and SE (here, only NAEP_MAIN missingness)
  xwalk <- xwalk[!(is.na(standard_error) & is.na(mean)), ]
  # calculate year_id
  xwalk[, year_id := ((year_start + year_end) / 2) %>% floor]
  # make sure mean is numeric
  xwalk[, mean := mean %>% as.numeric]
  xwalk <- xwalk[!is.na(mean), ]
  xwalk[, standard_error := standard_error %>% as.numeric]
  xwalk[, sample_size := sample_size %>% as.numeric]
  # calculate standard deviation
  if (!"standard_deviation" %in% colnames(xwalk)) xwalk[, standard_deviation := NA %>% as.numeric] else xwalk[, standard_deviation := standard_deviation %>% as.numeric]
  xwalk[is.na(sample_size), sample_size := 500]
  xwalk[!is.na(sample_size) & is.na(standard_error) & is.na(standard_deviation), standard_error := mean / 150] # median ratio between SE and mu is 150 in this dataset, with 25th and 75th quantiles at 100 and 200, respectively
  xwalk[!is.na(sample_size) & is.na(standard_deviation) & !is.na(standard_error), standard_deviation := standard_error * sqrt(sample_size)]
  xwalk[!is.na(sample_size) & is.na(standard_error) & !is.na(standard_deviation), standard_error := standard_deviation / sqrt(sample_size)]
  #xwalk <- xwalk[!is.na(standard_deviation), ]
  print(paste0("missing standard_error for ", nrow(xwalk[is.na(standard_error), ]), " observations; ",
               "missing standard_deviation for ", nrow(xwalk[is.na(standard_deviation), ]), " observations; ",
               "missing sample_size for ", nrow(xwalk[is.na(sample_size), ]), " observations (dropping)"))
  
  ### apply crosswalk vars - test subject
  # TIMSS (one reference) only has math and science components, so code which subject to crosswalk to which TIMSS test; PIRLS has only reading
  xwalk[, x_subject := NA_character_]
  xwalk[grep("Science", test, ignore.case=TRUE), x_subject := "Science"]
  xwalk[grep("Math|ProblemSolving|Numeracy", test, ignore.case=TRUE), x_subject := "Mathematics"]
  xwalk[grep("Reading|Language|Literacy", test, ignore.case=TRUE), x_subject := "Reading"]
  print(paste0("missing subject name for ", nrow(xwalk[is.na(x_subject), ]), " observations: ", paste(unique(xwalk[is.na(x_subject), test]), collapse=", "), " (dropping)"))
  xwalk <- xwalk[!is.na(x_subject), ]
  
  ### create reference data table of TIMSS and PIRLS data (the reference tests)
  reference <- xwalk[grep(ref_test, test), ]
  
  ### remove reference (TIMSS/PIRLS) from rest of crosswalk
  xwalk <- xwalk[!grep(ref_test, test), ]
  
  #### remove duplicates
  xwalk <- xwalk[!duplicated(xwalk)]
  
  ### create dataset of overlapping tests
  map[, grade_level := as.numeric(grade_level)]
  map[is.na(grade_level), grade_level := 9999]
  xwalk[, grade_level := as.numeric(grade_level)]
  xwalk[is.na(grade_level), grade_level := 9999]
  # save ??
  save_overlap <- FALSE
  # set difference
  overlap_vars <- c("x_subject", "grade_level", "year_id", "sex_id")
  ref_overlap <- copy(reference) %>% 
    .[, c("test", overlap_vars, "ihme_loc_id"), with=FALSE] %>% unique %>% 
    .[, t := paste(test, year_id, grade_level, sex_id, sep="__")] %>%
    .[, .(t, ihme_loc_id)] %>% unique %>%
    .[, overlap := paste(t, collapse=", "), by=c("ihme_loc_id")] %>% 
    .[, .(overlap, ihme_loc_id)] %>% unique
  xwalk_overlap <- xwalk[, c("test", overlap_vars, "ihme_loc_id"), with=FALSE] %>% unique
  overlap <- merge(xwalk_overlap, ref_overlap, by=c("ihme_loc_id"), all.x=TRUE) %>% 
    .[, overlap_tests := paste(unique(overlap), collapse=", "), by=c(overlap_vars, "test")] %>% 
    .[, overlap_ihme_loc_id := paste(unique(ihme_loc_id), collapse=", "), by=c(overlap_vars, "test", "overlap_tests")]
  new_overlaps <- overlap[, c(overlap_vars, "test", "overlap_tests", "overlap_ihme_loc_id"), with=FALSE] %>% unique
  diff <- new_overlaps[!map, on=c(overlap_vars, "test")]
  # save / print difference
  if (nrow(diff) > 0) print(paste0("missing ", nrow(diff), " combinations of testing variables - add these to the xwalk map before continuing"))
  if (save_overlap) write.xlsx(rbind(map, diff, fill=TRUE), map_path, row.names=FALSE)
  
  ### map crosswalks
  xwalk <- merge(xwalk, map[, .(test, x_subject, grade_level, year_id, sex_id, xwalk_include, xwalk_unique, xwalk_subject, xwalk_year_id, xwalk_grade_level, xwalk_sex_id)],
                 by=c("test", "x_subject", "grade_level", "year_id", "sex_id"), all.x=TRUE)
  if (nrow(xwalk[is.na(xwalk_unique) | xwalk_unique=="NA"]) > 0) print(paste0("missing crosswalk maps for ", nrow(xwalk[is.na(xwalk_unique) | xwalk_unique=="NA"]), " observations from tests: ",
                                                                              paste(unique(xwalk[is.na(xwalk_unique) | xwalk_unique=="NA", test]), collapse=", ")))
  #***********************************************************************************************************************
  
  
  #----PREP REFERENCE-----------------------------------------------------------------------------------------------------
  ### save reference for upload
  ref_save <- copy(reference)
  setnames(ref_save, vars[!vars %in% "sample_size"], paste0("xwalk_", vars[!vars %in% "sample_size"]))
  ref_save <- ref_save[, .(nid, file_path, table_num, ihme_loc_id, year_id, year_start, year_end, sample_size, 
                           age_group_id, sex_id, site_memo, smaller_site_unit, test, x_subject,
                           grade_level, xwalk_mean, xwalk_standard_error, xwalk_standard_deviation)]
  write.csv(ref_save, file.path(xwalk_out_path, "reference.csv"), row.names=FALSE)
  
  ### take average of sexes (weighted by sample size) to get both sex mean for crosswalking
  calculate_both_sex_mean <- function(dataset, dupe=TRUE) {
    
    if (dupe) dataset <- rbind(dataset, copy(dataset[sex_id==3])[, sex_id := 1], copy(dataset[sex_id==3])[, sex_id := 2])
    
    dataset_3 <- copy(dataset) # dataset[sex_id %in% c(1, 2), ]
    dataset_3[, sex := NA_character_]
    dataset_3[sex_id==3, sex := "both"]
    dataset_3[sex_id==2, sex := "female"]
    dataset_3[sex_id==1, sex := "male"]
    cols_wide_sex <- colnames(dataset)[!colnames(dataset) %in% c(vars, "sex_id", "sex", "hh_id", "int_year", 
                                                                 "mathematics", "reading", "language", "science", "comments", "table_num", "national_sample_size")]
    formula <- paste(cols_wide_sex, collapse=" + ")
    dataset_3 <- dcast.data.table(dataset_3, value.var=vars, as.formula(paste0(formula, " ~ sex")))
    if ("mean_both" %in% colnames(dataset_3)) {
      dataset_3[is.na(mean_both), mean_both := ((mean_female * sample_size_female) + (mean_male * sample_size_male)) / (sample_size_female + sample_size_male)]
      dataset_3[is.na(standard_error_both), standard_error_both := mean_both * ((standard_error_female + standard_error_male) / (mean_female + mean_male))]
      dataset_3[is.na(standard_deviation_both), standard_deviation_both := mean_both * ((standard_deviation_female + standard_deviation_male) / (mean_female + mean_male))]
      dataset_3[is.na(sample_size_both), sample_size_both := rowSums(dataset_3[is.na(sample_size_both), .(sample_size_female, sample_size_male)], na.rm=TRUE)]
    } else {
      dataset_3[, mean_both := ((mean_female * sample_size_female) + (mean_male * sample_size_male)) / (sample_size_female + sample_size_male)]
      dataset_3[, standard_error_both := mean_both * ((standard_error_female + standard_error_male) / (mean_female + mean_male))]
      dataset_3[, standard_deviation_both := mean_both * ((standard_deviation_female + standard_deviation_male) / (mean_female + mean_male))]
      dataset_3[, sample_size_both := rowSums(dataset_3[, .(sample_size_female, sample_size_male)], na.rm=TRUE)]
    }
    dataset <- melt(dataset_3, id.vars=cols_wide_sex, value.name="mean")
    dataset[, sex_id := NA_integer_]
    dataset[grep("_female", variable), sex_id := 2]
    dataset[grep("_male", variable), sex_id := 1]
    dataset[grep("_both", variable), sex_id := 3]
    dataset[grep("sample_size", variable), variable := "sample_size"]
    dataset[grep("mean", variable), variable := "mean"]
    dataset[grep("standard_error", variable), variable := "standard_error"]
    dataset[grep("standard_deviation", variable), variable := "standard_deviation"]
    dataset <- dcast.data.table(dataset, value.var="mean", as.formula(paste0(paste(c(cols_wide_sex, "sex_id"), collapse=" + "), " ~ variable")))
    
    ### fix colnames for merging
    dataset[, x_grade := grade_level]
    dataset[grade_level %in% c(2, 3, 4, 5, 6), x_grade := 4]
    dataset[grade_level %in% 7:12, x_grade := 8]
    
    ### set names
    setnames(dataset, vars, paste0("ref_", vars))
    
  }
  reference <- calculate_both_sex_mean(dataset=copy(reference))
  
  # save reference version for IQ crosswalk
  write.csv(reference, "PATH", row.names=FALSE)
  #***********************************************************************************************************************
  
  
  #----CROSSWALK----------------------------------------------------------------------------------------------------------
  ### tell me which groups aren't being crosswalked, then loop through test groups that are being crosswalked
  crosswalk_tests <- paste0(ref_test, "|", paste(test_groups, collapse="|"))
  if (length(unique(xwalk[!grep(crosswalk_tests, test), test])) > 0) print(paste0("not crosswalking: ", paste(unique(xwalk[!grep(crosswalk_tests, test), test]), collapse=", ")))
  for (test_ind in test_groups) {
    
    # make sure test is in xwalk data table
    if (xwalk[grep(test_ind, test), nid] %>% length > 1) {
      
      print(paste0("crosswalking ", test_ind))
      
      # set collapse vars
      collapse_vars_x <- c("xwalk_subject", "xwalk_grade_level", "xwalk_year_id", "xwalk_sex_id")  #collapse_vars
      
      ### keep just observations for this test
      xwalk_cross <- xwalk[grep(test_ind, test), ]
      if (test_ind=="IAEPI") { xwalk_cross <- xwalk_cross[!grep("IAEPII", test), ] }
      ### remove dupes (SACMEQ)
      xwalk_cross <- xwalk_cross[!duplicated(xwalk_cross[, .(nid, file_path, table_num, ihme_loc_id, year_start, year_end, 
                                                             age_group_id, sex_id, site_memo, smaller_site_unit, test, x_subject,
                                                             grade_level, mean, standard_error, standard_deviation, sample_size)]), ]
      
      ### crosswalks that don't have matches to TIMSS or PIRLS
      # crosswalk IEA (1970s-1980s) tests to NAEP_TREND tests, not TIMSS/PIRLS
      # crosswalk pII to PASEC
      if (test_ind %in% c("pII", "IEA")) {
        # copy reference so don't overwrite with new ref
        ref_save_for_later <- copy(reference)
        # set new crosswalk reference test name
        if (test_ind=="IEA") new_ref_test <- "NAEP_TREND" else if (test_ind=="pII") new_ref_test <- "PASEC"
        if (test_ind=="IEA") duplicate <- FALSE else if (test_ind=="pII") duplicate <- TRUE
        # save new "reference" test for xwalk of this individual test
        print(paste0("reading in ", file.path(xwalk_out_path, paste0(new_ref_test, ".csv")), " as new reference for ", test_ind))
        new_ref <- fread(file.path(xwalk_out_path, paste0(new_ref_test, ".csv"))) %>%
          setnames(., "sample_size", "xwalk_sample_size") %>%
          .[, c("nid", "file_path", "test", "ihme_loc_id", "sex_id", "year_id", paste0("xwalk_", vars), "national_sample_size", "grade_level"), with=FALSE] %>%
          setnames(., paste0("xwalk_", vars), vars) %>%
          calculate_both_sex_mean(., dupe=duplicate) %>%
          .[grep("Math|ProblemSolving|Numeracy", test, ignore.case=TRUE), x_subject := "Mathematics"] %>%
          .[grep("Reading|Language|Literacy", test, ignore.case=TRUE), x_subject := "Reading"] %>%
          .[, x_year := year_id]
        # bind together
        reference <- rbind(reference, new_ref, fill=TRUE)
      }
      
      if (test_ind=="PASEC") reference[test=="TIMSS_Mathematics" & grade_level==9, grade_level==8]
      
      # merge on reference with all vars
      xwalk_reference <- copy(reference) %>% .[is.na(grade_level), grade_level := x_grade] %>% setnames(., c("x_subject", "grade_level", "year_id", "sex_id"), collapse_vars_x)
      xwalk_cross <- merge(xwalk_cross, xwalk_reference[, c("ihme_loc_id", collapse_vars_x, paste0("ref_", vars)), 
                                                        with=FALSE] %>% unique, 
                           by=c("ihme_loc_id", collapse_vars_x), 
                           all.x=TRUE)
      
      collapse_vars_x <- c("xwalk_unique", "x_subject", "xwalk_year_id", "xwalk_grade_level", "xwalk_sex_id")
      
      ### crosswalk
      # take mean of reference test scores
      x_ref <- xwalk_cross[!is.na(ref_mean) & xwalk_include==1, c("ihme_loc_id", "xwalk_include", collapse_vars_x, paste0("ref_", vars)), with=FALSE] %>% unique %>%
        .[,      list(ref.mean = mean(ref_mean), 
                 ref.std.dev   = sqrt( sum((ref_sample_size - 1) * (ref_standard_deviation ^ 2)) / (sum(ref_sample_size) - length(ref_standard_deviation)) ), #( 1 / length(ref_standard_deviation) ) * sqrt(sum(ref_standard_deviation ^ 2)), #1/N*sqrt(SDa^2+SDb^2)
                 ref.std.error = ( 1 / length(ref_standard_error)) * sqrt(sum((ref_standard_error ^ 2) / ref_sample_size)), #1/N*sqrt(SEa^2/Na + SEb^2/Nb)
                 loc_overlap   = list(unique(ihme_loc_id)) %>% as.character,
                 num_overlap   = length(ihme_loc_id) ), 
          by=c("xwalk_unique", "xwalk_sex_id")]
      # take mean of scores on alternative test
      x_alt <- xwalk_cross[!is.na(ref_mean) & xwalk_include==1, c("ihme_loc_id", "xwalk_include", collapse_vars_x, vars), with=FALSE] %>% unique %>%
        .[,      list(test.mean = mean(mean), 
                 test.std.dev   = sqrt( sum((sample_size - 1) * (standard_deviation ^ 2)) / (sum(sample_size) - length(standard_deviation)) ), #( 1 / length(standard_deviation)) * sqrt(sum(standard_deviation ^ 2)), #1/N*sqrt(SDa^2+SDb^2)
                 test.std.error = ( 1 / length(standard_error)) * sqrt(sum((standard_error ^ 2) / sample_size)) ), #1/N*sqrt(SEa^2/Na + SEb^2/Nb)
          by=c("xwalk_unique", "xwalk_sex_id")]
      
      if (method=="ratio") {
        
        # calculate beta coefficient and use ratio to calculate crosswalked test scores
        xwalk_final_test <- merge(x_ref, x_alt, by=collapse_vars_x) %>%
          .[, beta                     := ref.mean / test.mean] %>%
          .[, beta_standard_deviation  := sqrt((test.std.dev / test.mean) ^ 2 + (ref.std.dev / ref.mean) ^ 2) * beta] %>% 
          .[, -c("test.mean", "test.std.error", "test.std.dev", "ref.mean", "ref.std.error", "ref.std.dev"), with=FALSE] %>%
          merge(xwalk_cross, ., by=c("xwalk_unique", "sex_id"), all.x=TRUE) %>%
          .[, xwalk_mean               := mean * beta] %>%
          .[, xwalk_standard_deviation := sqrt( ((standard_deviation / mean) ^ 2) + ((beta_standard_deviation / beta) ^ 2) ) * xwalk_mean] %>%
          .[, xwalk_standard_error     := xwalk_standard_deviation / sqrt(sample_size)]
        missing_combos <- xwalk_final_test[is.na(beta) | is.na(xwalk_mean), ]
        
      } else if (method=="z_score") {
        
        # calculate z-score and to calculate crosswalked test scores
        xwalk_final_test <- merge(x_ref, x_alt, by=collapse_vars_x) %>% merge(xwalk_cross, ., by=c("xwalk_unique", "sex_id"), all.x=TRUE) %>%
          .[, z_score                    := (mean - test.mean) / test.std.dev] %>%
          .[, z_score_standard_deviation := sqrt( (standard_deviation ^ 2) + (test.std.dev ^ 2) ) / test.std.dev] %>%
          .[, xwalk_mean                 := (z_score * ref.std.dev) + ref.mean] %>%
          .[, xwalk_standard_deviation   := sqrt( ((z_score_standard_deviation * ref.std.dev) ^ 2) + (ref.std.dev ^ 2) )] %>%
          .[, xwalk_standard_error       := xwalk_standard_deviation / sqrt(sample_size)] %>%
          .[, -c("test.mean", "test.std.error", "test.std.dev", "ref.mean", "ref.std.error", "ref.std.dev"), with=FALSE]
        missing_combos <- xwalk_final_test[is.na(z_score) | is.na(xwalk_mean), ]
        
      } else if (method=="regression") {
        
        ### check how many overlapping locs there are to know whether to do ratio or linear regression
        xwalk_combined <- merge(x_ref, x_alt, by=c("xwalk_unique", "xwalk_sex_id"))
        ### use standard linear regression if at least 2 overlapping locations, otherwise force intercept to (0, 0)
        x_regress <- xwalk_cross[!is.na(ref_mean) & xwalk_include==1, ] %>%
          merge(., xwalk_combined, by=c("xwalk_unique", "xwalk_sex_id"), all.x=TRUE) %>%
          ### prep columns to fill
          .[, intercept                       := 0.00000] %>%
          .[, intercept_error                 := 0.00000] %>%
          .[, slope                           := 0.00000] %>%
          .[, slope_error                     := 0.00000] %>%
          ### run regression for at least one overlap
          .[num_overlap > 2, intercept       := summary(lm(ref_mean ~ mean))$coefficients[1], by=c("xwalk_unique", "xwalk_sex_id")] %>%
          .[num_overlap > 2, intercept_error := summary(lm(ref_mean ~ mean))$coefficients[3], by=c("xwalk_unique", "xwalk_sex_id")] %>%
          .[num_overlap > 2, slope           := summary(lm(ref_mean ~ mean))$coefficients[2], by=c("xwalk_unique", "xwalk_sex_id")] %>%
          .[num_overlap > 2, slope_error     := summary(lm(ref_mean ~ mean))$coefficients[4], by=c("xwalk_unique", "xwalk_sex_id")] %>%
          ### force intercept to 0 if only one overlapping country
          .[num_overlap <= 2, slope           := summary(lm(ref_mean ~ 0 + mean))$coefficients[1], by=c("xwalk_unique", "sex_id")] %>%
          #.[num_overlap < 2, slope_error     := summary(lm(ref_mean ~ 0 + mean))$coefficients[2], by=collapse_vars_x]
          .[num_overlap <= 2, slope_error     := sqrt((test.std.dev / test.mean) ^ 2 + (ref.std.dev / ref.mean) ^ 2) * slope]
        ### run crosswalk
        xwalk_final_test <- merge(xwalk_cross,
                                  x_regress[, c("xwalk_sex_id", "xwalk_unique", "intercept", "intercept_error", "slope", "slope_error", "num_overlap", "loc_overlap"), with=FALSE] %>% unique,
                                  by=c("xwalk_unique", "xwalk_sex_id"), all.x=TRUE) %>%
          .[, xwalk_mean                      := (slope * mean) + intercept] %>%
          .[, xwalk_standard_deviation        := sqrt( ((sqrt( ((standard_deviation / mean) ^ 2) + ((slope_error / slope) ^ 2) ) * xwalk_mean) ^ 2) + (intercept_error ^ 2) )] %>%
          .[, xwalk_standard_error            := xwalk_standard_deviation / sqrt(sample_size)]
        missing_combos <- xwalk_final_test[is.na(slope) | is.na(xwalk_mean), ]
        
      }
      
      # look for missingness in crosswalk
      if (length(missing_combos$x_subject) > 0) { print(paste0("warning: missing xwalk results for ", length(unique(missing_combos$xwalk_unique)), " combinations of ", test_ind, ": ", 
                                                               paste(unique(missing_combos$xwalk_unique), collapse=", "), ", sexes ", paste(unique(missing_combos$xwalk_sex_id), collapse=", "))) }
      
      ### save final crosswalk
      write.csv(xwalk_final_test, "PATH", row.names=FALSE)
      
      if (test_ind %in% c("pII", "IEA")) { reference <- copy(ref_save_for_later) }
      
    } else {
      print(paste0("no observations for test '", test_ind, "' in dataset"))
    }
  }
  #***********************************************************************************************************************
  
  
  #----END FUNCTION-------------------------------------------------------------------------------------------------------
}
#***********************************************************************************************************************