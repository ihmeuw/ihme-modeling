#' @author
#' @date 2020/06/09
#' @description Sex-split within study group_review data. 
#' Many studies have data that is age-specific, and sex-specific, but not age-sex-specific.
#' This function takes those studies and applies the sex ratio (from within a given study)
#' to both-sex, age-specific data (from the same study).
#' Then you end up with age-sex-speicfic data from a given study. 
#' @function sex_split_group_review
#' @param dt bundle data that needs within-study sex splitting
#' @param 
#' @return data data that has had within study sex splitting applied
#' @return predict_sex$graph : Plot of both-sex mean against sex-split means saved in mrbrt_dir
#' @note This needs to be run before MR-BRT sex-splitting.  MR-BRT sex splitting still needs to 
#' be run after this, because it only sex splits data for which a sex ratio was given in the study.
#' 
#' 


sex_split_group_review <- function(data, out_dir, bv_id, plot = F) {
  #find NIDs that have sex and age split data separately
  within_study_nids <- sapply(unique(data$nid), function(x){
    small <- data[nid==x,]
    if(("age" %in% small$specificity) & ("sex" %in% small$specificity)){
      return (x)
    } else {
      return(NA)
    }
  })
  within_study_nids <- within_study_nids[!is.na(within_study_nids)]
  
  within_study <- data[nid %in% within_study_nids,]
  within_study[, split := 0]
  message(paste("there are", length(unique(within_study$nid)), "NIDs for within-study sex splitting"))
  
  #split the age specific data using that study's sex weights
  df_split <- copy(within_study)
  
  cvs <- names(df_split)[grepl("^cv", names(df_split))]
  
  # pull only age and sex rows (not total or diag_test)
  df_split <- df_split[specificity %in% c("age", "sex") & split == 0,]
  
  ## CALC CASES MALE/FEMALE
  df_split[, cases_total:= sum(cases), by = c("nid", "group", "specificity", "measure", "location_id", cvs)]
  df_split[, prop_cases := cases / cases_total]
  
  ## DEAL WITH WHAT HAPPENS IF PROP_CASES IS ZERO 
  
  ## CALC SS MALE/FEMALE
  df_split[, ss_total:= sum(sample_size), by = c("nid", "group", "specificity", "measure", "location_id", cvs)]
  df_split[, prop_ss := sample_size / ss_total]
  
  ## CALC SE
  df_split[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  df_split[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  ## RATIO
  df_split[, ratio := prop_cases / prop_ss]
  df_split[, se_ratio:= sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) )]
  ## DEAL WITH UNDEFINED RATIO WHEN PROP_CASES == 0 
  df_split[which(is.nan(se_ratio)), se_ratio := 0]
  df_ratio <- df_split[specificity == "sex", c("nid", "group", "sex", "location_id", "measure", "ratio", "se_ratio", "prop_cases", "prop_ss", "year_start", "year_end"), with = F]
  
  ## CREATE NEW OBSERVATIONS
  age.sex <- copy(df_split[specificity == "age"])
  message(paste("there are", nrow(age.sex), "age-specific rows of data being sex-split with within-study ratios"))
  age.sex[,specificity := "age,sex"]
  
  setnames(age.sex, "seq", "crosswalk_parent_seq")
  age.sex[,seq := NA]
  age.sex[,c("ratio", "se_ratio", "split",  "cases_total",  "prop_cases",  "ss_total",	"prop_ss",	"se_cases",	"se_ss") := NULL]
  male <- copy(age.sex[, sex := "Male"])
  female <- copy(age.sex[, sex := "Female"])
  
  age.sex <- rbind(male, female)
  age.sex.original <- copy(age.sex)
  # merge ratios by location-sex 
  # do not include year: for some studies, both-sex data and single-sex data come from slightly different year ranges -> including year causes drop
  age.sex <- merge(age.sex, df_ratio, by = c("nid", "group", "sex", "measure", "location_id"))
  # only keep year from original dataset - NOT ratio dataset
  age.sex[, c("year_start.y", "year_end.y") := NULL]
  setnames(age.sex, c("year_start.x", "year_end.x"), c("year_start", "year_end"))
  
  ## CALC MEANS
  ## PROBLEM - standard error here is zero for those with ratio and se_ratio of zero
  age.sex[, standard_error := sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2)]
  age.sex[, mean := mean * ratio]
  age.sex[, cases := cases * prop_cases]
  age.sex[, sample_size := sample_size * prop_ss]
  age.sex[, note_modeler := paste(note_modeler, "WITHIN-STUDY sex split using sex ratio", round(ratio, digits = 2))]
  if("cv_confirmed" %in% names(age.sex)) age.sex[cv_confirmed == 1 | is.na(cv_confirmed), group_review := 1]
  age.sex[,c("ratio", "se_ratio", "prop_cases", "prop_ss") := NULL]
  age.sex[, c("lower", "upper", "uncertainty_type_value","effective_sample_size") := NA]
  
  ## CREATE UNIQUE GROUP TO GET PARENTS
  age.sex.m <- age.sex[,c("nid","group", "measure", "location_id"), with=F]
  age.sex.m <- unique(age.sex.m, by=c("nid","group", "measure", "location_id"))
  
  ## GET PARENTS
  parent <- merge(age.sex.m, within_study, by= c("nid","group", "measure", "location_id"))
  # parent is only the rows with specificity = "age" or "sex" - not "total" "year" or "diag_test"
  parent <- parent[specificity == "age" | specificity == "sex"]
  # mark all parent data as group_review = 0
  parent[, group_review := 0]
  parent[specificity == "age" | specificity == "sex", note_modeler := paste0("parent data, has had within-study sex-splitting applied")]
  
  ## FINAL DATA
  original <- within_study[!seq %in% parent$seq]
  total <- rbind(original, age.sex, fill = T) ## DON'T RETURN PARENTS - only rows w/ specificity diag_test, etc
  
  #clean up columns so can combine back onto orig data
  total[, "split" := NULL]
  
  #in main dt replace the unsplit rows with the split rows
  #remove parent rows (ie only age or sex specific) from nids that were within study split & replace with the within study split rows (age and sex specific)
  prep <- copy(data[!(nid %in% within_study_nids),])
  prep
  data <- rbind(prep, total, fill = T)

  age.sex.original[,original_mean := cases/sample_size] 
  age.sex.original<- age.sex.original[, c("nid","group", "measure", "location_id", "sex", "age_start", "age_end", "original_mean")]
  compare <- merge(age.sex, age.sex.original, by = c("nid","group", "measure", "location_id", "sex", "age_start", "age_end"))
  if (plot){
    pdf(file = paste0(out_dir, "_within_study_sex_split_on_bv_id_", bv_id, ".pdf"), height = 5, width = 12)
    p <- ggplot(compare, aes(x = original_mean, y = mean, color = sex)) + 
      geom_point() + 
      ggtitle(paste0("Within-Study Sex Splitting of ", name_short, ": Original vs Sex-Split Proportion"))
    print(p)
    2
    dev.off()
    
    fwrite(compare,paste0(out_dir, "_within_study_sex_split_on_bv_id_", bv_id, ".csv"))
    message("within-study group review sex split plot output to", out_dir)
  }

  return(data)
}
