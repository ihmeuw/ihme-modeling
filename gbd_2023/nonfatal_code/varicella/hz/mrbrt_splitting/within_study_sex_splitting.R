### WITHIN STUDY SEX SPLIT FUNCTION
### THIS FUNCTION IS USED TO SEX SPLIT SYSTEMATIC REVIEW DATA WHERE SEX SPECIFIC DATA EXISTS FOR SOME ROWS AND NOT OTHERS

calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}


## AGE SEX SPLITTING
age_sex_split <- function(raw_dt){
  raw_dt[, crosswalk_parent_seq:= NA]
  cvs <- names(raw_dt)[grepl("^cv", names(raw_dt))]

  #find NIDs that have sex and age split data separately
  within_study_nids <- sapply(unique(raw_dt$nid), function(x){
    small <- raw_dt[nid==x,]
    if(("age" %in% small$specificity) & ("sex" %in% small$specificity)){
      return (x)
    } else {
      return(NA)
    }
  })
  within_study_nids <- within_study_nids[!is.na(within_study_nids)]
  
  within_study <- raw_dt[nid %in% within_study_nids,]
  within_study <- calculate_cases_fromse(within_study)
  within_study[, split := 0]
  message(paste("there are", length(unique(within_study$nid)), "NIDs for within-study sex splitting"))
  
  df_split <- copy(within_study)
  df_split <- df_split[specificity %in% c("age", "sex") & split == 0,] #NS 6_2023: redundant split == 0 all have 0 as assigned above? 

  ## CALC CASES (DEATHS) MALE/FEMALE AND CASES THAT ARE AGE SPLIT
  df_split[, cases_total:= sum(cases), by = c("nid", "group", "specificity", "measure", "location_id", cvs)]
  df_split[, prop_cases := cases / cases_total]

  ## CALC SAMPLE SIZE MALE/FEMALE AND PROP IN EACH AGE GROUP
  df_split[, ss_total:= sum(sample_size), by = c("nid", "group", "specificity", "measure", "location_id", cvs)]
  df_split[, prop_ss := sample_size / ss_total]

  ## CALC SE
  df_split[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  df_split[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]

  ## RATIO
  df_split[, ratio := prop_cases / prop_ss]
  df_split[, se_ratio:= sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) )]
  df_ratio <- df_split[specificity == "sex", c("nid", "group", "sex", "location_id", 
                                               "measure", "ratio", "se_ratio", "prop_cases", 
                                               "prop_ss", "year_start", "year_end", cvs), with = F]

  ## CREATE NEW OBSERVATIONS
  age.sex <- copy(df_split[specificity == "age"])
  message(paste("there are", nrow(age.sex), "age-specific rows of data being sex-split with within-study ratios"))
  age.sex[,specificity := "age,sex"]

  age.sex[is.na(crosswalk_parent_seq), crosswalk_parent_seq := as.integer(crosswalk_parent_seq)][,crosswalk_parent_seq := seq]

  age.sex$seq = NA_integer_
  age.sex[,c("ratio", "se_ratio", "split",  "cases_total",  "prop_cases",  "ss_total",	
             "prop_ss",	"se_cases",	"se_ss") := NULL]
  male <- copy(age.sex[, sex := "Male"])
  female <- copy(age.sex[, sex := "Female"])

  age.sex <- rbind(male, female)
  age.sex.original <- copy(age.sex)
  age.sex <- merge(age.sex, df_ratio, by = c("nid", "group", "sex", "measure", 
                                             "location_id", "year_start", "year_end", cvs))

  ## CALC MEANS
  age.sex[, standard_error := sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2)]
  age.sex[, mean := mean * ratio]
  age.sex[, cases := cases * prop_cases]
  age.sex[, sample_size := sample_size * prop_ss]
  age.sex[,note_modeler := paste("within study age,sex split using sex ratio", round(ratio, digits = 2))]
  age.sex[cases>sample_size, cases:=sample_size]
  age.sex[cases==sample_size,  mean:=1]
  age.sex[,c("ratio", "se_ratio", "prop_cases", "prop_ss") := NULL]
  age.sex[, c("lower", "upper", "uncertainty_type_value","effective_sample_size") := NULL]

  ## CREATE UNIQUE GROUP TO GET PARENTS
  age.sex.m <- age.sex[,c("nid","group", "measure", "location_id", "year_start", "year_end", cvs), with=F]
  age.sex.m <- unique(age.sex.m, by=c("nid","group", "measure", "location_id", "year_start", "year_end", cvs))

  ## GET PARENTS
  parent <- merge(age.sex.m, within_study, by= c("nid","group", "measure", "location_id", "year_start", "year_end", cvs))
  parent <- parent[specificity == "age" | specificity == "sex" | specificity == "total", ]
  parent[, group_review:=0]
  parent[, note_modeler := paste0("parent data, has been age-sex split")]

  ## FINAL DATA
  original <- within_study[!seq %in% parent$seq]
  total <- rbind(original, age.sex, fill = T) 

  #clean up columns so can combine back onto orig data
  total[, "split" := NULL]
  
  #remove parent rows 
  prep <- copy(raw_dt[!(nid %in% within_study_nids),])
  #prep
  data <- rbind(prep, total, fill = T)
  
  ## MESSAGE AND RETURN
  if (nrow(parent) == 0) print(paste0("nothing to age-sex split"))
  else print(paste0("split ", length(parent[, unique(nid)]), " nids"))
  return(data)
}
