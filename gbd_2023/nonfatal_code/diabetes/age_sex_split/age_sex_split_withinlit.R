################################################################################
#Purpose: Within-literature Age-Sex Split function
#Adaptations include:
#    - to subset the data needing within age sex split, using the splitting column
#      that is unique to diabetes bundles
################################################################################

age_sex_split <- function(raw_dt){
  df_split <- copy(raw_dt)
  
  ## SUBSET TO DATA THAT NEEDS SPLITTING
  #cvs <- names(df_split)[grepl("^cv", names(df_split))]
  #df_split[, split := length(specificity[specificity == "age,sex"]), by = c("nid", "group", "location_id", "specificity", "measure", "year_start", "year_end", "standardized_case_definition", "diab_case_def_crosswalk")]
  df_split <- df_split[splitting == 3]

   ## CALC TOTAL CASES AND PROPORTION OF CASES FOR MALE/FEMALE
  df_split[, cases_total:= sum(cases), by = c("nid", "specificity", "measure", "location_id","splitting", "year_start", "year_end", "standardized_case_definition", "diab_case_def_crosswalk")]
  df_split[, prop_cases := cases / cases_total]
  
  ## CALC TOTAL SAMPLE SIZE AND PROPORTION SAMPLE SIZE FOR MALE/FEMALE
  df_split[, ss_total:= sum(sample_size), by = c("nid", "specificity", "measure", "location_id", "splitting","year_start", "year_end","standardized_case_definition", "diab_case_def_crosswalk")]
  df_split[, prop_ss := sample_size / ss_total]
  
  ## CALC SE OF PROPORTION CASES, AND PROPORTION SAMPLE SIZE
  df_split[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  df_split[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  ## RATIO FOR PROPORTION CASES TO PROPORTION SAMPLE SIZE AND RATIO FOR STANDARD ERROR
  df_split[, ratio := prop_cases / prop_ss]
  df_split[, se_ratio:= sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) )]
  
  ##SAVE RATIOS IN NEW DF
  df_ratio <- df_split[specificity == "sex", c("nid", "group", "sex", "location_id", "measure", "ratio", "se_ratio", "prop_cases", "prop_ss", "year_start", "year_end", "splitting", "standardized_case_definition", "diab_case_def_crosswalk"), with =F]    
  
  ## CREATE NEW OBSERVATIONS EXDPANDING AGE SPECIFIC ROW BY SEX
  age.sex <- copy(df_split[specificity == "age"]) #subset the by age rows only, to use as a base to create new observations
  age.sex[,specificity := "age,sex"] #change the to age,sex so we know its split !? I don't know if we need this tbh
  age.sex[,crosswalk_parent_seq := seq]
  age.sex[, seq := ""]
  age.sex[,c("ratio", "se_ratio",  "cases_total",  "prop_cases",  "ss_total",	"prop_ss",	"se_cases",	"se_ss") := NULL] #remove these newly created. columns that don't belong in the bundle
  male <- copy(age.sex[, sex := "Male"]) #essentially duplicating the data set one set for male, another for female
  female <- copy(age.sex[, sex := "Female"])
  
  age.sex <- rbind(male, female)   
  
  #MERGE RATIOS ONTO NEW DF WITH EXPANDED ROWS
  age.sex <- merge(age.sex, df_ratio, by = c("nid", "group", "sex", "measure", "location_id", "year_start", "year_end", "splitting", "standardized_case_definition", "diab_case_def_crosswalk")) 
  
  ## CALC NEW MEANS
  age.sex[, standard_error := sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2)]
  age.sex[, mean := mean * ratio]
  age.sex[, cases := cases * prop_cases]
  age.sex[, sample_size := sample_size * prop_ss]
  age.sex[,note_modeler := paste(note_modeler, " | age,sex split using sex ratio", round(ratio, digits = 2))]
  print(paste0("any rows with mean>1 after processing? ", any(age.sex$mean>1)))
  age.sex[mean > 1, `:=` (group_review = 0, note_modeler = paste0(note_modeler, " | group reviewed out because age-sex split over 1"))]
  age.sex[,c("ratio", "se_ratio", "prop_cases", "prop_ss") := NULL]
  age.sex[, c("lower", "upper", "uncertainty_type_value","effective_sample_size") := ""] #not sure when this is calculated again
  
  ## CREATE UNIQUE GROUP TO GET PARENTS
  age.sex.m <- age.sex[,c("nid","group", "measure", "location_id", "year_start", "year_end", "splitting", "standardized_case_definition", "diab_case_def_crosswalk"), with=F]    
  age.sex.m <- unique(age.sex.m, by=c("nid","group", "measure", "location_id", "year_start", "year_end", "splitting", "standardized_case_definition", "diab_case_def_crosswalk"))
  
  ## GET ORIGINAL PARENT ROWS
  parent <- merge(age.sex.m, raw_dt, by= c("nid","group", "measure", "location_id", "year_start", "year_end", "splitting", "standardized_case_definition", "diab_case_def_crosswalk"))
  parent[specificity == "age" | specificity == "sex", group_review:=0]
  parent[, note_modeler := paste0(note_modeler, " | parent data, has been age-sex split")]
  
  ## FINAL DATA
  original <- raw_dt[!seq %in% parent$seq]
  total <- rbind(original, age.sex, fill = T) ## DON'T RETURN PARENTS
  
  ## MESSAGE AND RETURN
  if (nrow(parent) == 0) print(paste0("nothing to age-sex split"))
  else print(paste0("split ", length(parent[, unique(nid)]), " nids"))
  return(total)
}

