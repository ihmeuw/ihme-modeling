## AGGREGATE TO NATIONAL MARKETSCAN FOR CROSSWALKING
aggregate_marketscan <- function(mark_dt){
  dt <- copy(mark_dt)
  marketscan_dt <- copy(dt[cv_marketscan == 1])
  by_vars <- c("age_start", "age_end", "year_start", "year_end", "sex")
  marketscan_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by = by_vars]
  marketscan_dt[, `:=` (location_id = 102, location_name = "United States", mean = cases/sample_size,
                        lower = NA, upper = NA)]
  z <- qnorm(0.975)
  marketscan_dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  marketscan_dt <- unique(marketscan_dt, by = by_vars)
  full_dt <- rbind(dt, marketscan_dt)
  return(full_dt)
}

## FOR DECOMP 2 ADD AND REPLACE DATA FOR CROSSWALKING
add_replace_data <- function(raw_dt){
  dt <- copy(raw_dt)
  irt_dt <- fread(paste0("FILEPATH"))
  irt_dt[age_end == 120, age_end := 99]
  new_dt <- as.data.table(read.xlsx(paste0("FILEPATH")))
  dt <- dt[!nid %in% c("ID", "ID")]
  all_dt <- rbindlist(list(dt, irt_dt, new_dt), fill = T, use.names = T)
  return(all_dt)
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

## CALCULATE CASES FROM STANDARD ERROR
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

## AGE SEX SPLITTING
age_sex_split <- function(raw_dt){
  df_split <- copy(raw_dt)
  cvs <- names(df_split)[grepl("^cv", names(df_split))]
  df_split[, split := length(specificity[specificity == "age,sex"]), by = c("nid", "group", "location_id", "specificity", "measure", "year_start", "year_end", cvs)]
  df_split <- df_split[specificity %in% c("age", "sex") & split == 0,]
  
  ## CALC CASES MALE/FEMALE
  df_split[, cases_total:= sum(cases), by = c("nid", "group", "specificity", "measure", "location_id", cvs)]
  df_split[, prop_cases := cases / cases_total]
  
  ## CALC PROP MALE/FEMALE
  df_split[, ss_total:= sum(sample_size), by = c("nid", "group", "specificity", "measure", "location_id", cvs)]
  df_split[, prop_ss := sample_size / ss_total]
  
  ## CALC SE
  df_split[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  df_split[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  ## RATIO 
  df_split[, ratio := prop_cases / prop_ss]
  df_split[, se_ratio:= sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) )]
  df_ratio <- df_split[specificity == "sex", c("nid", "group", "sex", "location_id", "measure", "ratio", "se_ratio", "prop_cases", "prop_ss", "year_start", "year_end", cvs), with = F]
  
  ## CREATE NEW OBSERVATIONS
  age.sex <- copy(df_split[specificity == "age"])
  age.sex[,specificity := "age,sex"]
  age.sex[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
  age.sex[,seq := ""]
  age.sex[,c("ratio", "se_ratio", "split",  "cases_total",  "prop_cases",  "ss_total",	"prop_ss",	"se_cases",	"se_ss") := NULL]
  male <- copy(age.sex[, sex := "Male"])
  female <- copy(age.sex[, sex := "Female"])
  
  age.sex <- rbind(male, female)   
  age.sex <- merge(age.sex, df_ratio, by = c("nid", "group", "sex", "measure", "location_id", "year_start", "year_end", cvs))
  
  ## CALC MEANS
  age.sex[, standard_error := sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2)]
  age.sex[, mean := mean * ratio]
  age.sex[, cases := cases * prop_cases]
  age.sex[, sample_size := sample_size * prop_ss]
  age.sex[,note_modeler := paste(note_modeler, " | age,sex split using sex ratio", round(ratio, digits = 2))]
  age.sex[mean > 1, `:=` (group_review = 0, exclude_xwalk = 1, note_modeler = paste0(note_modeler, " | group reviewed out because age-sex split over 1"))]
  age.sex[,c("ratio", "se_ratio", "prop_cases", "prop_ss") := NULL]
  age.sex[, c("lower", "upper", "uncertainty_type_value","effective_sample_size") := ""]
  
  ## CREATE UNIQUE GROUP TO GET PARENTS
  age.sex.m <- age.sex[,c("nid","group", "measure", "location_id", "year_start", "year_end", cvs), with=F]    
  age.sex.m <- unique(age.sex.m, by=c("nid","group", "measure", "location_id", "year_start", "year_end", cvs))
  
  ## GET PARENTS
  parent <- merge(age.sex.m, raw_dt, by= c("nid","group", "measure", "location_id", "year_start", "year_end", cvs))
  parent[specificity == "age" | specificity == "sex", group_review:=0]
  parent[, note_modeler := paste0(note_modeler, " | parent data, has been age-sex split")]
  
  ## FINAL DATA
  original <- raw_dt[!seq %in% parent$seq]
  total <- rbind(original, age.sex, fill = T) ## DON'T RETURN PARENT ROWS
  
  ## MESSAGE AND RETURN
  if (nrow(parent) == 0) print(paste0("nothing to age-sex split"))
  else print(paste0("split ", length(parent[, unique(nid)]), " nids"))
  return(total)
}

## GET SUMMARIES OF DRAWS
summaries <- function(dt, draw_vars){
  sum <- copy(dt)
  sum[, mean := rowMeans(.SD), .SDcols = draw_vars]
  sum[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draw_vars]
  sum[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draw_vars]
  sum[, c(draw_vars) := NULL]
  return(sum)
}

## COL ORDER FUNCTION
col_order <- function(dt){
  epi_order <- fread("FILEPATH", head = F)
  epi_order <- tolower(as.character(epi_order[!V1 == "", V1]))
  names_dt <- tolower(names(dt))
  setnames(dt, names(dt), names_dt)
  for (name in epi_order){
    if (name %in% names(dt) == F){
      dt[, c(name) := ""]
    }
  }
  extra_cols <- setdiff(names(dt), tolower(epi_order))
  new_epiorder <- c(epi_order, extra_cols)
  setcolorder(dt, new_epiorder)
  return(dt)
}
