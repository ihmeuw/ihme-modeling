######################################################
### Title: Redistribution Functions

#' Function to call in burden estimates
#'
#' @param cause_id a numeric id corresponding to the cause for which alcohol-attributable burden needs to be pulled
#' @param annual a boolean; do we need annual estimates or only estimates for a select number of years? 
#' @param measure_id a numeric; 3 to pull ylds, 4 to pull ylls
#'
#' @return a dataframe of 1000 draws of the alcohol attributable burden for the specified cause
#' @export
#'
#' @examples read_burden(cause_id = 420, annual = T, measure_id = 3)
read_burden <- function(cause_id, annual, measure_id, years){
  print(cause_id)
  
  if(annual){
    
    if (measure_id == 3){
      burden  <- interpolate(gbd_id_type='cause_id', gbd_id=cause_id, source='como', measure_id = measure_id, location_id= location, 
                             sex_id = sexes, age_group_id = ages, gbd_round_id=como_gbd_round, decomp_step = como_decomp, 
                             version = como_version, reporting_year_start=1990, reporting_year_end=como_year_end)
    } else if (measure_id == 4){
      burden  <- interpolate(gbd_id_type='cause_id', gbd_id=cause_id, source='codcorrect', measure_id = measure_id, location_id= location, 
                             sex_id = sexes, age_group_id = ages, gbd_round_id=cod_gbd_round, decomp_step = cod_correct_decomp, 
                             version = cod_correct_version, reporting_year_start=1990, reporting_year_end=cod_year_end)
    }
  } else {
    if (measure_id == 3){
      burden <- get_draws(gbd_id_type='cause_id', gbd_id=cause_id, source="como", location_id=location, 
                          sex_id=sexes, age_group_id=ages, year_id=years, measure_id=measure_id, metric_id=3, 
                          gbd_round_id=como_gbd_round, decomp_step = como_decomp, version_id = como_version) 
    } else if (measure_id == 4){
      burden <- get_draws(gbd_id_type='cause_id', gbd_id=cause_id, source="codcorrect", location_id=location, 
                          sex_id=sexes, age_group_id=ages, year_id=years, measure_id=measure_id, metric_id=1, 
                          gbd_round_id=cod_gbd_round, decomp_step = cod_correct_decomp, version_id = cod_correct_version) 
    }
  }
  
  return(burden)
  
}

# Function to call in and aggregate paf files
#' Title
#'
#' @param cores 
#' @param paf_direct 
#' @param locate 
#' @param yrs 
#' @param cause 
#'
#' @return
#' @export
#'
#' @examples
aggregateFiles <- function(file){
  
  df <- fread('FILEPATH')
  name <- strsplit(gsub(".csv", "", file), "_")[[1]]
  df$measure <- name[2]
  df$sex_id <- name[5]
  
  return(df)
  
}

# New function for quickly standardizing the population with ylds
#' Title
#'
#' @param your_ylds 
#' @param pops 
#'
#' @return
#' @export
#'
#' @examples
pop_standardize <- function(your_ylds, pops){
  pops$run_id <- NULL
  your_ylds <- melt(your_ylds, id.vars = c("age_group_id", "cause_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id"))
  your_ylds <- merge(your_ylds, pops, by = c("age_group_id", "location_id", "sex_id", "year_id"), all = T)
  
  print("Merging YLDs with population")
  your_ylds <- your_ylds[, value := value*population]
  
  print("Computing standardized population values")
  your_ylds$population <- NULL
  your_ylds$metric_id <- 1
  
  print("YLDs are now population standardized!")
  return(your_ylds)
}


#' Redistribute PAFs to child etiologies
#'
#' @param dt datatable that needs to have the redistribution applied; must be in long format (rather than wide by draws)
#'
#' @return a datatable with the final PAFs 
#' @export
#'
#' @examples redistribute_pafs(paf_new)
redistribute_pafs <- function(df){
  
  # calculate proportion of total cirrhosis ylls that are attributable to each cause
  df[, total := sum(value), by = c("age_group_id", "location_id", "sex_id", "year_id", "parent", "measure", "draw")]
  df[, attrib_percent := value/total]
  
  # calculate proportion of total cirrhosis ylls that are attributable to each cause
  df[etiology %in% c("alcohol"), alcohol_percent := attrib_percent]
  df[, alcohol_percent := max(alcohol_percent, na.rm = T), by = c("age_group_id", "location_id", "sex_id", "year_id", "parent", "measure", "draw")]
  
  df[alcohol_percent <= paf, paf_without_alc := paf - alcohol_percent]
  df[alcohol_percent > paf, paf_without_alc := 0]
  
  
  # account for negative pafs; they are logically implausible so we will need to discuss with nonfatal teams down the line to get in alignment w/ envelope sizes
  negatives <- df[alcohol_percent > paf]
  
  if (nrow(negatives) > 0){
    negatives <- negatives[,c("age_group_id", "year_id", "draw", "location_id", "sex_id")] %>% 
      dplyr::group_by(age_group_id, year_id, location_id, sex_id) %>% 
      dplyr::summarise(draws = n())
    
    dir.create('FILEPATH')
    write.csv(negatives, 'FILEPATH', row.names = F)
  }
  
  
  # redistribute
  df[! etiology %in% c("alcohol", "nash"), redist_denom := sum(attrib_percent), by = c("age_group_id", "location_id", "sex_id", "year_id", "parent", "measure", "draw")]
  
  df[! etiology %in% c("alcohol", "nash") & redist_denom != 0, new_paf := paf_without_alc/redist_denom] #(paf_without alcohol * attrib_percent/redist_donom)/attrib_percent
  df[! etiology %in% c("alcohol", "nash") & redist_denom == 0, new_paf := 0] 
  df[etiology %in% c("alcohol"), new_paf := 1]
  df[etiology %in% c("nash"), new_paf := 0]
  
  df[new_paf > 1, new_paf := 1]
  
  # clean up
  df[,c("total", "attrib_percent", "alcohol_percent", "paf_without_alc", "redist_denom","paf", "location_id", "measure_id", "metric_id", "parent", "etiology", "value")] <- NULL
  
  return(df)
}
