##################################################
## Project: CVPDs
## Script purpose: Calculate estimated under-reporting ratio
## Date: June 2022
## Author: USERNAME
##################################################

# ARGUMENTS -------------------------------------  
calculate_urr <- function(true_cases, reported_cases, cause, drawwise){
  # true cases = draws of GBD cases
  # reported cases = draws of estimated number of reported cases we would have expected (from RegMod)
  # cause = format of the draw objects depends on cause so specify if this is measles or flu
  # drawwise = do you want 1000 draws of the URR or urrjust a mean URR value?
  
  # convert flu to all-age, both-sex counts and from most-granular to national level so matched RegMod
  if(cause == "flu" | cause == "pertussis"){
    true_cases <-  true_cases[, flu_count := sum(flu_count), by = c("location_id", "year_id", "draw")] %>% unique(., by = c("location_id", "year_id", "draw"))
    id_cols <- c("year_id", "draw", "sex_id", "age_group_id", "measure_id", "metric_id") # no locs because locs are aggregated to country
    agg_cols <- c("flu_count", "rsv_count")
    true_cases <- aggregate_to_national(true_cases, id_cols = id_cols, agg_cols = agg_cols)
  }
  
  if(drawwise){
    if(cause == "measles"){
      # already all-age and both sex but need to cast long to facilitate urr calculation
      true_cases_long <- melt(true_cases, id.vars = "location_id", value.name = "gbd_prevyrs", variable.name = "draw")
      true_cases_long$draw <- as.numeric(substr(true_cases_long$draw, 14, nchar(as.character(true_cases_long$draw))))
      
    } else if (cause == "flu" | cause == "pertussis"){
      # already long, fix names
      setnames(true_cases, "flu_count", "gbd_prevyrs")
      true_cases_long <- copy(true_cases)
    }
    
    # cast RegMod estimates long, merge to prep for URR calc
    reported_cases_long <- melt(reported_cases, id.vars = "location_id", value.name = "regmod_prevyrs", variable.name = "draw")
    reported_cases_long$draw <- as.numeric(substr(reported_cases_long$draw, 14, nchar(as.character(reported_cases_long$draw))))
    
    urr <- merge(reported_cases_long, true_cases_long, by = c("location_id", "draw"))
    
  } else {
    # calculate using median of RegMod cases and mean of GBD estimates
    
    if(cause == "measles"){
      # collapse draws to means because don't have extreme RegMod issue in GBD
      true_cases <- collapse_point(true_cases, draws_name = "prevyrs_draw")
      setnames(true_cases, "mean", "gbd_prevyrs")
      
    } else if (cause == "flu" | cause == "pertussis"){
      # cast wide and collapse to mean true cases by location
      true_cases_wide <- dcast(true_cases, location_id ~ draw, 
                               value.var = "flu_count")
      setnames(true_cases_wide, paste0(0:999), paste0("prevyrs_draw_", 0:999), skip_absent = T)
      true_cases <- collapse_point(true_cases_wide, draws_name = "prevyrs_draw")
      setnames(true_cases, "mean", "gbd_prevyrs")
      
    }
    # collapse to median RegMod estimates of reported cases by location to deal with large draws
    reported_cases_long <- melt(reported_cases, id.vars = "location_id", measure.vars = paste0("prevyrs_draw_", 0:999),
                           variable.name = "draw", value.name = "regmod_prevyrs")
    reported_cases_long <- reported_cases_long[, regmod_prevyrs := median(regmod_prevyrs), by = c("location_id")] %>% unique(., by = c("location_id"))

    # merge reported and true means to prep for URR calc
    urr <- merge(true_cases, reported_cases_long, by = "location_id")
    
  }
  
  # calculate URR
  urr[, urr:= regmod_prevyrs/gbd_prevyrs]
  
  # drop unneeded cols
  if (drawwise) urr <- urr[,.(location_id, draw, urr, regmod_prevyrs, gbd_prevyrs)] else urr <- urr[,.(location_id, urr, regmod_prevyrs, gbd_prevyrs)]
  
  #fix infinites and NAs that arise when the gbd cases OR the gbd cases and the regmod cases are 0
  if(nrow(urr[is.na(urr) & gbd_prevyrs != 0])  == 0 & nrow(urr[is.infinite(urr) & gbd_prevyrs != 0 & regmod_prevyrs != 0]) == 0){
    urr[is.na(urr) | is.infinite(urr), urr := 0]
  } else {
    message("STOP! You have rows with NAs or INFs for the URR that don't also have 0 in the denominator OR 0 in the numerator and denominator.
           Figure out why this is happening before proceeding")
  }
  
  
}
