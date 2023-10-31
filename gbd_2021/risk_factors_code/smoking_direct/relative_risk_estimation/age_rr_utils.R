#--------------------------------------------------------------------------------------
# Purpose: useful functions for age specific RR of smoking on CVDs
#--------------------------------------------------------------------------------------

# Setup ---------------------------
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_ids.R")
library(data.table)
library(ggplot2)
rei_table <- get_ids("rei")
cause_table <- get_ids("cause")

apply_age_pattern <- function(ro_pair, risk_curve_draws_df, age_pattern_draws_df, draws_in_log = T, return_draws_log = F, plot = T, plot_path){
  # Will apply the age pattern to an 'all-age' exposure-dependent risk curve
  #
  #  @causeid = integer cause_id for the RO pair of interest
  #  @reid = integer rei_id for the RO pair of interest
  #  @risk_curve_draws_df = dataframe of MR-BRt risk curve draws with columns 'exposure' and 'draw_0'...'draw_999'. 
  #                     Should not be age specific
  #  @age_pattern_draws_df = dataframe of age pattern attenuation factor draws. Either as returned from the 'weighted_average'
  #                     or 'simple_average' function, or read in directly from the draw csv
  #  @draws_in_log = boolean; T if risk curves draws are in log space, F if in normal space; default F
  #  @return_draws_log = boolean; T if you want to return and plot risk curve in log space, F if in normal space; default F
  
  if(sum(!c("exposure", paste0("draw_",0:999)) %in% colnames(risk_curve_draws_df))){
    stop("Risk curve draws should have 'exposure' and 'draw_1'...'draw_1000' as column names, apparently it does not")
  }
  if( sum(colnames(risk_curve_draws_df) %like% "age")){
    stop(paste0("Risk curve draws should not be age specific, remove column(s): ", paste0(
      colnames(risk_curve_draws_df)[colnames(risk_curve_draws_df) %like% "age"], collapse = ","
    )))
  }
  
  
  risk_byvars <- colnames(risk_curve_draws_df)[! colnames(risk_curve_draws_df) %like% "draw"]
  
  # reshape both df long
  risk_curve <- melt(risk_curve_draws_df, id.vars = risk_byvars, value.name = "rr")
  age_pct <- melt(age_pattern_draws_df, id.vars = "age_group_id", measure.vars = paste0("draw_", 0:999), value.name = "pct_change")
  
  # merge together and apply attenuation factor (the attenuation factor is on the RR scale)
  risk_curve <- merge(risk_curve, age_pct, by = "variable", allow.cartesian = T)
  if(draws_in_log & return_draws_log){
    risk_curve[, age_specific_rr := rr*pct_change]
  }else if(draws_in_log){
    risk_curve[, age_specific_rr := exp(rr*pct_change)]
    # risk_curve[exposure==0, age_specific_rr :=1]
  }else if(return_draws_log){
    risk_curve[, age_specific_rr := log(rr*pct_change)]
  }else{
    
  }
  
  risk_curve[, `:=` (rr = NULL, pct_change = NULL)]
  
  risk_curve<- as.data.table(dcast(risk_curve, ... ~ variable, value.var = "age_specific_rr"))
  
  if(plot){
    ylab <- ifelse(return_draws_log, "log(RR)", "RR")
    path <- plot_path
    visualize_age_specific_curve(risk_curve, ylab, plot_name = paste0("smoking-", ro_pair), plot_path=path)
  }
  
  return(risk_curve)
}

visualize_age_specific_curve <- function(risk_curve, ylab, plot_name, plot_path){
  # For use in plotting age specific curves
  #
  #  @risk_curve = dataframe of age specific curves. Must have 'draw_0'...'draw_999' and 'age_group_id'
  #  @ylab = character to be used as y-axis label in plot. ex: RR or log(RR)
  #  @plot_name = character to be used as plot title in plot. 
  
  ages <- get_age_metadata(19)
  risk_curve <- merge(risk_curve, ages, by = "age_group_id")
  risk_curve[, rrmean := apply(.SD, 1, mean), .SDcols = paste0("draw_", 0:999)]
  risk_curve[, rrupper := apply(.SD, 1, quantile, 0.975), .SDcols = paste0("draw_", 0:999)]
  risk_curve[, rrlower := apply(.SD, 1, quantile, 0.025), .SDcols = paste0("draw_", 0:999)]
  
  pdf(plot_path)
  plot <- ggplot(risk_curve, aes(x = exposure, y = rrmean, ymin=rrlower, ymax = rrupper))+
    geom_line()+geom_ribbon(alpha = 0.2)+theme_bw()+ geom_abline(slope = 0, intercept = 1, color = "blue", alpha = 1/5)+
    facet_wrap(~age_group_name)+labs( y= ylab, title = plot_name)
  print(plot) 
  dev.off()
  print(plot)
}

function(dt, exposure_col = "b_0"){
  # Function to replace NA draws AND drop draws that are greater than 6 MADs
  # df - dataframe of wide draws with identifying columns cause_id, b_0 (or other exposure column name specified in exposure_col argument), 
  #      draws are named "draw_1"..."draw_1000"
  #      each draw value in ln(RR)
  # exposure_col - name of column that identifies the name of the alternative exposure column name
  # returns wide dataframe by draw in log space
  
  temp <- copy(dt)
  # Replacing NA columns with adjacent draw
  df <- temp %>% # This dataframe will only have columns that don't have NAs
    dplyr::select_if(~ !all(is.na(.)))
  
  missing_draws <- setdiff(names(temp), names(df)) # gets names of missing draws
  if(length(missing_draws) >0 ){
    print(paste0("There are ", length(missing_draws), " missing draws in the dataframe."))
  }
  
  
  if(length(missing_draws) != 0){
    for(draw in missing_draws){
      df[, paste0(draw) := get(paste0('draw_', as.integer(gsub('draw_', '', draw)) + 1))]
    }
  }
  
  all_draws <- grep("draw", names(df), value = T)
  id_vars <- setdiff(names(df), all_draws)
  df_long <- melt(df, id.vars = id_vars)
  df_long[, median := lapply(.SD, function(x) median(x, na.rm = T)), .SDcols = "value", by = exposure_col]
  df_long[, mad := lapply(.SD, function(x) mad(x, na.rm = T)), .SDcols = "value", by = exposure_col]
  df_long[, drop := ifelse(abs(value - median)/mad > 6, 1, 0)]
  
  drop_draws <- as.character(unique(df_long[drop == 1]$variable))
  if(length(drop_draws) >0 ){
    print(paste0("Dropping ", length(drop_draws), " draws that are greater than 6 MADs."))
  }
  if(length(drop_draws) != 0){
    df[, c(drop_draws) := NULL]
    
    draw_options <- grep("draw", names(df), value = T)
    new_draws <- sample(draw_options, length(drop_draws), replace = T)
    
    add_draws <- df[, new_draws, with = F]
    colnames(add_draws) <- drop_draws
    
    out <- cbind(df, add_draws)
    
    return(out)
  } else {
    return(df)
  }
}
