#--------------------------------------------------------------------------------------
# Functions for using metabolic mediator age trends to generate
#     age specific risk curves. 
#--------------------------------------------------------------------------------------

# Setup ---------------------------
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_ids.R")
library(data.table)
library(ggplot2)
rei_table <- get_ids("rei")
cause_table <- get_ids("cause")

# This is the folder of the current 'best' mediator age trends
mediator_pct_draw_dir =  "FILEPATH"


# Load functions -------------------
weighted_average <- function(causeid, reid, plot = T){
  
  # If the GBD mediation matrix has mediators for the risk-cause pair, will take a weighted average of the age patterns (represented as draws of the attenuation factor)
  #  @causeid = integer cause_id for the RO pair of interest
  #  @reid = integer rei_id for the RO pair of interest

  rei_name <- rei_table[rei_id == reid, rei]
  cause_name <- cause_table[cause_id == causeid, acause]
  
  # Read in the mediation matrix from GBD 2019 
  mediation <- fread("FILEPATH/mediation_matrix_draw_gbd_2019.csv")
  mediation <- mediation[, c("rei_id", "cause_id", "med_id", "mean_mediation"), with = F]
  
  mediators <- mediation[cause_id == causeid  & rei_id == reid]
  
  if(nrow(mediators) > 0 ){
    print(paste0(rei_name, " and ", cause_name, " is mediated though ", paste0(rei_table[rei_id %in% mediators[, med_id], rei], collapse = ",")))
    
  }else{
    stop(paste0(rei_name, " and ", cause_name, " has no mediators"))
  }
  
  # Read in the draws for each mediator
  files <- paste0(mediator_pct_draw_dir, mediators$med_id, "_", causeid, ".csv")
  combined_draws <- rbindlist(lapply(files, fread))
  
  # Reshape long
  combined_draws_long <- melt(combined_draws, measure.vars = paste0("draw_",0:999))
  combined_draws_long <- merge(combined_draws_long, mediators[,.(med_id, mean_mediation)], by.x = "rei_id", by.y = "med_id")
  
  # Take weighted average
  combined_draws_long[, weight := mean_mediation/sum(mean_mediation), by = c("cause_id", "age_group_id", "variable")]
  combined_draws_long[, avg_value := sum(value*weight), by = c("cause_id", "age_group_id", "variable")]
  
  # Clean, reshape wide and return
  combined_draws_long[,`:=` (rei_id = NULL, value = NULL, mean_mediation = NULL, weight = NULL)]
  combined_draws_long <- unique(combined_draws_long)
  average_draws <- as.data.table(dcast(combined_draws_long, ... ~ variable, value.var = "avg_value"))
  
  if(plot){
    # Add columns for plotting
    combined_draws <- merge(combined_draws, rei_table, by = "rei_id")
    combined_draws <- merge(combined_draws, mediators[,.(med_id, mean_mediation)], by.x = "rei_id", by.y = "med_id")
    combined_draws[, label := paste0("mediator - ", rei, " (MF: ", round(mean_mediation, digits = 4), ")")]
    
    visualize_weighted_average(mediator_draws = combined_draws, average_draws = average_draws[, label := "weighted average"], plot_name = paste0(rei_name, "-", cause_name))
  }
  
  
  return(average_draws)
  
}

simple_average <- function(causeid, med_ids, plot = T){
  # Will take a simple average of the specified mediators and outcome
  # Will error if specified mediators are not among those we have age patterns for (bmi, sbp, fgp, ldl)
  #   or if that mediator does not have an age pattern for that outcome (ex: ldl and hemmohagic stroke)
  #
  #  @causeid = integer cause_id for the RO pair of interest
  #  @med_ids = vector of integer rei_ids to mediate through. Must be among bmi, sbp, fpg, ldl
  
  cause_name <- cause_table[cause_id == causeid, acause]
  
  # Read in the draws for each mediator
  files <- paste0(mediator_pct_draw_dir, med_ids, "_", causeid, ".csv")
  combined_draws <- rbindlist(lapply(files, fread))
  
  # Reshape long
  combined_draws_long <- melt(combined_draws, measure.vars = paste0("draw_",0:999))
  
  # Take normal average
  combined_draws_long[, avg_value := mean(value), by = c("cause_id", "age_group_id", "variable")]
  
  # Clean, reshape wide and return
  combined_draws_long[,`:=` (rei_id = NULL, value = NULL)]
  combined_draws_long <- unique(combined_draws_long)
  average_draws <- as.data.table(dcast(combined_draws_long, ... ~ variable, value.var = "avg_value"))
  
  if(plot){
    # Add columns for plotting
    combined_draws <- merge(combined_draws, rei_table, by = "rei_id")
    combined_draws[, label := paste0("mediator - ", rei)]
    
    visualize_weighted_average(mediator_draws = combined_draws, average_draws = average_draws[, label := "simple average"], plot_name = paste0(cause_name))
  }
  
  return(average_draws)

}

apply_age_pattern <- function(causeid, reid, risk_curve_draws_df, age_pattern_draws_df, draws_in_log = F, return_draws_log = F, plot = T){
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
  
  rei_name <- rei_table[rei_id == reid, rei]
  cause_name <- cause_table[cause_id == causeid, acause]
  
  if(sum(!c("exposure", paste0("draw_",0:999)) %in% colnames(risk_curve_draws_df))){
    stop("Risk curve draws should have 'exposure' and 'draw_0'...'draw_999' as column names, apparently it does not")
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
  
  # merge together and apply attenuation factor
  risk_curve <- merge(risk_curve, age_pct, by = "variable", allow.cartesian = T)
  
  
  if(draws_in_log & return_draws_log){
    # draws in log, out in log
    risk_curve[exp(rr) >= 1, age_specific_rr := log((exp(rr)-1)*pct_change+1)]
    risk_curve[exp(rr) < 1 , age_specific_rr := log(1/(pct_change * ((1/exp(rr)) - 1) + 1))]
  }else if(draws_in_log){
    # draws in log, out in normal
    risk_curve[exp(rr) >= 1, age_specific_rr := (exp(rr)-1)*pct_change+1]
    risk_curve[exp(rr) < 1, age_specific_rr := 1/(pct_change * ((1/exp(rr)) - 1) + 1)]
  }else if(return_draws_log){
    # draws in normal, out in log
    risk_curve[rr >= 1, age_specific_rr := log((rr-1)*pct_change+1)]
    risk_curve[rr < 1, age_specific_rr := log(1/(pct_change * ((1/rr) - 1) + 1))]
  }else{
    # draws in normal, out normal
    risk_curve[rr >= 1, age_specific_rr := (rr-1)*pct_change+1]
    risk_curve[rr < 1, age_specific_rr := 1/(pct_change * ((1/rr) - 1) + 1)]
  }
  
  risk_curve[, `:=` (rr = NULL, pct_change = NULL)]
  
  risk_curve<- as.data.table(dcast(risk_curve, ... ~ variable, value.var = "age_specific_rr"))
  

  if(plot){
    ylab <- ifelse(return_draws_log, "log(RR)", "RR")
    
    visualize_age_specific_curve(risk_curve, ylab, plot_name = paste0(rei_name, "-", cause_name))
  }
  
  return(risk_curve)
}

visualize_age_specific_curve <- function(risk_curve, ylab, plot_name){
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
  
  
  plot <- ggplot(risk_curve, aes(x = exposure, y = rrmean, ymin=rrlower, ymax = rrupper))+
    geom_line()+geom_ribbon(alpha = 0.2)+theme_bw()+
    facet_wrap(~age_group_name)+labs( y= ylab, title = plot_name)
  print(plot)  
  
}

visualize_weighted_average <- function(mediator_draws, average_draws, plot_name){
  # For use in plotting mediator pct change age pattern alongside average of mediators
  #
  #  @mediator_draws = dataframe of 'raw' mediator draws. Must have 'draw_0'...'draw_999' and 'label' (for use in plotting)
  #  @average_draws = dataframe of averaged mediator draws. Must have 'draw_0'...'draw_999' and 'label' (for use in plotting)
  #  @plot_name = character to be used as plot title in plot. 
  
  
  both_draws <- rbind(mediator_draws, average_draws, fill = T)
  
  both_draws[, pctmean := apply(.SD, 1, mean), .SDcols = paste0("draw_", 0:999)]
  both_draws[, pctupper := apply(.SD, 1, quantile, 0.975), .SDcols = paste0("draw_", 0:999)]
  both_draws[, pctlower := apply(.SD, 1, quantile, 0.025), .SDcols = paste0("draw_", 0:999)]
  
  plot <- ggplot(both_draws, aes(x = age_start, y = pctmean, ymin=pctlower, ymax = pctupper))+
    geom_line()+geom_ribbon(alpha = 0.2)+theme_bw()+
    facet_wrap(~label)+labs(y= "Attenuation factor", x = "Age (start)", title = plot_name)
  
  print(plot)
  
}
