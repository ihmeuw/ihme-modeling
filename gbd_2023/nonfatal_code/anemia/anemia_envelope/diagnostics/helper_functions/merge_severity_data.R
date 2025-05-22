
# merge together severity data --------------------------------------------

merge_severity_data <- function(input_data_list){
  
  total_df <- data.table::copy(input_data_list$total_anemia_prevalence)
  modsev_df <- data.table::copy(input_data_list$mod_sev_anemia_prevalence)
  severe_df <- data.table::copy(input_data_list$severe_anemia_prevalence)
  
  total_df$Severity <- 'Total Anemia'
  modsev_df$Severity <- 'Mod + Sev Anemia'
  severe_df$Severity <- 'Severe Anemia'
  
  df <- data.table::rbindlist(
    list(total_df, modsev_df, severe_df),
    use.names = TRUE,
    fill = TRUE
  )
  
  return(list(
    mean_hemoglobin = data.table::copy(input_data_list$mean_hemoglobin),
    anemia = df
  ))
}
