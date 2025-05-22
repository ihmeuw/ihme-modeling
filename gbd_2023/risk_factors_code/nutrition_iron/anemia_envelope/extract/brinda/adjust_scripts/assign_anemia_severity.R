# source libraries --------------------------------------------------------

library(data.table)

# anemia severity assignment ----------------------------------------------

assign_anemia_severity <- function(input_df) {
  df <- copy(input_df)

  anemia_levels <- fread(file.path(getwd(), 'extract/new_who_thresholds_w_age_group_ids.csv'))
  severity_cols <- c("mild", "moderate", "severe", "anemic")
  adj_col_names <- c("brinda", "who")

  for (r in seq_len(nrow(anemia_levels))) {
    for (c in severity_cols) {
      lower_hb <- as.numeric(anemia_levels[[paste0("hgb_lower_", c)]][r])
      upper_hb <- as.numeric(anemia_levels[[paste0("hgb_upper_", c)]][r])

      for (a in adj_col_names) {
        sev_col_name <- paste("anemia", c, a, sep = "_")
        hb_col_name <- paste0(a, "_adj_hemog")
        
        i_vec <- df$age_year >= anemia_levels$age_group_years_start[r] &
          df$age_year < anemia_levels$age_group_years_end[r] &
          df$sex_id == anemia_levels$sex_id[r] &
          df$cv_pregnant == anemia_levels$pregnant[r]
        
        set(
          x = df,
          i = which(i_vec),
          j = sev_col_name,
          value = 0
        )
        
        i_vec <- i_vec &
          !(is.na(df[[hb_col_name]])) &
          df[[hb_col_name]] >= lower_hb &
          df[[hb_col_name]] < upper_hb
        
        set(
          x = df,
          i = which(i_vec),
          j = sev_col_name,
          value = 1
        )
      }
    }
  }

  return(df)
}
