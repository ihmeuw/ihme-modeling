
# load in libraries -------------------------------------------------------

library(data.table)

# append NIDs from GBD 2021 bundles that aren't present -------------------

add_missing_nids <- function(input_df){
  df <- copy(input_df)
  
  var_map <- list(
    hemoglobin_alt_adj = "FILEPATH/4754/to_use.csv",
    anemia_anemic_adj = "FILEPATH/8108/to_use.csv",
    anemia_mod_sev_adj = "FILEPATH/8120/to_use.csv",
    anemia_severe_adj = "FILEPATH/8117/to_use.csv"
  )
  
  gbd2021_df <- lapply(seq_len(length(var_map)), \(x){
    fread(var_map[[x]]) |>
      dplyr::mutate(
        var = names(var_map)[x],
        mean = val,
        from_2021 = TRUE
      )
  }) |>
    rbindlist(use.names = TRUE, fill = TRUE)
  
  missing_nid_vec <- setdiff(unique(gbd2021_df$nid), unique(df$nid))
  gbd2021_df <- gbd2021_df[nid %in% missing_nid_vec]
  
  df <- rbindlist(
    list(df, gbd2021_df),
    use.names = TRUE,
    fill = TRUE
  )
  
  return(df)
}

