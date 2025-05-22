md_file_list <- list.files(
  path = 'FILEPATH',
  full.names = TRUE
)

anemia_cutoffs <- read.csv(file.path(getwd(), 'extract/new_who_thresholds_w_age_group_ids.csv'))

hb_cols <- c(
  'hemoglobin_raw','hemoglobin_alt_adj','brinda_adj_hemog','who_adj_hemog'
)

sev_cols <- c('anemia_mild','anemia_moderate','anemia_severe','anemia_anemic')
col_categories <- c("_raw","_adj","_brinda","_who")

level_col_df <- data.frame(
  lower = c("hgb_lower_mild","hgb_lower_moderate","hgb_lower_severe","hgb_lower_anemic"),
  upper = c("hgb_upper_mild","hgb_upper_moderate","hgb_upper_severe","hgb_upper_anemic")
)

for(i in md_file_list) {
  print(i)
  dat <- haven::read_dta(i) |>
    data.table::setDT()
  
  for(r in seq_len(nrow(anemia_cutoffs))) {
    for(h in seq_len(length(hb_cols))) {
      if (hb_cols[h] %in% colnames(dat)) {
        temp_severity_cols <- paste0(sev_cols, col_categories[h])
        
        for(x in seq_len(nrow(level_col_df))) {
          lower_hb <- as.numeric(anemia_cutoffs[[level_col_df$lower[x]]][r])
          upper_hb <- as.numeric(anemia_cutoffs[[level_col_df$upper[x]]][r])
          anemia_col <- temp_severity_cols[x]
          
          if(r == 1) {
            dat[[anemia_col]] <- NA_integer_
          }
          
          i_vec <- which(
            dat$age_year >= anemia_cutoffs$age_group_years_start[r] &
              dat$age_year < anemia_cutoffs$age_group_years_end[r] &
              dat$sex_id == anemia_cutoffs$sex_id[r] &
              dat$cv_pregnant == anemia_cutoffs$pregnant[r] &
              !(is.na(dat[[hb_cols[h]]]))
          )
          dat[[anemia_col]][i_vec] <- 0
          
          i_vec <- which(
            dat$age_year >= anemia_cutoffs$age_group_years_start[r] &
              dat$age_year < anemia_cutoffs$age_group_years_end[r] &
              dat$sex_id == anemia_cutoffs$sex_id[r] &
              dat$cv_pregnant == anemia_cutoffs$pregnant[r] &
              !(is.na(dat[[hb_cols[h]]])) &
              dat[[hb_cols[h]]] >= lower_hb &
              dat[[hb_cols[h]]] < upper_hb
          )
          dat[[anemia_col]][i_vec] <- 1
          
        }
      }
    }
  }
  
  for(a in col_categories) {
    new_col <- new_col <- paste0('anemia_mod_sev', a)
    if(new_col %in% colnames(dat)) {
      dat[[new_col]] <- NULL
    }
  }
  
  for(a in col_categories) {
    mod_col <- paste0('anemia_moderate', a)
    sev_col <- paste0('anemia_severe', a)
    if(all(c(mod_col, sev_col) %in% colnames(dat))) {
      new_col <- paste0('anemia_mod_sev', a)
      dat[[new_col]] <- NA_integer_
      
      i_vec <- which(
        !(is.na(dat[[mod_col]])) | !(is.na(dat[[sev_col]]))
      )
      dat[[new_col]][i_vec] <- 0
      
      i_vec <- which(
        !(is.na(dat[[mod_col]])) & dat[[mod_col]] == 1 |
          !(is.na(dat[[sev_col]])) & dat[[sev_col]] == 1
      )
      dat[[new_col]][i_vec] <- 1
    }
  }
  
  haven::write_dta(
    data = dat,
    path = i
  )
}
