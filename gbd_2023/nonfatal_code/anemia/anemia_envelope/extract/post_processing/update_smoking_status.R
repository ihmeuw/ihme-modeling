md_file_list <- list.files(
  path = 'FILEPATH',
  full.names = TRUE
)

smoking_map <- data.table::fread(file.path(getwd(), 'extract/post_processing/smoking_map.csv'))

for(i in md_file_list) {
  dat <- haven::read_dta(i) |>
    data.table::setDT()
  
  if('hemog_alt_smoke_adjust' %in% colnames(dat)) {
    message('updating: ', i)
    dat$smoking_hb_adj <- NA_integer_
    if('smoking_number' %in% colnames(dat)) {
      for(r in seq_len(nrow(smoking_map))) {
        i_vec <- which(
          !(is.na(dat$smoking_number)) &
            dat$smoking_number >= smoking_map$lower_smoke[r] &
            dat$smoking_number < smoking_map$upper_smoke[r]
        )
        dat$smoking_hb_adj[i_vec] <- smoking_map$hb_adj[r]
      }
    }
    if('smoking_status' %in% colnames(dat)) {
      i_vec <- which(dat$smoking_status == 1 & is.na(dat$smoking_hb_adj))
      dat$smoking_hb_adj[i_vec] <- 3
    }
    
    i_vec <- which(dat$age_year < 5 | is.na(dat$smoking_hb_adj))
    dat$smoking_hb_adj[i_vec] <- 0
    
    if(!('hemoglobin_alt_adj' %in% colnames(dat))) {
      dat$hemoglobin_alt_adj <- NA_real_
    }
    
    i_vec <- which(
      is.na(dat$hemoglobin_alt_adj) &
        !(is.na(dat$hemog_alt_smoke_adjust))
    )
    
    dat$hemoglobin_alt_adj[i_vec] <- dat$hemog_alt_smoke_adjust[i_vec] +
      dat$smoking_hb_adj[i_vec]
    
    if('hemoglobin_raw' %in% colnames(dat)) {
      i_vec <- which(
        dat$hemoglobin_alt_adj > dat$hemoglobin_raw &
          dat$smoking_hb_adj > 0
      )
      dat$hemoglobin_alt_adj[i_vec] <- dat$hemoglobin_raw[i_vec] 
    }
  }
  
  haven::write_dta(
    data = dat,
    path = i
  )
}
