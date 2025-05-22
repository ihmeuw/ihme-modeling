md_file_list <- list.files(
  path = 'FILEPATH',
  full.names = TRUE
)

for(i in md_file_list) {
  print(i)
  dat <- haven::read_dta(i) |>
    data.table::setDT()
  
  if('age_month' %in% colnames(dat)) {
    i_vec <- which(
      !(is.na(dat$age_month)) & 
        dat$age_month < 60
    )
    
    dat$age_year[i_vec] <- dat$age_month[i_vec] / 12
  }
  
  if('age_day' %in% colnames(dat)) {
    i_vec <- which(
      !(is.na(dat$age_day)) &
        dat$age_day <= 365.25
    )
    
    dat$age_year[i_vec] <- dat$age_day[i_vec] / 365.25
  }
  
  haven::write_dta(
    data = dat,
    path = i
  )
}
