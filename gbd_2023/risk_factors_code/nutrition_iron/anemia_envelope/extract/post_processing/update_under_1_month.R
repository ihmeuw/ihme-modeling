md_file_list <- list.files(
  path = 'FILEPATH',
  full.names = TRUE
)

orginal_under_1_path <- 'FILEPATH'

for(f in md_file_list) {
  dat <- haven::read_dta(f, col_select = 'age_year')
  print(f)
  if(min(dat$age_year, na.rm = TRUE) == 0) {
    dat <- haven::read_dta(f)
    file_name <- basename(f)
    if(!('age_day' %in% colnames(dat))) {
      new_dat <- dat |> dplyr::filter(!(is.na(age_year)) & age_year != 0)  
      
      haven::write_dta(
        data = new_dat,
        path = f
      )

      haven::write_dta(
        data = dat,
        path = file.path(orginal_under_1_path, file_name)
      )
    }
  }
}
