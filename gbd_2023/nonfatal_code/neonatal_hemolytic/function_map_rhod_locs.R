update_location_names <- function(data) {
  rename_list <- c(
    'Bolivia' = 'Bolivia (Plurinational State of)',
    'Bosnia Herzegovina' = 'Bosnia and Herzegovina',
    'Czech Republic' = 'Czechia',
    'Hong Kong' = 'Hong Kong Special Administrative Region of China',
    'Iran' = 'Iran (Islamic Republic of)',
    'Russia' = 'Russian Federation',
    'Syria' = 'Syrian Arab Republic',
    'Turkey' = 'Türkiye',
    'United States' = 'United States of America',
    'Venezuela' = 'Venezuela (Bolivarian Republic of)',
    'Vietnam' = 'Viet Nam',
    'South Korea' = 'Republic of Korea'
  )

  for (original_name in names(rename_list)) {
    data[location_name == original_name, location_name := rename_list[[original_name]]]
  }
}
