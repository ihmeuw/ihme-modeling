
# source library ----------------------------------------------------------

library(data.table)
library(stringr)

# create config file ------------------------------------------------------

config_df <- fread(file.path(getwd(), "mrbrt/square_measure_type/config_map.csv"))

config_df$good_model <- TRUE

config_df <- config_df[
  ref %like% "anemia_anemic_(raw|who|brinda)" &
    anemia_category %in% 2:6,
  `:=`(good_model = FALSE, last_prior = 0.1, convexity = NA_character_, r_linear = FALSE)
]

config_df <- config_df[
  ref %like% "anemia_mod_sev_(raw|who|brinda)" &
    anemia_category %in% 4:5,
  `:=`(good_model = FALSE, last_prior = 0.1, convexity = NA_character_, r_linear = FALSE)
]

fwrite(
  x = config_df,
  file = file.path(getwd(), "mrbrt/square_measure_type/config_map.csv")
)
