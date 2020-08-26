source('FILEPATH/get_location_metadata.R')
source('FILEPATH/register.R')
source('FILEPATH/sendoff.R')
source('FILEPATH/utility.r')

locs <- get_location_metadata(location_set_id = 35)
countries <- subset(locs, level == 3)
countries <- countries[, c('location_id', 'location_name', 'ihme_loc_id')]

data <- fread('FILEPATH')
data <- merge(data, countries)

data$is_outlier[data$ihme_loc_id == 'EST'] <- 1
data$is_outlier[data$ihme_loc_id == 'SVK'] <- 1
data$is_outlier[data$ihme_loc_id == 'RUS' &
                  data$age_start %in% c(15)] <- 1
data$is_outlier[data$ihme_loc_id == 'HRV' &
                  data$age_start %in% c(15, 20, 30, 60, 65)] <- 1
data$is_outlier[data$ihme_loc_id == 'URY' &
                  data$age_start %in% c(15, 50)] <- 1
data$is_outlier[data$ihme_loc_id == 'LVA' &
                  data$age_start %in% c(25, 40, 55, 80)] <- 1
data$is_outlier[data$ihme_loc_id == 'SVN' &
                  data$age_start %in% c(25)] <- 1
data$is_outlier[data$ihme_loc_id == 'UKR' &
                  data$age_start %in% c(25, 35, 40, 45, 50, 60, 65, 70, 75)] <- 1
data$is_outlier[data$ihme_loc_id == 'TUR' &
                  data$age_start %in% c(30, 35, 45, 50, 80)] <- 1
data$is_outlier[data$ihme_loc_id == 'GEO' &
                  data$age_start %in% c(30, 40, 50)] <- 1
data$is_outlier[data$ihme_loc_id == 'KAZ' &
                  data$age_start %in% c(35)] <- 1
data$is_outlier[data$ihme_loc_id == 'HUN' &
                  data$age_start %in% c(35, 60)] <- 1
data$is_outlier[data$ihme_loc_id == 'ESP' &
                  data$age_start %in% c(40, 75)] <- 1
data$is_outlier[data$ihme_loc_id == 'BRA' &
                  data$age_start %in% c(60, 70)] <- 1
data$is_outlier[data$ihme_loc_id == 'BIH' &
                  data$age_start %in% c(65)] <- 1
data$is_outlier[data$ihme_loc_id == 'CZE' &
                  data$age_start %in% c(75)] <- 1
data$is_outlier[data$ihme_loc_id == 'IND' &
                  data$age_start %in% c(80)] <- 1

write.csv(
  data,
  'FILEPATH'
)

# launch ST-GPR model

# Arguments
me_name <- 'inj_recv_care_prop'
path_to_config <-
  'FILEPATH'
my_model_id <- 14
decomp_step <- 'iterative'
holdouts <- 0
draws <- 0
project <- 'proj_injuries'

# Registration
run_id <- register_stgpr_model(
  me_name = me_name,
  path_to_config = path_to_config,
  my_model_id = my_model_id,
  decomp_step = decomp_step,
  holdouts = holdouts,
  draws = draws
)

# Sendoff
st_gpr_sendoff(run_id, project)
