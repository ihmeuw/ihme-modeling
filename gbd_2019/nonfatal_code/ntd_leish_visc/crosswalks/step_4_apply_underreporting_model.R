# Aggregate and apply UR model for STGPR
#
##########################################

rm(list = ls())
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- paste0(data_root, "FILEPATH")
  draws_dir <- paste0(data_root, "FILEPATH")
  interms_dir <- paste0(data_root, "FILEPATH")
  logs_dir <- paste0(data_root, "FILEPATH")
  location_id <- 214
}

# Source relevant libraries
library(data.table)
library(stringr)
library(argparse)
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
my_shell <- "FILEPATH"
code_dir   <- paste0(code_root, "FILEPATH") 

decomp_step <- 'step4'
gbd_round_id <- 6

### ======================= MAIN ======================= ###

# helper functions
isolate_all_sex_rows <- function(DT){
  # returns rows where location-years have Male and Female rows 
  # it is NOT robust to where "Both" is present
  dt <- copy(DT)
  dt[, keep := 0]
  loc_years <- unique(dt[, c("year_start", "year_end", "location_name")])
  
  for ( i in 1:nrow(loc_years)){
    
    yr_start      <- loc_years[i, year_start]
    yr_end        <- loc_years[i, year_end]
    loc_name      <- loc_years[i, location_name]
    rows          <- dt[year_start == yr_start & year_end == yr_end & location_name == loc_name]
    cats          <- unique(rows[, c("sex")])
    
    if ("Male" %in% cats[,sex] & "Female" %in% cats[, sex]){
      
      if ("Both" %in% cats){
        stop("This function only isolates where same loc years data points have both Male and Female. 'Both' as sex present which is funciton is not robust to")
      }
      
      dt[year_start == yr_start & year_end == yr_end & location_name == loc_name, keep := 1]
    }}
  
  data <- dt[keep == 1]
  return(data)
}

isolate_all_age_rows <- function(dt){
  # returns rows where location-years have age_start and age_end spanning 0-99
  
  dt[, keep := 0]
  loc_years <- unique(dt[, c("year_start", "year_end", "location_name")])
  
  for ( i in 1:nrow(loc_years)){
    yr_start      <- loc_years[i, year_start]
    yr_end        <- loc_years[i, year_end]
    loc_name      <- loc_years[i, location_name]
    rows          <- dt[year_start == yr_start & year_end == yr_end & location_name == loc_name]
    age_cats      <- unique(rows[, c("age_start", "age_end")])
    
    if (min(age_cats[, age_start]) == 0 & max(age_cats[, age_end]) == 99){
      
      dt[year_start == yr_start & year_end == yr_end & location_name == loc_name, keep := 1]
      
    }}
  
  data <- dt[keep == 1]
  return(data)
  
}

#'[ Aggregate data -- update pull from bundle

leish_bundle <- get_bundle_version(ADDRESS, fetch = 'all')
leish_bundle[, age_start := value_age_start]
leish_bundle[, age_end := value_age_end]

leish_bundle[nid == NID, age_group_id := 22]
leish_bundle   <- leish_bundle[source_type != "Survey - cross-sectional" & input_type == "extracted" & is_outlier == 0]
leish_bundle[age_end == 120, age_end := 99]

# for all-age all-sex, keep
leish_bundle_aa_as  <- leish_bundle[age_start == 0 & age_end == 99 & sex == "Both"]
leish_bundle_aa_as  <- leish_bundle_aa_as[, .("cases" = sum(cases)), by = c("location_name", "year_start", "year_end","nid")]

# for all-age sex-specific, if have both sexes and same nid -- aggregate them 
leish_bundle_aa_ss  <- leish_bundle[age_start == 0 & age_end == 99 & sex != "Both"]
leish_bundle_aa_ss  <- isolate_all_sex_rows(leish_bundle_aa_ss)
leish_bundle_aa_ss  <- leish_bundle_aa_ss[, .("cases" = sum(cases)), by = c("location_name", "year_start", "year_end","nid")]

# for age-specific all-sex points, if they have all ages and same nid-- aggregate them
leish_bundle_as_as  <- leish_bundle[(age_start != 0 | age_end != 99) & sex == "Both"]
leish_bundle_as_as  <- isolate_all_age_rows(leish_bundle_as_as)
leish_bundle_as_as  <- leish_bundle_as_as[, .("cases" = sum(cases)), by = c("location_name", "year_start", "year_end","nid")]

# for sex-specific age-specific points, if they have all sexes, all ages, and same nid -- aggregate them
leish_bundle_ss_ss  <- leish_bundle[(age_start != 0 | age_end != 99) & sex != "Both"]
leish_bundle_ss_ss  <- isolate_all_sex_rows(leish_bundle_ss_ss)
leish_bundle_ss_ss  <- isolate_all_age_rows(leish_bundle_ss_ss)
leish_bundle_ss_ss  <- leish_bundle_ss_ss[, .("cases" = sum(cases)), by = c("location_name", "year_start", "year_end","nid")]

# bind all data, subset to post 1980 data and non-zero cases
nat_leish_data <- rbind(leish_bundle_ss_ss, leish_bundle_as_as, leish_bundle_aa_ss, leish_bundle_aa_as)
nat_leish_data <- nat_leish_data[year_start > 1979 & cases > 0]

# fix location names
nat_leish_data[location_name == "Venezuela", location_name := "Bolivarian Republic of Venezuela"]
nat_leish_data[location_name == "Syria", location_name := "Syrian Arab Republic"]
nat_leish_data[location_name == "Macedonia", location_name := "North Macedonia"]
nat_leish_data[location_name == "Iran", location_name := "Islamic Republic of Iran"]
nat_leish_data[location_name == "Cote d'Ivoire", location_name := "Republic of Côte d'Ivoire"]

# add location ids
locs <- get_location_metadata(35, gbd_round_id = gbd_round_id)
locs <- locs[, .(location_id, location_name)]

nat_leish_data <- merge(nat_leish_data, locs, by = "location_name")
nat_leish_data <- nat_leish_data[!(location_name == "Georgia" & location_id == 533)]

fwrite(nat_leish_data, paste0(interms_dir, 'FILEPATH'))

#'[ Incorporate underreporting model into all-national data]

data      <- nat_leish_data

# load underreporting model
load(paste0(interms_dir, "FILEPATH"))

ilogit   <- function(x)1/(1+exp(-x))
haqi     <- get_covariate_estimates(1099, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
sdi      <- get_covariate_estimates(881, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
leish_endemic <- fread("FILEPATH")

#set number of repeats
n_reps<-1000

draw_string<-seq(1,n_reps,1)
draw_names<-paste0("draw_", draw_string)
draw_df<-data.frame(matrix(nrow=nrow(data), ncol=length(draw_string)))

#introduce a floor value
floor_threshold<-0.1

predicted_draws <- draw_df
predicted_cases <- draw_df

for (j in 1:nrow(data)){
  
  LocToPull <- data$location_id[j]
  pred_year <- data$year_start[j]
  pred_path <- as.factor('vl')
  pred_haqi <- haqi[location_id==LocToPull & year_id==pred_year]$mean_value
  pred_sdi  <- sdi[location_id==LocToPull & year_id==pred_year]$mean_value
  
  for (k in 1:n_reps){
    pred<-predict(mod[[k]], data.frame(year=pred_year, pathogen=pred_path, sdi=pred_sdi), se=TRUE)
    
    predicted_draws[j,k]<-ilogit(rnorm(1,
                                       mean = pred$fit,
                                       sd = (1.96*pred$se.fit)))
    predicted_cases[j,k]<-data$cases[j]/predicted_draws[j,k]
  }
  print(paste0("Completed ",j, " of ", nrow(data), ' draws'))
}

#
demographics_denominators<-NA
for (k in 1:nrow(data)){
  demographics<-get_population(age_group_id=22,
                               location_id=data$location_id[k],
                               year_id=data$year_start[k],
                               decomp_step = decomp_step, 
                               gbd_round_id = gbd_round_id
  )
  demographics_denominators[k]<-demographics$population
  print(paste0("Pulling ", k, " of ", nrow(data)))
}

demographics_denominators<-data.frame(demographics_denominators)
demographics_denominators<-t(demographics_denominators)

#calculated incidence for each of the predicted_cases draws
predicted_incidence<-predicted_cases/demographics_denominators

#calculate the mean and the variance of the draws in terms of cases and incidence
summary_incidence<-data.frame(mean=rep(NA, nrow(predicted_incidence)),
                              variance=rep(NA, nrow(predicted_incidence)))
summary_cases<-data.frame(mean=rep(NA, nrow(predicted_incidence)),
                          variance=rep(NA, nrow(predicted_incidence)))

for (s in 1:nrow(summary_incidence)){
  summary_incidence$mean[s]<-mean(t(predicted_incidence)[,s])
  summary_incidence$variance[s]<-var(t(predicted_incidence)[,s])
}
for (s in 1:nrow(summary_cases)){
  summary_cases$mean[s]<-mean(t(predicted_cases)[,s])
  summary_cases$variance[s]<-var(t(predicted_cases)[,s])
}

#currently have 1,000 as sample_size. Unsure if this is correct
st_gpr_input<-data.frame(me_name=rep("ntd_vl", 
                                     seq = nrow(summary_incidence)),
                         location_id=data$location_id,
                         nid=data$nid,
                         year_id=data$year_start,
                         age_group_id=rep(22, nrow(data)),
                         sex_id=rep(3, nrow(data)),
                         data=summary_incidence$mean,
                         variance=summary_incidence$variance,
                         sample_size=rep(1000, nrow(data)),
                         measure = "continuous",
                         is_outlier = 0)

case_count_output<-data.frame(me_name=rep("ntd_vl_case_count", nrow(summary_incidence)),
                              location_id=data$location_id,
                              nid=data$nid,
                              year_id=data$year_start,
                              age_group_id=rep(22, nrow(data)),
                              sex_id=rep(3, nrow(data)),
                              data=summary_cases$mean,
                              variance=summary_cases$variance,
                              sample_size=rep(1000, nrow(data)),
                              raw_cases=data$cases)

write.csv(st_gpr_input, file=paste0(interms_dir, "FILEPATH"))
write.csv(case_count_output, file=paste0(interms_dir, "FILEPATH"))

# save crosswalk version

st_gpr_input <- as.data.table(st_gpr_input)
st_gpr_input[, underlying_nid := NA]
st_gpr_input[, seq:= NA]
st_gpr_input[, sex := 'Both']

setnames(st_gpr_input, 'data', 'val')

st_gpr_input[, crosswalk_parent_seq := ADDRESS]
st_gpr_input[, unit_value_as_published := 1]
all_data_out_file <-paste0(interms_dir, "FILEPATH")
openxlsx::write.xlsx(st_gpr_input, all_data_out_file,  sheetName = "extraction")

all_data_description <- "VL STGPR GBD 2019 Crosswalk version"

all_data_cw_md  <- save_crosswalk_version(bundle_version_id = ADDRESS ,
                                          data_filepath =  all_data_out_file,
                                          description = all_data_description)