# qsub out sequela split code, get run id from interms


### ======================= BOILERPLATE ======================= ###

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

# source relevant libraries 
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
library(stringr)
library(data.table)
source("FILEPATH")
source("FILEPATH")
my_shell <- paste0("FILEPATH")

gbd_round_id <- ADDRESS
decomp_step <- 'step2'

run_file <- fread(paste0("FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
draws_dir    <- paste0(run_dir, "FILEPATH")
interms_dir    <- paste0(run_dir, "FILEPATH")
params_dir  <- paste0(data_root, "FILEPATH")

### ======================= Main Execution ======================= ###

study_dems <- readRDS(paste0(data_root, 'FILEPATH', gbd_round_id, '.rds'))
full_loc_set <- study_dems$location_id
full_age_set <- study_dems$age_group_id
full_sex_id  <- study_dems$sex_id
full_year_id <- study_dems$year_id

# pass run id from step 3
stgpr_run_id <- fread(paste0(interms_dir, "FILEPATH"))
stgpr_run_id <- stgpr_run_id[,run_id]

# age trend
draws_global<-get_draws(source = "epi",
                        gbd_id_type = ADDRESS, 
                        gbd_id = ADDRESS,
                        year_id = 2010,
                        location_id = 1,
                        decomp_step = decomp_step,
                        gbd_round_id = gbd_round_id,
                        status = 'best'
                        )

leish_endemic_vl <- fread(paste0(data_root, "FILEPATH"))
unique_vl_locations <- unique(leish_endemic_vl[value_endemicity == 1, location_id])

subnat_prop <- fread(paste0(data_root, "FILEPATH"))

zero_draws <- gen_zero_draws(modelable_entity_id = 1, location_id = 1, measure_id = c(5,6), metric_id = 3, year_id = 1980:max(full_year_id), gbd_round_id = gbd_round_id, team = 'epi')

# *** zero draws where age_group_id is 2 or 3
draws_global[age_group_id %in% c(2,3), paste0('draw_', 0:999) := 0]

# these are same
draws_global<-draws_global[,-1:-4]
draws_global<-draws_global[,-1001:-1004]

#implement this in ratio space
draws_ratio<-draws_global
for(k in 1:ncol(draws_global)){
  draw_string<-data.frame(draws_global[,..k])
  draw_string<-subset(draw_string, draw_string!=0)
  draws_ratio[,k]<-draws_global[,..k]/min(draw_string)
}

#this gives a global proportion model at draw level
setDF(draws_ratio)

#create a blank epi template that would need to be filled - aim to produce a csv for each location year
#sex_id order matches the draws_master sex_id order
empty_df<-data.frame(location_id=rep(NA,(2*length(full_age_set))),
                     year_id=rep(NA,(2*length(full_age_set))),
                     age_group_id=full_age_set,
                     sex_id=c(rep(1,length(full_age_set)),rep(2,length(full_age_set))),
                     measure_id=rep(NA,(2*length(full_age_set))),
                     modelable_entity_id=rep(NA,(2*length(full_age_set)))
)

zero_matrix<-draws_global*0
zero_df<-cbind(empty_df, zero_matrix)

# save as an RData for qsub loop to reference
save.image(file = paste0(interms_dir, "FILEPATH"))

# qsub out

my_locs <- full_loc_set

ifelse(!dir.exists(paste0(draws_dir, "FILEPATH")), dir.create(paste0(draws_dir, "FILEPATH")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, "FILEPATH")), dir.create(paste0(draws_dir, "FILEPATH")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, "FILEPATH")), dir.create(paste0(draws_dir, "FILEPATH")), FALSE)

param_map <- data.table(location_id = my_locs, stgpr_run_id = stgpr_run_id, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
num_jobs  <- nrow(param_map)
fwrite(param_map, paste0(interms_dir, "FILEPATH"), row.names = F)

# ** need to profile
qsub(job_name = paste0("ADDRESS", stgpr_run_id),
     shell    = my_shell,
     code     = paste0(code_root, "FILEPATH"),
     args     = list("--param_path", paste0(interms_dir, "FILEPATH")),
     project  = "ADDRESS",
     m_mem_free = "10G",
     fthread = "4",
     archive = NULL,
     h_rt = "00:00:40:00",
     queue = "ADDRESS",
     num_jobs = num_jobs)