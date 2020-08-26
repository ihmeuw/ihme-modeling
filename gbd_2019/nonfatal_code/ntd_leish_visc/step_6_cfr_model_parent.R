# extend gbd 2017 cfr estimates with new locations and custom values
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common IHME IO Paths
if (!is.na(Sys.ADDRESS()["EXEC_FROM_ARGS"][[1]])) {
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
library(data.table)
library(stringr)
library(ggplot2)
library(plotly)
library(lme4)
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
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
location_ids <- study_dems$location_id

run_id <- fread(paste0(interms_dir, "FILEPATH"))
run_id <- run_id[,run_id]

# create directory
ifelse(!dir.exists(paste0(draws_dir, "FILEPATH")), dir.create(paste0(draws_dir, "FILEPATH")), FALSE)
locs <- get_location_metadata(35, gbd_round_id = gbd_round_id, decomp_step = decomp_step)

# load gbd 2017 modeled grs
load(file=paste0(data_root, 'FILEPATH'))

i <- 0
l_ids <- c() 
new_subnat <- c(214, 165, 51, 73, 16)

for (i in 1:length(cfr_list)){
  cfrs <- as.data.table(cfr_list[[i]])
  loc_id <- unique(cfrs$location_id)
  l_ids <- c(l_ids, loc_id)
  # overwrite locations for new subnationals use national to overwrite
  # new subnationals: Nigeria, Pakistan, Poland, Italy, Philippines
  if (loc_id %in% new_subnat){
    new_ids <- locs[parent_id == loc_id, location_id]
    for (l_id in new_ids){
      # write out children with parents values
      cfrs[, location_id := l_id]
      fwrite(cfrs, paste0(draws_dir, "FILEPATH", l_id, ".csv"))    
      }
  } else {
  fwrite(cfrs, paste0(draws_dir, "FILEPATH", loc_id, ".csv"))
  }
  cat("\nWrote ", i, " of ", length(cfr_list))
}

# add cfr to each for italy subnationals and monaco c(35494, 35495, 35496, 35497, 35498, 35499, 35500, 35501, 35502, 35503, 35504, 35505, 35506, 35507, 35508, 35509, 35510, 35511, 35512, 35513, 35514, 367)
# values -  unifrom distribution 6 - 10%
cfr_europe_ids <- c(35494, 35495, 35496, 35497, 35498, 35499, 35500, 35501, 35502, 35503, 35504, 35505, 35506, 35507, 35508, 35509, 35510, 35511, 35512, 35513, 35514, 367)
cfr_draw <- copy(cfrs[, .(age_group_id, sex_id, year_id, location_id, cause_id, measure_id)])
row <- as.data.table(t(data.table(runif(n = 1000, min = 0.06, max = 0.10))))
setnames(row, as.character(1:1000))
cfr_draw <- cbind(cfr_draw, row)

for (loc_id in cfr_europe_ids){
  cfr_draw[, location_id := loc_id]
  fwrite(cfr_draw, paste0(draws_dir, "FILEPATH", loc_id, ".csv"))
}

# add custom cfr rate for Africa
#  uniform distribution of 10%-30% for all age and sex groups 

# SSA location_id is 166
locs <- get_location_metadata(35, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
afr_r <- locs[parent_id == 166, location_id] # level 2
afr_n <- locs[parent_id %in% afr_r]
afr_sn<- locs[parent_id %in% afr_n[, location_id]]

afr_locs <- rbind(afr_sn, afr_n)
afr_locs <- afr_locs[most_detailed == 1, location_id]

cfr_draw <- copy(cfr_draw[, .(age_group_id, sex_id, year_id, location_id, cause_id, measure_id)])
row <- as.data.table(t(data.table(runif(n = 1000, min = 0.10, max = 0.30))))
setnames(row, as.character(1:1000))
cfr_draw <- cbind(cfr_draw, row)

for (loc_id in afr_locs){
  cfr_draw[, location_id := loc_id]
  fwrite(cfr_draw, paste0(draws_dir, "FILEPATH", loc_id, ".csv"))
}

# add cfr for new Norway location
cfr_draw <- as.data.table(cfr_list[[285]])
if (unique(cfr_draw[,location_id]) == 4910) { cat('pull Oslo subnational as skeleton - location is restricted but need structure for code')}
norway_sn <- locs[parent_id == 90, location_id]

for (loc_id in norway_sn){
  cfr_draw[, location_id := loc_id]
  fwrite(cfr_draw, paste0(draws_dir, "FILEPATH", loc_id, ".csv"))
}
 
# send out qsub to calculate deaths from cfr
ifelse(!dir.exists(paste0(draws_dir, "FILEPATH")), dir.create(paste0(draws_dir, "FILEPATH")), FALSE)

my_locs    <- location_ids
grs <- fread(paste0(data_root, 'FILEPATH'))
grs <- grs[year_start == 1980, .(location_id, value_endemicity)]
  
param_map <- data.table(location_id = my_locs, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
param_map <- merge(param_map, grs, by = 'location_id', all.x = TRUE)
if (nrow(param_map[is.na(value_endemicity)])> 0 ) {stop('gr endemicity merge did not work')}
  
num_jobs  <- nrow(param_map)
fwrite(param_map, paste0(interms_dir, "FILEPATH"), row.names = F)
  
qsub(job_name = paste0("ADDRESS", run_id),
     shell    = my_shell,
     code     = paste0(code_root, "FILEPATH"),
     args     = list("--param_path", paste0(interms_dir, "FILEPATH")),
     project  = "ADDRESS",
     m_mem_free = "5G",
     fthread = "1",
     archive = NULL,
     h_rt = "00:00:15:00",
     queue = "ADDRESS",
     num_jobs = num_jobs)
