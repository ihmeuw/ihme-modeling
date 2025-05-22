
# Purpose: Launch the parallelized backcasting of lead exposure

rm(list=ls())

library(data.table)
library(feather)

SHARED_FUN_DIR <- 'FILEPATH'
source(file.path(SHARED_FUN_DIR, 'get_demographics_template.R'))
source(file.path(SHARED_FUN_DIR, 'get_location_metadata.R'))
source(file.path(SHARED_FUN_DIR,"get_crosswalk_version.R"))

type<-"unmed" #"total" or "unmed", this determines whether we are running the total CVD PAFs or the unmediated PAFs only
run_iq<-F #do we need to run the IQ shift portion of the code? T = yes. Use T if type ="unmed"


# Configure run settings
RELEASE <- 16
N_DRAWS <- 250
THREADS <- 32 #64
MRBRT_DRAWS <- ifelse(type=="total", 
                      'FILEPATH/rr_total_cov_draws.csv', #total CVD RR draws
                      'FILEPATH/outer_draws.csv') #unmediated RR draws
RUN_ID  <- 221323# envir_lead_exp st-gpr run-id; update as needed #run_id is the stgpr_version_id

VERSION <- 27 #folder version number
DESCRIPTION <- paste0('STGPR model', RUN_ID, ' with MR-BRT draws from ', MRBRT_DRAWS, ' 250 draw version - with code update')
RESTART_PARTIAL_RUN <- T # TRUE will relaunch only jobs that didn't complete, FALSE will launch all locations regardless of whether or not the corresponding output file exists
crosswalk_v<-47380 #crosswalk version id of the lead exp bundle
loc_set<-22 #location set id

user <- Sys.getenv("USER")

# Establish directories
code.dir <- "FILEPATH"
input_dir <- file.path("FILEPATH", RUN_ID, "draws_temp_0")
output_root <- file.path('FILEPATH', paste0('release_', RELEASE))
data_dir <- 'FILEPATH'
rr_dir <- file.path(data_dir, 'rr') 
exp_dir <- file.path(data_dir, 'exp')
weight_dir <- file.path(data_dir, 'ensemble_weights')
version_dir <- file.path(output_root, paste0('version_', VERSION))
type_dir <- file.path(output_root, paste0('version_', VERSION),paste0(type))

for (subdir in c('blood_backcast', 'blood_lead_dens', 'blood_pafs', 'bone_backcast', 'bone_pafs', 'ensemble_output', 'iq_shifts', 'pr_above')) {
  dir.create(file.path(type_dir, subdir), recursive = T, showWarnings = F)
}


# Save version configuration information to version log
config <- data.frame(release_id = RELEASE, version = VERSION, mrbrt_draws = MRBRT_DRAWS,
                     description = DESCRIPTION, user = user, run_date = Sys.Date())

config_file <- file.path(version_dir, 'version_config_log.csv')
write.table(config, config_file, row.names = F, sep = ',', append = file.exists(config_file), col.names = !file.exists(config_file))



demo_epi<-get_demographics_template('epi',release_id = RELEASE)[,.(location_id)]
demo_epi<-unique(demo_epi$location_id)

loc_meta <- get_location_metadata(location_set_id = loc_set, release_id = RELEASE)[location_id %in% demo_epi]
loc_meta[, super_region_id := as.factor(super_region_id)] # make this factor to use as categorical variable in mean->SD regression below
run_locs <- loc_meta[is_estimate == 1, .(location_id, super_region_id)][, mean := 1]

# check for failures ########################################
if (RESTART_PARTIAL_RUN == T) {
  
  run_locs[, complete := file.exists(file.path(type_dir, 'bone_pafs', paste0(location_id, '_bone_lead_edens.csv')))]
  
  if (run_iq) {
    run_locs[, iq_complete := file.exists(file.path(type_dir, 'iq_shifts', paste0(location_id, '_iq_shift_edens.csv')))]
    run_locs[, complete := complete & iq_complete]
  }
  
}

table(run_locs$complete)

#do we need to run all of the prep data?
if(type=="unmed"){

  demog_link <- get_demographics_template('cod', release_id = RELEASE)[, demog_id := 1:.N]
feather::write_feather(demog_link, file.path(version_dir, 'demog_link.feather'))

# Read in the crosswalked data
extraction<-as.data.table(get_crosswalk_version(crosswalk_v))[age_start >= 1 & age_end < 5, ]
extraction[,standard_deviation:=sqrt(variance)]
setnames(extraction,"val","mean")
extraction <- merge(extraction[, .(location_id, standard_deviation, mean)], loc_meta[, .(location_id, super_region_id)], by = 'location_id', all.x = T)

mod <- lm(log(standard_deviation) ~ 0 + mean + super_region_id, data = extraction)
saveRDS(mod, file.path(version_dir, 'mean_sd_mod.RDS'), compress = FALSE)


# Move input files off of FILEPATH and into version dir to avoid need for archive nodes
weights <- fread(file.path(weight_dir, 'envir_lead_blood.csv'))[1, ]
weights <- weights[, -c('location_id', 'year_id', 'sex_id', 'age_group_id'), with = F]
feather::write_feather(weights, file.path(version_dir, "blood_lead_weights.feather"))

iq_rr <- fread(file.path(rr_dir, 'blood_lead_coefs.csv'))
iq_rr <- melt(iq_rr, id.vars = 'age', value.name = 'iq_rr', variable.name = 'draw', variable.factor = FALSE)
iq_rr <- iq_rr[age == 'age_two', ][, age := NULL]
iq_rr[, draw := as.integer(gsub('rr_', '', draw))]
feather::write_feather(iq_rr, file.path(version_dir, 'blood_lead_coefs.feather')) 


# Prep MR-BRT RR draws and shift relative to TMREL
tmrel  <- fread('FILEPATH/bone_lead_tmrel_draws.csv')
tmrel <- tmrel[year_id == max(year_id) & location_id == 1 & sex_id == 1, ]
tmrel <- melt.data.table(tmrel, id.vars = c('age_group_id'), 
                         measure.vars = paste0("draw_", 0:999), variable.factor = F, 
                         variable.name = 'draw', value.name = 'bone_lead')

rr <- fread(MRBRT_DRAWS)
rr <- melt.data.table(rr, id.vars = 'risk', measure.vars = paste0('draw_', 0:999), variable.factor = F,
                      value.name = 'log_rr', variable.name = 'draw')
rr[, rr := exp(log_rr)]
setnames(rr, 'risk', 'bone_lead')


mean_rrs <- copy(rr)[, .(rr = mean(rr)), by = 'bone_lead']
tmrel$tmrel_rr <- approx(x = mean_rrs$bone_lead, y = mean_rrs$rr, xout = tmrel$bone_lead)$y
tmrel[, bone_lead := NULL]

rr <- merge(rr, tmrel, by = 'draw', allow.cartesian = T)
rr[, rr := rr / tmrel_rr]
rr[, draw := as.integer(gsub('draw_', '', draw))]

feather::write_feather(rr[, .(age_group_id, draw, bone_lead, rr)], file.path(version_dir, 'mrbrt_bone_cvd_rr_draws.feather'))
}

# Launch jobs ###########################################################
#Slurm Project and output
project <- "-A USERNAME "
slurm.output.dir <- paste0("-o FILEPATH/", user, "FILEPATH -e FILEPATH", user, "FILEPATH")
calc.script <- file.path(code.dir, "edensity_backcast.R")
r.shell <- "FILEPATH -s"
runtime <- "2:00:00" #use to be 1:00:00

MEM <- '50G'#use to be 100
QUEUE <- 'all.q'
for (i in which(run_locs$complete==F)) {
    run_loc <- run_locs[i, location_id]
    jname <- paste0("lead_bc_", run_loc)
    sys.sub <- paste0("sbatch ", project, slurm.output.dir, " -J ", jname, " -c ", THREADS, " --mem=", MEM,  " -t ", runtime, " -p ", QUEUE)
    args <- paste(RELEASE, RUN_ID, VERSION, THREADS, N_DRAWS, run_loc, run_iq, type_dir,version_dir)
    system(paste(sys.sub, r.shell, calc.script, args))
  }

# check for failures again #################################################

run_locs[, bone_complete := file.exists(file.path(type_dir, 'bone_pafs', paste0(location_id, '_bone_lead_edens.csv')))]
run_locs[, iq_complete := file.exists(file.path(type_dir, 'iq_shifts', paste0(location_id, '_iq_shift_edens.csv')))]


table(run_locs$bone_complete, run_locs$iq_complete, useNA = 'ifany', deparse.level = 2)


