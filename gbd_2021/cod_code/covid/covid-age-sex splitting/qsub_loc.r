## Setup
Sys.umask("0002")
Sys.setenv(MKL_VERBOSE = 0)
suppressMessages({library(data.table)})
setDTthreads(1)

## Arguments
parser <- argparse::ArgumentParser()
parser$add_argument("--outputs-directory", required = TRUE, help = "Directory in which to write outputs")
parser$add_argument("--rake_hosp", required = TRUE, help = "Rake hospitalization output or not")
parser$add_argument("--fhs", required = TRUE, help = "FHS run or not")
parser$add_argument("--apply_scalars", required = TRUE, help = "Apply EM scalars or pull in scaled deaths directly")
args <- parser$parse_args()

idx <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
output_dir <- args$outputs_directory
rake_hosp <- args$rake_hosp
fhs <- args$fhs
apply_scalars <- args$apply_scalars

print(paste("Task id:", idx))
print(paste("Output directory:", output_dir))

user <- Sys.info()["user"]
code_dir <- sprintf('FILEPATH', user)
source(file.path(code_dir,"calc_loc_age_sex_ensembles.R"))
source(file.path(code_dir,"sex_split.R"))
source(file.path("FILEPATH/get_location_metadata.R"))

loc_list <- fread(file.path(output_dir, "locs.csv"))
loc_id <- loc_list$location_id[idx]
print(paste("Location ID:", loc_id))

dt <- readRDS(file.path(output_dir, "data.rds"))
print(str(dt))

gbd_hierarchy <- get_location_metadata(location_set_id = 35, release_id = 9)

print('calculating infections, hospitalizations, and deaths')
calc_loc_age_sex(loc_id, dt, output_dir, rake_hosp, fhs, apply_scalars)
print('done')