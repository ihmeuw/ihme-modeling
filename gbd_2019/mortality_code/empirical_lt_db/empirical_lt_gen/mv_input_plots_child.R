## Title: parallelize plotting .jpgs of life tables for machine vision (child script)

library(data.table)
library(argparse)

# Get arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="character", required=TRUE,
                    help='The ELT version used for this run')
parser$add_argument('--loc', type="character", required=TRUE,
                    help='The ihme_loc_id for this child script')
args <- parser$parse_args()

# Get arguments
version_id <- args$version_id
loc <- args$loc
user <- Sys.getenv("USER")

code_directory <- paste0("FILEPATH/empirical-lt-gen-code")
source(paste0(code_directory,"/elt_functions.R"))

main_dir <- paste0("FILEPATH/emirical-lt-dir/",version_id)
dir <- paste0(main_dir,"/machine_vision")
if(!dir.exists(dir)) dir.create(dir)

# all_lts.csv comes from gen_empir_lts.R script
dt <- fread(paste0(main_dir,"/all_lts.csv"))

# subset to the location of interest
dt <- dt[ihme_loc_id==loc,]


# plot to single jpgs for machine vision
# =================================================================================
plot_for_machine_vision_single_loc <- function(dt, dir){

  message(paste0(Sys.time()," : beginning plotting for machine vision"))

  # add plot numbers
  if(!("plot_num" %in% names(dt))) dt <- add_plot_nums(dt)
  total_n_plots <- length(unique(dt$plot_num))

  # create log-mean variable from qx variable
  dt[, log_mean := log(qx)]

    p <- function(i){
          temp <- dt[plot_num==i]
              loc <- temp$ihme_loc_id[1]
              year <- temp$year[1]
              sex_name <- temp$sex[1]
              source_type <- temp$source_type[1]
              smooth_width <- temp$smooth_width[1]
          plot_single_lt(temp, include_title=F)
          dir.create(paste0(dir,'/',loc,'/predict/'), recursive=T, showWarnings=F)
          filepath <- paste0(dir,'/',loc,'/predict/',loc,'_',year,'_',
                            sex_name,'_',source_type,'_',smooth_width,'.jpg')
          ggsave(filepath, width = 20, height = 20, units = "cm", dpi = 320)
    }

    message('Generating and saving plots')
    time_start <- Sys.time()
    lapply(unique(dt$plot_num), p)
    time_end <- Sys.time()
    time_elapsed <- round(difftime(time_end,time_start,units='mins'),3)
    message(paste0('Done! time elapsed: ', time_elapsed, ' minutes'))
}

## run function
if(nrow(dt)>0){
  plot_for_machine_vision_single_loc(dt, dir)
  print(paste0("Plots saved for ", loc))
} else {
  print(paste0("No life tables found for ", loc))
}


# END
