## Author: NAME 
## Date: February 3, 2016
## Purpose: Run and compile the with-shock envelope and life-tables

# source("PATH")

############
## Settings
############

rm(list=ls()); library(foreign); library(data.table)

if (Sys.info()[1] == 'Windows') {
  username <- "USER"
  root <- "J:/"
} else {
  username <- Sys.getenv("USER")
  root <- "/home/j/"
  code_dir <- paste("PATH", username, "PATH", sep="")  
  setwd(code_dir)
}

test <- F              # if T, no files are deleted and no jobs are submitted.
start <-  1         # code piece to start (1 to run whole thing)
end <- 2
errout <- T

source(paste0(root,"PATH"))
source(paste0(root,"PATH"))
source(paste0(root,"PATH"))

## set up options that apply to all jobs
r_shell <- "r_shell.sh"
stata_shell <- "stata_shell.sh"
stata_shell_mp <- "stata_shell_mp.sh"
errout_paths <- paste0("-o PATH",username,
                       "/output -e PATH",username,"/errors ")
proj <- "-P proj_mortenvelope " # can also be proj_mortenvelope

## set up arguments to pass

locations <- get_locations(level="all") 
locations2 <- get_locations(level="all", gbd_type = "sdi") ## Ready for second round, 2/23/17
locations <- rbind(locations,locations2[!locations2$location_id %in% unique(locations$location_id),]) # this is the correct line for golf_2/ hotel

## save stuff so we're not runninig get_locs or get_age_maps
est <- get_locations(level="estimate")
est <- write.csv(est, paste0(root, "PATH"), row.names=F)

lowest <- get_locations(level="lowest")
lowest <- write.csv(lowest, paste0(root, "PATH"), row.names=F)

ages <- data.table(get_age_map(type="lifetable"))[,.(age_group_name_short, age_group_id)]
ages <- write.csv(ages, paste0(root, "PATH"), row.names=F)

## cod correct version of shock aggregator outputs                       
library(RMySQL)
myconn <- dbConnect(RMySQL::MySQL(), host="HOST", username="USER", password="PASSWORD") # Requires connection to shared DB
sql_command <- paste0("SELECT shock_version_id FROM cod.shock_version WHERE gbd_round_id = 4 AND shock_version_status_id = 1;")
v_id <- dbGetQuery(myconn, sql_command)
output_version_id <- as.integer(v_id[1, 'shock_version_id'])
print(paste0("using shock_aggregator output version: ", output_version_id))
dbDisconnect(myconn)
# output_version_id <- 21 # for manually updating the shock aggregator version.

#envelope version
# env_version <- 140 # for manual envelope choice if needed
env_version <- get_proc_version(model_name="death number", model_type="estimate", run_id="recent")
print(paste0("using envelope version: ", env_version))

#######################################
# DELETE FILES FOR OTHER PIECES OF CODE TO ENSURE WE DON'T USE OLD VERSIONS
# #######################################
env_dir <- "PATH"
setwd(env_dir)
if (start <= 1 & test==F) unlink("*.csv") ## fill in the directory from which you're deleting stuff,  only get rid of .csvs, not dirs


lt_dir <- "PATH"
setwd(lt_dir)
if (start <= 1 & test==F) for (ff in dir(lt_dir)) file.remove(paste0(ff))


qx_dir <- "PATH"
dir.create(qx_dir)
setwd(qx_dir)
if (start <= 1 & test==F) for (ff in dir(qx_dir)) file.remove(paste0(ff))

######################
# AGGREGATING SHOCKS
######################
# 
# 
## find a way to attach this to the end of the shock aggregator. 
## This code should always run after the SA runs.
# setwd(code_dir)
# dir.create(paste0("PATH", output_version_id))
# 
# if(!file.exists(paste0("PATH", output_version_id, "/shocks_1.csv"))){
#   jname <- "aggregate_shocks"
#   mycores <- 17
#   sys.sub <- paste0("qsub ",ifelse(errout,errout_paths,""),"-cwd ", proj, "-N ", jname, " -pe multi_slot ", mycores, " ", "-l mem_free=", 2 * mycores, "G")
#   script <- "01_aggregate_shocks.R"
#   args <- paste(output_version_id, sep=" ")
#   if (test) print(paste(sys.sub, "r_shell.sh", script, args))
#   if (!test) system(paste(sys.sub, "r_shell.sh", script, args))
# }

# 
# 
# ######################################
# # RUN SHOCKS ############
# ######################################
# 
# ####################
setwd(code_dir)
if (start <= 1 & end >= 1) {
  ## Add shocks to the envelope and LT
  jlist1 <- c()
  mycores <- 6
  for (loc in locations$location_id) { 
    if(!file.exists(paste0("PATH", loc, ".csv"))){
      ihme_loc_id <- locations$ihme_loc_id[locations$location_id==loc]
      if (loc %in% c(44634,44635,44636,44637,44639)) ihme_loc_id <- locations$location_id[locations$location_id == loc]
      jname <- paste("add_shocks",ihme_loc_id, sep="")
      hold <- paste(" -hold_jid \"",paste("aggregate_shocks",collapse=","),"\"",sep="")
      sys.sub <- paste0("qsub ",ifelse(errout,errout_paths,""),"-cwd ", proj, "-N ", jname, " ", "-pe multi_slot ", mycores, " ", "-l mem_free=", 2 * mycores, "G ", hold)
      script <- "02_add_shocks_env.R"
      args <- paste(ihme_loc_id, loc, output_version_id, env_version, sep=" ")
      
      if (test) print(paste(sys.sub, r_shell, script, args))
      if (!test) system(paste(sys.sub, r_shell, script, args))
      jlist1 <- c(jlist1, jname)
    }
  }
}
# 
# #########################
# ## COMPILE SHOCKS########
# #########################

if(start <= 2 & end >= 2){
  jname <- "compile_shocks"
  mycores <- 2
  if(start<2) hold <- paste(" -hold_jid \"",paste(jlist1,collapse=","),"\"",sep="")
  sys.sub <- paste0("qsub ",ifelse(errout,errout_paths,""),"-cwd ", proj, "-N ", jname, ifelse(start < 2,hold,""), " -pe multi_slot ", mycores, " ", "-l mem_free=", 2 * mycores, "G ")
  script <- "03_compile_shocks.R"
  args <- paste0(env_version)
  if (test) print(paste(sys.sub, r_shell, script, args))
  if (!test) system(paste(sys.sub, r_shell, script, args))
}


