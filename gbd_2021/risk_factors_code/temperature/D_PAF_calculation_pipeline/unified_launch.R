## Launches processing for location-level exposure data, rrMax, TMREL, and PAF calculations, in that order.
# source("/FILEPATH/unified-launch.R", echo = T)

## SYSTEM SETUP ----------------------------------------------------------
# Clear memory
#rm(list=ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]


# Drives 
j <- if (os == "Linux") "/FILEPATH/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("/FILEPATH/", user, "/") else if (os == "Windows") "H:/"


# Base filepaths
work_dir <- paste0(j, '/FILEPATH/')
share_dir <- '/FILEPATH/' # only accessible from the cluster


## LOAD DEPENDENCIES -----------------------------------------------------
library(data.table)
library(dplyr)
library(ggplot2)
library(argparse)
source("/FILEPATH/r/get_location_metadata.R")
source("/FILEPATH/r/get_cause_metadata.R")
source("/FILEPATH/r/get_ids.R")
#source(paste0(code_dir, "FILEPATH/csv2objects.R"))
source("../../helper_functions/csv2objects.R")
Sys.setenv("RETICULATE_PYTHON"='/FILEPATH')  # Set the Python interpreter path
#Sys.setenv("RETICULATE_PYTHON"='/FILEPATH')
library(jobmonr, lib.loc="/FILEPATH")




## DEFINE ARGS -----------------------------------------------------------
parser <- ArgumentParser()
# shared and era2melt arguments
parser$add_argument("--year_start", help = "first of years to evaluate",
                    default = 1990, type = "integer")
parser$add_argument("--year_end", help = "last of years to evaluate",
                    default = 2021, type = "integer")
parser$add_argument("--release_id", help = "GBD release ID",
                    default = 10, type = "integer")
parser$add_argument("--proj", help = "project to run on",
                    default = 'NAME', type = "character")
parser$add_argument("--exp_outdir", help = "output path for era2melt step",
                    default = '/FILEPATH/', type = "character")
parser$add_argument("--resume", help = "whether you're resuming an existing pipeline",
                    default = F, type = "logical")
parser$add_argument("--config.path", help = "path to config file",
                    default = '/FILEPATH/config', type = "character")
parser$add_argument("--config.version", help = "name of config file version",
                    default = 'config20220728b', type = "character")

# rrMax arguments
parser$add_argument("--popType", help = "source of population numbers, either fixed or annual",
                    default = 'annual', type = "character")
parser$add_argument("--missingOnly", help = "whether to replace all files or only fill in those that are missing",
                    default = T, type = "logical")
parser$add_argument("--rrMax_outRoot", help = "directory to which results will be output",
                    default = "/FILEPATH/rr/", type = "character")

# tmrelCalculator arguments
parser$add_argument("--tmrel_min", help = "lower bound of TMREL",
                    default = 6.6, type = "integer")
parser$add_argument("--tmrel_max", help = "upper bound of TMREL",
                    default = 34.6, type = "integer")
parser$add_argument("--ltrim", help = "lower trim percentile for TMREL",
                    default = 0.05, type = "integer")
parser$add_argument("--utrim", help = "upper trim percentile for TMREL",
                    default = 0.95, type = "integer")
parser$add_argument("--indir", help = "folder to pull relative risk samples from",
                    #default = "/FILEPATH/rr/updated_with_gamma_0.05-0.95/", type = "character")
                    default = "/FILEPATH/rr/updated_with_gamma_0.05-0.95/", type = "character")
parser$add_argument("--updateConfig", help = "whether to update config file",
                    default = F, type = "logical")
parser$add_argument("--codAppend", help = "whether to append cause of death values in TMREL calcs",
                    default = F, type = "logical")
parser$add_argument("--saveGlobalToCC", help = "whether to save global to CC in TMREL calcs",
                    default = F, type = "logical")

# pafCalc arguments
parser$add_argument("--scenario", help = "name of scenario to run",
                    default = 'rcp45', type = "character")
parser$add_argument("--PAF_outRoot", help = "root of output path",
                    default = '/FILEPATH/', type = "character")
parser$add_argument("--outVersion", help = "name of output version",
                    default = 'version_15.0', type = "character")

args <- parser$parse_args()
list2env(args, environment()); rm(args)


# Get year list and the year part of the GBD round title
yearList <- year_start:year_end
gbd_round_year <- strsplit(get_ids("release")[release_id == .GlobalEnv$release_id, release_name], " ")[[1]][2]




## BODY ------------------------------------------------------------------
# Set up Jobmon pipeline ---------------------------------------
# Create a tool
my_tool <- tool(name='temperature_tool')
Sys.setenv(NAME_CPUS_PER_TASK = 1)

# Set the tool compute resources
jobmonr::set_default_tool_resources(
  tool=my_tool,
  default_cluster_name='NAME',
  resources=list(
    'cores'=1,
    'queue'='all.q',
    'runtime'="2m",
    'memory'='1G'
  )
)

# Bind a workflow to the tool
wf <- workflow(my_tool,
               #workflow_args=paste0('temperature_workflow_', Sys.Date()),
               workflow_args=paste0('temperature_workflow_2022-09-17'),
               name='temperature_workflow')

# Create templates
era2melt_tt <- task_template(tool=my_tool,
                             template_name='era2melt_templ',
                             command_template=paste("OMP_NUM_THREADS=8 {rshell} -s {script} --loc_id {loc_id} --year {year} --gbd_round_year {gbd_round_year}",
                                                    "--outDir {outDir}"),
                             task_args = list('outDir', 'gbd_round_year'),
                             node_args = list('loc_id', 'year'),
                             op_args = list('rshell', 'script'))
intermission_tt <- task_template(tool=my_tool,
                                 template_name='intermission_templ',
                                 command_template='echo {echo_str}',
                                 node_args=list('echo_str'))
tmrelCalculator_tt <- task_template(tool=my_tool,
                                    template_name='tmrelCalculator_templ',
                                    command_template=paste("OMP_NUM_THREADS=8 {rshell} -s {script} --loc_id {loc_id} --cause_list {cause_list}",
                                                           "--acause_list {acause_list} --outdir {outdir} --indir {indir} --tmrel_min {tmrel_min}",
                                                           "--tmrel_max {tmrel_max} --year_list {year_list} --config_file {config_file} --job_name {job_name}",
                                                           "--saveGlobalToCC {saveGlobalToCC}"),
                                    task_args = list('cause_list', 'acause_list', 'outdir', 'indir', 'tmrel_min', 'tmrel_max', 'config_file', 'saveGlobalToCC'),
                                    node_args = list('loc_id', 'year_list', 'job_name'),
                                    op_args = list('rshell', 'script'))
rrMax_tt <- task_template(tool=my_tool,
                          template_name='rrMax_templ',
                          command_template=paste("OMP_NUM_THREADS=8 {rshell} -s {script} --causeList {causeList} --outdir {outdir} --popType {popType}",
                                                 "--tempZone {tempZone} --rrDir {rrDir} --tmrelDir {tmrelDir}",
                                                 "--description {description} --release_id {release_id} --yearStart {yearStart} --yearEnd {yearEnd}"),
                          task_args = list('outdir', 'popType', 'rrDir', 'tmrelDir', 'description', 'release_id', 'yearStart', 'yearEnd'),
                          node_args = list('causeList', 'tempZone'),
                          op_args = list('rshell', 'script'))
assembleComplete_tt <- task_template(tool=my_tool,
                                     template_name='assembleComplete_templ',
                                     command_template=paste("OMP_NUM_THREADS=1 {rshell} -s {script} --config_file {config_file} --outdir {outdir}"),
                                     task_args = list('config_file', 'outdir'),
                                     op_args = list('rshell', 'script'))
pafCalc_tt <- task_template(tool=my_tool,
                            template_name='pafCalc_templ',
                            command_template=paste("OMP_NUM_THREADS=8 {rshell} -s {script} --loc_id {loc_id} --year_list {year_list}", 
                                                   "--outDir {outDir} --proj {proj} --scenario {scenario} --job_name {job_name}",
                                                   "--release_id {release_id} --config_version {config_version} --exp_outdir {exp_outdir}",
                                                   "--rrMax_outRoot {rrMax_outRoot} --rrDir {rrDir} --tmrelRoot {tmrelRoot}"),
                            task_args = list('outDir', 'scenario', 'year_list', 'release_id', 'config_version', 'exp_outdir',
                                             'rrMax_outRoot', 'rrDir', '--tmrelRoot'),
                            node_args = list('loc_id', 'job_name'),
                            op_args = list('rshell', 'script', 'proj'))
addYear_tt <- task_template(tool=my_tool,
                            template_name='addYear_templ',
                            command_template=paste("OMP_NUM_THREADS=1 {rshell} -s {script} --paf_version {paf_version} --outDir {outDir}"),
                            task_args = list('outDir'),
                            node_args = list('paf_version'),
                            op_args = list('rshell', 'script'))
pafUpload_tt <- task_template(tool=my_tool,
                              template_name='pafUpload_templ',
                              command_template=paste("OMP_NUM_THREADS=24 {rshell} -s {script} --paf_version {paf_version} --outDir {outDir}",
                              "--year_start {year_start} --year_end {year_end} --release_id {release_id}"),
                              task_args = list('outDir', 'release_id'),
                              node_args = list('paf_version', 'year_start', 'year_end'),
                              op_args = list('rshell', 'script'))


# Set the script task template compute resources
era2melt_tt_resources <- jobmonr::set_default_template_resources(
  task_template=era2melt_tt,
  default_cluster_name='NAME',
  resources=list(
    'cores'=8,
    'queue'='all.q',
    'project'='NAME',
    'runtime'="15m",
    #'memory'='150G',
    'memory'='40G',
    'constraints'='archive'
  )
)
tmrelCalculator_tt_resources <- jobmonr::set_default_template_resources(
  task_template=tmrelCalculator_tt,
  default_cluster_name='NAME',
  resources=list(
    'cores'=8,
    'queue'='all.q',
    'project'='NAME',
    'runtime'="10m",
    #'memory'='250G'
    'memory' = '20G'
  )
)
rrMax_tt_resources <- jobmonr::set_default_template_resources(
  task_template=rrMax_tt,
  default_cluster_name='NAME',
  resources=list(
    'cores'=8,
    'queue'='all.q',
    'project'='NAME',
    'runtime'="10m",
    #'memory'='100G'
    'memory'='20G'
  )
)
assembleComplete_tt_resources <- jobmonr::set_default_template_resources(
  task_template=assembleComplete_tt,
  default_cluster_name='NAME',
  resources=list(
    'cores'=1,
    'queue'='all.q',
    'project'='NAME',
    'runtime'="10m",
    'memory'='20G'
  )
)
pafCalc_tt_resources <- jobmonr::set_default_template_resources(
  task_template=pafCalc_tt,
  default_cluster_name='NAME',
  resources=list(
    'cores'=8,
    'queue'='all.q',
    'project'='NAME',
    'runtime'="20m",
    'memory'='35G'
  )
)
addYear_tt_resources <- jobmonr::set_default_template_resources(
  task_template=addYear_tt,
  default_cluster_name='NAME',
  resources=list(
    'cores'=1,
    'queue'='all.q',
    'project'='NAME',
    'runtime'="15m",
    'memory'='10G'
  )
)
pafUpload_tt_resources <- jobmonr::set_default_template_resources(
  task_template=pafUpload_tt,
  default_cluster_name='NAME',
  resources=list(
    'cores'=1,
    'queue'='all.q',
    'project'='NAME',
    'runtime'="72h",
    'memory'='90G'
  )
)


# Make task list
all_tasks <- list()

## Generate exposure calculation tasks -----------------------------------
# Get locations to run on
locMeta <- get_location_metadata(location_set_id = 35, release_id = release_id)
loc_ids <- locMeta[(is_estimate==1 & most_detailed==1) | level==3 , location_id]
loc_ids <- loc_ids[order(loc_ids)]

# Generate arg list for jobs
arg.list <- list(loc_id = loc_ids, year = yearList, outdir = exp_outdir)
name.stub <- "melt"
name.args <- c("loc_id", "year")

# Set R shell
#r.shell <- "/FILEPATH/health_fin_forecasting_shell_singularity.sh"
r.shell <- "/FILEPATH/execRscript.sh"

# Convert the list of arguments into a data table with all combinations of all arguments
arg.table <- data.table(merge = 1, job_name = "")
for (arg in names(arg.list)) {
  temp <- data.table(unlist(arg.list[arg]), 1)
  names(temp) <- c(paste0("arg.", arg), "merge")

  arg.table <- merge(arg.table, temp, by = "merge", allow.cartesian = T, all = T)
}

arg.vars <- grep("^arg.", names(arg.table), value = T)
arg.table[, args := do.call(paste, .SD), .SDcols = c(arg.vars, "job_name")]

arg.table[, merge := NULL]


exp_subtasks <- list()
exp_numtasks <- nrow(arg.table)
print("Generating exposure tasks")
pb = txtProgressBar(min = 0, max = exp_numtasks, initial = 0, style = 3)
for (i in 1:exp_numtasks) {
  # Add the associated script task
  print(sprintf("Building era2melt task %d of %d", i, exp_numtasks))
  task <- task(task_template=era2melt_tt,
              name = arg.table$job_name[i],
              rshell = r.shell,
              script = paste0("exposure_processing/era2melt.R"),
              outDir = exp_outdir,
              gbd_round_year = gbd_round_year,
              loc_id = arg.table$arg.loc_id[i],
              year = arg.table$arg.year[i]
  )
  exp_subtasks <- append(exp_subtasks, task)
  all_tasks <- append(all_tasks, task)
  setTxtProgressBar(pb,i)
}
close(pb)


## Generate intermission task ----------------------------------
# This task exists purely to reduce processing time when building the pipeline.
# It allows the subsequent TMREL tasks to only have one dependency rather than each having some 35000.
intermission_task <- task(task_template=intermission_tt,
              name='intermission',
              upstream_tasks=exp_subtasks,
              echo_str="intermission")
all_tasks <- append(all_tasks, intermission_task)


## Generate TMREL calculation tasks ----------------------------
# Read in config file elements
config_file <- file.path(config.path, paste0(config.version, ".csv"))
csv2objects(config_file, exclude = ls())
#indir <- paste0(rrRootDir, rrVersion, "/")

if (updateConfig==T) {
  runDate <- as.character(format(Sys.Date(), "%Y%m%d"))
  description <- paste0(rrVersion, "_", project, "_", runDate)

  outdir <- file.path(tmrelRootDir, paste0(config.version, "_", runDate))

  update.config(update = rbind(data.frame(object = "description", value = description, format = "str"),
                               data.frame(object = "tmrelDir", value = outdir, format = "str"),
                               data.frame(object = "rrDir", value = indir, format = "str")),
                in.file = config_file, replace = T)
  
  tmrelDir <- copy(outdir) # Just making sure these two values are properly updated. May not be necessary.
  rrDir <- copy(indir)
} else {
  outdir <- tmrelDir
}


dir.create(outdir, recursive = T)


# Get cause metadata
causeMeta <- get_cause_metadata(cause_set_id=4, gbd_round_id=gbd_round_id)
causeMeta <- causeMeta[acause %in% causeList & level==3, ][, .(cause_id, cause_name, acause)]

cause_list <- paste(causeMeta$cause_id, collapse = ",")
acause_list <- paste(causeMeta$acause, collapse = ",")

# Get location metadata
if (project=="gbd") {
  locMeta <- get_location_metadata(location_set_id = 35, gbd_round_id = gbd_round_id)
  loc_ids <- locMeta[is_estimate==1 | level<=3, location_id]
  loc_ids <- loc_ids[order(loc_ids)]

  yearList <- paste(yearList, collapse = ",")
  name.args <- c("loc_id", "year")

  project <- "NAME"

} else {
  if (codAppend==T) {
    folder <- paste(rev(rev(strsplit(cod_source, "/")[[1]])[-1]), collapse = "/")
    files  <- list.files(folder)

    cod <- rbindlist(lapply(files, function(file) {fread(paste0(folder, "/", file))}), fill = T)
    cod <- cod[, "V1" := NULL]

    write.csv(cod, cod_source, row.names = F)
  }
  cod <- fread(cod_source)
  loc_ids <- unique(cod$location_id)
  loc_ids <- loc_ids[order(loc_ids)]
  yearList <- unique(cod$year_id)
  saveGlobalToCC <- F


  name.args <- c("loc_id", "year")

  project <- "PROJECT"
}


# Create list of arguments
yearList <- strsplit(yearList, ",")[[1]]
arg.list <- list(loc_id = loc_ids, cause_list = cause_list, acause_list = acause_list, outdir = outdir, indir = indir,
                 tmrel_min = tmrel_min, tmrel_max = tmrel_max, year = yearList, config = config_file, saveGlobalToCC = saveGlobalToCC)
name.stub <- "tmrel"

# Set up run environment
r.shell <- "/FILEPATH/execRscript.sh"


# Convert the list of arguments into a data table with all combinations of all arguments
arg.table <- data.table(merge = 1, missing = 1, running = 0, failed = 0, attempts = 0, job_name = "")
for (arg in names(arg.list)) {
  temp <- data.table(unlist(arg.list[arg]), 1)
  names(temp) <- c(paste0("arg.", arg), "merge")

  arg.table <- merge(arg.table, temp, by = "merge", allow.cartesian = T, all = T)
}

arg.vars <- grep("^arg.", names(arg.table), value = T)
arg.table[, job_name := do.call(paste, c(name.stub, .SD, sep = "_")), .SDcols = paste0("arg.", name.args)]
arg.table[, args := do.call(paste, .SD), .SDcols = c(arg.vars, "job_name")]

arg.table[, merge := NULL]

# Generate TMREL calculation tasks
tmrel_subtasks <- list()
tmrel_numtasks <- nrow(arg.table)
print("Generating TMREL tasks")
pb = txtProgressBar(min = 0, max = tmrel_numtasks, initial = 0, style = 3)
for (i in 1:tmrel_numtasks) {
  # Add the associated script task
  #print(sprintf("Building TMREL task %d of %d", i, tmrel_numtasks))
  task <- task(task_template=tmrelCalculator_tt,
               name = arg.table$job_name[i],
               upstream_tasks = list(intermission_task),
               rshell = r.shell,
               script = "TMREL_processing/tmrelCalculator.R",
               cause_list = cause_list,
               acause_list = acause_list,
               outdir = outdir,
               indir = indir,
               tmrel_min = tmrel_min,
               tmrel_max = tmrel_max,
               config_file = config_file,
               saveGlobalToCC = saveGlobalToCC,
               loc_id = arg.table$arg.loc_id[i],
               year_list = arg.table$arg.year[i],
               job_name = arg.table$job_name[i]
  )
  tmrel_subtasks <- append(tmrel_subtasks, task)
  all_tasks <- append(all_tasks, task)
  setTxtProgressBar(pb,i)
}
close(pb)


## Generate second intermission --------------------------------
int_task2 <- task(task_template=intermission_tt,
                          name='intermission2',
                          upstream_tasks=tmrel_subtasks,
                          echo_str="intermission 2")
all_tasks <- append(all_tasks, int_task2)


## Generate rrMax calculation tasks ----------------------------
# Set up run environment
#csv2objects(config_file, exclude = ls())
r.shell <- "/FILEPATH/execRscript.sh"

outdir <- paste0(rrMax_outRoot, config.version)
dir.create(outdir)
dir.create(paste0(outdir, "/rrMax"))


arg.list <- list(cause = causeList, version = config.version, popType = popType, zone = 6:28, rrDir = rrDir, tmrelDir = tmrelDir, description = description)
name.stub <- "rrMax"
name.args <- c("cause", "zone")

arg.table <- data.table(merge = 1, missing = 1, running = 0, failed = 0, attempts = 0, job_name = "")
for (arg in names(arg.list)) {
  temp <- data.table(unlist(arg.list[arg]), 1)
  names(temp) <- c(paste0("arg.", arg), "merge")
  
  arg.table <- merge(arg.table, temp, by = "merge", allow.cartesian = T, all = T)
}

arg.vars <- grep("^arg.", names(arg.table), value = T)
arg.table[, job_name := do.call(paste, c(name.stub, .SD, sep = "_")), .SDcols = paste0("arg.", name.args)]
arg.table[, args := do.call(paste, .SD), .SDcols = c(arg.vars, "job_name")]

arg.table[, merge := NULL]

if (missingOnly) {
  arg.table <- arg.table[missing == 1]
}


# Generate rrMax calculation tasks
rrMax_subtasks <- list()
rrMax_numtasks <- nrow(arg.table)
print("Generating rrMax tasks")
pb = txtProgressBar(min = 0, max = rrMax_numtasks, initial = 0, style = 3)
for (i in 1:rrMax_numtasks) {
  # Add the associated script task
  #print(sprintf("Building rrMax task %d of %d", i, rrMax_numtasks))
  task <- task(task_template=rrMax_tt,
               name = arg.table$job_name[i],
               upstream_tasks = list(int_task2),
               rshell = r.shell,
               script = paste0("RRmax_processing/rrMaxCalc.R"),
               outdir = outdir,
               popType = popType,
               rrDir = rrDir,
               tmrelDir = tmrelDir,
               description = description,
               release_id = release_id,
               yearStart = year_start,
               yearEnd = year_end,
               causeList = arg.table$arg.cause[i],
               tempZone = arg.table$arg.zone[i]
  )
  rrMax_subtasks <- append(rrMax_subtasks, task)
  all_tasks <- append(all_tasks, task)
  setTxtProgressBar(pb,i)
}
close(pb)



## Generate third intermission --------------------------------
int_task3 <- task(task_template=intermission_tt,
                          name='intermission3',
                          upstream_tasks=rrMax_subtasks,
                          echo_str="intermission 3")
all_tasks <- append(all_tasks, int_task3)



## Generate PAF calculation tasks ------------------------------
# Get location metadata
if (proj=="forecasting") {
  outdir <- paste0(PAF_outRoot, outVersion)
  
  cod <- fread(cod_source)
  loc_ids <- unique(cod$location_id)
  
  loc_ids <- loc_ids[order(loc_ids)]
  project <- "NAME"
} else {
  outdir <- paste0(PAF_outRoot, outVersion)
  locSet <- 35
  locMeta <- get_location_metadata(location_set_id = locSet, release_id = release_id)
  loc_ids <- locMeta[(is_estimate==1 & most_detailed==1) | level==3 , location_id]
  loc_ids <- loc_ids[order(loc_ids)]
  project <- "NAME"
  
}

dir.create(outdir, recursive = T)
dir.create(paste0(outdir, "/heat"))
dir.create(paste0(outdir, "/cold"))
dir.create(paste0(outdir, "/sevs"))
dir.create(paste0(outdir, "/sevs/raw"))


#yearList <- strsplit(yearList, ",")
arg.list <- list(loc_id = loc_ids, year = yearList, outdir = outdir, proj = proj, scenario = scenario, config_file = config_file, calcSevs = T)
name.stub <- "paf"
name.args <- c("loc_id", "year")


# Set R shell to use
r.shell <- "/FILEPATH/execRscript.sh"

# Convert the list of arguments into a data table with all combinations of all arguments
arg.table <- data.table(merge = 1, missing = 1, running = 0, failed = 0, attempts = 0, job_name = "")
for (arg in names(arg.list)) {
  temp <- data.table(unlist(arg.list[arg]), 1)
  names(temp) <- c(paste0("arg.", arg), "merge")
  
  arg.table <- merge(arg.table, temp, by = "merge", allow.cartesian = T, all = T)
}

arg.vars <- grep("^arg.", names(arg.table), value = T)
arg.table[, job_name := do.call(paste, c(name.stub, .SD, sep = "_")), .SDcols = paste0("arg.", name.args)]
arg.table[, args := do.call(paste, .SD), .SDcols = c(arg.vars, "job_name")]

arg.table[, merge := NULL]

# Generate pafCalc subtasks
calc_subtasks <- list()
pafcalc_numtasks <- nrow(arg.table)
print("Generating pafCalc tasks")
pb = txtProgressBar(min = 0, max = pafcalc_numtasks, initial = 0, style = 3)
for (i in 1:pafcalc_numtasks) {
  # Add the associated script task
  #print(sprintf("Building pafCalc task %d of %d", i, pafcalc_numtasks))
  task <- task(task_template=pafCalc_tt,
               name = arg.table$job_name[i],
               upstream_tasks = list(int_task3),
               rshell = r.shell,
               script = paste0("PAF_processing/pafCalc_sevFix.R"),
               proj = proj,
               outDir = outdir,
               scenario = scenario,
               release_id = release_id,
               config_version = config.version,
               exp_outdir = exp_outdir,
               rrMax_outRoot = rrMax_outRoot,
               rrDir = indir,
               tmrelRoot = tmrelDir,
               year_list = arg.table$arg.year[i],
               loc_id = arg.table$arg.loc_id[i],
               job_name = arg.table$job_name[i]
  )
  calc_subtasks <- append(calc_subtasks, task)
  all_tasks <- append(all_tasks, task)
  setTxtProgressBar(pb,i)
}
close(pb)

# Generate addYear task
addYear_task <- task(task_template=addYear_tt,
                     name = "addYear_temper",
#                     upstream_tasks = calc_subtasks,
                     rshell = r.shell,
                     script = paste0("PAF_processing/addYear.R"),
                     outDir = PAF_outRoot,
                     paf_version = strsplit(outVersion, "_")[[1]][2]
)
all_tasks <- append(all_tasks, addYear_task)

# Generate pafUpload task
pafUpload_task <- task(task_template=pafUpload_tt,
                       name = "pafUpload_temper",
                       upstream_tasks = list(addYear_task),
                       rshell = r.shell,
                       script = paste0("PAF_processing/pafUpload.R"),
                       outDir = PAF_outRoot,
                       release_id = release_id,
                       paf_version = strsplit(outVersion, "_")[[1]][2],
                       year_start = year_start,
                       year_end = year_end
)
all_tasks <- append(all_tasks, pafUpload_task)



# Add tasks to the workflow ------------------------------------
wf <- add_tasks(wf, all_tasks)

# Run tasks ----------------------------------------------------
print(paste0("Running pipeline with name ", wf$workflow_args, " and resume ", resume))
wfr <- run(
  workflow=wf,
  resume=resume,
  seconds_until_timeout=604800)
