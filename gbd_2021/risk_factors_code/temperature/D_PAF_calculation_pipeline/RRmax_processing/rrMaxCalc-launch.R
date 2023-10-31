## Launches relative risk max calculations.
# source("/FILEPATH/rrMaxCalc-launch.R", echo = T)

## SYSTEM SETUP ----------------------------------------------------------
# Clear memory
rm(list=ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]


# Drives 
j <- if (os == "Linux") "/home/j/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("/homes/", user, "/") else if (os == "Windows") "H:/"


# Base filepaths
work_dir <- paste0(j, '/FILEPATH/')
share_dir <- '/FILEPATH/' # only accessible from the cluster
code_dir <- if (os == "Linux") paste0("/FILEPATH/", user, "/") else if (os == "Windows") ""


## LOAD DEPENDENCIES -----------------------------------------------------
library("data.table")
library("dplyr")
library(argparse)
source(paste0(code_dir, "FILEPATH/csv2objects.R"))
library(jobmonr)




## DEFINE ARGS -----------------------------------------------------------
parser <- ArgumentParser()
parser$add_argument("--proj", help = "project to run on",
                    default = 'proj_erf', type = "character")
parser$add_argument("--config.path", help = "path to config file",
                    default = '/FILEPATH/config', type = "character")
parser$add_argument("--config.version", help = "name of config file version",
                    default = 'config20210706', type = "character")
parser$add_argument("--popType", help = "source of population numbers, either fixed or annual",
                    default = 'annual', type = "character")
parser$add_argument("--missingOnly", help = "whether to replace all files or only fill in those that are missing",
                    default = T, type = "logical")
parser$add_argument("--outRoot", help = "directory to which results will be output",
                    default = "/FILEPATH/rr/", type = "character")
parser$add_argument("--yearStart", help = "first year of run",
                    default = 1990, type = "integer")
parser$add_argument("--yearEnd", help = "last year of run",
                    default = 2020, type = "integer")
parser$add_argument("--resume", help = "whether you're resuming an existing pipeline",
                    default = 0, type = "integer")
args <- parser$parse_args()
list2env(args, environment()); rm(args)


config.file <- file.path(config.path, paste0(config.version, ".csv"))

csv2objects(config.file, exclude = ls())

version <- config.version


## BODY ------------------------------------------------------------------
# set up run environment
r.shell <- "/FILEPATH/execRscript.sh"

outdir <- paste0(outRoot, version)
dir.create(outdir)
dir.create(paste0(outdir, "/rrMax"))


arg.list <- list(cause = causeList, version = version, popType = popType, zone = 6:28, rrDir = rrDir, tmrelDir = tmrelDir, description = description)
name.stub <- "rrMax"
name.args <- c("cause", "zone")

arg.table <- data.table(merge = 1, missing = 1, running = 0, failed = 0, attempts = 0, job.name = "")
for (arg in names(arg.list)) {
  temp <- data.table(unlist(arg.list[arg]), 1)
  names(temp) <- c(paste0("arg.", arg), "merge")
  
  arg.table <- merge(arg.table, temp, by = "merge", allow.cartesian = T, all = T)
}

arg.vars <- grep("^arg.", names(arg.table), value = T)
arg.table[, job.name := do.call(paste, c(name.stub, .SD, sep = "_")), .SDcols = paste0("arg.", name.args)]
arg.table[, args := do.call(paste, .SD), .SDcols = c(arg.vars, "job.name")]

arg.table[, merge := NULL]

if (missingOnly) {
  arg.table <- arg.table[missing == 1]
}



# Set up Jobmon pipeline ---------------------------------------
# Create a tool
my_tool <- tool(name='temper_rrmax_calc_tool')

# Bind a workflow to the tool
wf <- workflow(my_tool,
               workflow_args=paste0('temper_rrmax_workflow_', Sys.Date()),
               name='temper_rrmax_workflow')

# Create templates
rrMax_tt <- task_template(tool=my_tool,
                            template_name='rrMax_templ',
                            command_template=paste("{rshell} -s {script} --causeList {causeList} --outdir {outdir} --popType {popType} --tempZone {tempZone} --rrDir {rrDir} --tmrelDir {tmrelDir}",
                                                   "--description {description} --release_id {release_id} --yearStart {yearStart} --yearEnd {yearEnd}"),
                            task_args = list('outdir', 'popType', 'rrDir', 'tmrelDir', 'description', 'release_id', 'yearStart', 'yearEnd'),
                            node_args = list('causeList', 'tempZone'),
                            op_args = list('rshell', 'script'))
assembleComplete_tt <- task_template(tool=my_tool,
                            template_name='assembleComplete_templ',
                            command_template=paste("{rshell} -s {script} --config.file {config.file} --outdir {outdir}"),
                            task_args = list('config.file', 'outdir'),
                            op_args = list('rshell', 'script'))


# Set the script task template compute resources
rrMax_tt_resources <- jobmonr::set_default_template_resources(
  task_template=rrMax_tt,
  default_cluster_name='NAME',
  resources=list(
    'cores'=8,
    'queue'='all.q',
    'runtime'="5m",
    'memory'='100G'
  )
)
assembleComplete_tt_resources <- jobmonr::set_default_template_resources(
  task_template=assembleComplete_tt,
  default_cluster_name='NAME',
  resources=list(
    'cores'=1,
    'queue'='all.q',
    'runtime'="10m",
    'memory'='20G'
  )
)


# Make tasks ---------------------------------------------------
all_tasks <- list()

# Generate RRmax calculation tasks
calc_subtasks <- list()
for (i in 1:nrow(arg.table)) {
  # Add the associated script task
  task <- task(task_template=rrMax_tt,
               name = arg.table$job.name[i],
               rshell = r.shell,
               script = paste0(code_dir, "FILEPATH/rrMaxCalc.R"),
               outdir = outdir,
               popType = popType,
               rrDir = rrDir,
               tmrelDir = tmrelDir,
               description = description,
               release_id = release_id,
               yearStart = yearStart,
               yearEnd = yearEnd,
               causeList = arg.table$arg.cause[i],
               tempZone = arg.table$arg.zone[i]
  )
  calc_subtasks <- append(calc_subtasks, task)
  all_tasks <- append(all_tasks, task)
}

# Generate assembleComplete task
assembleComplete_task <- task(task_template=assembleComplete_tt,
                     name = "assembleComplete_temper",
                     upstream_tasks = calc_subtasks,
                     rshell = r.shell,
                     script = paste0(code_dir, "FILEPATH/rrMaxAssembleComplete.R"),
                     config.file = config.file,
                     outdir = outdir,
)
all_tasks <- append(all_tasks, assembleComplete_task)


# Add tasks to the workflow
wf <- add_tasks(wf, subtasks)

# Run it
wfr <- run(
  workflow=wf,
  resume=resume)
