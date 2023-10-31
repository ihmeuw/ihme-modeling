## Launches PAF calculations for temperature.
# source("/FILEPATH/tmrelCalculator_launch.R", echo = T)

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
library(data.table)
library(ggplot2)
library(argparse)
source("/FILEPATH/r/get_location_metadata.R")
source("/FILEPATH/r/get_cause_metadata.R")
source(paste0(code_dir, "FILEPATH/csv2objects.R"))
Sys.setenv("RETICULATE_PYTHON"='/FILEPATH/')  # Set the Python interpreter path
library(jobmonr)




## DEFINE ARGS -----------------------------------------------------------
parser <- ArgumentParser()
parser$add_argument("--tmrel_min", help = "lower bound of TMREL",
                    default = 6.6, type = "integer")
parser$add_argument("--tmrel_max", help = "upper bound of TMREL",
                    default = 34.6, type = "integer")
parser$add_argument("--ltrim", help = "lower trim percentile",
                    default = 0.05, type = "integer")
parser$add_argument("--utrim", help = "upper trim percentile",
                    default = 0.95, type = "integer")
parser$add_argument("--updateConfig", help = "whether to update config file",
                    default = F, type = "logical")
parser$add_argument("--codAppend", help = "whether to append cause of death values",
                    default = F, type = "logical")
parser$add_argument("--saveGlobalToCC", help = "whether to save global to CC",
                    default = F, type = "logical")
parser$add_argument("--config.path", help = "path to config file",
                    default = '/FILEPATH/config', type = "character")
parser$add_argument("--config.version", help = "name of config file version",
                    default = 'configGoalkeepers20211230', type = "character")
parser$add_argument("--year_start", help = "first of years to evaluate",
                    default = 1990, type = "integer")
parser$add_argument("--year_end", help = "last of years to evaluate",
                    default = 2019, type = "integer")
parser$add_argument("--resume", help = "whether you're resuming an existing pipeline",
                    default = 0, type = "integer")
args <- parser$parse_args()
list2env(args, environment()); rm(args)


config.file <- file.path(config.path, paste0(config.version, ".csv"))

csv2objects(config.file, exclude = ls())

yearList <- year_start:year_end

indir <- paste0(rrRootDir, rrVersion, "/")

if (updateConfig==T) {
  runDate <- as.character(format(Sys.Date(), "%Y%m%d"))
  description <- paste0(rrVersion, "_", project, "_", runDate)

  outdir <- file.path(tmrelRootDir, paste0(config.version, "_", runDate))

  update.config(update = rbind(data.frame(object = "description", value = description, format = "str"),
                               data.frame(object = "tmrelDir", value = outdir, format = "str"),
                               data.frame(object = "rrDir", value = indir, format = "str")),
                in.file = config.file, replace = T)
} else {
  outdir <- tmrelDir
}


dir.create(outdir, recursive = T)


## GET CAUSE META-DATA ---------------------------------------------------
causeMeta <- get_cause_metadata(cause_set_id=4, gbd_round_id=gbd_round_id)
causeMeta <- causeMeta[acause %in% causeList & level==3, ][, .(cause_id, cause_name, acause)]

cause_list <- paste(causeMeta$cause_id, collapse = ",")
acause_list <- paste(causeMeta$acause, collapse = ",")

## GET LOCATION META-DATA ------------------------------------------------

if (project=="gbd") {
  locMeta <- get_location_metadata(location_set_id = 35, gbd_round_id = gbd_round_id)
  loc_ids <- locMeta[is_estimate==1 | level<=3, location_id]
  loc_ids <- loc_ids[order(loc_ids)]

  yearList <- paste(yearList, collapse = ",")
  name.args <- "loc_id"

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

  project <- "NAME"
}


## JOBMON PIPELINE -------------------------------------------------------
arg.list <- list(loc_id = loc_ids, cause_list = cause_list, acause_list = acause_list, outdir = outdir, indir = indir,
                 tmrel_min = tmrel_min, tmrel_max = tmrel_max, year = yearList, config = config.file, saveGlobalToCC = saveGlobalToCC)


name.stub <- "tmrel"


# set up run environment
r.shell <- "/FILEPATH/health_fin_forecasting_shell_singularity.sh"


if (length(name.args)==1) {
  mem <- "250G"
  slots <- 8
} else {
  mem <- "25G"
  slots <- 4
}

# Convert the list of arguments into a data table with all combinations of all arguments
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


# Set up Jobmon pipeline ---------------------------------------
# Create a tool
my_tool <- tool(name='temper_tmrelCalculator_tool')

# Bind a workflow to the tool
wf <- workflow(my_tool,
               workflow_args=paste0('temper_tmrelCalculator_workflow_', Sys.Date()),
               name='temper_tmrelCalculator_workflow')

# Create templates
tmrelCalculator_tt <- task_template(tool=my_tool,
                             template_name='tmrelCalculator_templ',
                             command_template=paste("{rshell} -s {script} --loc_id {loc_id} --cause_list {cause_list} --acause_list {acause_list} --outdir {outdir} --indir {indir}",
                                                    "--tmrel_min {tmrel_min} --tmrel_max {tmrel_max} --year_list {year_list} --config.file {config.file} --job.name {job.name}",
                                                    "--saveGlobalToCC {saveGlobalToCC}"),
                             task_args = list('cause_list', 'acause_list', 'outdir', 'indir', 'tmrel_min', 'tmrel_max', 'config.file', 'saveGlobalToCC'),
                             node_args = list('loc_id', 'year_list', 'job.name'),
                             op_args = list('rshell', 'script'))


# Set the script task template compute resources
tmrelCalculator_tt_resources <- jobmonr::set_default_template_resources(
  task_template=tmrelCalculator_tt,
  default_cluster_name='NAME',
  resources=list(
    'cores'=slots,
    'queue'='all.q',
    'runtime'="10m",
    'memory'=mem
  )
)


# Make tasks ---------------------------------------------------
all_tasks <- list()

# Generate TMREL calculation tasks
calc_subtasks <- list()
for (i in 1:nrow(arg.table)) {
  # Add the associated script task
  task <- task(task_template=tmrelCalculator_tt,
               name = arg.table$job.name[i],
               rshell = r.shell,
               script = "/FILEPATH/tmrelCalculator.R",
               cause_list = cause_list,
               acause_list = acause_list,
               outdir = outdir,
               indir = indir,
               tmrel_min = tmrel_min,
               tmrel_max = tmrel_max,
               config.file = config.file,
               saveGlobalToCC = saveGlobalToCC,
               loc_id = arg.table$arg.loc_id[i],
               year_list = arg.table$arg.year[i],
               job.name = arg.table$job.name[i]
  )
  calc_subtasks <- append(calc_subtasks, task)
  all_tasks <- append(all_tasks, task)
}


# Add tasks to the workflow
wf <- add_tasks(wf, subtasks)

# Run tasks ----------------------------------------------------
wfr <- run(
  workflow=wf,
  resume=resume)




## Make plots for analysis (manual only)
#tmrel <- do.call(rbind, lapply(loc_ids, function(loc_id) {fread(paste0(outdir, "/tmrel_", loc_id, "_summaries.csv"))}))
if (F) {
  tmrel <- do.call(rbind, lapply(yearList, function(y) {do.call(rbind, lapply(loc_ids, function(loc_id) {fread(paste0(outdir, "/tmrel_", loc_id, "_", y, "_summaries.csv"))}))}))
  
  
  data.frame(year_id = c(1990:2015, 2017, 2019),
             do.call(rbind, lapply(c(1990:2015, 2017, 2019), function(y) {summary(tmrel$tmrelMean[tmrel$year_id==y])})))
  
  tmrel %>% ggplot(aes(x = meanTempCat, y = tmrelMean)) + geom_point(alpha = 0.05) +
    geom_smooth() + facet_wrap(~year_id) + theme_minimal()
  
  tmrel %>% filter(year_id==2019) %>% ggplot(aes(x = meanTempCat, y = tmrelMean)) + geom_point(alpha = 0.05) + geom_smooth() + theme_minimal()
  
  tmrel %>% filter(year_id==2019 & meanTempCat>=6) %>% ggplot(aes(x = meanTempCat, y = tmrelMean, color = as.factor(location_id))) +
    geom_point(alpha = 0.05) + geom_smooth(se = F) + guides(color = FALSE) + theme_minimal()
  
  
  tmrel %>% filter(year_id==2019 & tmrelMean>10) %>% ggplot(aes(x = meanTempCat, y = tmrelMean, color = as.factor(location_id))) +
    geom_line(alpha = 0.05) + guides(color = FALSE) + theme_minimal()
  
  
  tmrel %>% filter(location_id==1) %>% ggplot(aes(x = meanTempCat, y = tmrelMean, color = year_id)) + geom_point(alpha = 0.05) + geom_smooth() + theme_minimal()
  
  
  tmrel %>% filter(location_id==1 & year_id==2019) %>% ggplot(aes(x = meanTempCat)) +
      geom_ribbon(aes(ymin = tmrelLower, ymax = tmrelUpper)) + geom_line(aes(y = tmrelMean))
  
  
  
  global <- fread(paste0(outdir, "/tmrel_1.csv"))
  globalL <- melt(global, id.vars = c("meanTempCat", "location_id", "year_id"), measure.vars = paste0("tmrel_", 0:999))
  
  globalL %>% ggplot(aes(x = meanTempCat, y = value)) + geom_point(aes(group = variable), alpha = 0.01) +
    stat_summary(fun.y = mean, geom = "line") + theme_minimal() + ggtitle(indir.sub)
  
  
  
  
  
  
  
  
  
  test <- tmrel[c(grep("BRA", ihme_loc_id), grep("CHN", ihme_loc_id), grep("IND", ihme_loc_id), grep("USA", ihme_loc_id)), ][year_id==2020, ]
  test[, iso3 := substr(ihme_loc_id, 1, 3)]
  subSums <- test %>% group_by(iso3, meanTempCat) %>% summarise(mean = mean(tmrelMean), sd = sd(tmrelMean),
                                                     min = min(tmrelMean), p25 = quantile(tmrelMean, 0.25), median = median(tmrelMean),
                                                     p75 = quantile(tmrelMean, 0.75), max = max(tmrelMean))
}
