#clear memory
rm(list=ls())

library("data.table")
library("ggplot2")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_cause_metadata.R")
source("FILEPATH/csv2objects.R")
source("FILEPATH/submitExperiment.R")

#define args:
tmrel_min <- 6.6
tmrel_max <- 34.6
lTrim <- 0.05
uTrim <- 0.95
year_list <- 1990:2024
codcorrect_version <- 422
release <- 9

user <- Sys.getenv("USER")

updateConfig <- F
codAppend <- F
saveGlobalToCC <- F

config.path    <- "FILEPATH"
config.version <- 'config20230201'
config.file <- file.path(config.path, paste0(config.version, ".csv"))
csv2objects(config.file, exclude = ls())

project  <- "USERNAME"

indir <- file.path(rrRootDir, rrVersion)

if (updateConfig == T) {
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


### GET CAUSE META-DATA ###
causeMeta <- get_cause_metadata(cause_set_id = 4, release_id = release)
causeMeta <- causeMeta[acause %in% causeList & level == 3, ][, .(cause_id, cause_name, acause)]

cause_list <- paste(causeMeta$cause_id, collapse = ",")
acause_list <- paste(causeMeta$acause, collapse = ",")


### GET LOCATION META-DATA ###
if (project == "gbd") {
  locMeta <- get_location_metadata(location_set_id = 35, release_id = release)
  loc_ids <- locMeta[is_estimate == 1 | level <= 3, location_id]

  locMetaFhs <- get_location_metadata(location_set_id = 39, release_id = 9)
  loc_ids_fhs <- locMetaFhs[most_detailed != 1 & level %in% 3:4, location_id]

  loc_ids <- setdiff(loc_ids_fhs, loc_ids)
  loc_ids <- loc_ids[order(loc_ids)]

  year_list <- paste(year_list, collapse = ",")
  name.args <- "loc_id"

  project <- "PROJECT"

} else if (project == "test") {
  locMeta <- get_location_metadata(location_set_id = 35, release_id = release)
  loc_ids <- locMeta[is_estimate == 1 | level <= 3, location_id]
  loc_ids <- loc_ids[order(loc_ids)]

  name.args <- c("loc_id", "year")

  project <- "PROJECT"


} else {
  if (codAppend == T) {
    folder <- paste(rev(rev(strsplit(cod_source, "/")[[1]])[-1]), collapse = "/")
    files  <- list.files(folder)

    cod <- rbindlist(lapply(files, function(file) {fread(paste0(folder, "/", file))}), fill = T)
    cod <- cod[, "V1" := NULL]

    write.csv(cod, cod_source, row.names = F)
  }
  cod <- fread(cod_source)
  loc_ids <- unique(cod$location_id)
  loc_ids <- loc_ids[order(loc_ids)]
  saveGlobalToCC <- F

  name.args <- c("loc_id", "year")

  project <- "PROJECT"
}

limits <- fread("FILEPATH")
limits <- limits[is.na(lnRr_all_cause) == F & meanTempDegree %in% 6:28,
                 ][, `:=` (min = min(dailyTempCat), max = max(dailyTempCat)),
                   by = "meanTempDegree"]
limits <- unique(limits[, .SD, .SDcols = c("min", "max", "meanTempDegree")])
setnames(limits, "meanTempDegree", "meanTempCat")
write.csv(limits, paste0(outdir, "/limits.csv"), row.names = F)

arg.list <- list(loc_id = loc_ids, cause_list = cause_list, acause_list = acause_list, outdir = outdir, indir = indir,
                 tmrel_min = tmrel_min, tmrel_max = tmrel_max, year = year_list, config = config.file,
                 saveGlobalToCC = saveGlobalToCC, codcorrect_version = codcorrect_version)

name.stub <- "tmrel"

# set up run environment
project <- "-A USERNAME "
sge.output.dir <- paste0(" -o /FILEPATH/", user, "/USERNAME/%x.o%j.out -e /FILEPATH/", user, "/USERNAME/%x.e%j.err ")
r.shell <- "/FILEPATH/execRscript.sh -s"
calc.script <- "/FILEPATH/tmrelCalculator.R"

if (length(name.args) == 1) {
  mem <- "250G"
  slots <- 8
} else {
  mem <- "25G"
  slots <- 4
}

runtime <- "1:00:00"

for (run_loc in loc_ids) {
  jname <- paste0("tmrel_", run_loc)
  sys.sub <- paste0("sbatch ", project, sge.output.dir, " -J ", jname, " -c ", slots, " --mem=", mem,  " -t ", runtime, " -p long.q")
  args <- paste(run_loc, cause_list, acause_list, outdir, indir, tmrel_min, tmrel_max, year_list, config.file, saveGlobalToCC, codcorrect_version, jname)
  system(paste(sys.sub, r.shell, calc.script, args))
}

status <- data.table(loc_id = loc_ids, status = file.exists(paste0(outdir, "/USERNAME_", loc_ids, "_summaries.csv")))
table(status$status)

missing <- loc_ids[status$status == F]

runtime <- "3:00:00"

for (run_loc in missing) {
  jname <- paste0("tmrel_", run_loc)
  sys.sub <- paste0("sbatch ", project, sge.output.dir, " -J ", jname, " -c ", slots, " --mem=", mem,  " -t ", runtime, " -p all.q")
  args <- paste(run_loc, cause_list, acause_list, outdir, indir, tmrel_min, tmrel_max, year_list, config.file, saveGlobalToCC, codcorrect_version, jname)
  system(paste(sys.sub, r.shell, calc.script, args))
}