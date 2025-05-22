#clear memory
rm(list=ls())

library("data.table")
library("ggplot2")
library("parallel")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_cause_metadata.R")
source("FILEPATH/save_results_risk.R")
source("FILEPATHY/csv2objects/csv2objects.R")
source("FILEPATHY/submitExperiment.R")



#define args:
scenarios <- c("ssp119", "ssp245", "ssp585")
scenario <- scenarios[3]
extrapolate <- 10
resolution <- "era5"
models <- "hx_agreement"

user <- Sys.getenv("USER")


config.path    <- "FILEPATH"
config.version <- 'configFhs20240910'
config.file <- file.path(config.path, paste0(config.version, ".csv"))
csv2objects(config.file, exclude = ls())



### GET LOCATION META-DATA ###
if (project == "forecasting") {
  yearList <- (gbd_round_year + 1):2100
  continuousZones <- T

  if (continuousZones == T) {
    zoneType <- "continuous"
  } else {
    zoneType <- "binned"
  }

  cluster_proj  <- "PROJECT"
  calcSevs <- F
  maxDraw <- 499

  outRoot <- "FILEPATH"
  outVersion <- paste0(resolution, "res_extrapolate", extrapolate, "_", zoneType, "_", maxDraw + 1, "draw_", scenario)
  outdir <- file.path(outRoot, outVersion)

  gbd_round_id <- 7

  locMeta <- get_location_metadata(location_set_id = 39, release_id = 9)
  loc_ids <- locMeta[level %in% 3:4 | most_detailed == 1, ][order(location_id), location_id]

  tempMeltDir <- file.path("FILEPATH", scenario)
  rrMaxDir <- 'none'

} else {
  yearList <- start_year:gbd_round_year

  outRoot <- paste0("FILEPATH", release)
  outVersion <- config.version
  outdir <- file.path(outRoot, 'temperature', 'pafs', outVersion)
  locSet <- 35

  continuousZones <- F

  if (continuousZones == T) {
    zoneType <- "continuous"
  } else {
    zoneType <- "binned"
  }


  locMeta <- get_location_metadata(location_set_id = locSet, release_id = release)
  loc_ids <- locMeta[(is_estimate == 1 & most_detailed == 1) | level == 3 , location_id]
  loc_ids <- loc_ids[order(loc_ids)]
  cluster_proj <- "PROJECT"
  calcSevs <- T
  tempMeltDir <- "FILEPATH"
  rrMaxDir <- file.path("FILEPATH/sevs", config.version, "rrMax")

  maxDraw <- 499
}


dir.create(outdir, recursive = T)
dir.create(file.path(outdir, "heat"))
dir.create(file.path(outdir, "cold"))
dir.create(file.path(outdir, "sevs"))
dir.create(file.path(outdir, "summaries"))
dir.create(file.path(outdir, "clean_summaries"))
dir.create(file.path(outdir, "sevs", "raw"))
dir.create(file.path(outdir, "rrDist"))



limits <- fread("FILEPATH")
limits <- limits[is.na(lnRr_all_cause) == F & meanTempDegree %in% 6:28, ][, `:=` (min = min(dailyTempCat), max = max(dailyTempCat)), by = "meanTempDegree"]
limits <- unique(limits[, .SD, .SDcols = c("min", "max", "meanTempDegree")])
setnames(limits, "meanTempDegree", "meanTempCat")
write.csv(limits, file.path(outdir, "limits.csv"), row.names = F)

arg.list <- list(loc_id = loc_ids, year = yearList, outdir = outdir, project = project,
                 scenario = scenario, extrapolate = extrapolate, config.file = config.file,
                 calcSevs = calcSevs, tmrelDir = tmrelDir, rrDir = rrDir, tempMeltDir = tempMeltDir,
                 rrMaxDir = rrMaxDir, continuousZones = continuousZones, release_id = release,
                 maxDraw = maxDraw)

name.stub <- "paf"
name.args <- c("loc_id", "year", "scenario")

saveRDS(arg.list, paste0(outdir, "/submissionArguments.rds"))


# set up run environment
sge.output.dir <- paste0(" -o FILEPATH", user, "/output/%x.o%j.out -e FILEPATH", user, "/errors/%x.e%j.err ")
r.shell <- "FILEPATH/execRscript.sh"
save.script <- "FILEPATH/pafCalc_sevFix.R"


if (continuousZones == T) {
  mem <- "75G"
} else {
  mem <- "50G"
}
slots <- 8


arg.table <- launch.jobs(arg.list = arg.list, name.stub = name.stub, name.args = name.args, outfile.suffix = ".csv",
                         pause = 1, max.attempts = 1, return = T, relaunch = T, queue = 'long', project = cluster_proj, wait = T)


status <- data.table(expand.grid(loc_ids, yearList, scenarios))
names(status) <- c('location_id', 'year_id', 'scenario')

status[, outdir := outdir]
status[, fname := paste0(location_id, "_", year_id, ".csv")]

status[, `:=` (heat = file.exists(file.path(outdir, "heat", fname)),
               cold = file.exists(file.path(outdir, "cold", fname)),
               sevs = file.exists(file.path(outdir, "sevs", "raw", fname)))]


#View(status)
table(status[year_id < 2100, heat])
table(status$year_id, status$heat)
table(status[year_id < 2100, .(location_id, heat)])

table(status[year_id < 2100, .(location_id, heat)])


loc_ids <- unique(status[heat == F & year_id < 2100, location_id])




demog <- get_demographics('cod', release_id = release)
years_to_add <- demog$year_id[demog$year_id > gbd_round_year]

if (length(years_to_add) > 0) {

  add_years <- function(infile, inyear, outyears) {
    tmp <- fread(infile)
    for (outyear in outyears) {
      write.csv(tmp[, year_id := outyear], gsub(paste0("_", inyear, ".csv"), paste0("_", outyear, ".csv"), infile))
    }
  }


  for (risk in c("heat", "cold", "sevs")) {
    input_dir <- paste0(outdir, "/", risk)
    files <- list.files(path = input_dir,
                        pattern = paste0("_", gbd_round_year, ".csv"),
                        full.names = T)

    mclapply(files, function(file) {
      add_years(file, inyear = gbd_round_year, outyears = years_to_add)
      })
  }

}

status <- data.table(expand.grid(loc_ids, c(yearList, years_to_add)))
names(status) <- c('location_id', 'year_id')

status[, fname := paste0(location_id, "_", year_id, ".csv")]

status[, `:=` (heat = file.exists(file.path(outdir, "heat", fname)),
               cold = file.exists(file.path(outdir, "cold", fname)),
               sevs = file.exists(file.path(outdir, "sevs", "raw", fname)))]

best <- T

cmd <- paste0("sbatch -J save_results_risk_temperature",
              " --mem 400G -c 24 -t 72:00:00",
              " -p long.q -A USERNAME -C archive ",
              " -o FILEPATH/", user, "/output/%x.o%j.err",
              " -e FILEPATH/", user, "/errors/%x.e%j.out",
              " /FILEPATH/execRscript.sh -s ",
              " /FILEPATH/pafUpload.R ",
              paste("heat", config.version, release, best))
system(cmd)



cmd <- paste0("sbatch -J save_results_risk_temperature",
              " --mem 400G -c 24 -t 72:00:00",
              " -p long.q -A USERNAME -C archive ",
              " -o FILEPATH/", user, "/output/%x.o%j.err",
              " -e FILEPATH/", user, "/errors/%x.e%j.out",
              " /FILEPATH/execRscript.sh -s ",
              " /FILEPATH/pafUpload.R ",
              paste("cold", config.version, release, best))
system(cmd)



source("FILEPATH/save_results_risk.R")

paf.version    <- paste0(config.version, "; 2020 data correction")
input_dir.heat <- paste0(outdir, "/heat")
input_dir.cold <- paste0(outdir, "/cold")
mei.heat       <- 20263 #26852
mei.cold       <- 20262 #26849
years          <- 1990:2022

save_results_risk(input_dir = input_dir.heat,
                  input_file_pattern = "{location_id}_{year_id}.csv",
                  modelable_entity_id = mei.heat,
                  year_id = years,
                  n_draws = 1000,
                  description = paste0("HEAT PAFs: ", paf.version),
                  risk_type = "paf",
                  measure = 4,
                  mark_best = T,
                  release_id = 9)


save_results_risk(input_dir = input_dir.cold,
                  input_file_pattern = "{location_id}_{year_id}.csv",
                  modelable_entity_id = mei.cold,
                  year_id = years,
                  n_draws = 500,
                  description = paste0("COLD PAFs: ", paf.version),
                  risk_type = "paf",
                  measure = 4,
                  mark_best = T,
                  release_id = 9)