rm(list=ls())

start <- Sys.time()

# Determine if this is an interactive session or a batch job
print(commandArgs())
INTERACTIVE = commandArgs()[2] == '--interactive'

# If running as a batch job, read in the command line arguments
if (INTERACTIVE == F) {
  arg <- commandArgs(trailingOnly = T)  #[-(1:5)]  # First args are for unix use only
  loc_id <- arg[1]
  release <- arg[2]
  sev_dir <- arg[3]

} else {
  release <- 16
  loc_id <- 44866
  sev_dir <- "FILEPATH"
}


library("data.table")
source("FILEPATH/get_demographics_template.R")
source("FILEPATH/get_cause_metadata.R")



### GET DEMOGRAPHIC LEVELS AND CAUSES NEEDED FOR PAF UPLOAD ###
# Get all of the levels of age and sex #
demog <- get_demographics_template(gbd_team = "USERNAME", release_id = release)[location_id == loc_id, ]
demog <- unique(demog)
warning("Demographic template pulled")
warning(dim(demog))

# Find all of the most detailed causes for which the level 3 is in our cause list #
causeMeta <- get_cause_metadata(cause_set_id = 2, release_id = release)
warning("Cause metadata pulled")

causeMeta <- cbind(causeMeta, sapply(1:nrow(causeMeta), function(i) {
  as.integer(strsplit(causeMeta[i, path_to_top_parent], split = ",")[[1]][4])}))
names(causeMeta)[ncol(causeMeta)] <- "l3_id"

causeList3 <- causeMeta[level %in% c(0,3), ][, .(cause_id, acause)]
names(causeList3) <- c("l3_id", "l3_acause")

causeList3 <- merge(causeList3, causeMeta, by = "l3_id", all.x = T)[most_detailed == 1 | l3_acause == "_all", ]
causeList3[l3_acause == "_all",  `:=` (cause_id = l3_id, acause = l3_acause)]
causeList3 <- causeList3[, .(l3_acause, cause_id, acause)]
warning("Export cause list created")

start <- Sys.time()

max_year <- max(demog$year_id)

for (year in 1990:(max_year - 1)) {
  message(year)

  sevs <- fread(file.path(sev_dir, "raw", paste0(loc_id, "_", year, ".csv")))[is.na(rei_id) == F, ][, c("age_group_id", "sex_id") := NULL]
  maxDraw <- max(as.integer(gsub('draw_', '', grep('draw_', names(sevs), value = T))))

  sevs <- rbind(sevs, sevs[, lapply(.SD, function(x) {mean(x, na.rm = T)}), by = c("location_id", "year_id", "rei_id"), .SDcols = paste0("draw_", 0:maxDraw)][, acause := "_all"])

  sevs <- merge(sevs, demog[year_id == year, ], by = c("location_id", "year_id"), all = T, allow.cartesian = T)
  message("SEVs merged to demographic template")

  setnames(sevs, "acause", "l3_acause")
  sevs <- merge(sevs, causeList3, by = "l3_acause", all.x = T, allow.cartesian = T)
  sevs[, l3_acause := NULL]
  message("SEVs merged to export cause list")


  write.csv(sevs, paste0(sev_dir, "/", loc_id, "_", year, ".csv"), row.names = F)

  if (year == max_year - 1) {
    sevs[, year_id := max_year]
    write.csv(sevs, paste0(sev_dir, "/", loc_id, "_", max_year, ".csv"), row.names = F)

  }
}

difftime(Sys.time(), start)