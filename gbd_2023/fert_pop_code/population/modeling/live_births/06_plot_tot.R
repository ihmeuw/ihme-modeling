################################################################################
## Description: Plot total live births by sex_id for every location and overlay
##              previous gbd_year as a comparator.
################################################################################

library(data.table)
library(readr)
library(ggplot2)
library(mortcore)
library(mortdb)

rm(list = ls())
USER <- Sys.getenv("USER")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--live_births_vid", type = "character",
                    help = 'The version number for this run of live births, used to read in settings file')
parser$add_argument("--test", type = "character",
                    help = 'Whether this is a test run of the process')
parser$add_argument("--code_dir", type = "character", 
                    help = 'Location of code')
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$live_births_vid <- "99999"
  args$test <- "T"
  args$code_dir <- "CODE_DIR_HERE"
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", ifelse(test, "test/", ""), live_births_vid, "/run_settings.csv")
load(settings_dir)
list2env(settings, envir = environment())

age_groups <- fread(paste0(output_dir, "/inputs/age_groups.csv"))
location_hierarchy <- fread(paste0(output_dir, "/inputs/all_reporting_hierarchies.csv"))

# Prep births for graphing -----------------------------------------------------

tot_births <- get_mort_outputs("birth", "estimate",
                               run_id = live_births_vid,
                               age_group_id = 169,
                               sex_id = 3)
tot_births[, c("upload_birth_estimate_id", "run_id") := NULL]
tot_births[, Type := "New"]

prev_tot_births <- get_mort_outputs("birth", "estimate",
                                    gbd_year = get_gbd_year(get_gbd_round(gbd_year) - 1),
                                    age_group_id = 169,
                                    sex_id = 3)
prev_tot_births[, c("upload_birth_estimate_id", "run_id") := NULL]
prev_tot_births[, Type := "Old"]

births <- rbind(tot_births, prev_tot_births)
births[, sex_id := as.factor(sex_id)]

births <- merge(births, location_hierarchy[, .(ihme_loc_id)],
                by = "ihme_loc_id")

# Prep additional comparators --------------------------------------------------

wpp <- fread("FILEPATH")
wpp <- wpp[, .(collection_date_start, country_ihme_loc_id, value, sex_id)]
wpp <- wpp[sex_id == 3]
setnames(wpp, c("collection_date_start", "country_ihme_loc_id", "value"), 
         c("year_id", "ihme_loc_id", "births"))
wpp[, Type := "WPP"]

births <- rbind(wpp, births, fill = T)

# Create plots -----------------------------------------------------------------

pdf(paste0(output_dir, "diagnostics/run_id_", live_births_vid,
           "_total_births_by_loc.pdf"))

for(loc in unique(births$ihme_loc_id)){

  plot <- ggplot(data = births[ihme_loc_id == loc],
                 aes(y = mean, x = year_id)) +
    geom_line(aes(color = Type)) +
    geom_ribbon(data = births[ihme_loc_id == loc & Type != "WPP"],
                aes(ymin = lower, ymax = upper, fill = Type),
                alpha = 0.25) +
    theme_bw() +
    ggtitle("Total live births over time, both sexes: ", loc) +
    ylab("Total live births") +
    xlab("Year") +
    theme(legend.position="bottom")

  print(plot)
}

dev.off()
