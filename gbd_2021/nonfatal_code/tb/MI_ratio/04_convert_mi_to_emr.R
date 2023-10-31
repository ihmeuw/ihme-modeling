## EMPTY THE ENVIRONMENT
rm(list = ls())

## SET UP FOCAL DRIVES
os <- .Platform$OS.type
if (os=="windows") {
  ADDRESS <-"ADDRESS"
  ADDRESS <-"ADDRESS"
  ADDRESS <-"ADDRESS"
} else {
  ADDRESS <-"ADDRESS"
  ADDRESS <-paste0("ADDRESS/", Sys.info()[7], "/")
  ADDRESS <-"ADDRESS"
}

## LOAD FUNCTIONS AND PACKAGES
source(paste0(ADDRESS, "FILEPATH/get_covariate_estimates.R"))
source(paste0(ADDRESS, "FILEPATH/get_location_metadata.R"))
source(paste0(ADDRESS, "FILEPATH/get_age_metadata.R"))
library(writexl)
library(data.table)
library(ggplot2)
library(readxl)

#############################################################################################
###                               HELPER OBJECTS AND FUNCTIONS                            ###
#############################################################################################

## HELPER OBJECTS
mvid        <- "v.1"
decomp_step <- "step4"
emr_dir     <- paste0(ADDRESS, "FILEPATH")
dur_dir     <- paste0(ADDRESS, "FILEPATH")

#############################################################################################
###                            CONVERT MI RATIO TO EMR                                    ###
#############################################################################################

## PULL DATA
emr <- as.data.table(read_excel(paste0(emr_dir, "emr_mrbrt_age_dummy_haq_sr_", mvid, ".xlsx")))
dur <- fread(paste0(dur_dir, "pred_duration_haq_2019_11_18.csv"))
setnames(dur, old = "year_id", new = "year_start")

## MERGE
data <- merge(emr, dur, by = c("location_id", "year_start"))
data[, mean := mean/new_duration]

## CLEAN
data[, `:=` (lower = NA, upper = NA, year_id = NULL, sex_id = NULL)]
data[, `:=` (new_duration = NULL, duration_low = NULL, duration_high = NULL)]
data[, note_modeler := as.character(note_modeler)]
data[, note_modeler := paste0("EMR from MR-BRT (rm HIV from den.); age, sex, and SR dummies; used duration, version - ", mvid)]

## SAVE
writexl::write_xlsx(list(extraction=data), path = paste0(paste0(emr_dir, "emr_based_duration_", mvid, ".xlsx")))

#############################################################################################
###                                 PLOT RESULTS BY SDI                                   ###
#############################################################################################

## PLOT BY SDI
plotting <- data[, .(location_id, ihme_loc_id, year_start, sex, age_start, age_end, mean)]
plotting <- plotting[!(ihme_loc_id %like% "_")]
plotting <- plotting[year_start == 2019]

## GET METADATA
ages <- get_age_metadata(age_group_set_id = 12, gbd_round_id = 6)
ages <- ages[, .(age_group_years_start, age_group_years_end, age_group_weight_value)]
setnames(ages, old = names(ages), new = c("age_start", "age_end", "age_weight"))

## PREP FOR AGGREGATION
for(my_age in seq(5, 55, 10)) ages[age_start %in% c(my_age, my_age + 5), age := my_age]
ages[age_start < 5, age := 0]
ages[age_start > 60,age := 65]

## AGGREGATE
ages     <- ages[, .(age_weight = sum(age_weight)), by = "age"]
setnames(ages, "age", "age_start")

## MERGE
plotting <- merge(plotting, ages, by = "age_start")

## AGE STANDARDIZE
plotting[, age_std_emr := mean*age_weight]
plotting <- plotting[, .(age_std_emr = sum(age_std_emr)), by = .(ihme_loc_id, location_id, year_start, sex)]
plotting <- plotting[order(location_id, year_start, sex)]

## GET LOCATION DATA
locs     <- get_location_metadata(location_set_id = 35)
locs     <- locs[, .(location_id, super_region_name)]
plotting <- merge(plotting, locs, by = "location_id")

## GET SDI
sdi <- get_covariate_estimates(covariate_id = 881, location_id = unique(plotting$location_id), year_id = unique(plotting$year_start), decomp_step = decomp_step)
sdi <- sdi[, .(location_id, year_id, mean_value)]
setnames(sdi, old = c("mean_value", "year_id"), new = c("sdi", "year_start"))

## MERGE SDI
plotting <- merge(plotting, sdi, by = c("location_id", "year_start"))

## PLOT
ggplot(data = plotting, aes(x = sdi, y = age_std_emr, colour = super_region_name)) +
  stat_smooth(method ="lm", se = FALSE, alpha = 0.5, colour = 'black', size = .5)+
  geom_text(aes(label = ihme_loc_id), nudge_y = 0.002, color = "black") +
  labs(x = "SDI", y = "Predicted age-standardized EMR", colour = "") +
  facet_wrap(~sex) + geom_point(size = 3.25, alpha = 0.75) + theme_bw() +
  theme(legend.position = "bottom", legend.text=element_text(size=16), strip.text = element_text(size=18),
        axis.title = element_text(size=22), axis.text = element_text(size = 20))
