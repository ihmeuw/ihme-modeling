
## Purpose: Prepare DDM data for upload to the mortality database

rm(list=ls())
library(data.table); library(haven); library(argparse); library(readstata13);
library(assertable); library(DBI);
library(mortdb); library(mortcore)

if (Sys.info()[1] == "Linux") {
  root <- "FILEPATH"
  username <- Sys.getenv("USER")
} else {
  root <- "FILEPATH"
}

parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of DDM')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help='GBD round')
parser$add_argument('--mark_best', type="character", required=TRUE,
                    help='True/False mark run as best')
args <- parser$parse_args()

version_id <- args$version_id
gbd_year <- args$gbd_year
mark_best <- args$mark_best

# Generate new DDM data version
data_version_id <- gen_new_version(model_name = "ddm", model_type = "data", comment = paste0("DDM data linked to estimate run v", version_id))
parent_list_estimate <- list("ddm data" = data_version_id)
gen_parent_child(parent_runs = parent_list_estimate, child_process = "ddm estimate", child_id = version_id)


main_dir <- paste0("FILEPATH")

# Get location metadata
ap_old <- data.table(get_locations(gbd_type = 'ap_old', level = 'estimate', gbd_year = gbd_year))
ap_old <- ap_old[location_name == "Old Andhra Pradesh"]

locations <- data.table(get_locations(gbd_year = gbd_year))
locations <- locations[!(grepl("KEN_", ihme_loc_id) & level == 4)]
locations <- rbind(locations, ap_old)
locations <- locations[, list(ihme_loc_id, location_id, location_name)]

## D07 Prep
d07 <- data.table(read_dta(paste0("FILEPATH"), encoding = "latin1"))
d07[, year_id := floor(year)]
setnames(d07, "year", "viz_year")
setnames(d07, "comp", "mean")

split <- strsplit(d07$iso3_sex_source, "&&")
iso3.sex.source <- do.call(rbind, split)
d07[, iso3 := iso3.sex.source[,1]]
d07[, sex_id := iso3.sex.source[,2]]
d07[, source := iso3.sex.source[,3]]

d07[detailed_comp_type == "seg", method_id := 7]
d07[detailed_comp_type == "ggb", method_id := 8]
d07[detailed_comp_type == "ggbseg", method_id := 9]
d07 <- d07[, list(ihme_loc_id, year_id, year1, year2, viz_year, source, sex_id, comp_type, method_id, mean, nid, underlying_nid)]
d07[is.na(method_id), method_id := 6]

## D08 Prep
d08 <- data.table(read_dta(paste0("FILEPATH"), encoding = "latin1"))
d08 <- d08[order(iso3_sex_source, year, comp_type),]
setnames(d08, "year", 'year_id')
# create age, sex, and source variables
split <- strsplit(d08$iso3_sex_source, "&&")
iso3.sex.source <- do.call(rbind, split)
d08[, iso3 := iso3.sex.source[,1]]
d08[, sex_id := iso3.sex.source[,2]]
d08[, source := iso3.sex.source[,3]]

d08[comp_type == "seg", method_id := 7]
d08[comp_type == "ggb", method_id := 8]
d08[comp_type == "ggbseg", method_id := 9]

d07_child <- d07[comp_type == "u5"]
d07_adult <- d07[comp_type != "u5"]
d08_child <- d08[!is.na(u5_comp), list(ihme_loc_id, sex_id, source, year_id, method_id = 6, mean = u5_comp, outlier = 0)]
d08_child <- unique(d08_child)

## Subset adult date from d08
d08_adult <- d08[comp_type %in% c("ggb", "seg", "ggbseg"), list(ihme_loc_id, sex_id, source, year_id, method_id, mean = comp, outlier = exclude)]


# Create 'mean_id' variable to merge datasets together
d07_child <- d07_child[order(ihme_loc_id, year_id, sex_id, source, method_id, mean)]
d07_child[, mean_id:=seq(.N), by=c('ihme_loc_id', 'year_id', 'sex_id', 'source', 'method_id')]

d07_adult <- d07_adult[order(ihme_loc_id, year_id, sex_id, source, method_id, mean)]
d07_adult[, mean_id:=seq(.N), by=c('ihme_loc_id', 'year_id', 'sex_id', 'source', 'method_id')]


d08_child <- d08_child[order(ihme_loc_id, year_id, sex_id, source, method_id, mean)]
d08_child[, mean_id:=seq(.N), by=c('ihme_loc_id', 'year_id', 'sex_id', 'source', 'method_id')]
child <- merge(d08_child, d07_child[, list(ihme_loc_id, year_id, sex_id, source, method_id, mean_id, nid, underlying_nid, viz_year, age_group_id = 1, year1, year2)],
               by = c('ihme_loc_id', 'year_id', 'sex_id', 'source', 'method_id', 'mean_id'), all.x = T)


d08_adult <- d08_adult[order(ihme_loc_id, year_id, sex_id, source, method_id, mean)]
d08_adult[, mean_id:=seq(.N), by=c('ihme_loc_id', 'year_id', 'sex_id', 'source', 'method_id')]
d08_adult[ihme_loc_id == "BGD" & year_id == 2001 & method_id %in% c(7,8,9), mean_id :=1]
adult <- merge(d08_adult, d07_adult[, list(ihme_loc_id, year_id, sex_id, source, method_id, mean_id, nid, underlying_nid, viz_year, age_group_id = 199, year1, year2)],
               by = c('ihme_loc_id', 'year_id', 'sex_id', 'source', 'method_id', 'mean_id'))

tmp <- assert_values(adult, c('year1', 'year2'), "not_na")

data <- rbind(adult, child)


############################
# Generate source_type_id
############################
data[source == "VR", source_type_id := 1]
data[grepl("SRS", source), source_type_id := 2]
data[grepl("DSP", source), source_type_id := 3]
data[source == "CENSUS", source_type_id := 5]
data[source == "SURVEY", source_type_id := 16]


data[grepl("VR", source) & is.na(source_type_id), source_type_id := 1]
data[source == "VR-SSA" & grepl("ZAF", ihme_loc_id), source := "VR-SSA"]

data[source == "MOH survey", source_type_id := 34]


data[source == "FFPS", source_type_id := 38]
data[source == "SUPAS", source_type_id := 39]

data[source == "SSPC-DC" , source_type_id := 50]

data[source == "SUSENAS", source_type_id := 40]
data[source == "HOUSEHOLD", source_type_id := 42]
data[source == "HOUSEHOLD_HHC", source_type_id := 43]
data[source == "MCCD", source_type_id := 55]
data[source == "CR", source_type_id := 56]
data[source == "HOUSEHOLD_DLHS", source_type_id := 41]

tmp <- assert_values(data, c("source_type_id"), "not_na")


# Merge location_ids
data <- merge(data, locations, by = 'ihme_loc_id', all.x =T)
data <- data[, list(year_id, location_id, sex_id, age_group_id, method_id, source_type_id, nid, underlying_nid, viz_year, mean, outlier, deaths_citation = "", pop_citation = "", pop_start_year = year1, pop_end_year = year2)]
data[sex_id == "male", sex_id := "1"]
data[sex_id == "female", sex_id := "2"]
data[sex_id == "both", sex_id := "3"]
data[, sex_id := as.numeric(sex_id)]

data[, nid := gsub(";.*", "", nid)]
data[!grepl("^.;[0-9]", underlying_nid), underlying_nid := gsub(";.*", "", underlying_nid)]
data[grepl("^.;[0-9]", underlying_nid), under := gsub("*.;,*", "", underlying_nid)]
data[grepl("^.;[0-9]", underlying_nid), under := gsub(",.*", "", under)]
data[grepl("^.;[0-9]", underlying_nid), underlying_nid := under]
data[grepl("^[0-9]+,", underlying_nid), underlying_nid := gsub(",.*", "", underlying_nid)]
data[, under := NULL]

data[nid == "3287,287600", nid := "3287"]

tmp <- assert_values(data, c("location_id","age_group_id","source_type_id", "method_id", "sex_id", "nid", 'underlying_nid'), "not_na")

write.csv(data, paste0("FILEPATH"), row.names = FALSE)

# Upload ddm data file
upload_results(filepath = paste0("FILEPATH"),
               model_name = "ddm",
               model_type = "data",
               run_id = data_version_id,
               send_slack = T)

if (mark_best){
  update_status(model_name = "ddm",
                model_type = "data",
                run_id = data_version_id,
                new_status = "best",
                assert_parents=F)
}
