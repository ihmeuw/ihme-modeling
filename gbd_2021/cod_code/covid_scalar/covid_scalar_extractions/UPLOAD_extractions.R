rm(list=ls())
library(data.table)
library(openxlsx)
library(dplyr)

source("/filepath/get_bundle_data.R")
source("/filepath/get_bundle_version.R")
source("/filepath/save_bundle_version.R")
source("/filepath/upload_bundle_data.R")
source("/filepath/get_population.R")

dt <- read.xlsx("/filepath/2021_07_23_all_extractions_with_sample_size.xlsx")
# dt <- dt[, lapply(.SD, sum), by=cols, .SDcols=c("cases")] # deduplicate - this only needs to be done once and was done in the upload sheet
dt <- data.table(dt)

#dt <- dt[grepl(paste(c("IRIS", "Guatemala"), collapse="|"), dt$surveillance_name)] # fixing dropped sources
bundle_df <- get_bundle_data(bundle_id = 33, decomp_step = "iterative",
                             gbd_round_id = 7, export = FALSE)
cols <- setdiff(names(bundle_df), names(dt))

final <- dt

final$start_date <- convertToDate(final$start_date)
final$end_date <- convertToDate(final$end_date)
# final[, year_id := year(start_date + (end_date - start_date)/2)] # already run
final$start_date <- format(final$start_date, "%m/%d/%Y")
final$end_date <- format(final$end_date, "%m/%d/%Y")


final[, `:=` (year_start = year_id, year_end = year_id)]


# # Get population separately for all-age and age-specific rows, since get_row_population is slow
# # For all age, just merge on all-age population
# # Check that sex_id and age_demographer are all filled out.
# source(paste0(getwd(), "/get_row_population.R"))
# pop <- get_population(age_group_id = 22,
#                       sex_id = c(1,2,3),
#                       location_id = unique(final$location_id),
#                       year_id = unique(final$year_id),
#                       gbd_round_id = 7,
#                       decomp_step = "iterative")
# pop$run_id <- NULL
# final$year_id <- as.integer(final$year_id)
# final <- merge(final, pop, by = c("location_id", "year_id", "sex_id"))
#
# # # Only actually fill this population field to sample_size for all-age, both-sex rows
# final[age_start == 0 & age_end >= 99, sample_size := population]
# # # Now, only put the NAs into the get_row_population function!
# extraction_ss_filled <- get_row_population(final[is.na(sample_size)], gbd_round = 7, decomp_step = "iterative")
# extraction_ss_filled[, sample_size := pop_total]
# extraction_all <- rbind(extraction[!is.na(sample_size)], extraction_ss_filled, fill = T)
# extraction_all$pop_total <- NULL; extraction_all$population <- NULL
# write.xlsx(final, "/filepath/2021_06_15_all_extractions_with_sample_size.xlsx")

# FILL OTHER COLUMNS FOR UPLOAD
final$underlying_nid <- NA
final$field_citation_value <- NA
final$file_path <- NA
final$page_num <- NA
final$table_num <- NA
final$smaller_site_unit <- NA
final$site_memo <- NA
final$sex_issue <- 0
final$year_issue <- 0
final$age_issue <- 0
final$measure <- final$measure_type
  final[surveillance_name %like% "SINAN", measure:="incidence"]
  final[surveillance_name %like% "PRY" & measure_type == "suspected", case_status:=measure_type]
  final[surveillance_name %like% "PRY" & is.na(case_status), case_status:=measure_type]
  final[surveillance_name %like% "PRY", measure := "incidence"]
  final[surveillance_name %like% "NDSR", measure := "incidence"]
final$standard_error <- NA
final$unit_type <- "Person"
final$unit_value_as_published <- 1
final$measure_issue <- 0
final$measure_adjustment <- 0
final$uncertainty_type_value <- NA
final[grep("_", final$ihme_loc_id), representative_name := "Representative for subnational location only"]
final[is.na(representative_name), representative_name := "Nationally representative only"]
final$urbanicity_type <- "Mixed/both"
final$recall_type <- NA
final$recall_type_value <- NA
final$case_name <- final$parent_cause
final$case_definition <- NA
final$case_diagnostics <- NA
final$note_modeler <- NA
final$bundle_name <- NA
final$seq <- NA
final$effective_sample_size <- NA
final$design_effect <- NA
final$note_sr <- final$notes
final$input_type <- "extracted"
final$uncertainty_type <- NA
final$underlying_field_citation_value <- NA
final[is.na(is_outlier), is_outlier:=0]
final[sex %like% "both", sex:="Both"]
final[sex %like% "female", sex:="Female"]
final[sex %like% "male", sex:="Male"]
final <- final[,!c("mean", "upper", "lower")]
final[measure %like% " incidence", measure:="incidence"]
final[measure %like% "CFR", measure:="cfr"]
final[measure %like% "deaths", measure:="death"]

write.xlsx(final, "/filepath/2021_07_23_all_extractions_for_upload.xlsx")

# SPLIT BY CAUSE FOR UPLOAD
# dt <- read.xlsx("/filepath/2021_06_15_all_extractions_for_upload.xlsx")
# dt <- data.table(dt)

# extraction <- dt
 extraction <- final

# Cause_name fixes
extraction <- extraction[!cause_name %like% "avian" & !cause_name %like% "chlamydia"]
# aggregate the child causes to parent for select causes
extraction[is.na(cause_name), cause_name := parent_cause]
extraction[parent_cause %in% c("diphtheria", "tetanus", "varicella","pertussis"), cause_name := parent_cause]

message("Fixing Spelling")
extraction[cause_name %like% "rota", cause_name := "Rotavirus"]
extraction[cause_name %like% "amoeb", cause_name := "Amoebiasis"]
extraction[cause_name %like% "shigell" | cause_name %like% "Shigell", cause_name := "Shigella"]
extraction[cause_name %like% "cholera", cause_name := "Cholera"]
extraction[cause_name %like% "salmonel" | cause_name %like% "Salmonel", cause_name := "Salmonella"]
extraction[cause_name %like% "cryptospor", cause_name := "Cryptosporidium"]
extraction[cause_name %like% "campylo" | cause_name %like% "Campylo", cause_name := "Campylobacter"]
extraction[cause_name %like% "adeno", cause_name := "Adenovirus"]
extraction[cause_name %like% "aero", cause_name := "Aeromonas"]
extraction[cause_name %like% "noro", cause_name := "Norovirus"]
extraction[cause_name %like% "e.coli" | cause_name %like% "TEC", cause_name := "E. coli"]
extraction[cause_name %like% "clost" | cause_name %like% "difficile", cause_name := "C. diff"]
extraction[cause_name %like% "sync" | cause_name %like% "rsv", cause_name := "RSV"]

# naming of etiology-specific invasive infections
extraction[grepl(paste(c("meningo", "Meningo", "IMD", "Nm", "neiss", "m. invasive","meningitidis"), collapse='|'),cause_name), cause_name := "invasive_meningo"]

#Avoid changing "shigella" in diarrhea to invasive_hib
index <- grepl(paste(c("h. influenza", "hi", "Hi", "haemophilus", "Haemophilus"), collapse='|'),extraction[parent_cause %in% c("lri", "meningitis")]$cause_name)
extraction[parent_cause %in% c("lri", "meningitis")][index]$cause_name <- "invasive_hib"

extraction[grepl(paste(c("S. Pneum", "IPD", "streptococcus pneumo", "Pneumococc", "pneumococc", "pneumoniae"), collapse='|'),cause_name), cause_name := "invasive_pneumo"]
extraction[grepl(paste(c("Group B", "GBS", "group B", "gbs", "agalactiae"), collapse='|'),cause_name), cause_name := "invasive_gbs"]
extraction[!cause_name %like% "invasive_" & parent_cause == "meningitis", cause_name := "meningitis_other"]
extraction[cause_name %like% "diarrhea", cause_name := "diarrhea_unspecified"]
# this one has to come after meningitis to not capture Hib
extraction[cause_name %like% "flu"| cause_name %like% "Flu", cause_name := "influenza"]
# outlier any nonspecific LRI/URI due to potential COVID inclusion
message("Outliering nonspecific LRI/URI")
extraction[grepl(paste(c("lri", "URI"), collapse='|'),parent_cause), is_outlier := 1]
# but keep etiology-specific cases
extraction[grepl(paste(c("influenza", "RSV", "mycoplasma", "hib"), collapse='|'),cause_name), is_outlier := 0]

write.xlsx(extraction, "/filepath/2021_07_23_all_extractions_for_upload.xlsx")


### SPLIT THEN EXPORT FOR UPLOAD
#dir <- "/filepath"
dir <- paste0(getwd(),"/upload/")
extraction <- read.xlsx("/filepath/2021_07_23_all_extractions_for_upload.xlsx")
extraction <- data.table(extraction)

# 9665	WHO flunet only flu surveillance for covid impacts
flunet <- extraction[surveillance_name %like% "WHO FluNet"]
write.xlsx(flunet, paste0(dir, "upload_flunet.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9665, filepath=paste0(dir, "upload_flunet.xlsx"))
check <- get_bundle_data(bundle_id=9665)

# 9662	JRF monthly only surveillance for covid impacts
who_measles <- extraction[surveillance_name %like% "WHO Measles"]
write.xlsx(who_measles, paste0(dir, "upload_who_measles.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9662, filepath=paste0(dir, "upload_who_measles.xlsx"))
check <- get_bundle_data(bundle_id=9662)

# remove WHO measles and flunet for rest of uploads
extraction <- extraction[!surveillance_name %like% "WHO FluNet"]
extraction <- extraction[!surveillance_name %like% "WHO Measles"]
extraction <- extraction[measure %like% "deaths", measure:="death"]

# 9506	LRI
lri <- extraction[parent_cause %like% "lri" & !(cause_name %in% c("influenza", "RSV", "invasive_hib"))]
write.xlsx(lri, paste0(dir, "upload_lri.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9506, filepath=paste0(dir, "upload_lri.xlsx"))
check <- get_bundle_data(bundle_id=9506)

# 9647	pneumococcus surveillance for covid impacts
pneumo <- extraction[cause_name %like% "invasive_pneumo"]
write.xlsx(pneumo, paste0(dir, "upload_pneumo.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9647, filepath=paste0(dir, "upload_pneumo.xlsx"))
check <- get_bundle_data(bundle_id=9647)


# 9650	h influenzae b surveillance for covid impacts
# has rows from LRI and meningitis
h_influenzae <- extraction[cause_name %like% "invasive_hib"]
write.xlsx(h_influenzae, paste0(dir, "upload_h_influenzae.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9650, filepath=paste0(dir, "upload_h_influenzae.xlsx"))
check <- get_bundle_data(bundle_id=9650)

# 9653	rsv surveillance for covid impacts
rsv <- extraction[cause_name %like% "RSV"]
write.xlsx(rsv, paste0(dir, "upload_rsv.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9653, filepath=paste0(dir, "upload_rsv.xlsx"))
check <- get_bundle_data(bundle_id=9653)

# 9656	meningococcus surveillance for covid impacts
meningo <- extraction[cause_name %like% "invasive_meningo"]
write.xlsx(meningo, paste0(dir, "upload_meningo.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9656, filepath=paste0(dir, "upload_meningo.xlsx"))
check <- get_bundle_data(bundle_id=9656)

# 9659	gbs surveillance for covid impacts
gbs <- extraction[cause_name %like% "invasive_gbs"]
write.xlsx(gbs, paste0(dir, "upload_gbs.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9659, filepath=paste0(dir, "upload_gbs.xlsx"))
check <- get_bundle_data(bundle_id=9659)

# 9530	flu
flu <- extraction[cause_name %like% "influenza"]
write.xlsx(flu, paste0(dir, "upload_flu.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9530, filepath=paste0(dir, "upload_flu.xlsx"))
check <- get_bundle_data(bundle_id=9530)

# 9527	URI
uri <- extraction[parent_cause %like% "URI"]
write.xlsx(uri, paste0(dir, "upload_uri.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9527, filepath=paste0(dir, "upload_uri.xlsx"))
check <- get_bundle_data(bundle_id=9527)

# 9524	meningitis
meningitis <- extraction[cause_name %like% "meningitis_other"]
write.xlsx(meningitis, paste0(dir, "upload_meningitis.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9524, filepath=paste0(dir, "upload_meningitis.xlsx"))
check <- get_bundle_data(bundle_id=9524)

# 9521	varicella
varicella <- extraction[parent_cause %like% "varicella"]
write.xlsx(varicella, paste0(dir, "upload_varicella.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9521, filepath=paste0(dir, "upload_varicella.xlsx"))


# 9518	diarrhea
diarrhea <- extraction[parent_cause %like% "diarrhea"]
write.xlsx(diarrhea, paste0(dir, "upload_diarrhea.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9518, filepath=paste0(dir, "upload_diarrhea.xlsx"))
check <- get_bundle_data(bundle_id=9518)

# 9515	measles
measles <- extraction[parent_cause %like% "measles"]
write.xlsx(measles, paste0(dir, "upload_measles.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9515, filepath=paste0(dir, "upload_measles.xlsx"))
check <- get_bundle_data(bundle_id=9515)

# 9512	tetanus
tetanus <- extraction[parent_cause %like% "tetanus"]
write.xlsx(tetanus, paste0(dir, "upload_tetanus.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9512, filepath=paste0(dir, "upload_tetanus.xlsx"))
check <- get_bundle_data(bundle_id=9512)

# 9509	pertussis
pertussis <- extraction[parent_cause %like% "pertussis"]
write.xlsx(pertussis, paste0(dir, "upload_pertussis.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9509, filepath=paste0(dir, "upload_pertussis.xlsx"))
check <- get_bundle_data(bundle_id=9509)

# 9503	diphtheria
diphtheria <- extraction[parent_cause %like% "diphtheria"]
write.xlsx(diphtheria, paste0(dir, "upload_diphtheria.xlsx"), sheetName="extraction")
upload_bundle_data(bundle_id=9503, filepath=paste0(dir, "upload_diphtheria.xlsx"))
check <- get_bundle_data(bundle_id=9503)


#------------- VALIDATIONS
files <- list.files(dir)
files <- paste0(dir, files)
files <- lapply(files, read.xlsx)
files <- rbindlist(files, use.names=TRUE)

# check differences between uploaded rows and overall sheet
dt <- read.xlsx("/filepath/2021_07_23_all_extractions_for_upload.xlsx")
dt <- data.table(dt)
diffs <- all.equal(files, dt, ignore.col.order=TRUE, ignore.row.order=TRUE) # there should be no differences or only non-critical differences left!
row_diffs <- anti_join(files, dt) # there should be no differences!
# check differences in row numbers
files$key <- paste0(files$parent_cause, files$cause_name)
dt$key <- paste0(dt$parent_cause, dt$cause_name)
temp_files <- files[,.N, by="key"]
temp_dt <- dt[,.N, by="key"]
setnames(temp_dt, "N", "dt_n")
temp_files <- merge(temp_files, temp_dt)

# check duplicates
file_duplicates <- files[duplicated(files) | duplicated(files, fromLast=TRUE)] # should be empty if no duplicates
#file_duplicates$start_date <- convertToDate(file_duplicates$start_date)
#file_duplicates$end_date <- convertToDate(file_duplicates$end_date)
n <- file_duplicates[,.N, by=names(file_duplicates)]
write.xlsx(file_duplicates, paste0(getwd(), "/duplicate_covid_extractions.xlsx"))

# deduplicate by aggregating after fixing bolivia sex column. Only needs to be done once.
dt <- dt[sex_id==2, sex:="Female"] # fix bolivia - previously, sex_id == 2 was set to Male.

dt$cases <- as.integer(dt$cases)
cols <- names(dt)[!names(dt) %in% c("cases")]
final_dt <- dt[, lapply(.SD, sum), by=cols, .SDcols=c("cases")]
duplicates2 <- final_dt[duplicated(final_dt) | duplicated(final_dt, fromLast=TRUE)]
write.xlsx(final_dt, "/filepath/2021_07_23_all_extractions_for_upload.xlsx")

# make sure the right cause names were uploaded to the right bundles
ids <- c(9647,
         9650,
         9653,
         9656,
         9659,
         9530,
         9527,
         9524,
         9521,
         9518,
         9515,
         9512,
         9509,
         9506,
         9503,
         9665,
         9662)

bundle_check <- list()
for (i in 1:length(ids)) {
  dt <- get_bundle_data(bundle_id=ids[i])
  causes <- paste(unique(dt$cause_name))
  bundle_check[i] <- paste0(ids[i], " ", causes)
}


# SAVE BUNDLE VERSIONS
bv_tracker <- read.xlsx("/filepath/supporting_docs/bundle_bv_ids.xlsx")
bv <- c()

for (i in 1:nrow(bv_tracker)) {
  id <- bv_tracker$bundle_id[i]
  result <- save_bundle_version(id, "iterative", gbd_round_id=7, include_clinical=NULL)
  print(paste0(id, " bv is ", result$bundle_version_id))
  bv <- rbindlist(list(bv, data.table(result$bundle_version_id)))
}

# check some bv ids
test <- get_bundle_version(39104)
