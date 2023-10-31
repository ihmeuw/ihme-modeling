library(read.dbc, lib.loc="/filepath/rlibs/")
library(data.table)
library(openxlsx)

source("/filepath/get_location_metadata.R")

locs <- get_location_metadata(location_set_id=35, gbd_round_id=7)
locs <- locs[,.(location_id, ihme_loc_id)]

# read files
path <- "/filepath/Arq_202571159_meningite"
files <- list.files(path)
files <- lapply(files, function(x) {paste0(path, "/", x)})

cols <- c("NU_ANO", "SG_UF", "DT_NASC", "CS_SEXO", "CLASSI_FIN","CON_DIAGES", "ATE_HOSPIT")
data <- list()

for (i in 1:length(files)) {
  sheet <- read.dbc(files[[i]])
  sheet <- data.table(sheet)
  sheet <- sheet[,..cols]
  data[[i]] <- sheet
}

# combine files, add columns for extraction
combined_data <- rbindlist(data)

# add location_ids
state_ids <- read.xlsx("/filepath/state_id_map.xlsx")
state_ids <- merge(state_ids, locs)
combined_data$SG_UF <- as.double(combined_data$SG_UF)
combined_data2 <- merge(x=combined_data, y=state_ids, by.x="SG_UF", by.y="SG_UF")

# filter to only confirmed then add cause_names
combined_data2 <- combined_data2[CLASSI_FIN == 1,]
cause_names <- read.xlsx("/filepath/cause_names.xlsx")
combined_data2$CON_DIAGES <- as.double(combined_data2$CON_DIAGES)
combined_data2 <- merge(x=combined_data2, y=cause_names, by.x="CON_DIAGES", by.y="CON_DIAGES")

# clean up
# count by year, location, etiology, hospitalization status
combined_data3 <- combined_data2[cause_name != "Tuberculous meningitis",]
combined_data3$measure_type <- ifelse(combined_data3$ATE_HOSPIT == 1, "hospitalizations - inpatient",
                                     ifelse(combined_data3$ATE_HOSPIT == 2, "hospitalizations - outpatient", "hospitalizations - unknown"))
combined_data3[is.na(ATE_HOSPIT),]$measure_type <- "hospitalizations - unknown"
combined_data3 <- combined_data3[,.N,by=list(NU_ANO, ihme_loc_id, cause_name, measure_type)]
combined_data3 <- combined_data3[order(NU_ANO, measure_type)]


# export
write.xlsx(combined_data3, paste0(getwd(), "/brazil_sinan_meningitis_clinical_specific.xlsx"))
