rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

library('openxlsx')
library('readxl')
library('data.table')
library('stringr')
source("FILEPATH")
source("FILEPATH")

# NZL nids based on year on GHDx and NZL scoping 
nid_map <- data.table(year_start = c(2009:2016),
                      nid = c(305740:305743, 336673:336676))

# location data needed for extraction template
locs <- get_location_metadata(location_set_id = 35)
locs <- locs[, list(location_name, location_id, ihme_loc_id)]
locs <- locs[ihme_loc_id %like% "NZL"]

#limited use data (2013 - 2016) - 
females_lu <- as.data.table(read.xlsx("FILEPATH", sheet = "Females"))
males_lu <- as.data.table(read.xlsx("FILEPATH", sheet = "Males"))

# data  (2009 - 2012)
nzl_2009_2012 <- as.data.table(read_excel("FILEPATH"))

# start with limited use data
icd_columns <- grep("icd", colnames(females_lu), value = TRUE) # these 25 columns are the same in all files
females_lu <- females_lu[, c("gender","year", "ethnicgp", icd_columns), with = F]
males_lu <- males_lu[, c("gender","year", "ethnicgp", icd_columns), with = F]
lu_data <- rbind(females_lu, males_lu)
setnames(lu_data, c("gender", "year", "ethnicgp"), c("sex", "year_start", "ethnic_group"))

# years 2009 - 2012
nzl_2009_2012 <- nzl_2009_2012[,  c("Gender", "Year of birth", "Ethnic group", icd_columns), with = F]
setnames(nzl_2009_2012, c("Gender", "Year of birth", "Ethnic group"),
         c("sex", "year_start", "ethnic_group"))

all_data <- rbind(lu_data, nzl_2009_2012)
all_data$row_num <- seq.int(nrow(all_data)) # sets a row number for each row

all_data_melt <- copy(all_data)

# converts data long
all_data_melt = melt(all_data_melt, id.vars = c("sex", "year_start", "ethnic_group", "row_num"),
                     measure.vars = c(icd_columns))
all_data_melt <- all_data_melt[!value == ""]
all_data_melt[, c("variable") := NULL] #icd columns names 1 - 25
setnames(all_data_melt, "value", "icd_code")

all_data_melt[!ethnic_group == "Maori", ethnic_group := "Non-Maori"]
all_data_melt[sex == "F", sex := "Female"][sex == "M", sex := "Male"]
all_data_melt <- unique(all_data_melt) # removes any duplicate rows based on row_num - fixes double counting Laura noticed

# merge in icd 9 map
# merge in bundle ids 
icd9 <- as.data.table(read.xlsx("FILEPATH"))
#subset to relavent columns
icd9 <- icd9[, c('icd_code', "icd_name", "Level1-Bundel.ID", "Level2-Bundel.ID"), with = F ]
icd9[, icd_code := str_remove(icd9$icd_code, "[.]")] # removes decimal point so can be merged with data

data <- copy(all_data_melt)
data <- merge(data, icd9, by = "icd_code", all.x = T)
setnames(data, c("Level1-Bundel.ID", "Level2-Bundel.ID"), c("bundle_id", "total_bundle_id"))

data <- data[!duplicated(data[, c("row_num", "bundle_id", "total_bundle_id")])] # gets rid of rows with duplicate data to a single bundle_id

uniques <- c("sex", "year_start", "ethnic_group" , "bundle_id", "total_bundle_id")# , "icd_code")
data <- data[, occurrence := .N, by = c(uniques)]
data[, c("icd_code", "icd_name", "row_num") := NULL]

data <- unique(data)# removes any duplicate rows 

data[ethnic_group == "Non-Maori", location_id := 44851][ethnic_group == "Maori", location_id := 44850]
data <- merge(data, locs, by = "location_id")

pop_at_birth <- get_population(age_group_id = 164, location_id = c(44850, 44851), 
                               year_id = c(2009:2016), sex_id = c(1:2), gbd_round_id = 7,
                               decomp_step = "step3")

pop_at_birth <- pop_at_birth[, list(location_id, year_id, sex_id, population)]
pop_at_birth[sex_id == 1, sex := "Male"][sex_id == 2, sex := "Female"]
pop_at_birth[, sex_id := NULL]
setnames(pop_at_birth, c("year_id"), c("year_start"))

# adding population (sample_size)
data[, year_start := as.numeric(year_start)]
data <- merge(data, pop_at_birth, by = c("year_start", "sex", "location_id"))
data[, year_end := year_start]

# adding nids
data <- merge(data, nid_map, by = "year_start")
data[, c("underlying_nid", "seq", "is_outlier") := NA][, c("age_start", "age_end") := 0]
data[, c("source_type") := "Registry - congenital"]

# reorders columns
data <- data[, list(nid, underlying_nid, seq, is_outlier, source_type, location_name, location_id ,
                    ihme_loc_id, sex, year_start, year_end, age_start, age_end, occurrence,
                    population, bundle_id, total_bundle_id)]

data[, cv_livestill := 0]

laura_data <- as.data.table(read.xlsx("FILEPATH"))
raw_cv_includes_chromos_bundle <- laura_data[, list(DELETE.modeler_bundle_id, DELETE.modeler_bundle_name, raw_cv_includes_chromos)]
setnames(raw_cv_includes_chromos_bundle, c("DELETE.modeler_bundle_id", "DELETE.modeler_bundle_name"),c("bundle_id", "case_name"))
raw_cv_includes_chromos_bundle <- unique(raw_cv_includes_chromos_bundle)

nzl_data <- copy(data)
nzl_data <- nzl_data[!is.na(bundle_id) & bundle_id != 0]
nzl_data <- merge(nzl_data, raw_cv_includes_chromos_bundle, by = "bundle_id", all.x = T)

nzl_data[, cases := occurrence]
nzl_data[, sample_size := population]
nzl_data[, mean := cases/sample_size]

### flip cv_includes_chromos
nzl_data <- nzl_data[raw_cv_includes_chromos == "1", cv_excludes_chromos := "0"]
nzl_data <- nzl_data[raw_cv_includes_chromos == "0", cv_excludes_chromos := "1"]
nzl_data <- nzl_data[raw_cv_includes_chromos == "x", cv_excludes_chromos := '']

write.xlsx(nzl_data, paste0("FILEPATH"),
           row.names = FALSE, sheetName = "extraction")
