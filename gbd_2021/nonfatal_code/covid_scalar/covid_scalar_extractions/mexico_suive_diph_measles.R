library(openxlsx)
library(data.table)
library(lubridate)

file <- read.xlsx(paste0(getwd(), "/Mexico SUIVE extractions (pt2).xlsx")) #read.xlsx drops empty rows unless you specify skipEmptyRows=FALSE
    file$start_date <- convertToDate(file$start_date)
    file$end_date <- convertToDate(file$end_date)
    file$start_date <- format(file$start_date, "%m/%d/%Y")
    file$end_date <- format(file$end_date, "%m/%d/%Y")
dates <- read.xlsx(paste0(getwd(), "/dates.xlsx"))
    dates$start_date <- convertToDate(dates$start_date)
    dates$end_date <- convertToDate(dates$end_date)
locs <- read.xlsx(paste0(getwd(), "/mexico_locs.xlsx"))

n <- nrow(locs)

table <- data.table(start_date=rep(dates$start_date, times=n), end_date=rep(dates$end_date, times=n))
table$location_name <- rep(locs$location_name, each=nrow(dates))
table$ihme_loc_id <- rep(locs$ihme_loc_id, each=nrow(dates))
table$start_date <- format(table$start_date, "%m/%d/%Y")
table$end_date <- format(table$end_date, "%m/%d/%Y")

diph <- data.table(extractor=character(), nid=numeric(), surveillance_name=character(), source_type=character(), link=character(),
                   location_name=character(), ihme_loc_id=character(), age_start=numeric(),
                   age_end=numeric(), start_date=character(), end_date=character(),
                   parent_cause=character(), cause_name=character(), cases=numeric(),
                   sample_size=character(),measure_type=character(), case_status=character(),
                   group=numeric(), specificity=character(), group_review=numeric(), notes=character())
diph <- rbind(diph, list(start_date = table$start_date), fill=TRUE)
diph$end_date <- table$end_date
diph$location_name <- table$location_name
diph$ihme_loc_id <- table$ihme_loc_id
diph$parent_cause <- "diphtheria"

measles <- data.table(extractor=character(), nid=numeric(), surveillance_name=character(), source_type=character(), link=character(),
                   location_name=character(), ihme_loc_id=character(), age_start=numeric(),
                   age_end=numeric(), start_date=character(), end_date=character(),
                   parent_cause=character(), cause_name=character(), cases=numeric(),
                   sample_size=character(),measure_type=character(), case_status=character(),
                   group=numeric(), specificity=character(), group_review=numeric(), notes=character())

measles <- rbind(measles, list(start_date = table$start_date), fill=TRUE)
measles$end_date <- table$end_date
measles$location_name <- table$location_name
measles$ihme_loc_id <- table$ihme_loc_id
measles$parent_cause <- "measles"

combined <- rbind(diph, measles,fill=TRUE)
combined$extractor <- "name"
combined$surveillance_name <- "Mexico SUIVE"
combined$source_type <- "surveillance"
combined$link <- "https://www.gob.mx/salud/acciones-y-programas/historico-boletin-epidemiologico"
combined$age_start <- 0
combined$age_end <- 99
combined$sex <- "both"
combined$measure_type <- "incidence"
combined$case_status <- "Confirmed"
combined$cases <- 0

file <- rbind(file, combined)
write.xlsx(file, paste0(getwd(), "/Mexico_SUIVE_pt2.xlsx"))

