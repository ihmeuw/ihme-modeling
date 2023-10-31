rm(list = ls())

library(openxlsx)
library(lubridate)
library(data.table)

if (Sys.info()[1] == "Linux"){
  j <- "/filepath"
  h <- paste0("/filepath")
  k <- "/filepath/"
}else if (Sys.info()[1] == "Windows"){
  j <- "filepath"
  h <- "filepath"
  k <- "filepath"
}

# read in sheets 2017-2021
sheet_21 <- read.xlsx(paste0(h, "/weekly-infectious-disease-bulletin-year-202158a39264b75d415585b3025f3bea3c4a.xlsx"))
sheet_list <- lapply(1:4, function(x) {read.xlsx(xlsxFile=paste0(h, "/weekly-infectious-disease-bulletin-year-2020b781eb832978477c8f9ebad9af63aae3.xlsx"),
                                                 sheet = x)} )

# set column names for pre 2021 sheets. 2021 column names already ok -- format by removing periods.
sheet_list2 <- lapply(1:4, function(x) { setNames(sheet_list[[x]], unlist(sheet_list[[x]][1,])) })
sheet_list2 <- lapply(1:4, function(x) { sheet_list2 <- sheet_list2[[x]][-1,] })
colnames(sheet_21) <- gsub(".", " ", colnames(sheet_21), fixed=TRUE) # remove periods from column names

# find matching columns across all years
cols <- list(colnames(sheet_21), colnames(sheet_list2[[1]]), colnames(sheet_list2[[2]]),
             colnames(sheet_list2[[3]]), colnames(sheet_list2[[4]]))
cols <- Reduce(intersect, cols) # looked at this list in excel and removed any causes we don't want.
keep_cols <- c("Start",
               "End",
               "Cholera",
               "Diphtheria",
               "Measles",
               "Campylobacter enteritis",
               "Meningococcal Infection",
               "Pertussis",
               "Pneumococcal Disease (invasive)",
               "Haemophilus influenzae type b",
               "Salmonellosis(non-enteric fevers)",
               "Tetanus"
)

# combine 2020 and 2021 for date processing. Combine all other years into another DT for
# separate date processing.

# 2020 and 2021
sheet_20 <- sheet_list2[[1]]
format1 <- rbindlist(list(sheet_21, sheet_20), fill=TRUE)
format1 <- format1[,..keep_cols]

# pre 2020
format2 <- rbindlist(list(sheet_list2[[2]], sheet_list2[[3]], sheet_list2[[4]]))
format2 <- format2[,..keep_cols]

# format dates
# parsedatetime changes dates to type POSIXt which can then be reformatted to Date type

format1$Start <- as.Date(parse_date_time(format1$Start, c('dmy')))
format1$End <- as.Date(parse_date_time(format1$End, c('dmy')))
format1$Start <- format(format1$Start, "%m/%d/%Y")
format1$End <- format(format1$End, "%m/%d/%Y")

format2$Start <- as.numeric(format2$Start)
format2$End <- as.numeric(format2$End)
format2$Start <- convertToDate(format2$Start)
format2$End <- convertToDate(format2$End)
format2$Start <- format(format2$Start, "%m/%d/%Y")
format2$End <- format(format2$End, "%m/%d/%Y")


# format causes - 2021 and 2020
format1.2 <- melt(format1, id.vars = c("Start", "End"))
format2.2 <- melt(format2, id.vars = c("Start", "End"))

final <- rbindlist(list(format1.2, format2.2))

final[variable=="Cholera", parent_cause:="diarrhea"]
final[variable=="Cholera", cause_name:=variable]

final[variable=="Campylobacter enteritis", parent_cause:="diarrhea"]
final[variable=="Campylobacter enteritis", cause_name:=variable]

final[variable=="Meningococcal Infection", parent_cause:="meningitis"]
final[variable=="Meningococcal Infection", cause_name:=variable]

final[variable=="Pneumococcal Disease (invasive)", parent_cause:="meningitis"]
final[variable=="Pneumococcal Disease (invasive)", cause_name:=variable]

final[variable=="Haemophilus influenzae type b", parent_cause:="LRI"]
final[variable=="Haemophilus influenzae type b", cause_name:=variable]

final[variable=="Salmonellosis(non-enteric fevers)", parent_cause:="diarrhea"]
final[variable=="Salmonellosis(non-enteric fevers)", cause_name:=variable]

final[is.na(parent_cause), parent_cause:=variable]
final <- subset(final, select=-c(variable))

# remove blank placeholder 2021 rows and export
final <- final[!is.na(value),]

# format and add columns to match vpd extraction
colnames(final) <- tolower(colnames(final))
colnames(final)[1] <- "start_date"
colnames(final)[2] <- "end_date"
final$extractor <- "name"
final$surveillance_name <- "Singapore MOH weekly infectious disease bulletin"
final$source_type <- "surveillance"
final$link <- NA
final$location_name <- "Singapore"
final$ihme_loc_id <- "SGP"
final$age_start <- 0
final$age_end <- 99
final$sex <- "both"
final$measure_type <- "incidence"
final$case_status <- "unspecified"
final$sample_size <- "NA"
final$group	<- NA
final$specificity	<- NA
final$group_review	<- NA
final$notes <- NA

# write out sheet
write.xlsx(final, paste0(h, "singapore_moh_extract_2017_2021.xlsx"))
