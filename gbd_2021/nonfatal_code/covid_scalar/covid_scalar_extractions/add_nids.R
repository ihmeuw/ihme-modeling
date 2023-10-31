# year_id column was calculated as
# extraction[, year_id := year(start_date + (end_date - start_date)/2)]
rm(list=ls())

library(openxlsx)
library(data.table)
library(lubridate)

data <- read.xlsx("/filepath/2021_05_06_all_extractions.xlsx")
data <- data.table(data)
nids <- read.xlsx("/filepath/covid_nids.xlsx")
nids <- data.table(nids)

# format dates
data$start_date <- convertToDate(data$start_date)
data$end_date <- convertToDate(data$end_date)
data$start_date <- format(data$start_date, "%m/%d/%Y")
data$end_date <- format(data$end_date, "%m/%d/%Y")

# function: add NIDs by year
# inputs: full list of year_ids from the report series extractions (not unique years)
# nids_match to find the right surveillance name in the nids list
# data_match to find the right surveillance name in the extractions
add_nid <- function(years_list, nids_match, data_match) {
  nids_subset <- subset(nids, surveillance_name %like% nids_match)
      # add year_id column to nids
      split <- strsplit(nids_subset$surveillance_name, " ")
      split <- unlist(split)
      split <- unlist(lapply(split, as.numeric))
      split <- split[!is.na(split)]
      nids_subset$year_id <- split
  indices <- match(years_list, nids_subset$year_id)
  data <- data[surveillance_name %like% data_match, "nid" := nids_subset$nid[indices]]
}

#####################
# add NIDs for each source series.
#
#
#####################
years <- data[surveillance_name %like% "SINAN"]$year_id
add_nid(years, nids_match="SINAN", data_match="SINAN")

data[surveillance_name == "ECDC" & parent_cause == "diphtheria", "nid" := 472804]
data[surveillance_name == "ECDC" & parent_cause == "lri", "nid" := 472805]

## The only NA nids in the India HMIS extractions were for months April, May, June in 2020. These were the
## only reports available for download for 2020-2021, so they are the only reports associated with a
## 2020-2021 NID.
data[surveillance_name %like% "HMIS" & is.na(nid), "nid" := 472637]

years <- data[surveillance_name %like% "SUIVE"]$year_id
add_nid(years, nids_match="SUIVE", data_match="SUIVE")

years <- data[ihme_loc_id %like% "JPN" & surveillance_name %like% "notified_cases"]$year_id
add_nid(years, nids_match="IDWR-notified_cases", data_match="IDWR-notified_cases")
data[ihme_loc_id %like% "JPN" & surveillance_name %like% "sentinal", surveillance_name := "IDWR-sentinel_reporting"]
years <- data[ihme_loc_id %like% "JPN" & surveillance_name %like% "sentinel_reporting"]$year_id
add_nid(years, nids_match="IDWR-sentinel_reporting",data_match="IDWR-sentinel_reporting")

# NCDC
ncdc_nids <- nids[surveillance_name %like% "Nigeria CDC"]
  cause_year <- strsplit(ncdc_nids$surveillance_name, ": ")
  cause_year <- unlist(cause_year)
  cause_year <- data.table(cause_year)
  names(cause_year) <- "surveillance_name"
  cause_year <- cause_year[surveillance_name != "Nigeria CDC"]
  cause_year <- strsplit(cause_year$surveillance_name, " ")
  cause_year <- unlist(cause_year)
  cause_year <- data.table(cause_year)
  names(cause_year) <- "surveillance_name"
  parent_causes <- data.table(parent_cause = c(rep("Cholera",times=3),rep("Meningitis", times=2),rep("Measles",times=3)))
  years <- as.numeric(cause_year$surveillance_name)
  years <- years[!is.na(years)]
  parent_causes$year_id <- years
  parent_causes$nid <- ncdc_nids$nid
  parent_causes$parent_cause <- tolower(parent_causes$parent_cause)
  parent_causes[parent_cause == "cholera", parent_cause := "diarrhea"]

  ncdc_causes <- c("diarrhea", "measles", "meningitis")

  for (i in 1:length(ncdc_causes)) {
    subset_data <- data[ihme_loc_id %like% "NGA" & surveillance_name %like% "cdc"
                        & parent_cause == ncdc_causes[i]]
    subset_nid <- parent_causes[parent_cause == ncdc_causes[i]]
    index <- match(subset_data$year_id, subset_nid$year_id)
    data <- data[ihme_loc_id %like% "NGA" & surveillance_name %like% "cdc"
                 & parent_cause == ncdc_causes[i], "nid" := subset_nid$nid[index]]
  }
  data <- data[ihme_loc_id %like% "NGA" & surveillance_name %like% "cdc"
               & is.na(nid), "nid" := 472872] # assign the year_id = 2018 measles to the 2017 report because that's where it originated

# public health england
  phe_nids <- nids[surveillance_name %like% "Public Health England"]
  nids_formatted <- unlist(strsplit(phe_nids$surveillance_name, " "))
  nids_formatted[nids_formatted %like% "MMR"] <- "measles"

  quarters <- nids_formatted[nids_formatted %like% "Q"]
  parent_cause <- nids_formatted[nids_formatted %in% c("Meningitis", "Pertussis", "measles")]
  nids_formatted <- as.numeric(nids_formatted)
  year_id <- nids_formatted[!is.na(nids_formatted)]

  final_phe_nids <- data.table(parent_causes=parent_cause, year_ids=year_id, quarter=quarters, nid=phe_nids$nid)
  final_phe_nids$parent_causes <- tolower(final_phe_nids$parent_causes)

  for (i in 1:length(unique(final_phe_nids$parent_causes))) {
    cause <- unique(final_phe_nids$parent_causes)[i]
    cause_dt <- data[surveillance_name %like% "public health england" & parent_cause == cause]

    for (j in 1:length(unique(cause_dt$year_id))) {
      year <- unique(cause_dt$year_id)[j]

      cause_year_nids <- final_phe_nids[parent_causes == cause & year_id == year,]
      year_dt <- cause_dt[year_id==year]

      index <- match(year_dt$notes, cause_year_nids$quarter)
      data <- data[surveillance_name %like% "public health england" & parent_cause == cause & year_id == year,
                   "nid" := cause_year_nids$nid[index]]
    }
  }

  #extracted 2017-2018 quarters 1-3 from 2020 reports
  #extracted 2017-2018 quarter 4 from 2019 Q4 report
  data[surveillance_name %like% "public health england" & parent_cause == "pertussis"
       & is.na(nid) & notes == "Q1", "nid" := 472819]
  data[surveillance_name %like% "public health england" & parent_cause == "pertussis"
       & is.na(nid) & notes == "Q2", "nid" := 472822]
  data[surveillance_name %like% "public health england" & parent_cause == "pertussis"
       & is.na(nid) & notes == "Q3", "nid" := 472823]
  data[surveillance_name %like% "public health england" & parent_cause == "pertussis"
       & is.na(nid) & notes == "Q4", "nid" := 472923]

  # NZL_ESR
  data$temp_month_year <- months(parse_date_time(data$start_date, "m/d/y"))
  data$temp_month_year <- substring(data$temp_month_year, 1,3)
  data$temp_month_year <- paste0(data$temp_month_year, " ", data$year_id)

  nzl_nids <- nids[surveillance_name %like% "NZL_ESR"]
  names <- unlist(strsplit(nzl_nids$surveillance_name, "ESR "))
  names <- names[!names %like% "NZL_"]
  nzl_nids$month_year <- names

  index <- match(data[surveillance_name %like% "NZL_ESR"]$temp_month_year, nzl_nids$month_year)
  data[surveillance_name %like% "NZL_ESR", "nid" := nzl_nids$nid[index]]
  data <- data[,!"temp_month_year"]
  #####

  data[surveillance_name %like% "Singapore" & year_id %in% 2017:2020, "nid":=467105]
  data[surveillance_name %like% "Singapore" & year_id==2021, "nid":=473002]

  data[surveillance_name %like% "cdc - NNDSS" & year_id %in% 2019:2020, "nid":=468459]
  data[surveillance_name %like% "cdc - NNDSS" & year_id == 2018, "nid":=425620]
  data[surveillance_name %like% "cdc - NNDSS" & year_id == 2017, "nid":=425622]

  data[location_name %like% "Afghanistan" & year_id == 2019, "nid":=474358]
  data[location_name %like% "Afghanistan" & year_id == 2020, "nid":=474327]

  data$temp_month <- month(parse_date_time(data$start_date, "mdy"))
  unique(data[location_name=="Bhutan" & is.na(nid)]$temp_month) # gives 4, 7, 10
  data[location_name=="Bhutan" & temp_month == 4, nid:=473133]
  data[location_name=="Bhutan" & temp_month == 7, nid:=473134]
  data[location_name=="Bhutan" & temp_month == 10, nid:=473135]
  data <- data[,-"temp_month"]

  years <- data[surveillance_name %like% "DOM"]$year_id
  add_nid(years, nids_match="Dom", data_match="DOM")

  data[surveillance_name %like% "Ecuador" & parent_cause=="diarrhea", nid:=473335]
  data[surveillance_name %like% "Ecuador" & is.na(nid), nid:=473199]

  years <- data[surveillance_name %like% "Guatemala"]$year_id
  add_nid(years, nids_match="Guatemala", data_match="Guatemala")

  years <- data[surveillance_name %like% "Bolivia"]$year_id
  add_nid(years, nids_match="Bolivia", data_match="Bolivia")

  data[extractor=="mulugabi", surveillance_name:="WHO meningitis bulletin"]
  years <- data[extractor=="mulugabi"]$year_id
  add_nid(years, nids_match="WHO meningitis bulletin", data_match="WHO meningitis bulletin")

  data[surveillance_name=="WHO FluNet", nid:=475714]
  data[surveillance_name=="WHO Measles", nid:=419891]

  years <- data[surveillance_name %like% "Infectious Diseases and Poisonings in Poland"]$year_id
  add_nid(years, nids_match="Infectious Diseases and Poisonings in Poland", data_match="Infectious Diseases and Poisonings in Poland")

  data[surveillance_name %like% "Infectious Diseases and Poisonings in Poland" & year_id == 2018, nid:=429041]
  data[surveillance_name %like% "Infectious Diseases and Poisonings in Poland" & year_id == 2017, nid:=411344]

write.xlsx(data, "/filepath/2021_05_07_all_extractions.xlsx")
