library(openxlsx)
library(data.table)
data_dir <- paste0("FILEPATH/Downloads/")

# RSV extraction
dt <- fread(paste0(data_dir, "ECDC_surveillance_data_Respiratory_Syncytial_Virus.csv"))
# keep only cases and detection rate
dt <- dt[Indicator == "RSV detection" | Indicator == "Total specimens tested"]
# change "Time" to date format
dt$Time <- paste0(dt$Time, "-01")
dt$Time <- as.Date(dt$Time, format = "%Y-%m-%d")
# keep only 2017 and newer
dt <- dt[Time >= "2017-01-01"]
setnames(dt, c("Time"), c("start_date"))
dt$end_date <- as.Date("1970-01-01")
# get the last day of the month
for (i in 1:length(dt$start_date)){
  dt$end_date[i] <- seq(dt$start_date[i], length = 2, by = "months")[2]-1
}
# format to match extraction format
dt$start_date <- format(dt$start_date, "%m/%d/%Y")
dt$end_date <- format(dt$end_date, "%m/%d/%Y")

# Reshape to wide for case detection rate
dt <- dcast(dt, Population + start_date + RegionCode + end_date + RegionName ~ Indicator,
              value.var = "NumValue")
setnames(dt, c("RSV detection", "Total specimens tested"), c("cases", "sample_size"))
dt$cases <- as.integer(dt$cases); dt$sample_size <- as.integer(dt$sample_size)

# Sum across sentinel and nonsentinel (Population variable)
dt <- dt[, lapply(.SD, sum), by=.(start_date, end_date, RegionName, RegionCode), .SDcols=c("cases", "sample_size")]

extraction <- data.table(extractor = "name",
                         nid = NA_integer_,
                         surveillance_name = "ECDC", source_type = "surveillance",
                         link = "http://atlas.ecdc.europa.eu/public/index.aspx",
                         location_name = dt$RegionName,
                         location_id = NA_integer_,
                         age_start = 0,
                         age_end = 99,
                         sex = "both",
                         start_date = dt$start_date,
                         end_date = dt$end_date,
                         parent_cause = "lri",
                         cause_name = "RSV",
                         cases = dt$cases,
                         sample_size = dt$sample_size,
                         measure_type = "incidence",
                         case_status = "confirmed",
                         notes = "summed sentinel and non-sentinel surveillance")

# drop EU/EEA
extraction <- extraction[location_name != "EU/EEA"]

write.xlsx(extraction, "filepath/2021_03_18_ecdc_RSV.xlsx")

# Diphtheria extraction
dt <- fread(paste0(data_dir, "ECDC_surveillance_data_Diphtheria.csv"))
# keep only cases
dt <- dt[Indicator == "Reported cases"]
# change "Time" to date format
dt$Time <- paste0(dt$Time, "-01")
dt$Time <- as.Date(dt$Time, format = "%Y-%m-%d")
# keep only 2017 and newer
dt <- dt[Time >= "2017-01-01"]
setnames(dt, c("Time"), c("start_date"))
dt$end_date <- as.Date("1970-01-01")
# get the last day of the month
for (i in 1:length(dt$start_date)){
  dt$end_date[i] <- seq(dt$start_date[i], length = 2, by = "months")[2]-1
}
# format to match extraction format
dt$start_date <- format(dt$start_date, "%m/%d/%Y")
dt$end_date <- format(dt$end_date, "%m/%d/%Y")
extraction <- data.table(extractor = "name",
                         nid = NA_integer_,
                         surveillance_name = "ECDC", source_type = "surveillance",
                         link = "http://atlas.ecdc.europa.eu/public/index.aspx",
                         location_name = dt$RegionName,
                         location_id = NA_integer_,
                         age_start = 0,
                         age_end = 99,
                         sex = "both",
                         start_date = dt$start_date,
                         end_date = dt$end_date,
                         parent_cause = "diphtheria",
                         cause_name = dt$Population,
                         cases = as.integer(dt$NumValue),
                         sample_size = NA_integer_,
                         measure_type = "incidence",
                         case_status = "confirmed",
                         notes = NA_character_)

# drop EU/EEA
extraction <- extraction[location_name != "EU/EEA"]

write.xlsx(extraction, "filepath/2021_03_18_ecdc_diphtheria.xlsx")

