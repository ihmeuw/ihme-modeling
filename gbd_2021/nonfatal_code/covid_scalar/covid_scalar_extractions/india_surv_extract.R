## Load packages
library(rvest)
library(data.table)
library(openxlsx)

## Base data directory
data_dir <- paste0("filepath")

## Create reference tables
months_table <- data.table(index = 1:12,
                           letter = c("J", "K", "L", "A",
                                      "B", "C", "D", "E",
                                      "f", "G", "H", "I"),
                           month = c("January", "February",
                                     "March", "April",
                                     "May", "June",
                                     "July", "August",
                                     "September", "October",
                                     "November", "December"),
                           last_day = c(31, 28, 31, 30,
                                        31, 30, 31, 31,
                                        30, 31, 30, 31))
year_set_table <- data.table(year_set = c("2016-2017", "2017-2018", "2018-2019", "2019-2020", "2020-2021"),
                             nid = c(313533, 374936, 404047, 411111, NA_character_))

## Initialize extraction data table
extraction <- data.table()

## Loop through years of interest
for (year in year_set_table$year_set){
  # get the cause names present in this source
  cause_table <- data.table(cause_name = c("lri", "diphtheria", "pertussis", "tetanus", "measles", "diarrhea"),
                            description = c("Childhood Diseases - Pneumonia",
                                            "Childhood Diseases - Diphtheria",
                                            "Childhood Diseases - Pertussis",
                                            "Childhood Diseases - Tetanus Neonatorum",
                                            "Childhood Diseases - Measles",
                                            "Childhood Diseases - Diarrhoea")
  )
  if (year == "2016-2017"){
    # different cause names
    cause_table <- data.table(cause_name = c("diphtheria", "pertussis", "tetanus", "measles", "diarrhea"),
                              description = c("Number of cases of Diptheria reported in children below 5 years of age",
                                              "Number of cases of Pertusis reported in children below 5 years of age",
                                              "Number of cases of Tetanus neonatarum reported in children below 5 years of age",
                                              "Number of cases of Measles reported in children below 5 years of age" ,
                                              "Number of cases of Diarrhoea and Dehydration reported in children below 5 years of age")
    )
  }

  ## Continuation of the filepath
  data_extension <- file.path(year, year, "A.MonthWise")

  ## Figure out what months to loop through
  files.present <- list.files(paste0(data_dir, data_extension))

  ## If there is a complete year, extract all months, otherwise only extract the first months present
  if (length(files.present)==12) months_loop <- 1:length(files.present)
  if (length(files.present)<12 & length(files.present <=9)) months_loop <- 4:(3+length(files.present))

  ## Exception for 2016-2017 since we only need Jan-Mar 2017
  if (year == "2016-2017") months_loop <- 1:3

  ## Loop through months
  for (i in months_loop){
    # Grab the year based on the month, new year starts April
    if (i >= 4) { # if it is April or later
      year_id <- substr(year, 1,4) # use the first year in the pair
    } else year_id <- substr(year, 6,9) # otherwise the second year
    # Get the data path based on the month
    data_path <- paste0(data_dir, data_extension, "/",
                        months_table[i,]$letter, " - ",
                        months_table[i,]$month)
    # Rename the extension from .xls to .htm if this has not already been done
    if (!exists(paste0(data_path, ".htm"))) renamed <- file.rename(paste0(data_path, ".xls"), paste0(data_path, ".htm"))
    url <- paste0(data_path, ".htm")
    # Read in the htm file
    dt <- url %>%
      read_html() %>%
      html_nodes("table") %>% ## find all tables (cam also put specific xpath here)
      .[[1]] %>% ## take third table (might need to inspect a bit to find right #)
      html_table() ## read
    dt <- as.data.table(dt)
    # Get all unique locations, drop blank and Ministry
    locations <- unique(as.character(dt[2,]))[unique(as.character(dt[2,])) != ""]
    # Rename columns
    setnames(dt, c("category", "code", "description", "V4", as.character(dt[2,5:ncol(dt)])))
    # Subset to causes of interest
    dt_subset <- copy(dt[category %like% "Diseases"])
    dt_subset <- rbind(dt[3,], dt_subset)
    # urbanicity <- unique(as.character(dt_subset[3,]))[grep("Urban|Rural|Total",unique(as.character(dt_subset[3,])))]
    # Loop through all locations
    for (location in locations){
        # Get the case counts for a given location
        dt_tmp <- dt_subset[,..location]
        # Check that this is the total row, in the case of urbanicity splits being available
        if ((!dt_tmp[1,1]%like% "Total") & year != "2016-2017") stop("You are not grabbing the total row! stop and check urbanicity")
        # Loop through each cause present in that location-year-month
        for (desc in unique(cause_table$description)){
          # Grab GBD cause name from reference table
          cause_name <- cause_table[description==desc]$cause_name
          # Put description headers back into the table
          dt_tmp2 <- cbind(dt_subset[,1:3], dt_tmp)
          # Grab the right cause
          dt_tmp2 <- dt_tmp2[description == desc]
          case_count <- as.numeric(dt_tmp2[,..location])
          # Generate the correctly-formatted table for the data you have here!
          dt_new <- data.table(extractor = "name", nid = year_set_table[year_set == year]$nid,
                               surveillance_name = "HMIS", source_type = "surveillance",
                               link = "https://nrhm-mis.nic.in/hmisreports/frmstandard_reports.aspx",
                               location_name = location,
                               age_start = 0,
                               age_end = 4,
                               sex = "both",
                               start_date = paste0(i, "/1/", year_id),
                               end_date = paste0(i, "/", months_table[i,]$last_day, "/", year_id),
                               parent_cause = cause_name,
                               cause_name = NA_character_,
                               cases = case_count,
                               sample_size = NA_integer_,
                               measure_type = "incidence",
                               case_status = "unspecified",
                               notes = NA_character_
          )
          # Add to the overall extraction table
          extraction <- rbind(extraction, dt_new)
        }

      }
    }
}

# Some final formatting before upload of the locations
# Drop Ministry Rows
extraction <- extraction[!location_name %like% "M/O"]
# Add notes
extraction[cause_name == "tetanus", notes := "defined as tetanus neonatorum"]
extraction[cause_name == "lri", notes := "defined as pneumonia"]
# Get locations
# Rename "All India" to "India"
extraction[location_name == "All India", location_name := "India"]
extraction[location_name %in% c("Chandigarh", "A & N Islands","Dadra & Nagar Haveli", "Daman & Diu", "Lakshadweep","Puducherry"),
           location_name := "Other Union Territories"]
extraction[location_name == "Jammu & Kashmir", location_name := "Jammu & Kashmir and Ladakh"]
hierarchy <- fread("filepath/locations_2020.csv")
extraction <- merge(extraction, hierarchy[ihme_loc_id %like% "IND",.(location_name, location_id, ihme_loc_id)], by = "location_name")

write.xlsx(extraction, "filepath/2021_03_18_India_HMIS_extract.xlsx")

