
## ------------------------------------*---------------------------------------
## This script parses newish DYB years for all-cause mortality
## table 7 = pop; table 9 = births; table 19 = age-spec deaths
## ------------------------------------*---------------------------------------

rm(list=ls())
library(dplyr)      # rbind_all -- X
library(data.table) # rbindlist
library(stringr)    # str_split_fixed, str_extract
library(reshape2)   # melt
library(foreign)

## functions -------------------------------------------------------------------
trim <- function (x) gsub("^\\s+|\\s+$", "", x)  # trim whitespace

strip_dots <- function (x) gsub("\\.", "", x)

clean <- function(x) as.numeric(gsub("\\s|\\,", "", x)) # ALL whitespace

substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}

## setup -----------------------------------------------------------------------
dyb_dir <- "FILEPATH"
continent_list <- list("AFRICA", "ASIA", "EUROPE", "AMERICA, SOUTH", "AMERICA, NORTH", "OCEANIA")

population <- data.frame(country=character(0), 
                        year=numeric(0),
                        pop_both=numeric(0),
                        pop_male=numeric(0),
                        pop_female=numeric(0),
                        stringsAsFactors=FALSE)
                                          
deaths <- data.frame(country=character(0),
                     year=numeric(0),                   
                     deaths_male=numeric(0),
                     deaths_female=numeric(0),
                     deaths_both=numeric(0),
                     age_group=character(0),
                     stringsAsFactors=FALSE)
                     ## bind dataframes on here as they come in

births <- data.frame(country=character(0),
                     year=numeric(0),
                     births=numeric(0),
                     stringsAsFactors=FALSE)

dyb_files <- list.files(dyb_dir, pattern=".csv")
count <- 0
for (dyb in dyb_files) {
  cat(dyb, "\n")
  if (grepl("TBL_7|TBL_07", dyb)) {
    ## parse tables 7 ----------------------------------------------------------
    cat("TABLE 7\n")
    sheet7 <- as.data.frame(fread(paste0(dyb_dir, dyb), stringsAsFactors=FALSE))
    sheet7[, 7:19] <- NULL
    sheet7 <- sheet7[-c(1:4),]
    ## generate logicals for countries, years
    sheet7$years <- (grepl("[0-9]{4}", sheet7[,1]))
    
    sheet7$country <- as.vector(!grepl(paste(continent_list, collapse='|'), trim(sheet7[,1]))
                                & grepl("^[A-Z|Å]", trim(sheet7[,1])) 
                                & !grepl("^Total|Under|Unknown|Aper", trim(sheet7[,1])))
    sheet7 <- sheet7[!grepl(paste(continent_list, collapse='|'), sheet7[,1]),]

    ## count years, some countries have more than one year in this file
    len <- (dim(sheet7)[1] - sum(sheet7[,1] == "") - sum(sheet7$years) - sum(sheet7$country)) 
    table7 <- data.frame(country=character(len), 
                         year=numeric(len), 
                         pop_male=numeric(len),
                         pop_female=numeric(len),
                         pop_both=numeric(len),
                         age_group=character(len),
                         sheet <- rep(dyb, len),
                         stringsAsFactors=FALSE)

    k <- 0 # index of actual data points 1:len
    for (i in 1:dim(sheet7)[1]) {
      if (sheet7$country[i]==TRUE) {
        my_country <- str_split(sheet7[i, 1], " - ")[[1]][1]  # reset when we hit a new one
      } else if (sheet7$years[i]==TRUE) {
        my_year <- as.numeric(str_extract(sheet7[i,1], "[0-9]{4}"))
        my_subdiv <- gsub("\\(|\\)", "" ,str_extract(sheet7[i,1], "\\([A-Z]{4}\\)"))
      } else if (sheet7[i,1] == "") {
        next
      } else {
        k <- k + 1
        table7$country[k] <- my_country  
        table7$year[k] <- my_year
        table7$age_group[k] <- sheet7[i,1]
        table7$pop_both[k] <- as.numeric(gsub(",", "", sheet7[i,2])) # get rid of commas
        table7$pop_male[k] <- as.numeric(gsub(",", "", sheet7[i, 4]))
        table7$pop_female[k] <- as.numeric(gsub(",", "", sheet7[i, 6]))
        table7$subdiv[k] <- my_subdiv 
      }
    }
    
    table7[grepl("^C", table7$subdiv),]$subdiv <- "CENSUS"
    table7[grepl("^SS", table7$subdiv),]$subdiv <- "SURVEY"
    table7[grepl("^ES", table7$subdiv),]$subdiv <- "ESTIMATE"
    
    population <- bind_rows(list(population, table7))
    population$country <- gsub("[0-9]", "", population$country)
  }
  
  if (grepl("TBL_9|TBL_09", dyb)) {
    cat("TABLE 9\n")
    ## parse tables 09 ---------------------------------------------------------
    ## these tables have data for 5 years in columns 3,5,7,9,11
    sheet9 <- as.data.frame(fread(paste0(dyb_dir, dyb), stringsAsFactors=FALSE))

    years <- substrRight(sheet9[1,1], 11)
    years <- strsplit(years, split=" - ")
    yrs <- as.numeric(years[[1]][1]):as.numeric(years[[1]][2])
    
    for (i in 1:12) {
      if (sheet9[i,3] == years[[1]][1]) {
        year_row <- i; three <- TRUE
        sheet9 <- sheet9[3:dim(sheet9)[1], 1:12]
      } 
    }
    stopifnot(three == TRUE)
    
    ## get rid of continents and blanks
    sheet9 <- sheet9[!grepl(paste(continent_list, collapse='|'), trim(sheet9[,1]))
                     & sheet9[,1] != "",]
    
    sheet9$total <- as.vector(grepl("^Total", sheet9[,1]))
    sheet9$country <- as.vector(!grepl("Urban|Rural|Total", trim(sheet9[,1])))
    sheet9 <- sheet9[sheet9$country | sheet9$total,]
    
    len <- sum(sheet9$total)*5
    table9 <- data.frame(country=character(len), 
                         year=numeric(len),
                         births=numeric(len),
                         sheet=rep(dyb, len),
                         stringsAsFactors=FALSE)
    k <- 0
    for(i in 1:dim(sheet9)[1]) {
      if(sheet9$country[i] == TRUE) my_country <- str_split(sheet9[i, 1], " - ")[[1]][1]
      if(sheet9$total[i] == TRUE) {
        yr <- 1
        for(col in c(3,5,7,9,11)) {
          k <- k + 1
          table9$country[k] <- my_country
          table9$year[k] <- yrs[yr]
          table9$births[k] <- as.numeric(gsub(",", "", sheet9[i,col])) # this will coerce to NA if "..."
          yr <- yr + 1
        }
      }
    }
  
    table9 <- table9[!is.na(table9$births),] 
    table9$country <- gsub("[0-9]", "", table9$country)
    cat(nrow(table9), "rows added\n")

    births <- bind_rows(list(table9, births))
    
  }
  
  if (grepl("TBL_19|TABLE_19", dyb)) {
    cat("TABLE 19\n")
    ## parse tables 19  - DEATHS by age ----------------------------------------------
    ## one year per country -- similar format to table 7 -- both, male, female
    
    sheet19 <- as.data.frame(fread(paste0(dyb_dir, dyb), stringsAsFactors=FALSE))
    sheet19[, c(3,5,7:10)] <- NULL
    sheet19 <- sheet19[-c(1:5),]
    sheet19 <- sheet19[!grepl(paste(continent_list, collapse='|'), trim(sheet19[,1]))
                     & sheet19[,1] != "",]
    
    sheet19$country <- as.vector(grepl("^[A-Z|Å]", trim(sheet19[,1])) 
                                 & !grepl("^Total|Under|Unknown|Déc", trim(sheet19[,1])))
    sheet19$years <- (grepl("[0-9]{4}", sheet19[,1]))
    len <- (dim(sheet19)[1] - sum(sheet19$years) - sum(sheet19$country)) 
    
    table19 <- data.frame(country=character(len), 
                         year=numeric(len), 
                         deaths_male=numeric(len),
                         deaths_female=numeric(len),
                         deaths_both=numeric(len),
                         age_group=character(len),
                         sheet <- rep(dyb, len),
                         stringsAsFactors=FALSE)
    
    k <- 0 # index of actual data points 1:len
    for (i in 1:dim(sheet19)[1]) {
      if (sheet19$country[i]==TRUE) {
        my_country <- str_split(sheet19[i,1], " - ")[[1]][1]  # reset when we hit a new one
      } else if (sheet19$years[i]==TRUE) {
        my_year <- as.numeric(str_extract(sheet19[i,1], "[0-9]{4}"))
        my_subdiv <- str_extract(sheet19[i,1], "\\([\\+|(A-Z)+|\\|]*\\)")
      } else if (sheet19[i,1] == "") {
        next
      } else {
        k <- k + 1
        table19$country[k] <- my_country  
        table19$year[k] <- my_year
        table19$age_group[k] <- sheet19[i,1]
        table19$deaths_both[k] <- as.numeric(gsub(",", "", sheet19[i,2])) # get rid of commas
        table19$deaths_male[k] <- as.numeric(gsub(",", "", sheet19[i, 3]))
        table19$deaths_female[k] <- as.numeric(gsub(",", "", sheet19[i, 4]))
        table19$subdiv[k] <- my_subdiv
      }
    }
    table19$country <- gsub("[0-9]", "", table19$country)
    table19 <- table19[!is.na(table19$subdiv),] # drop if no source information
    cat(nrow(table19), "rows added\n")
    
    ## replace subdivs
    table19[grepl("C|U", table19$subdiv),]$subdiv <- "VR"
    table19[table19$subdiv == "(|)",]$subdiv <- "CENSUS"
    
    deaths <- bind_rows(list(table19, deaths))
    
  }
}

## parse country-names, standardizing to GBD location names and merge metadata --------------------------
## keep the three sheets separate as they will be used in different processes
source("J:/Project/Mortality/shared/functions/get_locations.r")
locs <- get_locations(level="countryplus")[,c("location_id", "location_name", "ihme_loc_id")]
data_locs <- unique(c(births$country, deaths$country, population$country))

## DYB names
from <- c("Bolivia (Plurinational State of)", "Brunei Darussalam", "China, Macao SAR", "China, Hong Kong SAR", 
          "Democratic People's Republic of Korea", "Iran (Islamic Republic of)", "Lao People's Democratic Republic",
          "Russian Federation", "Republic of Moldova","Republic of Korea", "Republic of South Sudan", "TFYR of Macedonia",
          "State of Palestine", "Bahamas", "Syrian Arab Republic", "United Kingdom of Great Britain and Northern Ireland", 
          "United States of America", "Venezuela (Bolivarian Republic of)", "Viet Nam", "United States Virgin Islands", 
          "United Republic of Tanzania","China", "Cabo Verde")

## GBD names         
to <- c("Bolivia", "Brunei", "Macao Special Administrative Region of China", "Hong Kong Special Administrative Region of China",
        "North Korea", "Iran", "Laos", "Russia", "Moldova", "South Korea", "South Sudan", "Macedonia",
        "Palestine", "The Bahamas", "Syria", "United Kingdom", 
        "United States", "Venezuela", "Vietnam", "Virgin Islands, U.S.", 
        "Tanzania", "China (without Hong Kong and Macao)", "Cape Verde")

## map them -- can use this function without loading plyr since dplyr is already loaded
births$country <- plyr::mapvalues(births$country, from, to, warn_missing=FALSE)
deaths$country <- plyr::mapvalues(deaths$country, from, to, warn_missing=FALSE)
population$country <- plyr::mapvalues(population$country, from, to, warn_missing=FALSE)

## merge them -- REMEMBER that in the formatted files COUNTRY should actually be ihme_loc_id
births <- merge(births, locs, by.x="country", by.y="location_name", all.x=TRUE)
deaths <- merge(deaths, locs, by.x="country", by.y="location_name", all.x=TRUE)
population <- merge(population, locs, by.x="country", by.y="location_name", all.x=TRUE)

## drop non GBD locs
births <- births[!is.na(births$ihme_loc_id),]
deaths <- deaths[!is.na(deaths$ihme_loc_id),]
population <- population[!is.na(population$ihme_loc_id),]

## names
names(births)[names(births) == "sheet....rep.dyb..len."] <- "sheet"
names(deaths)[names(deaths) == "sheet....rep.dyb..len."] <- "sheet"
names(population)[names(population) == "sheet....rep.dyb..len."] <- "sheet"

## format deaths --------------------------------------------------------------------
## Need to parse age-groups to merge in GBD age metadata -- may contain country names
## also will want to extract sheet years to save each separately (as with past updates)

deaths <- melt(deaths, 
               id.vars=c("country", "year", "location_id", "age_group", "ihme_loc_id", "sheet", "subdiv"), 
               variable.name = "sex", 
               value.name = "deaths",
               na.rm=TRUE)
deaths$sex <- gsub("deaths_", "", as.character(deaths$sex)) # won't actually include this in formatted files
deaths$sex_id <- NA
deaths[deaths$sex == "male",]$sex_id <- 1
deaths[deaths$sex == "female",]$sex_id <- 2
deaths[deaths$sex == "both",]$sex_id <- 0

## extract sheet year
deaths$sheet_year <- as.numeric(gsub("_", "", str_extract(deaths$sheet, "_[0-9]{4}_"))) 

## parse ages -- 90-120 is 90+ for our purposes
deaths$age_group <- gsub(" - ", "to", deaths$age_group)
deaths$age_group <- gsub(" \\+$", "plus", deaths$age_group)
deaths$age_group[deaths$age_group == "90to120"] <- "90plus"
deaths$age_group[grepl("Total" ,deaths$age_group)] <- "TOT"
deaths$age_group[grepl("Unknown", deaths$age_group)] <- "UNK"
deaths$age_group[deaths$age_group == "0"] <- "0to0"

## add in NIDs for each of the sheet years
deaths <- deaths[order(deaths$sheet_year, decreasing=TRUE),]

deaths$nid <- NA
deaths[deaths$sheet_year == 2012,]$nid <- 237478
deaths[deaths$sheet_year == 2013,]$nid <- 237445
deaths[deaths$sheet_year == 2014,]$nid <- 237681

deaths$sheet <- NULL
deaths$sheet_year <- NULL

## drop duplicates between sheet_years
deaths <- deaths[!duplicated(deaths[,c("location_id", "year", "sex", "age_group")]),] 

## reshape wide by age-group
deaths$age_group <- paste0("DATUM", deaths$age_group)

deaths_reshape <- dcast(deaths, 
                        formula = ... ~ age_group,
                        value.var="deaths"
)

## ddm input data naming
names(deaths_reshape)[names(deaths_reshape) == "ihme_loc_id"] <- "COUNTRY"
names(deaths_reshape)[names(deaths_reshape) == "year"] <- "YEAR"
names(deaths_reshape)[names(deaths_reshape) == "nid"] <- "NID"
names(deaths_reshape)[names(deaths_reshape) == "sex_id"] <- "SEX"
names(deaths_reshape)[names(deaths_reshape) == "subdiv"] <- "SUBDIV"
deaths_reshape$VR_SOURCE <- "DYB_ONLINE"
deaths_reshape$sex <- NULL
deaths_reshape$location_id <- NULL
deaths_reshape$country <- NULL

write.dta(deaths_reshape,
          "J:/WORK/02_mortality/02_inputs/02_all_age/01_deaths/dyb_vr_survey/DYB 2012 2013 2014/USABLE_VR_DEATHS_DYB_2012_2013_2014_GLOBAL.dta",
          version=10)


## similar for population -------------------------------------------------------
## except individual sheets are appended in c00b, so need to drop out 2012 sheet as it's already in there
population <- melt(population, 
               id.vars=c("country", "year", "location_id", "age_group", "ihme_loc_id", "sheet", "subdiv"), 
               variable.name = "sex", 
               value.name = "pop",
               na.rm=TRUE
               )

population$sex <- gsub("pop_", "", as.character(population$sex)) # won't actually include this in formatted files
population$sex_id <- NA
population[population$sex == "male",]$sex_id <- 1
population[population$sex == "female",]$sex_id <- 2
population[population$sex == "both",]$sex_id <- 0 ## egh

## extract sheet year
population$sheet_year <- as.numeric(gsub("_", "", str_extract(population$sheet, "_[0-9]{4}_"))) 
population <- population[population$sheet_year != 2012,]

## parse ages
population$age_group <- gsub(" - ", "to", population$age_group)
population$age_group <- gsub(" \\+$", "plus", population$age_group)
population$age_group[population$age_group == "90to120"] <- "90plus"
population$age_group[grepl("Total" ,population$age_group)] <- "TOT"
population$age_group[grepl("Unknown", population$age_group)] <- "UNK"
population$age_group[population$age_group == "0"] <- "0to0"
population$age_group[population$age_group == "0to412"] <- "0to4"

## add in NIDs
population$NID <- NA
population[population$sheet_year == 2013,]$NID <- 237433
population[population$sheet_year == 2014,]$NID <- 237669

population$age_group <- paste0("DATUM", population$age_group)

pop_reshape <- dcast(population, 
                        formula = ... ~ age_group,
                        value.var="pop"
)

# ddm input data names
names(pop_reshape)[names(pop_reshape) == "ihme_loc_id"] <- "COUNTRY"
names(pop_reshape)[names(pop_reshape) == "year"] <- "YEAR"
names(pop_reshape)[names(pop_reshape) == "nid"] <- "NID"
names(pop_reshape)[names(pop_reshape) == "sex_id"] <- "SEX"
names(pop_reshape)[names(pop_reshape) == "subdiv"] <- "SUBDIV"
pop_reshape$sex <- NULL
pop_reshape$location_id <- NULL
pop_reshape$country <- NULL
pop_reshape$sheet <- NULL
pop_reshape$sheet_year <- NULL

#save this one to rawdatadir
write.dta(pop_reshape,
          "FILEPATH/USABLE_CENSUS_POP_DYB_GLOBAL_2013_2014.dta",
          version=10)

## ======================================== END =============================================== ##

