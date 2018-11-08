########################################################
########################################################

##	Prep R

library(readstata13, lib = "FILEPATH")

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}


##	Load shared function

source(sprintf("FILEPATH/get_demographics.R",prefix))
source(sprintf("FILEPATH/get_location_metadata.R",prefix))
source(sprintf("FILEPATH/get_population.R",prefix))

## YF countries

yfCountries <- c("AGO","ARG","BEN","BOL","BRA","BFA","BDI","CMR","CAF","TCD","COL","COG","CIV","COD","ECU","GNQ","ETH","GAB","GHA","GIN","GMB","GNB",
                "GUY","KEN","LBR","MLI","MRT","NER","NGA","PAN","PRY","PER","RWA","SEN","SLE","SDN","SSD","SUR","TGO","TTO","UGA","VEN","ERI","SOM","STP","TZA","ZMB")

## File Paths

epiDir <- sprintf("FILEPATH", prefix)
inputDir <- sprintf("FILEPATH", prefix)

## Get Locations
Locations <- get_location_metadata(location_set_id = 8)
Locations$new_ihme_loc_id <- unlist(lapply(strsplit(Locations$ihme_loc_id,split="_"), function(x)x[[1]]))
Locations$endemic <- 0
Error <- {}
for (i in yfCountries){
  tmplocs <- which(Locations$new_ihme_loc_id == i)
  if(length(tmplocs) == 0) Error <- c(Error,i)
  Locations$endemic[tmplocs] <- 1
}

EndemicIDs <- Locations$location_id[which(Locations$endemic == 1)]

## Import Data
fullInc <- read.csv(sprintf("FILEPATH", inputDir))[,-1]
fullInc[] <- lapply(fullInc, as.character)
fullInc$cases <- as.numeric(fullInc$cases)
fullInc$mean <- as.numeric(fullInc$mean)
fullInc$standard_error <- as.numeric(fullInc$standard_error)
fullInc$sample_size <- as.numeric(fullInc$sample_size)
fullInc$effective_sample_size <- as.numeric(fullInc$effective_sample_size)

# 2017 data
data2017 <- read.csv(sprintf("FILEPATH",prefix))
data2017[] <- lapply(data2017, as.character)
data2017 <- data2017[data2017$measure == "incidence" & data2017$cases > 0 & data2017$confirmed_or_suspected == "confirmed",]
Toss <- which(data2017$location_id == "")
data2017 <- data2017[-Toss,]

tmploc <- which(fullInc$ihme_loc_id == "BRA")[1]
template <- fullInc[rep(tmploc, length(data2017[,1])),]
for (i in 1:length(data2017[,1])){
  template$location_id[i] <- data2017$location_id[i]
  template$year_id[i] <- 2017
  template$effective_sample_size[i] <- get_population(location_id = data2017$location_id[i], year_id = 2017)$population
  template$cases[i] <- as.numeric(data2017$cases[i])
  template$location_name[i] <- data2017$location_name[i]
  template$ihme_loc_id[i] <- data2017$ihme_loc_id[i]
  template$sample_size[i] <- template$effective_sample_size[i]
  template$mean[i] <- template$cases[i] / template$sample_size[i]
  template$standard_error[i] <- sqrt(template$mean[i] * (1 - template$mean[i]) / template$sample_size[i])
  template$upper[i] <- template$mean[i] + 1.96 * template$standard_error[i]
  template$lower[i] <- template$mean[i] - 1.96 * template$standard_error[i]
  template$nid[i] <- data2017$nid[i]
  template$field_citation_value[i] <- data2017$field_citation_value[i]
  template$extractor[i] <- "USERNAME"
  template$year_start[i] <- 2017
  template$year_end[i] <- 2017
  template$age_start[i] <- data2017$age_start[i]
  template$age_end[i] <- data2017$age_end[i]
  template$data_sheet_file_path[i] <- data2017$file_path[i]
}

template$age_end[template$age_end == 99] <- 100

fullInc <- rbind(fullInc, template)

## Clean data to make it all age

data <- fullInc[-2,c(1,2,3,6,7,8,9,10,11,12,13,15,16,18,26,29,30,57:58)]

data$cases[is.na(data$cases)] <- 0
data$age_start <- as.numeric(data$age_start)
data$age_end <- as.numeric(data$age_end)
data$year_start <- as.numeric(data$year_start)
data$year_end <- as.numeric(data$year_end)
data <- unique(data)

## Clean up age-specific data points (model is based on all-age, both-sex data)

AllAge <- which(data$age_start == 0 & data$age_end == 100)
dataAA <- data[AllAge,]
dataNAA <- data[-AllAge,]


## Check for partitioning age bins, re-aggregate them and drop everything else.
Keep <- {}
ULoc <- unique(dataNAA$location_id)
UYears <- unique(dataNAA$year_start[dataNAA$year_start == dataNAA$year_end])

for (tmploc in ULoc){
  for (tmpyear in UYears){
    tmprows <- which(dataNAA$location_id == tmploc & dataNAA$year_start == tmpyear)
    UGender <- unique(dataNAA$sex[tmprows])
    if (length(tmprows)){
      for (tmpGender in UGender){
        tmpnewrows <- which(dataNAA$location_id == tmploc & dataNAA$year_start == tmpyear & dataNAA$sex == tmpGender)
        tmpmat <- dataNAA[tmpnewrows,]
        tmpmat <- tmpmat[order(tmpmat$age_start),]
        if (length(tmpnewrows) > 1){
          tmpAges <- as.vector(t(as.matrix(tmpmat[c("age_start" , "age_end")])))
          tmpMaxGap <- max(diff(tmpAges)[seq(2,length(tmpAges)-1,by=2)])
          if (tmpAges[1] == 0 & tmpAges[length(tmpAges)] == 99 & tmpMaxGap <= 1){
            Keep <- c(Keep, tmpnewrows[1])
            dataNAA$age_start[tmpnewrows[1]] <- 0
            dataNAA$age_end[tmpnewrows[1]] <- 99
            dataNAA$cases[tmpnewrows[1]] <- sum(dataNAA$cases[tmpnewrows])
            dataNAA$sample_size[tmpnewrows[1]] <- sum(dataNAA$sample_size[tmpnewrows])
            dataNAA$mean[tmpnewrows[1]] <- dataNAA$cases[tmpnewrows[1]] / dataNAA$sample_size[tmpnewrows[1]]
            dataNAA$lower[tmpnewrows[1]] <- NA
            dataNAA$upper[tmpnewrows[1]] <- NA
            dataNAA$standard_error[tmpnewrows[1]] <- NA
            dataNAA$effective_sample_size[tmpnewrows[1]] <- NA
          }
        }
      }
    }
  }
}

dataAA <- rbind(dataAA,dataNAA[Keep,])

# Pull just those that are all age and all sex combined

AllAgeSex <- which(dataAA$sex == "Both")
dataAAS <- dataAA[AllAgeSex,]


## Where we have all-age, sex-sepcific data points for a location/year, combine them to be both sex and drop all other points

dataNAAS <- dataAA[-AllAgeSex,]

HasBoth <- {}
UYears <- unique(dataNAAS$year_start[dataNAAS$year_start == dataNAAS$year_end])
ULoc <- unique(dataNAAS$location_id)

for (tmploc in ULoc){
  for (tmpyear in UYears){
    tmpmatch <- which(dataNAAS$year_start == tmpyear & dataNAAS$location_id == tmploc)
    if (length(tmpmatch)){
      tmpmale <- which(dataNAAS$sex[tmpmatch] == "Male")
      tmpfemale <- which(dataNAAS$sex[tmpmatch] == "Female")
      if (length(tmpmale) == 1 & length(tmpfemale) == 1){
        HasBoth <- c(HasBoth,tmpmatch[1])
        dataNAAS$sex[tmpmatch[1]] <- "Both"
        dataNAAS$cases[tmpmatch[1]] <- sum(dataNAAS$cases[tmpmatch])
        dataNAAS$sample_size[tmpmatch[1]] <- sum(dataNAAS$sample_size[tmpmatch])
        dataNAAS$mean[tmpmatch[1]] <- dataNAAS$cases[tmpmatch[1]] / dataNAAS$sample_size[tmpmatch[1]]
        dataNAAS$lower[tmpmatch[1]] <- NA
        dataNAAS$upper[tmpmatch[1]] <- NA
        dataNAAS$standard_error[tmpmatch[1]] <- NA
      }
    }
  }
}

FinalData <- rbind(dataAAS,dataNAAS[HasBoth,])
Toss <- which(FinalData$ihme_loc_id == "")
FinalData <- FinalData[-Toss,]
FinalData <- unique(FinalData)

# Create skeleton dataset of every combination of iso, age, sex, & year

# Using cov to get national numbers for places where we would usually do subnational estimation (e.g., China)
AllPlaces <- get_demographics(gbd_team = "cov")
AllPop <- get_population(age_group_id = c(AllPlaces$age_group_id,22), location_id = AllPlaces$location_id, year_id = AllPlaces$year_id, sex_id = 1:3)

AllLoc <- get_location_metadata(location_set_id = 8)
AllLoc <- AllLoc[,c( "location_set_version_id","location_set_id","location_id","parent_id","is_estimate","location_name","location_type","super_region_id","super_region_name","region_id",
                    "region_name","ihme_loc_id")]


AllOut <- merge(AllPop,AllLoc, by = "location_id", all = TRUE)

Keep <- which(AllOut$is_estimate == 1 | AllOut$location_type == "admin0")
AllOut <- AllOut[Keep,]
AllOut$countryIso <- substr(AllOut$ihme_loc_id,1,3)

write.csv(AllOut,sprintf("FILEPATH", inputDir))

Keep <- which(AllOut$sex_id == 3 & AllOut$age_group_id == 22)
SubDat <- AllOut[Keep,]

names(SubDat)[3] <- "year_start"

FinalData$location_id <- as.integer(FinalData$location_id)


AllData <- merge(SubDat, FinalData, by = c("location_id","year_start"), all=TRUE)

AllData$endemic <- 0
for (i in yfCountries){
  tmplocs <- which(AllData$countryIso == i)
  AllData$endemic[tmplocs] <- 1
}

Keep <- which(AllData$endemic == 1)

AllData$cases[is.na(AllData$cases)] <- 0

dim(AllData)[1] == dim(SubDat)[1]

write.csv(AllData[Keep,], sprintf("FILEPATH/dataToModel2017.csv", inputDir))
