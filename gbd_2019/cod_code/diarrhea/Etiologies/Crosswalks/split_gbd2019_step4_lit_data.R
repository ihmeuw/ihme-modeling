#######################################################################
## Simple purpose: Pull in etiology and diarrhea bundle data from
## GBD 2019 scientific literature extraction, split into correct
## bundles so that they can be used in modeling.
#######################################################################
library(openxlsx)
library(plyr)
eti_info <- read.csv("/filepath/eti_rr_me_ids.csv")

## Append all files that have literature data ##
data_1 <- read.xlsx("filepath")
data_2 <- read.xlsx("filepath")
data_3 <- read.xlsx("filepath")
data_4 <- read.xlsx("filepath")
all_data <- rbind.fill(data_1, data_2, data_3, data_4)

# Indicator for new data
  all_data$gbd_round <- 2019

# We need the column named "cv_diag_pcr"
  all_data$cv_diag_pcr <- ifelse(!is.na(all_data$cv_diag_pcr), all_data$cv_diag_pcr, all_data$cv_pcr)
  all_data$cv_diag_pcr[is.na(all_data$cv_diag_pcr)] <- 0

# Remove bad columns
  all_data <- all_data[,-which(names(all_data) %in% c("bundle_id_","acause","confirmation_method","cv_pcr","cause_id"))]

# Remove NA values
  all_data[is.na(all_data)] <- ""

# Change all bundle_name similar to "Salmonella"
  all_data$bundle_name <- ifelse(all_data$bundle_name %like% "almonella", "Non-typhoidal Salmonella", as.character(all_data$bundle_name))

######################################################################################################
# Great! Now the issue is that all bundles exist in this file so split them to the correct location. #
  unique(all_data$bundle_name)

  all_data$modelable_entity_name <- all_data$bundle_name

  unique(all_data$modelable_entity_name)

  etiologies <- unique(all_data$modelable_entity_name)
  etiologies <- etiologies[etiologies != "Diarrheal diseases"]

for(i in 1:13){ # There should be 13 etiologies. If there are more unique values, review
  # To save in correct location, we need bundle_id, modelable_entity, and colloquial name
  me_name <- etiologies[i]

  bundle_id <- eti_info$bundle_id[eti_info$modelable_entity_name == me_name]
  modelable_entity <- eti_info$modelable_entity[eti_info$modelable_entity_name == me_name]
  name <- eti_info$name_colloquial[eti_info$modelable_entity_name == me_name]

  out_data <- subset(all_data, modelable_entity_name == me_name)

  write.csv(out_data, "filepath", row.names=F)

}

## Save the new diarrhea data
  out_data <- subset(all_data, modelable_entity_name == "Diarrheal diseases")

  write.csv(out_data, "filepath", row.names=F)


