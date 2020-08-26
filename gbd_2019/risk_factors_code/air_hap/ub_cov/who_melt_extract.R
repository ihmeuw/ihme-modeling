#-------------------Header------------------------------------------------
# Purpose: custom code to melt subset of 2008 WHO HH Energy Database into our collapse format for exposure data
#       Reads in file from FILEPATH (ADDRESS)
#       Saves file to FILEPATH (ADDRESS)
#          
#***************************************************************************

#clean library
rm(list=ls())

library("data.table")

options(max.print=1000000)

setwd("FILEPATH")

hap_subset <- fread(file="FILEPATH")

melted_hap_subset <- melt(hap_subset, id = c("nid", "ihme_loc_id", "year", "sample_size"), measured = c("hap_solid", "hap_none", "hap_electricity", "hap_kerosene", "hap_wood", "hap_crop", "hap_coal", "hap_dung", "missing_cooking_fuel_mapped"))

# copy data.table
melted_hap_subset2 <- melted_hap_subset

# convert factor var to character var
melted_hap_subset2$variable <- as.character(melted_hap_subset2$variable)

# sort key
sort_df <- data.frame(variable = c("hap_solid", "hap_none", "hap_electricity", "hap_gas", "hap_kerosene", "hap_wood", "hap_crop", "hap_coal", "hap_dung", "missing_cooking_fuel_mapped"), key = c(1:10))
sort_df$variable <- as.character(sort_df$variable) 

# subset hap to just electricty and gas
final_subset <- as.data.table(melted_hap_subset2)[variable %in% c("hap_solid", "hap_none", "hap_electricity", "hap_gas", "hap_kerosene", "hap_wood", "hap_crop", "hap_coal", "hap_dung", "missing_cooking_fuel_mapped"),]
final_subset <- merge(final_subset, sort_df, by = 'variable')
final_subset <- final_subset[order(nid, key),]

final_subset <- final_subset[,-c('key')]
final_subset[is.na(value),value:=0]

# length_fs <- dim(final_subset)
# length_fs <- length_fs[1]
# filepath <- rep('FILEPATH', mode = character, length = length_fs)

setcolorder(final_subset, c("nid", "ihme_loc_id", "year", "variable", "value", "sample_size"))

#save file by date or to separate folder for Censuses
write.csv(final_subset,'FILEPATH',
          row.names=F)
