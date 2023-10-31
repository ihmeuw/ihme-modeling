##################################################################################################
## Purpose: Motor Neuron Disease Clinical
## Date: 2020/12/09
## Created by: USERNAME
##################################################################################################

library(data.table)
library(openxlsx)

shared_fun_dir <- "FILEPATH"
functs <- c("get_bundle_version",
            "get_crosswalk_version",
            "save_crosswalk_version")

for (funct in functs){
  source(paste0(shared_fun_dir, funct, ".R"))
}


date <- gsub("-", "_", Sys.Date())
bundle_id <- 449
decomp_step <- "iterative"
path_to_data_2019_xwalk <- paste0("FILEPATH", date, "_", decomp_step, "_",bundle_id, "2019_best_xwalk_2020_norway_xwalk_data", ".xlsx")
path_to_data_2019_xwalk_mktscn <-  paste0("FILEPATH", date, "_", decomp_step, "_",bundle_id, "2019_best_xwalk_2020_norway_xwalk_2019_bundle_mktscn_2015_16_data", ".xlsx")
#Bundle and crosswalk ids:
bundle_version_id_2019 <- 15407
crosswalk_version_id_2019 <- 12422
bundle_version_id_2020 <- 36272


#Calling in the bundles and crosswalks
mnd_2019_best_bun <- get_bundle_version(bundle_version_id_2019, 
                                        fetch = "all")
mnd_2020_2019_copy_bun <- get_bundle_version(bundle_version_id_2020, 
                                             fetch = "all")
mnd_2019_best_xwalk <- data.table(get_crosswalk_version(crosswalk_version_id = crosswalk_version_id_2019))

#Copies to play with
mnd_19_bun <- copy(mnd_2019_best_bun)
mnd_20_bun <- copy(mnd_2020_2019_copy_bun)
mnd_19_xw <- copy(mnd_2019_best_xwalk)

#Number of rows
nrow(mnd_19_bun) 
nrow(mnd_20_bun)
nrow(mnd_19_xw)

#Investigating seq's
summary(mnd_19_bun[, seq])
summary(mnd_20_bun[, seq])
summary(mnd_19_xw[, seq])

#Checking the other seq related values
summary(mnd_19_bun[, origin_seq])
summary(mnd_20_bun[, origin_seq])
summary(mnd_19_xw[, origin_seq])

summary(mnd_19_bun[, crosswalk_parent_seq])
summary(mnd_20_bun[, crosswalk_parent_seq])
summary(mnd_19_xw[, crosswalk_parent_seq])

sort(unique(mnd_19_bun[seq > 60000, field_citation_value]))
sort(unique(mnd_20_bun[seq > 60000, field_citation_value]))
sort(unique(mnd_19_xw[seq > 60000, field_citation_value])) 

#Checking that its all clinical
clin_nids <- c(3822, 13334, 67132, 68367, 68535, 68719, 68722, 68725, 68728, 68731, 68734, 68737, 68740, 68743, 68746, 68749, 68752, 68755, 68758, 68761, 68764, 68767, 96714, 104246, 114636, 121408, 121415, 121416, 121417, 121418, 121419, 121420, 121421, 121422, 121423, 121424, 121425, 133665, 205018, 206640, 209949, 234670, 234671, 234672, 234673, 234674, 234692, 234693, 234693, 234703, 234703, 234704, 234738, 234738, 234740, 234740, 234741, 234741, 234742, 234742, 234742, 234745, 234746, 234747, 234750, 234758, 234760, 234761, 234762, 234764, 234765, 234766, 234769, 234771, 234772, 234774, 244369, 244370, 244371, 256692, 264064, 281819, 283639, 284419, 284421, 284422, 284439, 284440, 284442, 284444, 285520, 287201, 287202, 287203, 287204, 287204, 287207, 292437, 292574, 292575, 292577, 299375, 303732, 303746, 303747, 303749, 303750, 303752, 303753, 303754, 303755, 303756, 304745, 317423, 321359, 331084, 334464, 334465, 334466, 336203, 336817, 336820, 336822, 336824, 336825, 336826, 336827, 336828, 336829, 336830, 336831, 336832, 336833, 336834, 336835, 336836, 336837, 336838, 336839, 336840, 336841, 336842, 336843, 336844, 336845, 336846, 336848, 336849, 336850, 336851, 336852, 337129, 337619, 337619, 406980, 336847, 354896, 404395, 406980, 407536, 408336, 408680, 411100, 411786, 411787, 397812, 397813, 397814, 408789, 409153, 409154, 409155, 409156, 409157, 409158, 409159)
summary(mnd_19_bun[nid %in% clin_nids, seq])
summary(mnd_19_bun[!nid %in% clin_nids, seq])
summary(mnd_20_bun[nid %in% clin_nids, seq])
summary(mnd_20_bun[!nid %in% clin_nids, seq])
summary(mnd_19_xw[nid %in% clin_nids, seq])
summary(mnd_19_xw[!nid %in% clin_nids, seq]) 

nor_old <- c(4911, 4912, 4913, 4914, 4915, 4916, 4917, 4918, 4919, 4921, 4922, 4927, 4928) #Norway old location_ids

summary(mnd_19_bun[location_id %in% nor_old, seq])
sort(unique(mnd_19_bun[location_id %in% nor_old, field_citation_value]))
summary(mnd_20_bun[location_id %in% nor_old, seq])
summary(mnd_19_xw[location_id %in% nor_old, seq])

mnd_19_xw[, seq := as.character(seq)]
mnd_19_xw[, seq := ""]
mnd_19_xw[, crosswalk_parent_seq := origin_seq]

sort(unique(mnd_19_xw[crosswalk_parent_seq %in% 25558:33578, field_citation_value]))

sort(unique(mnd_20_bun[origin_seq %in% 25558:33578, field_citation_value])) 
# The bundle data contains a number of NIDs, including the Marketscan data. 

sort(unique(mnd_19_xw[ !crosswalk_parent_seq %in% mnd_20_bun[,origin_seq], field_citation_value]))

mnd_19_no_in <- mnd_19_xw[ !crosswalk_parent_seq %in% mnd_20_bun[,origin_seq], ] #Creating an object to interact with

dim(mnd_19_no_in)
#Dimensions look fine.

summary(mnd_19_no_in[, crosswalk_parent_seq]) 
summary(mnd_19_no_in[, seq]) 
summary(mnd_19_no_in[, origin_seq]) 

dim(mnd_20_bun[ seq %in% mnd_19_no_in[, crosswalk_parent_seq],])
dim(mnd_20_bun[ nid %in% mnd_19_no_in[, nid],])

summary(mnd_20_bun[ nid %in% mnd_19_no_in[, nid], origin_seq])
summary(mnd_19_no_in[, origin_seq]) 

sum(sort(mnd_20_bun[ nid %in% mnd_19_no_in[, nid], mean]) == sort(mnd_19_no_in[, mean]))
sum(sort(mnd_20_bun[ nid %in% mnd_19_no_in[, nid], upper]) == sort(mnd_19_no_in[, upper]))
sum(sort(mnd_20_bun[ nid %in% mnd_19_no_in[, nid], lower]) == sort(mnd_19_no_in[, lower]))

nrow(mnd_19_xw[nid %in% c(336847, 408680)])
nrow(mnd_20_bun[nid %in% c(336847, 408680)])
nrow(mnd_19_no_in)
#Same number of rows accross the board.
length(colnames(mnd_19_xw))
length(colnames(mnd_20_bun))

colnames(mnd_19_xw)[!colnames(mnd_19_xw) %in% colnames(mnd_20_bun)] 

#Removing additional variables
mnd_19_xw[, adjust_for := NULL]
mnd_19_xw[, age_range := NULL]
mnd_19_xw[, variance := NULL]
mnd_19_xw[, crosswalk_parent_seq := NULL]
#Checking same length
length(colnames(mnd_19_xw))
length(colnames(mnd_20_bun))

colnames(mnd_19_xw)[!colnames(mnd_19_xw) %in% colnames(mnd_20_bun)]
colnames(mnd_20_bun)[!colnames(mnd_20_bun) %in% colnames(mnd_19_xw)]

mnd_new <- rbind(mnd_19_xw[!nid %in% c(336847, 408680), ], mnd_20_bun[nid %in% c(336847, 408680)]) 

dim(mnd_new) == dim(mnd_19_xw)

mnd_new[, seq := as.character(seq)]
mnd_new[, seq := ""]
mnd_new[, crosswalk_parent_seq := origin_seq]

write.xlsx(mnd_new, path_to_data_2019_xwalk_mktscn, sheetName = "extraction")

result1 <- save_crosswalk_version(bundle_version_id = 36272,
                                  data_filepath = path_to_data_2019_xwalk_mktscn,
                                  description = "2019 best xwalk, norway location change for 2020 but no norway locations, 2015/2016 Marketscan from 2019 bundle data")