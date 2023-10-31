################################################################################################
###### Purpose: To prepare the hearing crosswalk data for input to mrBRT.
###### 1) Calculate the SE of each xwalk ratio
###### 2) Calculate the log of each xwalk ratio
###### 3) Writes updated sheet to an excel file
#################################################################################################

#setup
user <- Sys.info()["user"]
date <- gsub("-", "_", Sys.Date())
j <- "FILEPATH"
xwalk_temp <- paste0(j,"FILEPATH", user, "FILEPATH")

library(data.table)
library(openxlsx)
library(msm)


#Read in ratios dataset
ratios <- data.table(read.xlsx(paste0(xwalk_temp,"FILEPATH"), rowNames = FALSE))

#######################################################
##### Step 1: Calculate the SE of the xwalk ratio #####
#######################################################

#Doing this on xwalk_inverse rather than xwalk
#do it for one, throw an apply to do for every row
names(ratios)


calc_se <- function(x) {
  sqd_alt <- x[,alt_prev^2]
  sqd_ref <- x[,ref_prev^2]
  sqd_altse <- x[,alt_prev_se^2]
  sqd_refse <- x[,ref_prev_se^2]
  xwalk_se <- sqrt( (sqd_alt/sqd_ref) * ((sqd_altse/sqd_alt) + (sqd_refse/sqd_ref)) )
}

ratios[ , xwalk_se := calc_se(ratios)]
ratios$xwalk <- NULL
ratios$xwalk_logit <- NULL
ratios$notes <- NULL
colnames(ratios)[colnames(ratios) == "xwalk_inverse"] <- "xwalk"


########################################
##### STEP 2: Log the xwalk ratio ######
########################################
ratios[ , xwalk_log := log(xwalk)]

ratios$A <- NULL

ratios <- ratios[xwalk != 1, ]
head(ratios)


########################################
##### STEP 3: Write to File  ######
########################################
write.xlsx(x = ratios, file = paste0(xwalk_temp, "FILEPATH"))
