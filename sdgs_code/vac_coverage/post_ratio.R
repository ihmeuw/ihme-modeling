###########################################################
### Purpose: Post vaccination ratio		
###########################################################

###################
### Setting up ####
###################
rm(list=ls())
pacman::p_load(data.table, dplyr)

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  user <- Sys.info()[["user"]]
  h <- paste0("FILEPATH", user)
}

## Initialize
source(paste0(j, 'FILEPATH/init.r'))

## Source
source(db_tools)

###########################################################################################################################
# 
###########################################################################################################################

num <- "vacc_dpt3"
denom <- "vacc_dpt1"
have <- "vacc_dpt3"

## SET NAMES
num.short <- gsub("vacc_", "", num)
denom.short <- gsub("vacc_", "", denom)
ratio.name <- paste0("vacc_", num.short, "_", denom.short, "_ratio")

## WHAT DO I WANT TO MODEL
want <- setdiff(c(num, denom), have)

## GRAB DATA
df.ratio <-  paste0(data_root, "FILEPATH", ratio.name, ".rds") %>% readRDS %>% data.table
df.have <- paste0(data_root, "FILEPATH", have, ".rds") %>% readRDS %>% data.table

## Change names
cols <- c("gpr_mean", "gpr_lower", "gpr_upper")
cols.ratio <- gsub("gpr", "ratio", cols)
cols.have <- gsub("gpr", "have", cols)
setnames(df.ratio, cols, cols.ratio)
setnames(df.have, cols, cols.have)

## MERGE
df.have <- df.have[, c("location_id", "year_id", "age_group_id", "sex_id",  cols.have), with=F]
df <- merge(df.ratio, df.have, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE)

## MULTIPLY OUT
if (num==have) {
  df <- df[, gpr_mean := 1/ratio_mean * have_mean]
  df <- df[, gpr_lower := 1/ratio_upper * have_upper]
  df <- df[, gpr_upper := 1/ratio_lower * have_lower]
}
if (denom==have) {
  df <- df[, gpr_mean := ratio_mean * have_mean]
  df <- df[, gpr_lower := ratio_lower * have_lower]
  df <- df[, gpr_upper := ratio_upper * have_upper]
}

df <- df[, c(cols.ratio, cols.have) := NULL]
df <- df[, me_name := want]

## CAP estimates
df <- df[, (cols) := lapply(.SD, function(x) ifelse(x >= 1, 0.999, x)), .SDcols=cols]

saveRDS(df, paste0(data_root, "FILEPATH", want, ".rds"))
