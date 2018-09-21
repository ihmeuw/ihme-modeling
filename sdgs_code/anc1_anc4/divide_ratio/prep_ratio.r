###########################################################
### Project: HSA (maternal)
### Purpose: Prep ratio
###########################################################


###########################################################################################################################
# Prep ratios for ANC4/ANC1
###########################################################################################################################


## Prep ratio
prep.ratio <- function(num, denom, have, header) {
  
  ## SET NAMES
  num.short <- gsub(header, "", num)
  denom.short <- gsub(header, "", denom)
  ratio.name <- paste0(header, num.short, "_", denom.short, "_ratio")
  
  ## WHAT DO I WANT TO MODEL
  want <- setdiff(c(num, denom), have)
  
  ## GRAB DATA
  df.want <- paste0(data_root, "/exp/to_model/", want, ".csv") %>% fread ## FROM PREP
  df.have <- paste0(data_root, "/exp/modeled/best/", have, ".rds") %>% readRDS %>% data.table ## FROM POST MODEL
  
  ## MERGE
  df.have <- df.have[, .(location_id, year_id, age_group_id, sex_id, gpr_mean)]
  df <- merge(df.want, df.have, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE)
  
  ## TAKE RATIO
  if (num==have) df <- df[, data := gpr_mean/data]
  if (denom==have) df <- df[, data := data/gpr_mean]
  df$gpr_mean <- NULL
  
  ## CAP RATIO
  df <- df[data >= 1, data := 0.999]
  df <- df[, me_name := ratio.name]
  
  ## SAVE
  write.csv(df, paste0(data_root, "/exp/to_model/", ratio.name, ".csv"), na="", row.names=F)
  
  print(paste0("Saved ", data_root, "/exp/to_model/", ratio.name, ".csv"))
  return(df)
}