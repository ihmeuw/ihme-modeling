############################################################################################################
## Purpose: Split out 3-digit HRH codes using intermediate model results
###########################################################################################################

## clear memory
rm(list=ls())

## runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

pacman::p_load(plyr,rhdf5,data.table,magrittr,parallel)

## settings (make sure it's consistent with what was modeled (ex. clean))
output.version <- 1
by_sex <- F
clean <- T

## in/out
root.dir <- file.path(j,"FILEPATH")
in.dir <- file.path(j,"FILEPATH")
out.dir <- file.path(j,"FILEPATH")
run.db <- fread("FILEPATH")
model.dir <- "FILEPATH"

model_root <- "FILEPATH"
setwd(model_root)
source("FILEPATH")
source(file.path(j,"FILEPATH"))
locations <- get_location_metadata(gbd_round_id = 5, location_set_id = 22)

## determine suffix of files of interest based on sex disaggregation and whether or not data was cleaned (in post_collapse_hrh.R)
if (by_sex & clean){
  suffix <- paste0("_sex_",output.version,"_clean.csv")
} else if (by_sex & !clean) {
  suffix <- paste0("_sex_",output.version,".csv")
} else if (!by_sex & clean) {
  suffix <- paste0("_",output.version,"_clean.csv")
} else {
  suffix <- paste0("_",output.version,".csv")
}

## read in relevant 3-digit estimates to be split, as well as the cadres included in each 3-digit code
files <- list.files(in.dir)
files <- files[grepl(paste0("[0-9]",suffix,"|nurse_mid_ass",suffix),files)]
split_mes <- fread(file.path(j,"FILEPATH"))

## create skeleton of 3-digit surveys for which hrh_any is calculatable after splitting (codes cover all cadres)
## Currently making skeleton from nurse_mid_assoc so that it will include both ISCO 08 and 88 3-digit surveys
## To keep selection of surveys consistent with that applied to hrh_any, if using cleaned data read in template cleaned the same way as hrh_any
if (clean & by_sex) {
  total <- fread(file.path(root.dir,"hrh/clean_split_template_sex.csv"))
} else if (clean) {
  total <- fread(file.path(root.dir,"hrh/clean_split_template.csv"))
} else {
  total <- fread(file.path(in.dir,files[grepl("nurse_mid_ass",files)]))
}
metadata <- names(total)[!names(total) %in% c("var","mean","standard_error","design_effect","standard_deviation","standard_deviation_se","data","variance","me_name","region_id","level")]
total <- total[,metadata,with=F]

## loop through codes to be split
for (file in files){
  ## read in 3-digit estimates
  raw_data <- fread(file.path(in.dir,file))

  ## compile list of underlying 4-digit categories
  mes <- unique(split_mes[envelope_code == gsub(suffix,"",file),me_name])

  ## find their run_ids
  mes <- data.table(categ = mes, run_id = lapply(mes, function(x) {run.db[me_name == x,max(run_id)]}) %>% unlist)

  ## read in mean estimates from st-gpr and merge onto 3-digit dataset
  for (me in mes$categ){
    x <- model_load(mes[categ == me,run_id],'raked')[,list(location_id,year_id,gpr_mean)]
    setnames(x,"gpr_mean",me)
    raw_data <- merge(raw_data,x,by=c("location_id","year_id"))
  }

  ## calculate proportions of 3-digit code made up by each 4-digit category
  ## SPECIAL CASES: coding systems where the residual category differs from that of normal isco (ex. CSCO, where isco_88_222
  ## was a residual category for other 222 occupations besides physicians and pharmacists)
  if (grepl("isco_88_222",file)) raw_data[grepl("csco",tolower(occ_code_type)),c("hrh_phys","hrh_pharm") := NA]
  if (grepl("isco_88_322",file)) raw_data[grepl("csco|tha_ipums_1990",tolower(occ_code_type)),c("hrh_opt") := NA]
  if (grepl("isco_88_513",file)) raw_data[grepl("idn",tolower(occ_code_type)),c("isco_88_513_other") := NA]

  raw_data[,total := rowSums(.SD,na.rm=T), .SDcols = mes$categ]
  raw_data[, (mes$categ) := lapply(.SD, function(x) {x/total}), .SDcols = mes$categ]

  ## split data into each 4-digit category, append to the main cadre dataset, and write to folder for
  ## final st-gpr model. Also append split data to total dataset for summation into hrh_any
  for (col in mes$categ[!grepl("other",mes$categ)]){
    ## prepare split data by multiplying 4-digit code proportion by 3-digit code prevalence, and prep for binding
    ## onto main dataset
    split <- copy(raw_data)
    split[,me_name := col]
    split[,c("mean","data") := lapply(.SD, function(x) {x*get(col)}), .SDcols = c("mean","data")]
    split[,variance := variance*(get(col)**2)]
    split[,c(mes$categ,"total") := NULL]

    ## add split data to main cadre-specific dataset
    df <- fread(file.path(in.dir,paste0(col,suffix)))
    df <- rbind(df,split[!is.na(data)])
    df <- df[order(ihme_loc_id,year_start)]

    ## write cadre-specific file to final modeling folder
    write.csv(df,file.path(out.dir,paste0(col,suffix)),row.names = F)

    ## Add cadre data to the total dataset to aggregate into hrh_any
    ## Possible that this cadre is already in the total dataset (ex from other coding version's split). If not, start by filling the
    ## cadre with estimates from any surveys for which the 3-digit code was sufficient to provide an estimate for the cadre
    if (!col %in% names(total)){
      mapped <- fread(file.path(in.dir,paste0(col,suffix)))
      total <- merge(total,mapped[,c(metadata,"data","variance"),with=F],by=c(metadata),all.x=T)
      setnames(total,c("data","variance"),c(col,paste0(col,"_var")))
    }

    ## merge split data onto total dataset
    total <- merge(total,split[,c(metadata,"data","variance"),with=F],by=c(metadata),all.x=T)

    ## if survey did not have an estimate for the cadre but the data was not split, it should mean we are
    ## working with a cleaned dataset
    total[is.na(data),data := 0]
    total[is.na(get(col)) | get(col) == 0,c(col,paste0(col,"_var")) := list(data,variance)] ## fill in the estimates
    total[,c("data","variance") := list(NULL,NULL)]
  }
}

## also have to add hrh_nurseprof onto total dataset, which is directly mappable from 3-digits in both ISCO coding systems
col <- "hrh_nurseprof"
mapped <- fread(file.path(in.dir,paste0(col,suffix)))
total <- merge(total,mapped[,c(metadata,"data","variance"),with=F],by=c(metadata),all.x=T)
setnames(total,c("data","variance"),c(col,paste0(col,"_var")))
total[is.na(hrh_nurseprof),hrh_nurseprof := 0]

## list out all hrh cadres
hrh_categs <- c(split_mes[!grepl("other",me_name),unique(me_name)],"hrh_nurseprof")

## sum all categories to obtain estimate of hrh_any
total[,hrh_any := rowSums(.SD), .SDcols = hrh_categs]
total[,data := hrh_any/10000] ## convert estimate back to proportion
total[,variance := data*(1-data)/sample_size]

## read in existing hrh_any dataset and add summed split data to it
df <- fread(file.path(in.dir,paste0("hrh_any",suffix)))
df <- rbind(df,total,fill=T)
df[,me_name := "hrh_any"]

## check for odd results before writing to final prepped folder
df[is.na(data) | is.na(variance) | data == 0]

## write output to final modeling folder
write.csv(df,file.path(out.dir,paste0("hrh_any",suffix)),row.names = F)
