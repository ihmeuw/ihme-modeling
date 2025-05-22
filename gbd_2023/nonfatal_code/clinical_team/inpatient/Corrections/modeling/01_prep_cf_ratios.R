## Libraries #### 
USER = Sys.info()[7]
library(dplyr)
library(ggplot2)
library(data.table)
library(reticulate)
library(readxl)

reticulate::use_python("FILEPATH") 
source('~/db_utilities.R')
source("~/00a_prep_setup.R")

## Data prep #### 
read_folder = "FILEPATH"
df = fread(paste0(read_folder, 'FILEPATH'))

df[, dno_name := NULL]
df[, population := NULL]

### Sum across key variables #####
idvars1 = names(df)[!names(df)%in% c('estimate', 'count')]
key_vars = names(df)[!names(df)%in% c('count')]
key_dt = df[, key_vars, with = FALSE]
check = key_dt[, .N, by = key_vars]
check = check[N>=2, ]
df = df[, .(count = sum(count)), by = c(idvars1, 'estimate')]

#### Check bundles #####
df_bundles = unique(df[,.(bundle_id, source)])
df_bundles[, present:=1]
df_bundles = dcast(data=df_bundles, bundle_id ~ source, value.var = 'present')
df_bundles = merge(df_bundles, active_bundles, all.y = TRUE, by = 'bundle_id')
df_bundles[is.na(df_bundles)] = 0 # Replace all NAs with 0s
df_bundles = melt.data.table(df_bundles, id.vars = "bundle_id")
df_bundles_missing = df_bundles[value == 0,]

#### Check estimate ID relationships #####
form1 = as.formula(paste0(paste(idvars1, collapse = " + "), " ~ ", 'estimate'))
df_wide1 = dcast(data=df, formula = form1, value.var = 'count')
# check estimate IDs
# checks CF1
check2 = df_wide1[estimate_15 > estimate_14, ]  
# checks CF4
check3 = df_wide1[estimate_15 > estimate_17, ]
# checks CF5
# Medicare needs to be excluded from these because est ID 21 is the 5% sample
check4 = df_wide1[estimate_15 > estimate_21 & !source %in% c('Medicare'), ]
check5 = df_wide1[estimate_28 > estimate_21 & source %in% c('Medicare'), ] 
# checks CF6
#Medicare needs to be excluded from these because est ID 21 is the 5% sample
check6 = df_wide1[estimate_17 > estimate_21 & !source %in% c('Medicare'), ]
check7 = df_wide1[estimate_29 > estimate_21 & source %in% c('Medicare'), ] 

check8 = rbindlist(list(check2, check3, check4, check5, check6, check7))

write_folder = paste0("FILEPATH")  
write.csv(check6, paste0(write_folder, "FILEPATH"), row.names = FALSE)

### Drop data where estimate ID relationships aren't valid #####
# but keep data for the other CFs if it is valid
# check unique ids
if(nrow(unique(df_wide1[, idvars1, with = FALSE])) != nrow(df_wide1)) stop("The ID vars are not unique keys for the data")
drop_cf1=check2[, idvars1, with = FALSE]
drop_cf1[, drop_cf1 := 1]

drop_cf4=check3[, idvars1, with = FALSE]
drop_cf4[, drop_cf4 := 1]

drop_cf5=rbind(check4[, idvars1, with = FALSE], check5[, idvars1, with = FALSE])
drop_cf5[, drop_cf5 := 1]

drop_cf6=rbind(check6[, idvars1, with = FALSE], check7[, idvars1, with = FALSE])
drop_cf6[, drop_cf6 := 1]

drop_dt = merge(drop_cf1, drop_cf4, all = TRUE, by = idvars1)
drop_dt = merge(drop_dt, drop_cf5, all = TRUE, by = idvars1)
drop_dt = merge(drop_dt, drop_cf6, all = TRUE, by = idvars1)

df_wide1[, cf1 := estimate_15/estimate_14][, cf4 := estimate_15/estimate_17]
df_wide1[source != "Medicare", cf5 := estimate_15/estimate_21]
df_wide1[source != "Medicare", cf6 := estimate_17/estimate_21]
df_wide1[source == "Medicare", cf5 := estimate_28/estimate_21]
df_wide1[source == "Medicare", cf6 := estimate_29/estimate_21]

df_wide = merge(drop_dt, df_wide1, all = TRUE, by = idvars1)
df_wide[is.na(drop_cf1), drop_cf1 := 0]
df_wide[is.na(drop_cf4), drop_cf4 := 0]
df_wide[is.na(drop_cf5), drop_cf5 := 0]
df_wide[is.na(drop_cf6), drop_cf6 := 0]

# drop cases where cf4, cf5, cf6 = Inf 
data_errors = df_wide[(cf4 == Inf ) | (cf5 == Inf) | (cf6 == Inf)] 
more_data_errors = df_wide[cf1 > 1 | cf4 > 1 | cf5 > 1 | cf6 > 1]

df_wide[ drop_cf1 == 1, cf1:= NA]
df_wide[ drop_cf4 == 1, cf4:= NA]
df_wide[ drop_cf5 == 1, cf5:= NA]
df_wide[ drop_cf6 == 1, cf6:= NA]

df_wide[!is.na(cf1), cf1_yes := estimate_15][!is.na(cf1), cf1_no := estimate_14-estimate_15]
df_wide[!is.na(cf4), cf4_yes := estimate_15][!is.na(cf4), cf4_no := estimate_17-estimate_15]
df_wide[!is.na(cf5)&source!='Medicare', cf5_yes := estimate_15][!is.na(cf5)&source!='Medicare', cf5_no := estimate_21-estimate_15]
df_wide[!is.na(cf6)&source!='Medicare', cf6_yes := estimate_17][!is.na(cf6)&source!='Medicare', cf6_no := estimate_21-estimate_17]
df_wide[!is.na(cf5)&source=='Medicare', cf5_yes := estimate_28][!is.na(cf5)&source=='Medicare', cf5_no := estimate_21-estimate_28]
df_wide[!is.na(cf6)&source=='Medicare', cf6_yes := estimate_29][!is.na(cf6)&source=='Medicare', cf6_no := estimate_21-estimate_29]

df2 = melt.data.table(df_wide, id.vars = idvars1, 
                      measure.vars = c('cf1_yes','cf1_no','cf4_yes','cf4_no','cf5_yes','cf5_no','cf6_yes','cf6_no'))

check = df2[is.na(value)] 
check = check[!source %in% c('NZL', 'HCUP', 'PHL')]

#### Drop the inconsistent data #####
df2=df2[!is.na(value)]

df2$cf = as.character(lapply(strsplit(as.character(df2$variable), split="_"), "[", 1))
df2$outcome = as.character(lapply(strsplit(as.character(df2$variable), split="_"), "[", 2))
df2[, variable := NULL]

idvars2 = names(df2)[!names(df2)%in% c('value', 'outcome')]
form2 = as.formula(paste0(paste(c(idvars2), collapse = " + "), " ~ ", 'outcome'))
df2_wide = dcast(data=df2, formula = form2, value.var = 'value')
df2_wide[, sample_size := no + yes]

all_cf_bundles_names = data.table(bun_names)
all_cf_bundles_names = all_cf_bundles_names[is.na(sort_order), sort_order := 9999]
all_cf_bundles_names = all_cf_bundles_names[order(sort_order)][, sort_order := NULL]

# subset of bundles for modeling
df2_wide = df2_wide[bundle_id %in% all_cf_bundles_names$bundle_id, ]
df2_wide = merge(df2_wide, all_cf_bundles_names[, .(bundle_id, bundle_name)], all.x=TRUE, by="bundle_id")

## Check bundles again  #####
df_bundles = unique(df2_wide[,.(bundle_id, bundle_name, source)])
df_bundles[, present:=1]
df_bundles = dcast(data=df_bundles, bundle_id + bundle_name ~ source, value.var = 'present')
df_bundles = merge(df_bundles, active_bundles, all.y = TRUE, by = 'bundle_id')
df_bundles[is.na(df_bundles)] = 0 # Replace all NAs with 0s
df_bundles_missing = melt.data.table(df_bundles, id.vars = c("bundle_id", "bundle_name"))
df_bundles_missing = df_bundles_missing[value == 0,]

write.csv(df_bundles, paste0(write_folder, "FILEPATH"), row.names = FALSE)

## Combine under1 age groups where we don't want to model them ####
dt= as.data.table(read_xlsx("FILEPATH"))
dt = dt[,.(bundle_id, `<1 aggregate`, `<1 detail`)]
setnames(dt, names(dt), c('bundle_id', "under1_agg", "under1_det"))
dt[, under1_agg := ifelse(is.na(under1_agg), 0, 1)]
dt[, under1_det := ifelse(is.na(under1_det), 0, 1)]
det_bundles = dt[under1_det == 1, unique(bundle_id)]

# keep <1 age detail
dt1 = df2_wide[bundle_id %in% det_bundles, ]
# aggregate <1 age detail
dt2 = df2_wide[!bundle_id %in% det_bundles, ]
dt2[age_end<1, age_group_id := 28]
dt2[age_end<1, age_start:= 0]
dt2[age_end<1, age_midpoint := 0.5]
dt2[age_end<1, age_end := 1]
id_vars = names(dt2)[!names(dt2) %in% c('no', 'yes', 'sample_size')]
dt2 = dt2[, lapply(.SD, sum), by = id_vars, .SDcols = c('no', 'yes', 'sample_size')]

dt_final = rbind(dt1, dt2)

## Save data #####
saveRDS(dt_final, paste0(write_folder, "FILEPATH"))
