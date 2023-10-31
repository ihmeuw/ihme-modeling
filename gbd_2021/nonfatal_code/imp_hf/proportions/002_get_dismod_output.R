#!/usr/bin/Rscript

##########################################################################################
####### This is a child script of post_dismod_submitProcessing.R and does the following:
####### 1. Fetch dismod draws from 6 HF etiological proportion models
####### 2. Correct the results for IHD for all locations except for SSA using US marketscan proportion 
####### 3. Rescale the proportions such that sum of the 6 adjusted proportions adds up to 1 for each
####### location, year, and sex
####### 4. Split the 6 main causes into 21 subcauses
####### 5. Fetch HF and HF due to Chagas dismod prevalence draws (best) 
#######    a. Get HF etiological prevalence by multiply HF less Chagas prevalence by proportion of each
#######    subcause calculated in step 4
#######    b. Severity split HF due to Chagas prevalence into 3 mes using expert-provided proportions    
####### HF_2017 code rewritten by USER & USER from HF_2016 & before code by USER, USER, & USER
####### old: ADDRESSs
####### new: 
##########################################################################################

#######################################
## PREP ENVIRONMENT & GLOBAL VAR
#######################################

rm(list=ls())

datetime <- format(Sys.time(), '%Y_%m_%d')

pacman::p_load(plyr, data.table, parallel, RMySQL, openxlsx, ggplot2)

# source the central functions
central <- 'FILEPATH'
for (func in paste0(central, list.files(central))) source(paste0(func))

# cvd output folder
cvd_path = "FILEPATH"
task_id <- ifelse(is.na(as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))), 1, as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID")))

# Metadata
gbd_round_id <- VALUE
decomp_step <- "FILEPATH"
metadata <- get_demographics(gbd_team = "VALUE", gbd_round_id = gbd_round_id)

year_ids <- unique(metadata$year_id)
sex_ids  <- unique(metadata$sex_id)
age_group_ids = unique(metadata$age_group_id)
ages <- get_age_metadata(age_group_set_id =VALUE, gbd_round_id=VALUE)

args   <- commandArgs(trailingOnly = T)

loc_path <- args[1] #arg
locs <- fread(loc_path)
loc_id <- locs[task_id, c(Var1)]

pop_path <- args[2] #arg
pop <- readRDS(pop_path)


outputDir <- 'FILEPATH'
split_chagas <- "split_chagas" 

# hf etiology map
composite <- fread(paste0(cvd_path, '/composite_cause_list_2020.csv'))

strokes <- data.table(me=c(VALUE, VALUE, VALUE, VALUE, VALUE, VALUE),
                      name=c("VALUE", "VALUE", "VALUE", "VALUE", "VALUE", "VALUE"),
                      folder=c("VALUE", "VALUE", "VALUE", "VALUE", "VALUE", "VALUE"))
prop_meids <- data.table(prop_me_id=c(VALUE, VALUE, VALUE, VALUE, VALUE, VALUE, VALUE),
                           names=c('VALUE', 'VALUE', 'VALUE', 'VALUE', 'VALUE', 'VALUE', 'VALUE'))

composite <- merge(composite, prop_meids, on=prop_me_id, all=T)
#######################################
## 1. Fetch dismod draws from 7 HF etiological proportion models
## (rewrite of split17.do)
#######################################
gbd_ids <- unique(composite$prop_me_id)
gbd_ids <- gbd_ids[!is.na(gbd_ids)]

# get proportions
tmp_str <- ''
prop_draws <- function(gbd_id) {
  decomp_step <- "VALUE"
  tmp_str <- ifelse(gbd_id==VALUE, 'VALUE', ifelse(gbd_id=VALUE, 'VALUE',ifelse(gbd_id==VALUE, 'VALUE',ifelse(gbd_id==VALUE, 'VALUE',ifelse(gbd_id==VALUE, 'VALUE', ifelse(gbd_id==VALUE, 'VALUE', 'VALUE'))))))

  # Check to see if get_draws fills NA with zero and check to see if get_models behaves the same
  dat <- get_draws(gbd_id_type='VALUE', gbd_id=gbd_id, source='VALUE', location_id=loc_id, 
                     year_id=year_ids, sex_id=sex_ids, gbd_round_id=gbd_round_id, status='VALUE', measure_id=VALUE, decomp_step = decomp_step)
  #}
  dat[,c("measure_id","model_version_id","modelable_entity_id"):=NULL]
  setnames(dat, gsub("draw_", tmp_str, names(dat)))
  test <- names(dat)[grepl(tmp_str, names(dat)) ]
  dat <- melt(dat, id.vars = c("age_group_id", "sex_id", "year_id"),
              measure.vars = test) 
  dat$draw <- sub(".*_", "", dat$variable)
  names(dat)[names(dat) == "variable"] <- paste0(tmp_str, "variable")
  names(dat)[names(dat) == "value"] <- paste0(tmp_str, "value")

  return(dat)
}

# append results
datalist = list()
count = 0 
for (gbd_id in gbd_ids) {
  print(gbd_id)
  dat <- prop_draws(gbd_id)
  count <- count+1
  datalist[[count]] <- dat
}

x <- datalist[[1]]
for (i in 2:length(datalist)) x <- merge(x, datalist[[i]], by=c("age_group_id", "sex_id", "year_id", "draw"))

# keep only age groups used
df <- x[age_group_id %in% age_group_ids] 


#######################################
## 4. Split the 6 main causes into 21 subcauses
#######################################
# read from proportion file generated in pre_dismod step

proportion <- fread('FILEPATH')
proportion <- proportion[location_id==loc_id]
setnames(proportion, "cause_id.x", "cause_id")


# Fed in from rds so as not to parallelization get_population calls 
pop <- pop[location_id == unique(proportion$location_id)]
pop <- unique(pop[, .(age_group_id, sex_id, location_id, population, year_id)])

proportion <- merge(proportion, pop, by=c("age_group_id", "sex_id", "location_id", "year_id"))

proportion[, cases := prev_adj_final * population]
proportion[, sum_cases := sum(cases, na.rm=T), by=c("age_group_id", "sex_id", "location_id", "cause_id", "year_id")]
proportion[, sum_cases_den := sum(cases, na.rm=T), by=c("age_group_id", "sex_id", "location_id", "year_id")]
proportion[, prop_cases := sum_cases/sum_cases_den]
proportion[, sum_prop_den := sum(prop_cases, na.rm=T), by=c("age_group_id", "sex_id", "location_id", "year_id", "prop_bundle_id")]
proportion[, prop_final := prop_cases/sum_prop_den]
proportion[, prop_final := ifelse(is.na(prop_final), 0, prop_final)]

proportion <- proportion[, .(location_id, age_group_id, sex_id, cause_id, prop_bundle_id, year_end, prop_final, year_id)]
proportion$cause_id <- sub("^", "hf_target_prop", proportion$cause_id )
proportion <- dcast(proportion,location_id+age_group_id+sex_id+year_id~cause_id, value.var = "prop_final")

df <- merge(df, ages[,.(age_group_id, age_group_years_start)], by="age_group_id")
df <- df[age_group_years_start<15, ":="(ihd_value=0, left_value=0, toxin_value=0)]

# Scaling to 1
df[,check:=(toxin_value+ihd_value+right_value+left_value+pmd_value+valvular_value+stress_value)]
df[,toxin_value:= toxin_value/check]
df[,ihd_value:= ihd_value/check]
df[,right_value:= right_value/check]
df[,left_value:= left_value/check]
df[,pmd_value:= pmd_value/check]
df[,valvular_value:= valvular_value/check]
df[,stress_value:= stress_value/check]

df[,check:=NULL]


## shiny input
dismod_output <- df[, .(age_group_id, sex_id, year_id, left_value, ihd_value, pmd_value, valvular_value, right_value, stress_value, toxin_value)]
dismod_output <- dismod_output[,lapply(.SD, mean), by=c('age_group_id', 'sex_id', 'year_id')]
dismod_output[, location_id:=loc_id]
dir.create(paste0(outputDir, '/0_dismod_output/', datetime, '/'), showWarnings = FALSE)
saveRDS(dismod_output, paste0(outputDir, '/0_dismod_output/', datetime, '/', loc_id,'.rds')) 

# merge subcause proportion table with dismod result table for 7 main causes
df[,est_yr := year_id]
df[,year_id := sapply(year_id, function(x) ifelse(x %in% c(2019, 2020, 2021, 2022), 2020, x))]
df_hold <- copy(df)

#tmp <- join(df, proportion)
tmp <- merge(df, proportion, by=c("age_group_id", "sex_id", "year_id"))
tmp[tmp<0] <- 0 # possible negative values due to draws are assigned 0
tmp[is.na(tmp)] <- 0

tmp[,year_id:=est_yr]
tmp$est_yr <- NULL

## Split into 21 subcauses
# Pressure overload, right-heart
tmp[,adj_prop_509:=right_value*hf_target_prop509] #COPD
tmp[,adj_prop_516:=right_value*hf_target_prop516] #Interstitial
tmp[,adj_prop_511:=right_value*hf_target_prop511] #Pneumoconiosis - silicosis
tmp[,adj_prop_512:=right_value*hf_target_prop512] #Pneumoconiosis - asbestosis
tmp[,adj_prop_513:=right_value*hf_target_prop513] #Pneumoconiosis - coal workers
tmp[,adj_prop_514:=right_value*hf_target_prop514] #Pneumoconiosis - other
tmp[,adj_prop_1004:=right_value*hf_target_prop1004] #Pulmonary arterial hypertension

# Primary myocardial disease
tmp[,adj_prop_942:=pmd_value*hf_target_prop942] #Myocarditis
tmp[,adj_prop_944:=pmd_value*hf_target_prop944] #Other cardiomyopathy

# Valvular
tmp[,adj_prop_503:=valvular_value*hf_target_prop503] #Endocarditis
tmp[,adj_prop_643:=valvular_value*hf_target_prop643] #Congenital
tmp[,adj_prop_970:=valvular_value*hf_target_prop970] #Other non-rheumatic valve diseases
tmp[,adj_prop_492:=valvular_value*hf_target_prop492] #Rheumatic heart disease

# Toxin 
tmp[,adj_prop_938:=toxin_value*hf_target_prop938] #Alcoholic cardiomyopathy
tmp[,adj_prop_563:=toxin_value*hf_target_prop563] #Cocaine
tmp[,adj_prop_564:=toxin_value*hf_target_prop564] #Amphetamine

# Stress cardiomyopathy
tmp[,adj_prop_614:=stress_value*hf_target_prop614] #Thalassemia
tmp[,adj_prop_616:=stress_value*hf_target_prop616] #G6PD deficiency
tmp[,adj_prop_1032:=stress_value*hf_target_prop1032] #Thyroid Diseases
tmp[,adj_prop_500:=stress_value*hf_target_prop500] #Afib
tmp[,adj_prop_495:=stress_value*hf_target_prop495] #Isch stroke
tmp[,adj_prop_497:=stress_value*hf_target_prop497] #Subhem stroke
tmp[,adj_prop_496:=stress_value*hf_target_prop496] #Cerhem stroke
tmp[,adj_prop_589:=stress_value*hf_target_prop589] #CKD
tmp[,adj_prop_507:=stress_value*hf_target_prop507] #Other circulartory and cardiovascular
tmp[,adj_prop_618:=stress_value*hf_target_prop618] #Other anemias
tmp[,adj_prop_521:=stress_value*hf_target_prop521] #Cirrhosis


# Pressure overload, left-heart
tmp[,adj_prop_498:=left_value*hf_target_prop498] #Left Value

# Coronary Artery Disease
tmp[,adj_prop_493:=ihd_value*hf_target_prop493] #IHD

tmp[is.na(tmp)] <- 0 
tmp[tmp==Inf] <- 0 
print(names(tmp))

#########################
keep_cols <- c(grep("adj_prop_", names(tmp), value = TRUE), 'age_group_id','sex_id','year_id','draw')
tmp <- tmp[, keep_cols, with=FALSE]

prop_cols <- grep("adj_prop", names(tmp), value = TRUE)
tmp$row_sum <- rowSums(tmp[,..prop_cols])
tmp_lowsum <- tmp[row_sum>.99 & row_sum <1.01]

#######################################
## 5. Fetch HF and HF due to Chagas dismod prevalence draws (best) + Acute stroke
#######################################
# get HF envelope
keycols = c("location_id", "year_id", "age_group_id", "sex_id")

hf <- get_draws(gbd_id_type ='VALUE', gbd_id=VALUE, source='VALUE', location_id=loc_id, 
                year_id=year_ids, sex_id=sex_ids, gbd_round_id=gbd_round_id, status='best', measure_id=5, decomp_step=decomp_step)
draw_cols = grep("^draw_[0-999]?", names(hf), value=TRUE)
hf <- hf[age_group_id %in% age_group_ids]
hf <- melt(hf, id.vars = c("age_group_id","location_id","sex_id","year_id"),
                       measure.vars = draw_cols)
hf_check <- hf[,lapply(.SD, mean),
                 by = .(age_group_id, year_id, location_id, sex_id), .SDcols=c("value")]
setnames(hf, 'value', 'hf')

hf$metric_id <- VALUE
 

chagas <- get_draws(gbd_id_type='VALUE', gbd_id=VALUE, source='VALUE', location_id=loc_id, 
                    year_id=year_ids, sex_id=sex_ids, gbd_round_id=gbd_round_id, decomp_step = decomp_step, status='best', measure_id=VALUE)
chagas <- chagas[age_group_id %in% age_group_ids]

# get Chagas prev
chagas_prev <- melt(chagas, id.vars = c("age_group_id","location_id","sex_id","year_id"),
                    measure.vars = draw_cols)
chagas_check <- chagas_prev[,lapply(.SD, mean),
                            by = .(age_group_id, year_id, location_id, sex_id), .SDcols=c("value")]

chagas_shiny<- chagas_prev[,.(prev_346=mean(value)), by=c('age_group_id', 'sex_id', 'year_id', 'location_id')]
setnames(chagas_prev, 'value', 'prev_346')

# Get NRVD prev
# cvd_valvu_aort
cvd_valvu_aort <- get_draws(gbd_id_type='VALUE', gbd_id=VALUE, source='VALUE', location_id=loc_id, 
                            year_id=year_ids, sex_id=sex_ids, gbd_round_id=gbd_round_id, decomp_step=decomp_step, status='VALUE', measure_id=VALUE)
cvd_valvu_aort <- melt(cvd_valvu_aort, id.vars = c("age_group_id","location_id","sex_id","year_id"),
                    measure.vars = draw_cols)
cvd_valvu_aort_shiny <- cvd_valvu_aort[,.(prev_968=mean(value)), by=c('age_group_id', 'sex_id', 'year_id', 'location_id')]
setnames(cvd_valvu_aort, 'value', 'prev_968')

# cvd_valvu_mitral
cvd_valvu_mitral <- get_draws(gbd_id_type='VALUE', gbd_id=VALUE, source='VALUE', location_id=loc_id, 
                              year_id=year_ids, sex_id=sex_ids, gbd_round_id=gbd_round_id, decomp_step=decomp_step, status='VALUE', measure_id=VALUE)
cvd_valvu_mitral <- melt(cvd_valvu_mitral, id.vars = c("age_group_id","location_id","sex_id","year_id"),
                       measure.vars = draw_cols)
cvd_valvu_mitral_shiny <- cvd_valvu_mitral[,.(prev_969=mean(value)), by=c('age_group_id', 'sex_id', 'year_id', 'location_id')]
setnames(cvd_valvu_mitral, 'value', 'prev_969')

### Calculate HF prevalence without NRVD cause
hf <- as.data.table(Reduce(function(x,y) join(x,y, by=c("location_id","year_id","age_group_id","sex_id", "variable")), list(hf, chagas_prev, cvd_valvu_aort, cvd_valvu_mitral)))
hf[is.na(prev_968), prev_968 := 0]
hf[is.na(prev_969), prev_969 := 0]
hf[, hf_prev:=hf-prev_968-prev_969]#-prev_346] 
hf[, draw:=as.numeric(gsub("draw_","", variable))]
hf[, c('variable', 'hf', 'prev_346', 'prev_968', 'prev_969'):=NULL]


## Distribute HF less NRVD prevalence into prevalence of 21 HF etiologies
tmp2 <- join(tmp, hf, by = c('age_group_id', 'sex_id', 'year_id', 'draw'))
indx <- grep('adj_prop_', colnames(tmp2))

# subcause prev = HF-less-Chagas prev * subcause prop

for(j in indx){
  set(tmp2, i=NULL, j=j, value=tmp2[[j]]*tmp2[['hf_prev']])
}

tmp2$measure_id <- VLAUE # add prevalence measure
colnames(tmp2) <- gsub('adj_prop_', 'adj_prev_', colnames(tmp2)) # temporary colnames to avoid confusion
prev_cols <- grep("adj_prev_", names(tmp2), value = TRUE)

## Get Acute stroke prevalence and multiply by 5%
acute_isch <- get_draws(gbd_id_type='VALUE', gbd_id=VALUE, source='VALUE', location_id=loc_id, 
                              year_id=year_ids, sex_id=sex_ids, gbd_round_id=gbd_round_id, decomp_step=decomp_step, status='VALUE', measure_id=VALUE)
acute_isch <- melt(acute_isch, id.vars = c("age_group_id","location_id","sex_id","year_id"),
                         measure.vars = draw_cols)
acute_isch[, acute_ischemic_stroke := 0.05*value]
acute_isch$value <- NULL

acute_subhem <- get_draws(gbd_id_type='VALUE', gbd_id=VALUE, source='VALUE', location_id=loc_id, 
                        year_id=year_ids, sex_id=sex_ids, gbd_round_id=gbd_round_id, decomp_step=decomp_step, status='VALUE', measure_id=VALUE)
acute_subhem <- melt(acute_subhem, id.vars = c("age_group_id","location_id","sex_id","year_id"),
                   measure.vars = draw_cols)
acute_subhem[, acute_subhem_stroke := 0.05*value]
acute_subhem$value <- NULL

acute_cerhem <- get_draws(gbd_id_type='VALUE', gbd_id=VALUE, source='VALUE', location_id=loc_id, 
                        year_id=year_ids, sex_id=sex_ids, gbd_round_id=gbd_round_id, decomp_step=decomp_step, status='VALUE', measure_id=VALUE)
acute_cerhem <- melt(acute_cerhem, id.vars = c("age_group_id","location_id","sex_id","year_id"),
                   measure.vars = draw_cols)
acute_cerhem[, acute_cerhem_stroke := 0.05*value]
acute_cerhem$value <- NULL

# subtract it from the rest, turn it into HF d/t chronic stroke prevalences
tmp2[, variable := paste0("draw_", draw)]
tmp3 <- as.data.table(Reduce(function(x,y) join(x,y, by=c("location_id","year_id","age_group_id","sex_id","variable")), list(tmp2, acute_cerhem, acute_isch, acute_subhem)))
tmp3[, adj_prev_495_chronic := adj_prev_495 - acute_ischemic_stroke]
tmp3[, adj_prev_497_chronic := adj_prev_497 - acute_subhem_stroke]
tmp3[, adj_prev_496_chronic := adj_prev_496 - acute_cerhem_stroke]
setnames(tmp3, c("acute_ischemic_stroke", "acute_subhem_stroke", "acute_cerhem_stroke"), c("adj_pr", "adj_prev_497_acute", "adj_prev_496_acute"))
tmp3$adj_prev_496 <- NULL
tmp3$adj_prev_495 <- NULL
tmp3$adj_prev_497 <- NULL

prev_cols <- names(tmp3)[grepl("adj_prev_", names(tmp3))]

for (col in prev_cols) tmp3[get(col) < 0, paste0(col) := 0]


################# shiny input
prop_from_prev_cols <-c('age_group_id', 'sex_id', 'year_id', 'location_id', prev_cols, 'year_id')

prop_from_prev <- tmp3[, mget(prop_from_prev_cols)]
prop_from_prev <- prop_from_prev[,lapply(.SD, mean), by=c('age_group_id', 'sex_id', 'location_id', 'year_id')]
prop_from_prev <- as.data.table(Reduce(function(x,y) join(x,y, by=c('age_group_id', 'year_id', 'sex_id', 'location_id', 'year_id')), list(prop_from_prev, chagas_shiny, cvd_valvu_aort_shiny, cvd_valvu_mitral_shiny)))

colnames(prop_from_prev) <- gsub('adj_prev_', 'prev_', colnames(prop_from_prev)) 

prop_from_prev[prop_from_prev<0] <- 0 # possible negative values due are assigned 0
fraction_cols <-  grep("prev_", names(prop_from_prev), value = TRUE)
prop_from_prev[, sum_of_prev := rowSums(.SD, na.rm = TRUE), .SDcols = fraction_cols]
for(fraction_col in fraction_cols) {
  prop_from_prev[,c(fraction_col):=get(fraction_col)/sum_of_prev]
}
prop_from_prev[, sum_of_prev:=NULL]

colnames(prop_from_prev) <- gsub('prev_', 'prop_prev_', colnames(prop_from_prev)) 
prop_from_prev <- melt(prop_from_prev, id.vars=c('age_group_id', 'sex_id', 'year_id', 'location_id'))
dir.create(paste0(outputDir, '/2_prop_from_prev/', datetime, '/'), showWarnings = FALSE)
saveRDS(prop_from_prev, paste0(outputDir, '/2_prop_from_prev/', datetime, '/', loc_id,'.rds')) 
#######

# delete all existing files from output folder #might need to uncomment if job fails
## write each Chagas ME to a different folder in a different folder in save_results format after severity split
dir.create(paste0(outputDir, '1_corrected_prev_draws/'), showWarnings = FALSE)
dir.create(paste0(outputDir, '1_corrected_prev_draws/', datetime, '/'), showWarnings = FALSE)
prevDir <- paste0(outputDir, '1_corrected_prev_draws/', datetime, '/')

## write each ME to a different folder in a different folder in save_results format
for (col in prev_cols){
  
  cause <- print(as.numeric(gsub("\\D", "", col)))
  if (cause == VALUE) cause <- VALUE
  keep_cols <- c('measure_id',	'location_id',	'year_id',	'age_group_id',	'sex_id', col, 'draw')
  prev_dat <- tmp3[, mget(keep_cols)]
  prev_dat[, draws := paste("draw", draw, sep="_"), by=draw]
  prev_dat <- unique(prev_dat)
  prev_dat <- dcast(prev_dat, measure_id+location_id+age_group_id+sex_id+year_id ~ draws, value.var = col)

  ### UPDATE  
  prev_dat_incidence <- copy(prev_dat)
  prev_dat_incidence[, measure_id:=6]
  prev_dat_incidence[, paste0("draw_", 0:999):=0]
  
  prev_dat <- rbind(prev_dat, prev_dat_incidence)
  ###
  
  if (grepl("acute|chronic", col)) {
    prev_dat$modelable_entity_id <- strokes[name==col, me]
    subfolder <- strokes[name==col, folder]
  } else {
    prev_dat$modelable_entity_id <- as.integer(unique(composite[cause_id==cause]$prev_me_id))
    subfolder <- unique(composite[cause_id==cause]$prev_folder)
  }
  
  ## fix NA draw cols caused by mktscan file ##################
  for(j in seq_along(prev_dat)){
    set(prev_dat, i = which(is.na(prev_dat[[j]]) & is.numeric(prev_dat[[j]])), j = j, value = 0)
  }
  
  ## write to folder
  dir.create(file.path(prevDir, subfolder),showWarnings = FALSE)
  setwd(file.path(prevDir, subfolder))
  message(dim(prev_dat))
  write.csv(prev_dat, paste0(loc_id,'.csv'), row.names=F)
}
