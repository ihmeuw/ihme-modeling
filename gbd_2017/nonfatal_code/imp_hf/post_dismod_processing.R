#!FILEPATH

##########################################################################################
####### This is a child script of post_dismod_submitProcessing.R and does the following:
####### 1. Fetch dismod draws from 6 HF etiological proportion models
####### 2. Correct the results for IHD for all locations except for SSA using US marketscan proportion 
####### 3. Rescale the proportions such that sum of the 6 adjusted proportions adds up to 1 for each
####### location, year, and sex
####### 4. Split the 6 main causes into 20 subcauses
####### 5. Fetch HF and HF due to Chaga, HF due to nrvd_calcific and nrvd_ mitral dismod prevalence draws (best) 
#######    a. Get HF etiological prevalence by multiply HF less Chagas&nrvd prevalence by proportion of each
#######    subcause calculated in step 4
#######    b. Split HF due to congenital heart anomalies into 4 severity component prevalence    
##########################################################################################

#######################################
## PREP ENVIRONMENT & GLOBAL VAR
#######################################

rm(list = ls())
if (Sys.info()[1] == "Linux") folder_path <- "FOLDER" 
if (Sys.info()[1] == "Windows") folder_path <- "FOLDER"

# source the central functions
source(paste0(folder_path, "/FUNCTION_PATH/get_draws.R"))
source(paste0(folder_path, "/FUNCTION_PATH/get_location_metadata.R"))

library(data.table)
library(parallel)
library(plyr)
library(RMySQL)

# cvd output folder
cvd_path = "/CVD_FOLDER"
out_path = paste0(cvd_path, "/post_dismod/1_corrected_prev_draws/version")

# qsub diagnostic file path
qsub_output = paste0("/LOG_FOLDER/postdismod/output") 
qsub_errors = paste0("/LOG_FOLDER/postdismod/errors")

gbd_round_id=5
year_ids <- c(1990, 1995, 2000, 2005, 2010, 2017)
sex_ids  <- c(1,2)
age_group_ids = c(2:21,30,31,32,235)
loc_id   <- commandArgs()[4] #arg
split_chagas <- "split_chagas" 

# hf etiology map
composite <- fread(paste0(cvd_path, '/composite_cause_list.csv'))

#######################################
## 1. Fetch dismod draws from 6 HF etiological proportion models
#######################################
gbd_ids <- unique(composite$prop_me_id)
gbd_ids <- gbd_ids[!is.na(gbd_ids)]

# get proportions
tmp_str <- ''
prop_draws <- function(gbd_id) {
  tmp_str <- ifelse(gbd_id==2414, 'ihd_', ifelse(gbd_id==2415, 'htn_',ifelse(gbd_id==2416, 'cpm_',ifelse(gbd_id==2417, 'valvular_',ifelse(gbd_id==2418, 'cmp_',ifelse(gbd_id==2418, 'cmp_',ifelse(gbd_id==2419, 'other_',NA)))))))
  dat <- get_draws(gbd_id_type='modelable_entity_id', gbd_id=gbd_id, source='epi', location_id=loc_id, 
                   year_id=year_ids, sex_id=sex_ids, gbd_round_id=gbd_round_id, status='best', measure_id=18)
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
  dat <- prop_draws(gbd_id)
  count <- count+1
  datalist[[count]] <- dat
  print(gbd_id)
}

df <- Reduce(function(x,y) merge(x,y, by=c('age_group_id','sex_id','year_id', 'draw')), datalist)

# keep only age groups used
df <- df[age_group_id %in% age_group_ids] 

## shiny input
dismod_output <- df[, .(age_group_id, sex_id, year_id, other_value, ihd_value, htn_value, valvular_value, cpm_value, cmp_value)]
dismod_output <- dismod_output[,lapply(.SD, mean), by=c('age_group_id', 'sex_id', 'year_id')]
dismod_output[, location_id:=loc_id]
saveRDS(dismod_output, paste0('/CVD_FOLDER/post_dismod/0_dismod_output/', loc_id,'.rds'))
################
#######################################
## 2. Correct IHD for all locations except for SSA using US marketscan proportion 
#######################################
index_cols = c('sex_id', 'age_group_id', 'cause_id')
group_cols = c('sex_id', 'age_group_id')

## Calculate correction factors
# read from Marketscan data file created in pre_dismod step
mktscan <- readRDS(paste0(cvd_path, '/MarketScan_cleaned.rds'))
mktscan <- mktscan[, sex_id:=as.numeric(sex_id)]
mktscan <- mktscan[!cause_id%in%c(unique(na.omit(composite$composite_id)))] # keep only non-composite causes
mktscan <- mktscan[, .(sex_id, age_group_id, cause_id, cases_wHF)]
# aggregate HF counts from GBD 2017 data
total_HF <- mktscan[,.(total_HF_cases=sum(cases_wHF)), by=mget(group_cols)]
mktscan <- join(mktscan, total_HF, type='inner', by=group_cols)
# make correction factors
mktscan <- mktscan[, mk_prop:=cases_wHF/total_HF_cases]
mktscan[is.na(mk_prop), mk_prop:=0]
if (unique(mktscan[,.(test_prop=sum(mk_prop)), by=mget(group_cols)]$test_prop)!=1) stop("Proportions don't sum to 1")
# only keep IHD proportion
mktscan <- mktscan[cause_id==493, .(sex_id, age_group_id, mk_prop)] 
# make sure merge is complete
nrow_temp <- nrow(df)
df <- join(df, mktscan, type='inner', by=c('sex_id', 'age_group_id'))
if (nrow(df)<nrow_temp) stop('Missing sex_id or age_group_id in marketscan file.')

## Correct IHD for all locs except for SSA locations
ssa_ids <- as.vector(get_location_metadata(location_set_id =35, gbd_round_id=gbd_round_id)[super_region_id==166]$location_id)

if (loc_id %in% ssa_ids){
  df[,ihd_corrected_value:=ihd_value]
} else {
  df[, ihd_corrected_value:=ihd_value*mk_prop]
}

#######################################
## 3. Rescale the proportions 
#######################################
df[, scale_factor := rowSums(.SD, na.rm = TRUE), .SDcols = c("other_value", "cmp_value","cpm_value", "ihd_corrected_value", "htn_value" , "valvular_value")]
df[,other_prop:=other_value/scale_factor][,cmp_prop:=cmp_value/scale_factor][,cpm_prop:=cpm_value/scale_factor][,ihd_prop:=ihd_corrected_value/scale_factor][,htn_prop:=htn_value/scale_factor][,valvular_prop:=valvular_value/scale_factor]
df[, c("other_variable","other_value","cmp_variable","cmp_value","cpm_variable","cpm_value","ihd_variable","ihd_value","htn_variable","htn_value","valvular_variable","valvular_value","mk_prop","ihd_corrected_value"):=NULL]

#######################################
## 4. Split the 6 main causes into 20 subcauses
#######################################
# read from proportion file generated in pre_dismod step
proportion <- readRDS(paste0(folder_path, "/OUTPUT_PATH/heart_failure_SPLIT_AGGs_subnat.rds"))[location_id==loc_id]
proportion[,std_err_adj:=NULL]
proportion$cause_id <- sub("^", "hf_target_prop", proportion$cause_id )
proportion[,c('is_composite', 'year_start'):=NULL]
proportion <- dcast(proportion,location_id+age_group_id+sex_id+year_end~cause_id, value.var = "hf_target_prop")

# merge subcause proportion table with dismod result table for 6 main causes
tmp <- join(df, proportion)
tmp[tmp<0] <- 0 # possible negative values due to draws are assigned 0
tmp[is.na(tmp)] <- 0

## Split into 20 subcauses
# Split cardiopulmonary
tmp[,adj_prop_509:=cpm_prop*hf_target_prop509/hf_target_prop520] #COPD
tmp[,adj_prop_516:=cpm_prop*hf_target_prop516/hf_target_prop520] #Interstitial
tmp[,adj_prop_511:=cpm_prop*hf_target_prop511/hf_target_prop520] #Pneumoconiosis - silicosis
tmp[,adj_prop_512:=cpm_prop*hf_target_prop512/hf_target_prop520] #Pneumoconiosis - asbestosis
tmp[,adj_prop_513:=cpm_prop*hf_target_prop513/hf_target_prop520] #Pneumoconiosis - coal workers
tmp[,adj_prop_514:=cpm_prop*hf_target_prop514/hf_target_prop520] #Pneumoconiosis - other

# Split cardiomyopathy
tmp[,adj_prop_942:=cmp_prop*hf_target_prop942/hf_target_prop499] #Myocarditis
tmp[,adj_prop_938:=cmp_prop*hf_target_prop938/hf_target_prop499] #Alcoholic cardiomyopathy
tmp[,adj_prop_944:=cmp_prop*hf_target_prop944/hf_target_prop499] #Other cardiomyopathy

# Split other
tmp[,adj_prop_503:=other_prop*hf_target_prop503/hf_target_prop385] #Endocarditis
tmp[,adj_prop_614:=other_prop*hf_target_prop614/hf_target_prop385] #Thalassemia
tmp[,adj_prop_616:=other_prop*hf_target_prop616/hf_target_prop385] #G6PD deficiency
tmp[,adj_prop_619:=other_prop*hf_target_prop619/hf_target_prop385] #Oher endocrine, nutritional, blood, and immune disorders
tmp[,adj_prop_618:=other_prop*hf_target_prop618/hf_target_prop385] #Other anemias
tmp[,adj_prop_643:=other_prop*hf_target_prop643/hf_target_prop385] #Congenital
tmp[,adj_prop_507:=other_prop*hf_target_prop507/hf_target_prop385] #Other circulartory and cardiovascular
tmp[,adj_prop_970:=other_prop*hf_target_prop970/hf_target_prop385] #Other non-rheumatic valve diseases

# Rename standalone causes
tmp <- plyr::rename(tmp, c("ihd_prop"="adj_prop_493", "htn_prop"="adj_prop_498", "valvular_prop"="adj_prop_492"))
tmp[is.na(tmp)] <- 0
tmp[tmp==Inf] <- 0 
print(names(tmp))

## shiny corrected input 
adj_prop <- names(tmp)[grepl('adj_prop_', names(tmp))]
shiny_cols <- c('age_group_id', 'sex_id', 'year_id', 'other_prop', 'cmp_prop', 'cpm_prop', adj_prop, 'location_id')
dismod_corrected <- tmp[,mget(shiny_cols)]
dismod_corrected[, ':='(ihd_prop=adj_prop_493, htn_prop=adj_prop_498, valvular_prop=adj_prop_492)]
dismod_corrected <- dismod_corrected[,lapply(.SD, mean), by=c('age_group_id', 'sex_id', 'year_id', 'location_id')]
saveRDS(dismod_corrected, paste0('/CVD_FOLDER/post_dismod/0_dismod_output_corrected/', loc_id,'.rds'))

#########################
keep_cols <- c(grep("adj_prop_", names(tmp), value = TRUE), 'age_group_id','sex_id','year_id','draw','scale_factor')
tmp <- tmp[, keep_cols, with=FALSE]

#######################################
## 5. Fetch HF and HF due to Chagas, HF due valvu mitral, and HF due to valvu aortic dismod prevalence draws (best)
#######################################
# get HF envelope
keycols = c("location_id", "year_id", "age_group_id", "sex_id")

hf <- get_draws(gbd_id_type ='modelable_entity_id', gbd_id=2412, source='epi', location_id=loc_id, 
                year_id=year_ids, sex_id=sex_ids, gbd_round_id=gbd_round_id, status='best', measure_id=5)
draw_cols = grep("^draw_[0-999]?", names(hf), value=TRUE)
hf <- hf[age_group_id %in% age_group_ids]
hf <- melt(hf, id.vars = c("age_group_id","location_id","sex_id","year_id"),
                       measure.vars = draw_cols)
setnames(hf, 'value', 'hf')
hf$metric_id <- 3

# get HF due to Chagas prevalence draws; note: placeholder
chagas_update=T
if (chagas_update==T){
  chagas <- get_draws(gbd_id_type='modelable_entity_id', gbd_id=2413, source='epi', location_id=loc_id, 
                      year_id=year_ids, sex_id=sex_ids, gbd_round_id=5, status='best', measure_id=5)
} else {
  chagas <- readRDS(paste0(cvd_path, '/post_dismod/chagas_2016.rds'))
  chagas <- chagas[location_id==loc_id]
}
chagas <- chagas[age_group_id %in% age_group_ids]

# get Chagas prev
chagas_prev <- melt(chagas, id.vars = c("age_group_id","location_id","sex_id","year_id"),
                    measure.vars = draw_cols)
chagas_shiny<- chagas_prev[,.(prev_346=mean(value)), by=c('age_group_id', 'sex_id', 'year_id', 'location_id' )]
setnames(chagas_prev, 'value', 'prev_346')

# Get NRVD prev
# cvd_valvu_aort
cvd_valvu_aort <- get_draws(gbd_id_type='modelable_entity_id', gbd_id=19671, source='epi', location_id=loc_id, 
                            year_id=year_ids, sex_id=sex_ids, gbd_round_id=gbd_round_id, status='best', measure_id=5)
cvd_valvu_aort <- melt(cvd_valvu_aort, id.vars = c("age_group_id","location_id","sex_id","year_id"),
                    measure.vars = draw_cols)
cvd_valvu_aort_shiny <- cvd_valvu_aort[,.(prev_968=mean(value)), by=c('age_group_id', 'sex_id', 'year_id', 'location_id' )]
setnames(cvd_valvu_aort, 'value', 'prev_968')

# cvd_valvu_mitral
cvd_valvu_mitral <- get_draws(gbd_id_type='modelable_entity_id', gbd_id=19672, source='epi', location_id=loc_id, 
                              year_id=year_ids, sex_id=sex_ids, gbd_round_id=gbd_round_id, status='best', measure_id=5)
cvd_valvu_mitral <- melt(cvd_valvu_mitral, id.vars = c("age_group_id","location_id","sex_id","year_id"),
                       measure.vars = draw_cols)
cvd_valvu_mitral_shiny <- cvd_valvu_mitral[,.(prev_969=mean(value)), by=c('age_group_id', 'sex_id', 'year_id', 'location_id' )]
setnames(cvd_valvu_mitral, 'value', 'prev_969')

### Calculate HF prevalence less Chagas and the 2 NRVD causes
hf <- as.data.table(Reduce(function(x,y) join(x,y, by=c("location_id","year_id","age_group_id","sex_id", "variable")), list(hf, chagas_prev, cvd_valvu_aort, cvd_valvu_mitral)))
hf[, hf_prev:=hf-prev_346-prev_968-prev_969]
hf[, draw:=as.numeric(gsub("draw_","", variable))]
hf[, c('variable', 'hf', 'prev_346', 'prev_968', 'prev_969'):=NULL]
hf[hf<0] <- 0

## Distribute HF less Chagas& the 2 NRVD prevalence into prevalence of 21 HF etiologies
tmp2 <- join(tmp, hf, by = c('age_group_id', 'sex_id', 'year_id', 'draw'))
indx <- grep('adj_prop_', colnames(tmp2))

# subcause prev = HF-less-Chagas-less-NRVD prev * subcause prop
for(j in indx){
  set(tmp2, i=NULL, j=j, value=tmp2[[j]]*tmp2[['hf_prev']])
}
tmp2$measure_id <- 5 # add prevalence measure
colnames(tmp2) <- gsub('adj_prop_', 'adj_prev_', colnames(tmp2)) # temporary colnames to avoid confusion
prev_cols <- grep("adj_prev_", names(tmp2), value = TRUE)

################# shiny input
prop_from_prev_cols <-c('age_group_id', 'sex_id', 'year_id', 'location_id', prev_cols)
prop_from_prev <- tmp2[, mget(prop_from_prev_cols)]
prop_from_prev <- prop_from_prev[,lapply(.SD, mean), by=c('age_group_id', 'sex_id', 'year_id', 'location_id')]
prop_from_prev <- as.data.table(Reduce(function(x,y) join(x,y, by=c('age_group_id', 'sex_id', 'year_id', 'location_id')), list(prop_from_prev, chagas_shiny, cvd_valvu_aort_shiny, cvd_valvu_mitral_shiny)))

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
if (round(sum(unique(prop_from_prev[age_group_id==30 & sex_id==2 & year_id==2000]$value)), digits=10)!=1) stop('Sum does not add up to 1!')
if (round(sum(unique(prop_from_prev[age_group_id==3 & sex_id==1 & year_id==1990]$value)), digits=10)!=1) stop('Sum does not add up to 1!')
saveRDS(prop_from_prev, paste0('/CVD_FOLDER/post_dismod/2_prop_from_prev/', loc_id,'.rds'))

#######
# delete all existing files from output folder to avoid using old files by accident
do.call(file.remove, list(list.files(paste0(out_path,'*'), pattern='.rds', full.names = TRUE, recursive=TRUE))) 

get_congenital_props <- function(parent_meid){
  # Four part severity split (based on analysis from MEPS); all etiologies besides Chagas and NRVD
  props <- data.table(draw_generation_seed=rep(12345,4), 
                      child_meid=c(20090,2179,2180,2181), 
                      mean=c(0.367409594,0.187032749,0.121079336,0.324478321),
                      lower=c(0.353321033,0.129612546,0.08116928,0.282749077),
                      upper=c(0.382380074,0.254635609,0.15498155,0.369949262))
  
  set.seed(unique(props$draw_generation_seed))
  props <- as.data.table(props)
  props[, sd:=(upper-lower)/(2*1.96)]
  props[, sample_size:=mean*(1-mean)/sd^2]
  props[, alpha:=mean*sample_size]
  props[, beta:=(1-mean)*sample_size]
  
  props$tempID <- seq.int(nrow(props))
  draws_df <- data.table()
  
  for (i in seq_len(nrow(props))){
    tempID = props$tempID[[i]]
    draws = rbeta(1000*length(props$alpha[i]), props$alpha[i], props$beta[i])
    names(draws) <- paste0(rep('childprop_',each=1000), c(0:999))
    draws_row = as.data.table(as.list(draws))
    draws_row$tempID = props$tempID[[i]]
    draws_df <- rbind(draws_df, draws_row)
  }
  
  props <- as.data.table(join(props, draws_df))
  props[, c("split_version_id", "draw_generation_seed", "measure_id", "year_start", "year_end", "age_start", "age_end", "mean", "sex_id", "upper", "lower", "sd",
            "sample_size","alpha","beta","parent_meid", "tempID", "location_id"):=NULL]
  return(props)
}

congenital_split <- function(df, props, severity_type){
  df$modelable_entity_id <- composite[prev_folder==severity_type]$prev_me_id
  df <- merge(df, props, by.x='modelable_entity_id', by.y='child_meid', type='left')
  
  draw_cols <- grep("draw_", names(df), value = TRUE)
  childprop_cols <- grep("childprop_", names(df), value = TRUE)
  df[, (draw_cols):=lapply(1:1000, function(x) get(draw_cols[x])*get(childprop_cols[x]))]
  df[, (childprop_cols):=NULL]
    
  dir.create(file.path(out_path, severity_type))
  setwd(file.path(out_path, severity_type))
  message(dim(df))
  write.csv(df, paste0(loc_id,'.csv'), row.names=F)
  return(df)
}

## write each ME to a different folder in a different folder in save_results format
for (col in prev_cols){
  cause <- print(as.numeric(gsub("\\D", "", col)))
  keep_cols <- c('measure_id',	'location_id',	'year_id',	'age_group_id',	'sex_id', col, 'draw')
  prev_dat <- tmp2[, mget(keep_cols)]
  prev_dat[, draws := paste("draw", draw, sep="_"), by=draw]
  prev_dat <- dcast(prev_dat, measure_id+location_id+year_id+age_group_id+sex_id ~ draws, value.var = col)
  prev_dat$modelable_entity_id <- as.integer(unique(composite[cause_id==cause]$prev_me_id))
  
  ## fix NA draw cols caused by mktscan file ##################
  for(j in seq_along(prev_dat)){
    set(prev_dat, i = which(is.na(prev_dat[[j]]) & is.numeric(prev_dat[[j]])), j = j, value = 0)
  }
  
  ## write to folder
  subfolder <- unique(composite[cause_id==cause]$prev_folder)
  dir.create(file.path(out_path, subfolder),showWarnings = FALSE)
  setwd(file.path(out_path, subfolder))
  message(dim(prev_dat))
  write.csv(prev_dat, paste0(loc_id,'.csv'), row.names=F)
  
  if (cause==643){
    severity_types <- unique(composite[severity_id==643]$prev_folder)
    prev_dat$modelable_entity_id <- NULL
    props <- get_congenital_props(9580)
    list <- mapply("congenital_split", df=list(prev_dat), severity_type=severity_types, props=list(props), SIMPLIFY = FALSE,USE.NAMES = TRUE)
  }
}