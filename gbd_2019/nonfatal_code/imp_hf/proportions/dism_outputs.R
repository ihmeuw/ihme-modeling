
##
####### 1. Fetch dismod draws from 6 HF etiological proportion models
####### 2. Split the 6 main causes into 21 subcauses
####### 3. Fetch HF and HF due to Chagas/NRVD dismod prevalence draws (best) 
#######    a. Get HF etiological prevalence by multiply HF less Chagas/NRVD prevalence by proportion of each
#######    subcause calculated in step 4
#######    b. Severity split HF due to Chagas prevalence into 3 mes using expert-provided proportions    
##

datetime <- format(Sys.time(), '%Y_%m_%d')

pacman::p_load(plyr, data.table, parallel, RMySQL, openxlsx, ggplot2)

# source the central functions
source("get_draws.R")
source("get_location_metadata.R")
source("get_population.R")


# cvd output folder
cvd_path = "FILEPATH"

task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))

# parameters
gbd_round_id=6
year_ids <- c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)
sex_ids  <- c(1,2)
age_group_ids = c(2:21,30,31,32,235)
args   <- commandArgs(trailingOnly = T)
loc_id <- args[1] #arg
print(loc_id)
locs <- fread(loc_id)
loc_id <- locs[task_id, Var1]
outputDir <- 'FILEPATH'
split_chagas <- "split_chagas" 

# hf etiology map
composite <- fread(paste0(cvd_path, '/composite_cause_list.csv'))

# ID table
IDs <-data.table(name = c("VALUE"), 
                 bundle_id = c("VALUE"),
                 modelable_entity_id = c("VALUE"),
                 nid = c("VALUE"),
                 cause_id = c("VALUE"),
                 old_bundle_id = c("VALUE")
)

composite <- merge(composite, IDs, by.x="prop_bundle_id", by.y="old_bundle_id", all=T)

#######################################
## 1. Fetch dismod draws from 6 HF etiological proportion models
## (rewrite of split17.do)
#######################################

gbd_ids <- unique(composite$modelable_entity_id)
gbd_ids <- gbd_ids[!is.na(gbd_ids)]

# get proportions
tmp_str <- ''
prop_draws <- function(gbd_id) {
  decomp_step <- "step4"
  tmp_str <- "STR"
  dat <- get_draws(gbd_id_type='modelable_entity_id', gbd_id=gbd_id, source='epi', location_id=loc_id, 
                   year_id=year_ids, sex_id=sex_ids, gbd_round_id=6, status='best', measure_id=18, decomp_step = decomp_step)
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

x <- datalist[[1]]
for (i in 2:length(datalist)) x <- merge(x, datalist[[i]], by=c("age_group_id", "sex_id", "year_id", "draw"))

# keep only age groups used
df <- x[age_group_id %in% age_group_ids] 

## shiny input
dismod_output <- df[, .(age_group_id, sex_id, year_id, other_value, ihd_value, htn_value, valvular_value, cpm_value, cmp_value)]
dismod_output <- dismod_output[,lapply(.SD, mean), by=c('age_group_id', 'sex_id', 'year_id')]
dismod_output[, location_id:=loc_id]
dir.create(paste0(outputDir, '/0_dismod_output/', datetime, '/'), showWarnings = FALSE)
saveRDS(dismod_output, paste0(outputDir, '/0_dismod_output/', datetime, '/', loc_id,'.rds')) 

#######################################
## 4. Split the 6 main causes into 21 subcauses
#######################################
# read from proportion file generated in pre_dismod step

proportion <- fread('FILEPATH')[location_id==loc_id]

pop <- get_population(age_group_id = unique(proportion$age_group_id), location_id = unique(proportion$location_id), sex_id = c(1, 2), 
                      year_id = unique(proportion$year_id), decomp_step = "step4", gbd_round_id = 6)
pop[, sum_pop := sum(population), by=c("age_group_id", "location_id", "sex_id")]
pop[, avg_pop := sum_pop/length(unique(pop$year_id))]
pop <- unique(pop[, .(age_group_id, sex_id, location_id, avg_pop)])

proportion <- merge(proportion, pop, by=c("age_group_id", "sex_id", "location_id"))

proportion[, cases := prev_adj_final * avg_pop]
proportion[, sum_cases := sum(cases), by=c("age_group_id", "sex_id", "location_id", "year_end", "cause_id")]
proportion[, sum_cases_den := sum(cases), by=c("age_group_id", "sex_id", "year_end", "location_id", "bundle_id")]
proportion[, prop_final := sum_cases/sum_cases_den]
proportion[is.na(prop_final), prop_final := 0]

proportion <- proportion[, .(location_id, age_group_id, sex_id, cause_id, bundle_id, year_end, prop_final)]
proportion$cause_id <- sub("^", "hf_target_prop", proportion$cause_id )
proportion <- dcast(proportion,location_id+age_group_id+sex_id+year_end~cause_id, value.var = "prop_final")

# merge subcause proportion table with dismod result table for 6 main causes
tmp <- join(df, proportion)
tmp[tmp<0] <- 0 # possible negative values due to draws are assigned 0
tmp[is.na(tmp)] <- 0

## Split into 21 subcauses
# Split cardiopulmonary
tmp[,adj_prop_509:=cpm_value*hf_target_prop509] #COPD
tmp[,adj_prop_516:=cpm_value*hf_target_prop516] #Interstitial
tmp[,adj_prop_511:=cpm_value*hf_target_prop511] #Pneumoconiosis - silicosis
tmp[,adj_prop_512:=cpm_value*hf_target_prop512] #Pneumoconiosis - asbestosis
tmp[,adj_prop_513:=cpm_value*hf_target_prop513] #Pneumoconiosis - coal workers
tmp[,adj_prop_514:=cpm_value*hf_target_prop514] #Pneumoconiosis - other

# Split cardiomyopathy
tmp[,adj_prop_942:=cmp_value*hf_target_prop942] #Myocarditis
tmp[,adj_prop_938:=cmp_value*hf_target_prop938] #Alcoholic cardiomyopathy
tmp[,adj_prop_944:=cmp_value*hf_target_prop944] #Other cardiomyopathy

# Split other
tmp[,adj_prop_503:=other_value*hf_target_prop503] #Endocarditis
tmp[,adj_prop_614:=other_value*hf_target_prop614] #Thalassemia
#tmp[,adj_prop_390:=other_prop*hf_target_prop390/hf_target_prop385] #Iron deficiency anemia #02/28
#tmp[,adj_prop_388:=other_prop*hf_target_prop388/hf_target_prop385] #Iodine deficiency #02/28
tmp[,adj_prop_616:=other_value*hf_target_prop616] #G6PD deficiency
tmp[,adj_prop_619:=other_value*hf_target_prop619] #Oher endocrine, nutritional, blood, and immune disorders
tmp[,adj_prop_618:=other_value*hf_target_prop618] #Other anemias
tmp[,adj_prop_643:=other_value*hf_target_prop643] #Congenital
tmp[,adj_prop_507:=other_value*hf_target_prop507] #Other circulartory and cardiovascular
tmp[,adj_prop_970:=other_value*hf_target_prop970] #Other non-rheumatic valve diseases

# Split valvular
# tmp[,adj_prop_492:=valvular_prop*hf_target_prop4920/hf_target_prop492] #Rheumatic heart disease

# Rename standalone causes
tmp <- plyr::rename(tmp, c("ihd_value"="adj_prop_493", "htn_value"="adj_prop_498", "valvular_value"="adj_prop_492"))
tmp[is.na(tmp)] <- 0
tmp[tmp==Inf] <- 0 
print(names(tmp))

#########################
keep_cols <- c(grep("adj_prop_", names(tmp), value = TRUE), 'age_group_id','sex_id','year_id','draw')
tmp <- tmp[, keep_cols, with=FALSE]

#######################################
## 5. Fetch HF and HF due to Chagas/NRVD dismod prevalence draws (best)
#######################################
# get HF envelope
keycols = c("location_id", "year_id", "age_group_id", "sex_id")

hf <- get_draws(gbd_id_type ='modelable_entity_id', gbd_id="VALUE", source='epi', location_id=loc_id, 
                year_id=year_ids, sex_id=sex_ids, gbd_round_id=6, status='best', measure_id=5, decomp_step="step4")
draw_cols = grep("^draw_[0-999]?", names(hf), value=TRUE)
hf <- hf[age_group_id %in% age_group_ids]
hf <- melt(hf, id.vars = c("age_group_id","location_id","sex_id","year_id"),
                       measure.vars = draw_cols)
setnames(hf, 'value', 'hf')

hf$metric_id <- 3

# get HF due to Chagas prevalence draws
chagas_update=T
if (chagas_update==T){
  chagas <- get_draws(gbd_id_type='modelable_entity_id', gbd_id="VALUE", source='epi', location_id=loc_id, 
                      year_id=year_ids, sex_id=sex_ids, gbd_round_id=6, decomp_step = "step4", status='best', measure_id=5)
} 
chagas <- chagas[age_group_id %in% age_group_ids]

# get Chagas prev
chagas_prev <- melt(chagas, id.vars = c("age_group_id","location_id","sex_id","year_id"),
                    measure.vars = draw_cols)
chagas_shiny<- chagas_prev[,.(prev_346=mean(value)), by=c('age_group_id', 'sex_id', 'year_id', 'location_id' )]
setnames(chagas_prev, 'value', 'prev_346')

# Get NRVD prev
# cvd_valvu_aort
cvd_valvu_aort <- get_draws(gbd_id_type='modelable_entity_id', gbd_id="VALUE", source='epi', location_id=loc_id, 
                            year_id=year_ids, sex_id=sex_ids, gbd_round_id=6, decomp_step="step4", status='best', measure_id=5)
cvd_valvu_aort <- melt(cvd_valvu_aort, id.vars = c("age_group_id","location_id","sex_id","year_id"),
                    measure.vars = draw_cols)
cvd_valvu_aort_shiny <- cvd_valvu_aort[,.(prev_968=mean(value)), by=c('age_group_id', 'sex_id', 'year_id', 'location_id' )]
setnames(cvd_valvu_aort, 'value', 'prev_968')

# cvd_valvu_mitral
cvd_valvu_mitral <- get_draws(gbd_id_type='modelable_entity_id', gbd_id="VALUE", source='epi', location_id=loc_id, 
                              year_id=year_ids, sex_id=sex_ids, gbd_round_id=6, decomp_step="step4", status='best', measure_id=5)
cvd_valvu_mitral <- melt(cvd_valvu_mitral, id.vars = c("age_group_id","location_id","sex_id","year_id"),
                       measure.vars = draw_cols)
cvd_valvu_mitral_shiny <- cvd_valvu_mitral[,.(prev_969=mean(value)), by=c('age_group_id', 'sex_id', 'year_id', 'location_id' )]
setnames(cvd_valvu_mitral, 'value', 'prev_969')

### Calculate HF prevalence less Chagas and NRVD causes
hf <- as.data.table(Reduce(function(x,y) join(x,y, by=c("location_id","year_id","age_group_id","sex_id", "variable")), list(hf, chagas_prev, cvd_valvu_aort, cvd_valvu_mitral)))
hf[, hf_prev:=hf-prev_346-prev_968-prev_969]
hf[, draw:=as.numeric(gsub("draw_","", variable))]
hf[, c('variable', 'hf', 'prev_346', 'prev_968', 'prev_969'):=NULL]
hf[hf<0] <- 0

## Distribute HF less Chagas&NRVD prevalence into prevalence of 21 HF etiologies

tmp2 <- join(tmp, hf, by = c('age_group_id', 'sex_id', 'year_id', 'draw'))
indx <- grep('adj_prop_', colnames(tmp2))

# subcause prev = HF-less-Chagas prev * subcause prop
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
#prop_from_prev[,lapply(.SD, function(x){x/sum_of_prev}), by=c('age_group_id', 'sex_id', 'year_id', 'location_id'), .SDcols=cols]

colnames(prop_from_prev) <- gsub('prev_', 'prop_prev_', colnames(prop_from_prev)) 
prop_from_prev <- melt(prop_from_prev, id.vars=c('age_group_id', 'sex_id', 'year_id', 'location_id'))
#test <- as.data.table(ddply(prop_from_prev, .(age_group_id, location_id, sex_id, year_id), transform, fraction = value/sum(value)))
if (round(sum(unique(prop_from_prev[age_group_id==30 & sex_id==2 & year_id==1990]$value)), digits=10)!=1) stop('Sum does not add up to 1!')
if (round(sum(unique(prop_from_prev[age_group_id==3 & sex_id==1 & year_id==2000]$value)), digits=10)!=1) stop('Sum does not add up to 1!')
dir.create("FILEPATH", showWarnings = FALSE)
saveRDS(prop_from_prev, "FILEPATH") 
#######

# delete all existing files from output folder 
#do.call(file.remove, list(list.files(paste0(outputDir,'*'), pattern='.rds', full.names = TRUE, recursive=TRUE))) 
## write each Chagas ME to a different folder in a different folder in save_results format after severity split
dir.create("FILEPATH", showWarnings = FALSE)
prevDir <- paste0("FILEPATH")
severity_split <- function(df, severity_type){
  prop <- composite[prev_folder==severity_type]$severity_split
  df$modelable_entity_id <- composite[prev_folder==severity_type]$prev_me_id
  
  for (i in 0:999){
    col <- paste0("draw_", i)
    df$newcol <- df[,get(col)]*prop
    df[, c(col):=NULL]
    names(df)[names(df) == "newcol"] <- col
    #print(head(col.after))"LINK"
  }
  
  dir.create(file.path(prevDir, severity_type))
  setwd(file.path(prevDir, severity_type))
  message(dim(df))
  #saveRDS(df, paste0(loc_id,'.rds'))
  #fwrite(df, paste0(loc_id,'.csv'))
  write.csv(df, paste0(loc_id,'.csv'), row.names=F)
  return(df)
}

get_congenital_props <- function(parent_meid){
  
  # Four part severity split (based on analysis from MEPS); all etiologies besides Chagas and NRVD
  props <- data.table(draw_generation_seed=rep(12345,4), 
                      child_meid=c("VALUES"), 
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
  #props[, c("split_version_id", "draw_generation_seed", "measure_id", "year_start", "year_end", "age_start", "age_end", "mean", "sex_id", "upper", "lower", "sd",
 #           "sample_size","alpha","beta","parent_meid", "tempID", "location_id"):=NULL]
  return(props)
}

congenital_split <- function(df, props, severity_type){
  df$modelable_entity_id <- composite[prev_folder==severity_type]$prev_me_id
  df <- merge(df, props, by.x='modelable_entity_id', by.y='child_meid', type='left')
  
  draw_cols <- grep("draw_", names(df), value = TRUE)
  draw_cols <- draw_cols[!(grepl("generation", draw_cols))]
  childprop_cols <- grep("childprop_", names(df), value = TRUE)
  df[, (draw_cols):=lapply(1:1000, function(x) get(draw_cols[x])*get(childprop_cols[x]))]
  df[, (childprop_cols):=NULL]
  
  dir.create(file.path(prevDir, severity_type))
  setwd(file.path(prevDir, severity_type))
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
  prev_dat$modelable_entity_id <- as.integer(unique(composite[cause_id.x==cause]$prev_me_id))
  
  ## fix NA draw cols caused by mktscan file ##################
  for(j in seq_along(prev_dat)){
    set(prev_dat, i = which(is.na(prev_dat[[j]]) & is.numeric(prev_dat[[j]])), j = j, value = 0)
  }
  
  ## write to folder
  subfolder <- unique(composite[cause_id.x==cause]$prev_folder)
  dir.create(file.path(prevDir, subfolder),showWarnings = FALSE)
  setwd(file.path(prevDir, subfolder))
  message(dim(prev_dat))
  # saveRDS(prev_dat, paste0(loc_id,'.rds'))
  # fwrite(prev_dat, paste0(loc_id,'.csv'))
  write.csv(prev_dat, paste0(loc_id,'.csv'), row.names=F)
  
  if (cause=="VALUE"){
    severity_types <- unique(composite[severity_id==643]$prev_folder)
    prev_dat$modelable_entity_id <- NULL
    props <- get_congenital_props("VALUE")
    for (sev in severity_types) congenital_split(df=prev_dat, props=props, severity_type=sev)
  }
}


