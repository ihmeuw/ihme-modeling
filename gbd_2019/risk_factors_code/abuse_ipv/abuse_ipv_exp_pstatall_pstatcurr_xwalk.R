#--------------------------------------------------------------
# Name: author name
# Date: 2019-10-07
# Project:  IPV/CSA data processing pipeline
# Purpose:  Ever-partnered crosswalk for IPV exposure, bundle 319, decomp 2,3,4
#           To be applied after all other crosswalks (phys-only, 12 month, etc.)
#--------------------------------------------------------------

rm(list=ls())

# Map drives

if (Sys.info()['sysname'] == 'Linux') {
  j_drive <- '/home/j/' 
  h_drive <- '~/'
} else { 
  j_drive <- 'J:/'
  h_drive <- 'H:/'
}

fn_dir <- paste0(j_drive, "FILEPATH")

source(paste0(fn_dir,"get_draws.R"))
source(paste0(fn_dir,"get_age_metadata.R"))
source(paste0(fn_dir,"get_population.R"))
source(paste0(fn_dir,"get_crosswalk_version.R"))

## SETTINGS ------------------------------------- ##

#to test only one location:
#loc_id <- 538 #538 = Iowa

args <- commandArgs(trailingOnly = TRUE) #this is everything fed into this qsub
loc_id <- as.character(args[1])
#loc_id <- ipv_locs

## FNS ------------------------------------------ ##
mround <- function(x,base){
  base*round(x/base)
}
se <- function(x){
  sd(x)/sqrt(length(x))
}

draws <- paste0("draw_",0:999)

## PULL IPV DATA TO XWALK ----------------------  ##
# pull crosswalked ipv data

ipv <- fread(paste0(h_drive,"FILEPATH/ipv_step2_crosswalk.csv"))

# subset to rows need to xwalk
ipv[is.na(cv_pstatall),cv_pstatall:=0]
ipv[is.na(cv_pstatcurr),cv_pstatcurr:=0]
toxwalk <- copy(ipv[cv_pstatall==1 | cv_pstatcurr==1,])
noxwalk <- copy(ipv[cv_pstatall==0 & cv_pstatcurr==0,])

# get locs that need to be xwalked
ipv_locs <- unique(toxwalk$location_id)

## ---------------------------------------------- ##

# subset to one loc
# toxwalk <- toxwalk[location_id %in% loc_id,]

# pull age data
age_dt <- get_age_metadata(12)
age_dt[,age_group_weight_value:=NULL]
age_dt <- as.data.table(sapply(age_dt, as.integer))

## round ages
toxwalk[, `:=` (round_start = mround(age_start,5), round_end = mround(age_end,5)-1)]

# widen ages bins that collapsed to 0
toxwalk[round_end-round_start<=0 & age_start %% 5<=2, round_end := round_start + 4]
toxwalk[round_end-round_start<=0 & age_start %% 5>2, round_start := round_end - 4]

# make sure age bins make sense
if(nrow(toxwalk[round_end-round_start<=0,])>0){
  stop("oops, still have age bins to expand")
}

# get year_ids
toxwalk[,year_id:=(year_start+year_end)/2]
toxwalk[,year_id := year_id - (year_id %% 1)]
toxwalk[!(year_id %in% c(2017,2019)),year_id := mround(year_id,5)]
toxwalk[year_id>2019,year_id := 2019]


## PULL PROPORTION OF EVER PARTNERED WOMEN ---- ##

# load dismod model of women who have ever had an intimate partner; me_id 9380
# get_draws, gbd_id_type("modelable_entity_id") gbd_id(9380) source("epi") measure_id(6) sex_id(2) status("best") clear
epart_model <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 9380,
                         source = "epi", measure_id = 6, sex_id=2,
                         location_id = loc_id, version_id = 394967,
                         decomp_step = 'step4')

# subset to ages we have data for (though we only estimate for ages>=15)
# setnames(epart_model, c("age_group_years_start","age_group_years_end"),
#          c("age_start","age_end"))
epart_model <- epart_model[age_group_id>=6,]

# calc draw means
epart_model$mean <- rowMeans(epart_model[,draws,with=F])

# subset to vars of interest
epart_model <- epart_model[,c("location_id",
                              "year_id",
                              "age_group_id",
                              "sex_id",
                              "mean",
                              draws), with = F]
# "age_start",
# "age_end",


# pull population envelope
year_ids <- c(seq(1990,2015,5),2017,2019)
sex_ids <- 2
age_group_ids <- unique(epart_model$age_group_id)
pop <- get_population(age_group_id = age_group_ids, sex_id = 2, year_id = year_ids,
                      location_id = loc_id, decomp_step = 'step4')
pop[,run_id:=NULL]

# merge on pop counts
epart_model <- merge(epart_model, pop, by = c("age_group_id","location_id","year_id","sex_id"), all.x = T)

# switch to 0-4 age bins to match IPV data
# epart_model[,age_end:=age_end-1]
age_dt[,age_group_years_end:=age_group_years_end - 1]

# get age-year bins we need to match
ipv_ages <- unique(toxwalk[,.(round_start,round_end,year_id,location_id)])
message("could optionally do all locs at once here")
setnames(ipv_ages, c("round_start","round_end"),c("age_start","age_end"))
ipv_ages[age_end==99,age_end:=124] #this could alternately be 94?
ipv_ages <- unique(ipv_ages)

# add one row for each gbd age_group id that overlaps with each ipv data age_group-year
needs_aggr <- data.table()
for(i in 1:nrow(ipv_ages)){
  start <- ipv_ages[i,age_start]
  end <- ipv_ages[i,age_end]
  groups <- age_dt[age_group_years_start>=start & age_group_years_end <= end,]$age_group_id
  for(j in 1:length(groups)){
    needs_aggr <- rbind(needs_aggr, ipv_ages[i,], fill = T)
    needs_aggr[nrow(needs_aggr),age_group_id:=groups[j]]
  }
  # ipv_ages[i,aggr_age_ids:=paste(groups,collapse=",")]
}

needs_aggr <- needs_aggr[order(age_start,age_end,year_id)]

# merge on ever_partnered + population estimates
needs_aggr <- merge(needs_aggr, epart_model, by = c("year_id","age_group_id","location_id"), all.x=T, sort = F)
needs_aggr <- as.data.table(sapply(needs_aggr,as.numeric))
needs_aggr[,denom:=sum(population), by = .(age_start,age_end,year_id,location_id)]
needs_aggr[,age_group_scalar:=population/denom]
needs_aggr <- needs_aggr[,c("age_start","age_end","year_id","age_group_scalar","location_id",draws),with=F]

# multiply ever_partnered draws by fraction of age_group
adj_needs_aggr <- copy(needs_aggr)
for(j in draws){
  set(adj_needs_aggr, i=NULL, j=j, value=adj_needs_aggr[[j]]*adj_needs_aggr[['age_group_scalar']])
}

#create merge key to get metadata back after collapse sum
adj_needs_aggr[,merge_key:=1:.N, by = c("age_start","age_end","year_id","location_id")]
adj_needs_aggr[merge_key>1,merge_key:=0]
adj_needs_aggr[,merge_key:=cumsum(merge_key)]


# sum each unique combination of age_start, age_end, and year_id (we've already subsetted to one loc)
epart <- copy(adj_needs_aggr)
epart <- epart[,c("merge_key",draws),with=F]

epart_props <- as.data.table(rowsum(epart, epart$merge_key, reorder = FALSE))
# recreate merge_key. this assumes hasn't been reordered
epart_props[,merge_key:=1:.N]

# merge metadata back on
metadata <- unique(adj_needs_aggr[,.(age_start,age_end,year_id,location_id,merge_key)])
epart_props <- merge(epart_props, metadata, by = "merge_key", all.x = T)

# reorder cols
epart_props <- epart_props[,c("merge_key","age_start","age_end","year_id","location_id",draws),with=F]

# cast to long
long_epart <- melt(data = epart_props, id.vars = c("age_start","age_end","year_id","location_id"), measure.vars = draws,
                   variable.name = "draw", value.name = "ever_partnered")
if(nrow(long_epart[ever_partnered>1,])>0){
  stop("uh oh--somehow ever_partnered aggregated up to> 1")
}


## GENERATE 1000 DRAWS IPV DATA TO ADJUST ----------- ##

toxwalk[is.na(sample_size),est_ss:=TRUE]
toxwalk[!is.na(sample_size),est_ss:=FALSE]
## estimate sample_size where missing (assuming binomial distribution)
toxwalk[is.na(sample_size),sample_size := mean*(1-mean)/(standard_error^2)]

## account for study_design where applicable
toxwalk[is.na(effective_sample_size) & !is.na(design_effect), effective_sample_size := sample_size / design_effect]

## for all else, use sample_size
toxwalk[is.na(effective_sample_size),effective_sample_size := sample_size]

toxwalk[,standard_deviation := standard_error * sqrt(effective_sample_size)]

# replace mean = logit(mean)
ipv_draws <- data.table(matrix(NA, nrow = 1000, ncol = nrow(toxwalk)+1))
names(ipv_draws)[1] <- "draw"
ipv_draws$draw <- draws

for(i in 1:nrow(toxwalk)){
  mean <- toxwalk[i,]$mean
  ss <- round(toxwalk[i,]$effective_sample_size)
  seq <- toxwalk[i,]$seq
  var <- paste0("V",i+1)
  dr <- rbinom(n = 1000, size = ss, prob = mean)
  dr <- dr/ss
  ipv_draws[, (var) := dr] #invlogit here
  names(ipv_draws)[i+1] <- seq
}

ipv_long <- melt(ipv_draws, id.vars = "draw", variable.name = "seq", value.name = "ipv_mean")
ipv_long$seq <- as.integer(as.character(ipv_long$seq))
ipv_long <- merge(ipv_long, toxwalk[,.(seq,year_id,round_start,round_end,location_id)], by = "seq", all.x = T)
setnames(ipv_long, c("round_start","round_end"),c("age_start","age_end"))

long_epart[age_end==124,age_end:=99]
epart_ipv <- merge(ipv_long, long_epart, by = c("draw","year_id","age_start","age_end","location_id"),
                   all.x = T)

epart_ipv[,adj_ipv_mean:=ipv_mean * ever_partnered]


# cast to wide, get summary stats
# we're going to calculate standard dev instead of stderr, because across the board, the
# sds of the randomly generated binomial distribution match our ses, rather than our sds.

diagnostics <- data.table()
for(i in 1:nrow(toxwalk)){
  data <- toxwalk[i,.(mean,standard_error,standard_deviation,effective_sample_size,est_ss)]
  generated<- rbinom(n = 1000, size = round(toxwalk[i,effective_sample_size]), prob = toxwalk[i,mean])
  generated <- generated/round(toxwalk[i,effective_sample_size])
  summary <- data.table(summary_mean = mean(generated), summary_se = sd(generated))
  summary <- cbind(summary, data)
  diagnostics <- rbind(diagnostics, summary)
}


ipv_summary <- dcast(epart_ipv, formula = year_id+age_start+age_end+location_id+seq~draw, value.var = "adj_ipv_mean")
ipv_summary$mean <- rowMeans(ipv_summary[,draws,with=F])
for(i in 1:nrow(ipv_summary)){
  ste <- se(ipv_summary[i,draws,with=F]) #see diagnostics above for why we're doing this
  ipv_summary[i,standard_error:=ste]
  sd <- sd(ipv_summary[i,draws,with=F]) #see diagnostics above for why we're doing this
  ipv_summary[i,standard_dev:=sd]
}


# check out what it looks like
original <- toxwalk[seq %in% ipv_summary$seq,.(year_id,age_start,age_end,seq,mean,standard_error)]
ipv_summary[,(draws) := NULL]

# more diagnostics -- note that when we use sd, it stays more reasonably close to original se.
names(original)[5:6] <- c("or_mean","or_se")
compare <- copy(ipv_summary)[,.(seq,mean,standard_error,standard_dev)]
compare <- merge(compare, original, by = "seq")
compare[,cv_with_se:=standard_error/mean]
compare[,cv_with_sd:=standard_dev/mean]
compare[,org_cv:=or_se/or_mean]

# keep sd
ipv_summary[,standard_error:=NULL]
setnames(ipv_summary, "standard_dev","standard_error")


# format for uploader
final <- copy(ipv[seq %in% ipv_summary$seq,])
final[ ,c("mean", "standard_error") := NULL]
final <- merge(final, ipv_summary[,.(seq,mean,standard_error)], by = "seq", all = T)

final[,c("upper","lower") := NA]

if(length(names(final))!=length(names(ipv))){
  warning("wrong number of cols. something went wrong!")
}

if(nrow(final)!=nrow(ipv_summary)){
  warning("something went wrong with final merge!")
}

final[,uncertainty_type := "Confidence interval"]
final[,case_name := "ipv_ever_any"]
final[,note_modeler := paste0(note_modeler," | applied ever_partenred xwalk")]

# we ran crosswalks in two steps--- so some already have xwalk parent seqs
# for those that don't, give them xwalk parent seqs
final[is.na(crosswalk_parent_seq),crosswalk_parent_seq_temp := seq]
final[!is.na(crosswalk_parent_seq_temp), `:=` (crosswalk_parent_seq = crosswalk_parent_seq_temp, seq = NA)]
final[,crosswalk_parent_seq_temp:=NULL]

if(nrow(final[is.na(mean),])>0){
  warning("uh oh, mean missing")
}

if(nrow(final[is.na(upper) & is.na(lower) & is.na(effective_sample_size) & is.na(standard_error)])>0){
  warning("uh oh, variance missing")
}

if(nrow(final[mean < 0 | mean > 1])>0){
  warning("mean out of range")
}

zeros <- ipv[(cv_pstatall==1 | cv_pstatcurr ==1 ) & mean==0,]$seq


## save
save_path <- paste0(h_drive,"FILEPATH","bundle_319_step2_data_with_epart_xwalk_for_upload_",loc_id,".xlsx")
writexl::write_xlsx(list(extraction=final),save_path)