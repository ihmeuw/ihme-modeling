####################################################################################################################################################################
# 
# Purpose: Apply ever-partnered proportion adjustment to IPV data
#
####################################################################################################################################################################

rm(list=ls())

#set parameters
age_split_run_id <- 'RUN ID' #age split run id
bv <- 'BV ID'                #bundle version
xw_date <- 'DATE'            #crosswalk data

#gbd dir
relid <- 16
root <- 'FILEPATH'


##### 0. SET UP ####################################################################################################################################################

#libraries + central functions
pacman::p_load(openxlsx, ggplot2, stringr, reticulate, DescTools)
library(logitnorm, lib.loc = paste0('FILEPATH', Sys.info()['user'], '/rlibs'))
invisible(sapply(list.files("FILEPATH", full.names = T), source))

#age metadata
age_dt <- get_age_metadata(24, release_id = relid)
age_dt[,age_group_weight_value := NULL]
age_dt <- as.data.table(sapply(age_dt, as.integer))
age_dt[,age_group_years_end := age_group_years_end - 1]
ages <- copy(age_dt)
setnames(ages, c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))

#crosswalk package
reticulate::use_python("FILEPATH")
cw <- import("crosswalk")
library(crosswalk002)

#initialize draws
draws <- paste0("draw_",0:999)

#custom function to round
mround <- function(x,base) {
  base*round(x/base)
}

#custom function to calculate cases
calculate_cases_fromse <- function(raw_dt) {
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure %in% c("prevalence","proportion"), sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

#custom function to calculate se
get_se <- function(raw_dt) {
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure %in% c("prevalence","proportion"), standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}



##### 1. GET AND FORMAT IPV DATA ###################################################################################################################################

#read in age split data
ipv <- as.data.table(read.xlsx(paste0(root, 'FILEPATH', xw_date, '/ipv_bv', bv, '_2023_agesplit_', age_split_run_id, '_region159_update.xlsx')))
ipv[, sex_id:=2]

#re-tag tabulated data; edit marriage tags (eg, ever married == ever partnered)
ipv[!is.na(sample_details) & sample_details != '', cv_student_pop:=ifelse(sample_details=='students', 1, 0)]
ipv[!is.na(sample_details) & sample_details != '', cv_epart_pop:=ifelse(sample_details=='ever partnered', 1, 0)]
ipv[!is.na(sample_details) & sample_details != '', cv_currpart_pop:=ifelse(sample_details=='currently partnered', 1, 0)]
ipv[!is.na(sample_details) & sample_details != '', cv_emarr_pop:=ifelse(sample_details=='ever married', 1, 0)]
ipv[!is.na(sample_details) & sample_details != '', cv_currmarr_pop:=ifelse(sample_details=='currently married', 1, 0)]
ipv[cv_emarr_pop == 1, cv_epart_pop := 1]
ipv[cv_currmarr_pop == 1, cv_currpart_pop := 1]

#assume untagged data does not need this adjustment
ipv[is.na(cv_pstatall), cv_pstatall:=0]
ipv[is.na(cv_pstatcurr), cv_pstatcurr:=0]

#re-map old cv naming
ipv[is.na(cv_epart_pop) & !is.na(cv_pstatall), cv_epart_pop:=eval(parse(text='cv_pstatall'))]
ipv[is.na(cv_currpart_pop) & !is.na(cv_pstatcurr), cv_currpart_pop:=eval(parse(text='cv_pstatcurr'))]

#create merge id to get xwalked values back onto metadata later
ipv[, merge_id := 1:.N]

#subset to rows that need this adjustment
toxwalk <- copy(ipv[(cv_epart_pop==1 | cv_currpart_pop==1),])
noxwalk <- copy(ipv[cv_epart_pop==0 & cv_currpart_pop==0,])

#check that nothing is missing
if(nrow(ipv)!=(nrow(toxwalk) + nrow(noxwalk))){
  warning("some data points are being dropped!")
}

#get locs that need to be xwalked
ipv_locs <- unique(toxwalk$location_id)

#get years that need to be xwalked
toxwalk[, year_id := floor((year_start+year_end)/2)]
toxwalk[!year_id == 2023, year_id := mround(year_id,5)] 
toxwalk[year_id < 1990, year_id := 1990]



##### 2. PULL PROPORTION OF EVER PARTNERED WOMEN ###################################################################################################################

#load bested dismod model of women who have ever had an intimate partner: me_id == 9380
epart_model <- get_draws(gbd_id_type = "modelable_entity_id",
                         gbd_id = 'MODEL ID',
                         source = "epi",
                         measure_id = 6,
                         sex_id = 2,
                         location_id = ipv_locs,
                         release_id = 16)

#save a copy just in case
epart_backup <- copy(epart_model)

#subset to ages 15+
epart_model <- epart_model[age_group_id >= 7, ] 

#calc draw means
epart_model$mean <- rowMeans(epart_model[, draws, with=F])

#subset to vars of interest
epart_model <- epart_model[, c("location_id", "year_id", "age_group_id", "sex_id", "mean", draws), with = F]

#pull population envelope
year_ids <- sort(unique(c(seq(1990, 2020, 5), 2022:as.integer(format(Sys.Date(), "%Y")))))
age_group_ids <- unique(epart_model$age_group_id)
pop <- get_population(age_group_id = age_group_ids, sex_id = 2, year_id = year_ids, location_id = ipv_locs, release_id = relid)[, run_id:=NULL]

#merge epart draws on pop counts
epart_model <- merge(epart_model, pop, by = c("age_group_id","location_id","year_id","sex_id"), all.x = T)



##### 3. GENERATE DRAWS OF IPV DATA TO ADJUST ######################################################################################################################

#fill in standard error
toxwalk[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
z <- qnorm(0.975)
toxwalk[is.na(standard_error), standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]

#assertion error
toxwalk[mean >= 1, mean := 0.999]

#estimate sample_size where missing (assuming binomial distribution)
toxwalk[is.na(sample_size), sample_size := mean*(1-mean)/(standard_error^2)]

#initialize for ipv draws
ipv_draws <- data.table(matrix(NA, nrow = 1000, ncol = nrow(toxwalk)+1))
names(ipv_draws)[1] <- "draw"
ipv_draws$draw <- draws

#get logit calcs using the delta transform package
logit_means <- as.data.table(delta_transform(mean=toxwalk$mean, sd=toxwalk$standard_error, transformation='linear_to_logit'))
setnames(logit_means, c('mean_logit', 'sd_logit'), c('logit_mean', 'logit_se'))
toxwalk <- cbind(toxwalk, logit_means)

for(i in 1:nrow(toxwalk)){
  logit.mean <- toxwalk[i,]$logit_mean
  logit.se <- toxwalk[i,]$logit_se
  merge_id <- toxwalk[i,]$merge_id
  var <- paste0("V",i+1)
  dr <- rnorm(n = 1000, mean=logit.mean, sd=(logit.se))
  ipv_draws[, (var) := dr]
  names(ipv_draws)[i+1] <- merge_id
}

#make draws long
ipv_long <- melt(ipv_draws, id.vars = "draw", variable.name = "merge_id", value.name = "ipv_mean")

#now back-transform logit means to linear space
ipv_long[, ipv_mean:=invlogit(ipv_mean)]

#merge draws onto xwalk dt
ipv_long$merge_id <- as.integer(as.character(ipv_long$merge_id))
toxwalk$merge_id <- as.integer(as.character(toxwalk$merge_id))
ipv_long <- merge(ipv_long, toxwalk[,.(seq,crosswalk_parent_seq,year_id,age_start,age_end,age_group_id,location_id,merge_id)], by = "merge_id", all.x = T)



##### 4. CREATE EPART ADJUSTMENTS ##################################################################################################################################

#cast epart dt to long
long_epart <- melt(data = epart_model, id.vars = c("age_group_id","year_id","location_id"), measure.vars = draws,
                   variable.name = "draw", value.name = "ever_partnered")

#merge xwalk data table and epart model data table
epart_ipv <- merge(ipv_long, long_epart, by = c("draw","year_id","age_group_id","location_id"),
                   all.x = T)

#get adjusted means
epart_ipv[, adj_ipv_mean:=ipv_mean*ever_partnered]

#collapse estimates and bind
final <- epart_ipv[, .(mean.est = mean(adj_ipv_mean),
                   var.est = var(adj_ipv_mean),
                   upr.est = quantile(adj_ipv_mean, .975),
                   lwr.est = quantile(adj_ipv_mean, .025)), by = merge_id] %>% merge(ipv[merge_id %in% unique(epart_ipv$merge_id)], by = "merge_id")

#set all proper/granular age/sex groups derived from an aggregated group of 0 to also 0
final[mean==0, mean.est := 0]
setnames(final, c('mean', 'standard_error', 'variance'), c('nonepart.mean', 'nonepart.standard_error', 'nonepart.variance'), skip_absent=T)
setnames(final, c("mean.est", "var.est"), c("mean", "variance"), skip_absent=T)
setnames(final, c('upper', 'lower'), c('nonepart.upper', 'nonepart.lower'), skip_absent=T)
setnames(final, c('upr.est', 'lwr.est'), c('upper', 'lower'), skip_absent=T)

#clear out unadjusted se information
final[, standard_error:='']
final[, cases:='']

#apply same calcs to get year id in 'final'
final[,year_id:=floor((year_start+year_end)/2)]
final[year_id != 2023, year_id := mround(year_id,5)]
final[year_id<1990, year_id:=1990]

#check
if(nrow(final)!=length(unique(epart_ipv$merge_id))){
  warning("something went wrong with final merge!")
}

#fill in some fields and note that adjustment was made with date
final[!is.na(upper) & !is.na(lower),uncertainty_type := "Confidence interval"]
final[is.na(upper) & is.na(lower),uncertainty_type_value := NA]
final[!is.na(note_modeler), note_modeler := paste0(note_modeler," | applied ever_partnered xwalk")]
final[is.na(note_modeler), note_modeler := paste0("applied ever_partnered xwalk")]

#if crosswalk_parent_seq not already filled out by previous data processing steps, fill it in now and set seq to null
final[is.na(crosswalk_parent_seq), crosswalk_parent_seq:=seq]
final[!is.na(crosswalk_parent_seq), seq:='']

#check for 'bad' values
if(nrow(final[is.na(mean),])>0){
  warning("stop: mean missing")
}
if(nrow(final[is.na(upper) & is.na(lower) & is.na(effective_sample_size) & is.na(standard_error)])>0){
  warning("stop: variance missing")
}
if(nrow(final[mean < 0 | mean > 1])>0){
  warning("stop: mean out of range")
}
if(nrow(final[mean < lower | mean > upper])>0){
  warning("stop: mean out of ui")
}

#recreate year_id
final[, year_id:=floor((year_start+year_end)/2)]
final[, year_start:=year_id]
final[, year_end:=year_id]

#bind back on to data not adjusted in this step, drop merge id
to_upload <- rbind(final, noxwalk, fill=T)
to_upload[,merge_id:=NULL]

#save csv
epart_path <- paste0(root, 'FILEPATH', xw_date)
dir.create(epart_path)
write.csv(to_upload, paste0(epart_path,'/','ipv_bv', bv, '_agesplit', age_split_run_id,'_data_2020epartadj_raw_region159_launch_update.csv'), row.names = F)



##### 5. UPDATE CROSSWALK VERSION ##################################################################################################################################

#reset variance, standard error, upper, and lower for all data
to_upload[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
to_upload[, standard_error := as.numeric(standard_error)]
to_upload[is.na(variance), variance := standard_error^2]
to_upload[is.na(standard_error), standard_error := sqrt(variance)]

#final edits
all_data <- copy(to_upload)
all_data[is.na(upper) | mean >= upper, upper:=mean+1.96*standard_error]
all_data[is.na(lower) | mean <= lower, lower:=mean-1.96*standard_error]
all_data[lower<0, lower:=0]
all_data[upper>1, upper:=1]

#save to excel file
data <- copy(all_data)

#rename
setnames(data, 'mean', 'val')  

#save data for upload
data_filepath <- paste0(epart_path,'/','ipv_bv', bv, '_agesplit_with_run', age_split_run_id,'_region159_applied_2020epartadj_raw_stgprupload_after_yrbsoutliers_update.xlsx')
write.xlsx(data, data_filepath, sheetName='extraction')

#save xwalk version
xv_result <- save_crosswalk_version(bv,
                                    data_filepath = data_filepath,
                                    description = paste0(Sys.Date(), ' | xwalk ', xw_date, ' and age split run ', age_split_run_id, ' | 2024 epart adj'))
#get xwalk version
xv_result$crosswalk_version_id


