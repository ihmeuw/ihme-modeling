# Purpose: Estimate lymphedema prevalence due to LF

############## BOILERPLATE ############## 
rm(list = ls())
user <- Sys.info()[["user"]]
cause <- "ntd_lf"
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"
options(max.print=999999)

# toggle btwn production arg parsing vs interactive development
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  parser$add_argument("--release_id", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir  <- "FILEPATH"
  draws_dir   <- "FILEPATH"
  interms_dir <- "FILEPATH"
  logs_dir    <- "FILEPATH"
}

# 1. Setup libraries, functions, parameters, filepaths, etc. ---------------------------------------------------------------------
# install non-included libraries
path <- "FILEPATH"
if (!dir.exists(file.path(path))){dir.create(path)}
install.packages("BayesianTools", lib=path)
install.packages("Rcpp", lib=path)

# load libraries
library(BayesianTools, lib.loc=path)
library(Rcpp, lib.loc=path)
require(splines)
library(INLA)
library(ggplot2)
library(dplyr)

# source functions
source("FILEPATH")

# directories
params_dir <- "FILEPATH"
run_file <- fread("FILEPATH")
run_folder_path <- run_file[nrow(run_file), run_folder_path]
draws_dir <- "FILEPATH"
interms_dir <- "FILEPATH"
crosswalks_dir <- "FILEPATH"
lymph_dir <- "FILEPATH"
if (!dir.exists(file.path(lymph_dir))){dir.create(lymph_dir)}

# set parameters
draws <- 1000
release_id <- ADDRESS

# 2. Load GBD location information ---------------------------------------------------------------------
# get current GBD values for primary demographics
demo <- get_demographics(gbd_team = 'epi', release_id = release_id)
gbd_years <- sort(demo$year_id)
gbd_loc_ids <- sort(demo$location_id)
gbd_ages <- sort(demo$age_group_id)
gbd_sex_id <- sort(demo$sex_id)

# load LF geographic restrictions
lf_endems <- fread("FILEPATH")[value_endemicity == 1,]
lf_endems <- sort(unique(lf_endems$location_id))
## add India level-5 sub-nationals
### add Rural units to lf_endems and Urban to zero_locs
lvl5_rural <- c(43908,43909,43910,43911,43913,43916,43917,43918,43919,43920,43921,43922,
                43923,43924,43926,43927,43928,43929,43930,43931,43932,43934,43935,43936,
                43937,43938,43939,43940,43941,43942,44539)
lvl5_urban <- c(43872,43873,43874,43875,43877,43880,43881,43882,43883,43884,43885,43886,
                43887,43888,43890,43891,43892,43893,43894,43895,43896,43898,43899,43900,
                43901,43902,43903,43904,43905,43906,44540)
lvl_india <- c(lvl5_rural,lvl5_urban)
lf_endems <- c(lf_endems,lvl_india)
lf_endems <- unique(lf_endems)

# 3. Estimate lymphedema prevalence ---------------------------------------------------------------------
mod_data<-read.csv("FILEPATH")

#summarize main variables
summary(mod_data$lymph_mean)
summary(mod_data$mean_adjusted)
plot(mod_data$mean_adjusted,mod_data$lymph_mean)

#high value lymph  mean due to age splitting small sample sizes
#need to drop rows where age_start = 15 and age_end = 99 or age 0 to 99 (data that were not able to be age split due to low sample size)
mod_data$age_diff<-mod_data$age_end-mod_data$age_start
mod_data$age_start<-ifelse(mod_data$age_diff>25,40,mod_data$age_start)
mod_data<-mod_data[mod_data$age_diff<25,]

#drop where lymph_ss (sample size) <1 - implausible denominator for national level analysis
#outlier
mod_data<-mod_data[mod_data$lymph_ss>=25,]
mod_data<-mod_data[mod_data$lymph_mean<.2,]

#mod_data<-mod_data[mod_data$age_start<65,]
mod_data$lymph_mean<-ifelse(mod_data$age_start<=5,0,mod_data$lymph_mean)
mod_data$lymph_cases<-ifelse(mod_data$age_start<=5,0,mod_data$lymph_cases)

ggplot(mod_data, aes(x=mean_adjusted, y=lymph_mean, color=age_start)) +
  geom_point()

mod_data<-mod_data[!(mod_data$mean_adjusted<0.05 & mod_data$lymph_mean>.01),]

#add spline to mean_adjusted
mod1<-glm(lymph_mean~age_start+sex+bs(mean_adjusted, knots = c(.1,.15)), weights=lymph_ss, family=binomial(link="log"), data=mod_data)

preds<-as.data.frame(exp(predict(mod1)))
names(preds)[1]<-"prediction"

mod_data2<-mod_data[!is.na(mod_data$mean_adjusted),]

#append
mod_data3<-cbind(mod_data2,preds)
plot(mod_data3$lymph_mean,mod_data3$mean_adjusted)

plot(mod_data3$age_start,mod_data3$prediction)

#ggplot to view by age
ggplot(mod_data3, aes(x=mean_adjusted, y=prediction, color=age_start)) +
  geom_point()

###data processing to implement in INLA
age_info<-read.csv("FILEPATH")

#keep age_start, mean_adjusted, sex, lymph_mean
keeps <- c("age_start","mean_adjusted","sex","lymph_mean","location_id","year_start","lymph_ss","lymph_cases")
df = mod_data[keeps]

#rename year_start to year_id
df <- setnames(df, "year_start", "year_id")

#create indicator to flag what data are used to fit the model (dataset=0); dataset=1 will be the 
#prevalence draws used for prediction
df$dataset<-0

#pull in age group ids
df2<-merge(age_info,df,by="age_start")

#recode sex to male =1 female =2 
df2$sex_id<-ifelse(df2$sex=="Male",1,2)

#drop unnecessary variables
df2$X<-NULL
df2$age_group_name<-NULL
df2$age_end<-NULL
df2$sex<-NULL

###----------calculate mean prevalence across the draws -------- #
#get draws 
ages<-unique(age_info$age_group_id)

lf_prev<-get_model_results(gbd_team='epi', gbd_id=ADDRESS, release_id=16, status="best", age_group_id=ages, sex_id=c(1,2), location_id=lf_endems)

# convert to data table
prev2<-as.data.table(lf_prev)

# create ID for each row
# name as mean_adjusted to append to other data
prev2$mean_adjusted<-prev2$mean

# merge age_mid together
prev2_b<-merge(prev2,age_info,by="age_group_id")

lf_df3<-dplyr::select(prev2_b, age_group_id, age_mid, year_id, sex_id, location_id, mean_adjusted, age_start)

# create empty variables to enable prediction from model
lf_df3$lymph_mean<-NA
lf_df3$lymph_ss<-NA
lf_df3$lymph_cases<-NA

# dataset is an indicator variable that classifies observations as belonging to the final
# set we want to output; 0 - are records from the observations used to fit the model
lf_df3$dataset<-1

# append model inputs
### append all together to generate model dataset with rows for prediction
mod_1bL<-rbind(lf_df3,df2)

##############RUN MODEL ##############3
summary(mod_1bL$lymph_mean)
summary(mod_1bL$mean_adjusted)

plot(mod_1bL$mean_adjusted,mod_1bL$lymph_mean)
mod_1bL<-mod_1bL[!(mod_1bL$mean_adjusted<.05 & mod_1bL$lymph_mean>.05 & mod_1bL$dataset==0),]

mod_1bL<-mod_1bL[!(mod_1bL$mean_adjusted>.1 & mod_1bL$lymph_mean>.06 & mod_1bL$dataset==0),]

#Run model in INLA- test without regional_random effects
knots=seq(.05,.4)
formula <- lymph_cases~sex_id+mean_adjusted #+f(super_region_id, model="iid")
result2 <- inla(formula, family="xbinomial", control.compute=list(config = TRUE), control.predictor=list(link=1), Ntrials=lymph_ss, data=mod_1bL, verbose=F)

#result2 stores the results of the inla model
summary(result2)

#here the range = # of rows is the input dataset - this allows INLA to generate predictions
range<-nrow(mod_1bL)
results_simple<-result2$summary.fitted.values[1:range, ]

#append to input data
combined<-cbind(mod_1bL,results_simple)

#plot results of the model 
#variable - mean is the result of the model 
plot(combined$lymph_mean, combined$mean)
plot(combined$age_mid, combined$mean)
plot(combined$mean_adjusted, combined$mean)
plot(combined$year_id, combined$mean)

library(ggplot2)
#plot the mean LF prevalence on the x-axis and mean lymph prediction on the y-axis
ggplot(combined, aes(x=mean_adjusted, y=mean, color=age_start)) +
  geom_point()

high_30<-combined[combined$mean>.3,]
table(high_30$location_id)

#-------------------generate 1000 draws from inla
results <- inla.posterior.sample(1000, result2,use.improved.mean=FALSE)
predictions <- lapply(results, function(p) p$latent[1:range])
predictions <- do.call(cbind, predictions)

predictions2<-as.data.frame(predictions)
#bind the first columns 
draws3<-predictions2 %>%
  dplyr::rename_all(
    ~stringr::str_replace_all(.,"V","draw_")
  )
# change draw_1000 to draw_0
names(draws3)[names(draws3) == "draw_1000"] <- "draw_0"

ilog <- function(x) ((exp(x)/(1+exp(x))))
draws4<-draws3 %>% mutate_all(ilog)

# keep first six columns of dataset 
columns <- dplyr::select(mod_1bL, age_group_id, age_mid, year_id, sex_id, location_id, dataset, mean_adjusted)

draws<-cbind(columns,draws4)

# now just need to keep predictions from model, so subset out where dataset=1
draws<-draws[draws$dataset==1,]

ggplot(draws, aes(x=mean_adjusted, y=draw_0, color=age_group_id)) +
  geom_point()

#draws$mean_adjusted<-NULL
draws$dataset<-NULL

#reduce draws by 67% of cases as early lymphedema (stage 1-2)
for(j in grep('draw', names(draws))){
 set(draws, i= which(draws[[j]]<1), j= j, value=draws[[j]]*.33)
}

#output draws to folder by location
draw.cols <- paste0("draw_", 0:999)

draws2<-setDT(draws)
draws2[, id := .I]

#set all draws for age 5 and under to 0
draws2[age_group_id<4, (draw.cols) := 0, by=id]
draws2[age_group_id==388, (draw.cols) := 0, by=id]
draws2[age_group_id==389, (draw.cols) := 0, by=id]
draws2[age_group_id==238, (draw.cols) := 0, by=id]

#hard-code elimination values for togo, egypt, cambodia, thailand, sri lanka - achieved before 2017
draws2[location_id==141 & year_id>=2010, (draw.cols):=0, by=id] # Egypt
draws2[location_id==10 & year_id>=2010, (draw.cols):=0, by=id] # Cambodia
draws2[location_id==18 & year_id>=2010, (draw.cols):=0, by=id] # Thailand
draws2[location_id==218 & year_id>=2015, (draw.cols):=0, by=id] # Togo
draws2[location_id==17 & year_id >= 2010, (draw.cols):=0, by=id] # Sri Lanka

# set generated draws to max mean of original prevalence
lf_prev <- get_model_results(gbd_team = 'epi', gbd_id = ADDRESS, release_id = release_id, status="best", age_group_id = ages, sex_id = c(1,2), location_id = lf_endems)
lf_prev <- dplyr::select(lf_prev, age_group_id, year_id, sex_id, location_id, mean, lower, upper)
draws2 <- merge(draws2, lf_prev, by=c("location_id", "year_id", "sex_id", "age_group_id"))
draws2[, m_upper := apply(.SD, 1, quantile, 0.975), .SDcols = paste0("draw_", c(0:999))]
draws2[, m_mean := apply(.SD, 1, mean), .SDcols = paste0("draw_", c(0:999))]
draws2[, paste0("draw_", 0:999) := lapply(0:999, function(x) ifelse( (m_mean > mean), rnorm( .N, mean, ((upper-mean) / 1.96)**2 ), get(paste0('draw_',x)) ) )]

#write out results so that they can be used in ADL estimates
write.csv(draws2,"FILEPATH", row.names = FALSE)

fix <- draws2[location_id == 4766,]
write.csv(fix, "FILEPATH", row.names = FALSE)

# 4. Generate zero-draw files ---------------------------------------------------------------------
###### Output zero prevalence draws for non-endemic locations ######
# get list of non-endemic locations
non_endemic <- gbd_loc_ids[! gbd_loc_ids %in% lf_endems]
### additional zero-draws needed for locations with all zeros in GBD 2021
gbd_2021_zeros <- c(5, 6, 28, 68, 118, 119, 124, 126, 175, 183, 185, 
                    186, 203, 206, 212, 491, 493, 494, 496, 497, 498, 499, 
                    502, 503, 504, 506, 507, 513, 514, 516, 521, 4751, 
                    4754, 4763, 25342, 60908, 94364)
batanes <- c(53547)

### combine location_id lists for 
zero_locs <- c(non_endemic,gbd_2021_zeros,batanes)
### ensure no location_ids are duplicated
zero_locs <- unique(zero_locs)

# establish expected number of rows for each output draws file for validation step
exp_rows <- length(gbd_years) * length(gbd_ages) * length(gbd_sex_id) * 1

# generate, validate, and write-out all zero-draws files
for (lc in 1:length(zero_locs)) {
  loc <- zero_locs[lc]
  
  loc_zeros <- gen_zero_draws(modelable_entity_id = ADDRESS, location_id = loc, measure_id = c(5), metric_id = c(3), year_id = gbd_years, release_id = release_id, team = 'epi')
  
  ## verify that each location_id has 500 observations (10 years * 25 age groups * 2 sexes * 1 location)
  if (nrow(loc_zeros) != exp_rows) {
    stop(print(paste0("location_id ", loc,"'s .csv file has ", nrow(loc_zeros)," rows but should have ", exp_rows," rows.")))
  }
  
  write.csv(loc_zeros, "FILEPATH", row.names = FALSE)
  
  print(paste0("Finished writing-out draw file for location ", lc," of ", length(zero_locs)))
}

# 5. Format data, export draw files, and save lymphadeama prevalence estimates ---------------------------------------------------------------------
# adjust to match epi database requirements 
draws2 <- subset(draws2, select = -c(age_mid,mean_adjusted,id,mean,lower,upper,m_upper,m_mean))
draws2$modelable_entity_id <- ADDRESS
draws2$measure_id <- 5
draws2$metric_id <- 3

# write-out draw files by location_id
for(n in 1:length(lf_endems)){
  i <- lf_endems[[n]]
  upload_file <- draws2[draws2$location_id==i,]
  s1 <- setDT(upload_file)
  write.csv(s1,"FILEPATH", row.names = FALSE)
  print(paste0("Finished writing-out draw file for location ", n," of ", length(lf_endems)))
}

###### manually save results ######
# will return a new model version id
save_results_epi(input_dir = lymph_dir, 
                 input_file_pattern = "{location_id}.csv",
                 modelable_entity_id = ADDRESS,
                 description = "fixed lfmda cov measure and projected years",
                 measure_id = 5,
                 release_id = release_id,
                 mark_best = TRUE,
                 crosswalk_version_id = ADDRESS, 
                 bundle_id = ADDRESS
)
