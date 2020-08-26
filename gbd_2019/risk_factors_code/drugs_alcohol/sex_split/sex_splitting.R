####################################################################
## Purpose: Sex split alcohol use data
####################################################################

# Clean up and initialize with the packages we need
rm(list = ls())
library(data.table)
library(xlsx)
library(msm)
library(ggplot2)
library(plyr)
library(parallel)
library(RMySQL)
library(stringr)
library(Hmisc)
library(dummies)
library(mortdb, lib = 'FILEPATH')

date <- gsub("-", "_", Sys.Date())
draws <- paste0("draw_", 0:999)
topic <- "alc_use"

#Mr_BRT functions
source('FILEPATH')

# Central functions
functions_dir <- paste0('FILEPATH')
functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids", "get_bundle_data", "save_crosswalk_version", "save_bundle_version", "get_bundle_version")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))

mrbrt_helper_dir <- paste0('FILEPATH')
mrbrt_functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function",
                  "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function",
                  "load_mr_brt_preds_function", "plot_mr_brt_function")
invisible(lapply(mrbrt_functs, function(x) source(paste0(mrbrt_helper_dir, x, ".R"))))


#Getting locations
locs<- get_location_metadata(location_set_id =35, gbd_round_id = 6)
locs<- locs[, c("location_id", "super_region_name", "location_name", "super_region_id", "region_name", "region_id")]


#---------------## INPUT DATA
if (topic == "alc_use"){ 
 dt <- fread('FILEPATH')
  dt <- dt[is.na(group_review) | group_review == 1]
  dt <- dt[, lower := mean - 1.96*standard_error]
  dt <- dt[, upper := mean + 1.96*standard_error]
  dt <- dt[lower <= 0, lower := 0]
  dt <- dt[upper >= 1, upper := 1]
  dt[,age_middle:= (age_start + age_end) / 2]

  table(dt$measure)
  table(is.na(dt$measure))

  dt <- dt[is.na(measure), measure := "proportion"]
  dt <- dt[measure == "prevalence", measure := "proportion"]
  dt <- dt[is.na(sample_size) | sample_size > 10]
  bundle <- copy(dt)
}


if (topic == "gday"){
  bundle <- get_bundle_version(9407)
  bundle <- bundle[is.na(group_review) | group_review == 1]
}

#---------------## CHECK DATA FOR SEX SPLIT

#Checking if we need to sex split (any sex = Both)
message(paste0("You", ifelse(nrow(bundle[sex=="Both",])>0,"", "don't"), " need to sex-split."))

# Give a heads up about how many sex-specific data points you have to XW.
num_both_sex <- nrow(bundle[sex=="Both",]) 
message(paste0("Starting to sex-split. You have ", num_both_sex, " both-sex data points to be split."))


#---------------## FIND SEX MATCHES
dem_sex_dt <- copy(bundle)

if (topic == "alc_use"){
  cv_drop<-names(bundle)[names(bundle)%like%"cv" & (names(bundle)!="cv_recall_1m" & names(bundle)!="cv_recall_1week")]
  dem_sex_dt <- dem_sex_dt[measure %in% c("proportion")] #n=70774
}


dem_sex_dt <- get_cases_sample_size(dem_sex_dt)
dem_sex_dt <- get_se(dem_sex_dt)

table(is.na(dem_sex_dt$standard_error)) #346 still have it...
dem_sex_dt <- dem_sex_dt[!is.na(standard_error)]

dem_sex_dt <- calculate_cases_fromse(dem_sex_dt)



if (topic == "alc_use"){
  dem_sex_matches <- find_sex_match(dem_sex_dt, measure_vars = c("proportion"))
}

dem_sex_matches<- merge(locs, dem_sex_matches, by= "location_id")


num_matches <- nrow(dem_sex_matches) 
message(paste0("Preparing for MR BRT. You have ", num_matches, " matches."))

## Pull out both-sex data and sex-specific data. both_sex is used for predicting and sex_specific will be the data we use.
df<- copy(bundle)
both_sex <- df[sex=="Both",] 
both_sex[, id := 1:nrow(both_sex)]

sex_specific <- df[sex!="Both",] 

n <- names(sex_specific)


#---------------## MR BRT
#Calculate ratios
message("calculating ratios")

mrbrt_sex_dt <- calc_sex_ratios(dem_sex_matches)
mrbrt_sex_dt <- mrbrt_sex_dt[, id := paste0(nid, " (", location_name, ": ",year_start,"-",year_end,")")]


if (F){
######## Graphs #############
# #Explore matched ratios (all data) by year
 ggplot(data = mrbrt_sex_dt, aes(x= year_start, y = ratio)) + geom_hline(yintercept = 1) + geom_point(aes(size = 1/ratio_se, color = super_region_name), alpha = .4) +
   labs(x = "Year Start", y = "Female:Male Sex Ratio", title = "AD Sex Split") + theme_bw() + theme(text=element_text(size = 12))

#Explore ratios by age and region
ggplot(data = mrbrt_sex_dt, aes(x= midage, y = ratio)) + geom_hline(yintercept = 1) + geom_point(aes(size = 1/ratio_se, color = super_region_name), alpha = .4) +
  labs(x = "Mid Age", y = "Female:Male Sex Ratio", title = "AD Sex Split") + theme_bw() + theme(text=element_text(size = 12))

plyr::count(mrbrt_sex_dt$super_region_name) #Not enough data for each of the super regions. Will need to combine them somehow.
plyr::count(mrbrt_sex_dt$region_name) #Not enough data for each of the super regions. Will need to combine them somehow.

#Explore ratios by age, facet by super region (data that will be fed into the model)
ggplot(data = mrbrt_sex_dt, aes(x= midage, y = ratio)) + geom_hline(yintercept = 1) + geom_point(aes(size = 1/ratio_se), alpha = .4) + facet_wrap(~ super_region_name) +
  labs(x = "Mid Age", y = "Female:Male Sex Ratio", title = "AD Sex Split") + theme_bw() + theme(text=element_text(size = 12))

}

##############################

#Since an age pattern seems to exist, restricT age range to be <= 25 years consistent with age-splitting threshold - Eliminates good chunck of noise and yelds for age pattern

restrict_sex_dt <- copy(mrbrt_sex_dt)
restrict_sex_dt <-restrict_sex_dt[, age_spam := age_end-age_start]
restrict_sex_dt <-restrict_sex_dt[age_spam <=25,]

num_lost <- num_matches - nrow(restrict_sex_dt)
message(paste0("WARNING: You have lost ", num_lost, " matches after restricting the age range."))

plyr::count(restrict_sex_dt$region_name)

#Explore ratios by age and region
ggplot(data = restrict_sex_dt, aes(x= midage, y = ratio)) + geom_hline(yintercept = 1) + geom_point(aes(size = 1/ratio_se, color = super_region_name), alpha = .4) +
  labs(x = "Mid Age", y = "Female:Male Sex Ratio", title = "AD Sex Split") + theme_bw() + theme(text=element_text(size = 12))

#Explore restricted ratios by age, facet by super region (data that will be fed into the model)
ggplot(data = restrict_sex_dt, aes(x= midage, y = ratio)) + geom_hline(yintercept = 1) + geom_point(aes(size = 1/ratio_se), alpha = .4) + facet_wrap(~ super_region_name) +
  labs(x = "Mid Age", y = "Female:Male Sex Ratio", title = "AD Sex Split") + theme_bw() + theme(text=element_text(size = 12))



#-----------RESTRICTED AGES AND YOUTH COVARIATE ONLY

mrbrt_dir  <-'FILEPATH'
model_name <- paste0(topic,"_sexsplit_", date, "_fixed_id")

mrbrt_sex_dt<-restrict_sex_dt[, youth:=(ifelse(midage<50, 1, 0))]
plyr::count(mrbrt_sex_dt$youth) 

table(mrbrt_sex_dt$region_name)

for (k in unique(mrbrt_sex_dt$region_name)){

  mrbrt_sex_dt$region_name <- gsub(" ", "_",mrbrt_sex_dt$region_name)
  k <- gsub(" ", "_",k)

  mrbrt_sex_dt <- mrbrt_sex_dt[region_name == k,paste0("cv_",k):= 1]
  mrbrt_sex_dt <- mrbrt_sex_dt[region_name != k,paste0("cv_",k):= 0]

  }

summary(mrbrt_sex_dt)

sex_model <- run_mr_brt(
  output_dir = mrbrt_dir,
  model_label = paste0(model_name),
  data = mrbrt_sex_dt[(age_end-age_start <= 25) & ratio_se > 1e-05],
  mean_var = "log_ratio",
  se_var = "log_se",
  list(cov_info("youth", "X"),
       cov_info("youth", "Z"),
       cov_info("cv_East_Asia", "X"),
       cov_info("cv_Southeast_Asia", "X"),
       cov_info("cv_Central_Asia", "X"),
       cov_info("cv_Central_Europe", "X"),
       cov_info("cv_Eastern_Europe", "X"),
       cov_info("cv_High-income_Asia_Pacific", "X"),
       cov_info("cv_Western_Europe", "X"),
       cov_info("cv_Southern_Latin_America", "X"),
       cov_info("cv_High-income_North_America", "X"),
       cov_info("cv_Caribbean", "X"),
       cov_info("cv_Andean_Latin_America", "X"),
       cov_info("cv_Central_Latin_America", "X"),
       cov_info("cv_Tropical_Latin_America", "X"),
       cov_info("cv_North_Africa_and_Middle_East", "X"), 
       cov_info("cv_South_Asia", "X"),
       cov_info("cv_Eastern_Sub-Saharan_Africa", "X"),
       cov_info("cv_Southern_Sub-Saharan_Africa", "X"),
       cov_info("cv_Western_Sub-Saharan_Africa", "X"),
       cov_info("cv_Oceania", "X"),
       cov_info("cv_East_Asia", "Z"),
       cov_info("cv_Southeast_Asia", "Z"),
       cov_info("cv_Central_Asia", "Z"),
       cov_info("cv_Central_Europe", "Z"),
       cov_info("cv_Eastern_Europe", "Z"),
       cov_info("cv_High-income_Asia_Pacific", "Z"),
       cov_info("cv_Western_Europe", "Z"),
       cov_info("cv_Southern_Latin_America", "Z"),
       cov_info("cv_High-income_North_America", "Z"),
       cov_info("cv_Caribbean", "Z"),
       cov_info("cv_Andean_Latin_America", "Z"),
       cov_info("cv_Central_Latin_America", "Z"),
       cov_info("cv_Tropical_Latin_America", "Z"),
       cov_info("cv_North_Africa_and_Middle_East", "Z"),
       cov_info("cv_South_Asia", "Z"),
       cov_info("cv_Eastern_Sub-Saharan_Africa", "Z"),
       cov_info("cv_Southern_Sub-Saharan_Africa", "Z"),
       cov_info("cv_Western_Sub-Saharan_Africa", "Z"),
       cov_info("cv_Oceania", "Z")),
study_id = "id",
method = "trim_maxL",
trim_pct = 0.1,
overwrite_previous = T
)


covs <-  names(mrbrt_sex_dt)[grepl(names(mrbrt_sex_dt), pattern = "cv_")]
if (topic == "alc_use"){
covs <- covs[3:22]
}

tiny <- as.data.frame(mrbrt_sex_dt)
tiny <- tiny[,c("youth", covs)]
tiny$x_intercept <- 1 
tiny$z_intercept <- 1 
tiny <- unique(tiny)

preds<-as.data.table(predict_mr_brt(sex_model, newdata = tiny, z_newdata = tiny, write_draws = F)$model_summaries)
                     

preds <- preds[X_youth == preds$Z_youth,]
preds <- preds[X_cv_East_Asia == preds$Z_cv_East_Asia,]
preds <- preds[X_cv_Southeast_Asia == preds$Z_cv_Southeast_Asia,]
preds <- preds[X_cv_Central_Asia == preds$Z_cv_Central_Asia,]
preds <- preds[X_cv_Eastern_Europe == preds$Z_cv_Eastern_Europe,]
preds <- preds[X_cv_High.income_Asia_Pacific == preds$Z_cv_High.income_Asia_Pacific,]
preds <- preds[X_cv_Central_Europe == preds$Z_cv_Central_Europe,]
preds <- preds[X_cv_Western_Europe == preds$Z_cv_Western_Europe,]
preds <- preds[X_cv_Southern_Latin_America == preds$Z_cv_Southern_Latin_America,]
preds <- preds[X_cv_High.income_North_America == preds$Z_cv_High.income_North_America,]
preds <- preds[X_cv_Caribbean == preds$Z_cv_Caribbean,]
preds <- preds[X_cv_Andean_Latin_America == preds$Z_cv_Andean_Latin_America,]
preds <- preds[X_cv_Central_Latin_America == preds$Z_cv_Central_Latin_America,]
preds <- preds[X_cv_Tropical_Latin_America == preds$Z_cv_Tropical_Latin_America,]
preds <- preds[X_cv_North_Africa_and_Middle_East == preds$Z_cv_North_Africa_and_Middle_East,]
preds <- preds[X_cv_South_Asia == preds$Z_cv_South_Asia,]
preds <- preds[X_cv_Eastern_Sub.Saharan_Africa == preds$Z_cv_Eastern_Sub.Saharan_Africa,]
preds <- preds[X_cv_Southern_Sub.Saharan_Africa == preds$Z_cv_Southern_Sub.Saharan_Africa,]
preds <- preds[X_cv_Western_Sub.Saharan_Africa == preds$Z_cv_Western_Sub.Saharan_Africa,]
preds <- preds[X_cv_Oceania == preds$Z_cv_Oceania,]

cols<-c("Y_mean", "Y_mean_lo", "Y_mean_hi")
preds<-preds[, (cols):=lapply(.SD, exp), .SDcols = cols]


traindata<-as.data.table(sex_model$train_data)

traindata<-traindata[, trimmed:=ifelse(w==0, "Trimmed", "Not Trimmed")]

#Youth x Non-Youth
ggplot(data = preds, aes(x = X_youth, y = Y_mean)) + geom_line() + geom_ribbon(aes(ymin = Y_mean_lo, ymax = Y_mean_hi)) + geom_hline(yintercept = 1) + geom_point(data = traindata, aes(y = ratio, x = youth, size = 1/ratio_se, color = trimmed), alpha = .4)

#Data points feeding the model
ggplot(data = traindata, aes(x= midage, y = 1/ratio)) + geom_hline(yintercept = 1) + geom_point(aes(size = 1/ratio_se, color = trimmed), alpha = .4) + facet_wrap(~ region_name) +
  labs(x = "Mid Age", y = "Female:Male Sex Ratio", title = "AD Sex Split") + theme_bw() + theme(text=element_text(size = 12))

ggplot(data = traindata, aes(x= midage, y = 1/ratio)) + geom_hline(yintercept = 1) + geom_point(aes(size = 1/ratio_se, color = trimmed), alpha = .4) + facet_wrap(~ region_name) +
  labs(x = "Mid Age", y = "Male:Female Sex Ratio", title = "AD Sex Split") + theme_bw() + theme(text=element_text(size = 12))


#Age span and uncertainty of the predicted ratios

preds<-preds[X_youth==1, X_lower:=15]
preds<-preds[X_youth==1, X_upper:=20]
preds<-preds[X_youth==0, X_lower:=20]
preds<-preds[X_youth==0, X_upper:=85]

ggplot(data = traindata[1/ratio < 50], aes(x = midage, y = 1/ratio)) + 
  geom_hline(yintercept = 1) + 
  geom_point(aes(size = 1/ratio_se, color = super_region_name, shape = trimmed), alpha = .4) + 
  geom_pointrange(data = preds, aes(ymin = Y_mean_lo, ymax = Y_mean_hi, y = Y_mean, x = ((X_lower + X_upper)/2))) +
  geom_errorbarh(data = preds, aes(xmin = X_lower, xmax = X_upper, x = ((X_lower + X_upper)/2), y = 1/Y_mean), height = .001) + 
  labs(x = "Age", y = "Female:Male Sex Ratio", title = "AD Sex Split") + 
  theme_bw() + theme(text=element_text(size = 12))+ facet_wrap(~ region_name)

if (testing){
  
  testing_preds <- copy(preds)
  names(testing_preds[,22:42])
  testing_preds[,22:42] <- NULL
  
}



## Pull out covariates to return later.
sex_covariates <- sex_model$model_coefs

sex_covariates <- fread('FILEPATH')

#---------------## GET AND PREPARE POPULATION FOR SEX SPLITTING
# We need the population for each data point. Instead of pooling the population (for ex if you were age 3-10) for all years, we just take the midpoint (3-10 would become 6)
both_sex[, year_mid := (year_start + year_end)/2]
both_sex[, year_floor := floor(year_mid)]
both_sex[, age_mid := (age_start + age_end)/2]
both_sex[, age_floor:= floor(age_mid)]


if (topic == "alc_use"){
  both_sex[, youth := ifelse(age_mid<50, 1, 0)]
}
#---------------## PREDICT
# Predicted ratio with write_draws = TRUE

sex_preds <- predict_mr_brt(sex_model, newdata = tiny, z_newdata = tiny, write_draws = T)

sex_draws <- as.data.table(sex_preds$model_draws)

names(sex_draws) <- gsub("-",".",names(sex_draws))

sex_draws <- sex_draws[X_youth == sex_draws$Z_youth,]
sex_draws <- sex_draws[X_cv_East_Asia == sex_draws$Z_cv_East_Asia,]
sex_draws <- sex_draws[X_cv_Southeast_Asia == sex_draws$Z_cv_Southeast_Asia,]
sex_draws <- sex_draws[X_cv_Central_Asia == sex_draws$Z_cv_Central_Asia,]
sex_draws <- sex_draws[X_cv_Eastern_Europe == sex_draws$Z_cv_Eastern_Europe,]
sex_draws <- sex_draws[X_cv_High.income_Asia_Pacific == sex_draws$Z_cv_High.income_Asia_Pacific,]
sex_draws <- sex_draws[X_cv_Central_Europe == sex_draws$Z_cv_Central_Europe,]
sex_draws <- sex_draws[X_cv_Western_Europe == sex_draws$Z_cv_Western_Europe,]
sex_draws <- sex_draws[X_cv_Southern_Latin_America == sex_draws$Z_cv_Southern_Latin_America,]
sex_draws <- sex_draws[X_cv_High.income_North_America == sex_draws$Z_cv_High.income_North_America,]
sex_draws <- sex_draws[X_cv_Caribbean == sex_draws$Z_cv_Caribbean,]
sex_draws <- sex_draws[X_cv_Andean_Latin_America == sex_draws$Z_cv_Andean_Latin_America,]
sex_draws <- sex_draws[X_cv_Central_Latin_America == sex_draws$Z_cv_Central_Latin_America,]
sex_draws <- sex_draws[X_cv_Tropical_Latin_America == sex_draws$Z_cv_Tropical_Latin_America,]
sex_draws <- sex_draws[X_cv_North_Africa_and_Middle_East == sex_draws$Z_cv_North_Africa_and_Middle_East,]
sex_draws <- sex_draws[X_cv_South_Asia == sex_draws$Z_cv_South_Asia,]
sex_draws <- sex_draws[X_cv_Eastern_Sub.Saharan_Africa == sex_draws$Z_cv_Eastern_Sub.Saharan_Africa,]
sex_draws <- sex_draws[X_cv_Southern_Sub.Saharan_Africa == sex_draws$Z_cv_Southern_Sub.Saharan_Africa,]
sex_draws <- sex_draws[X_cv_Western_Sub.Saharan_Africa == sex_draws$Z_cv_Western_Sub.Saharan_Africa,]
sex_draws <- sex_draws[X_cv_Oceania == sex_draws$Z_cv_Oceania,]




# Using the draws to get the predicted ratio & SE of male/female
sex_draws[, c("X_intercept", "Z_intercept") := NULL]
setnames(sex_draws, "X_youth", "youth")
setnames(sex_draws, old = c("X_cv_East_Asia"                   
                            ,"X_cv_Southeast_Asia"              
                            ,"X_cv_Central_Asia"                
                            ,"X_cv_Central_Europe"              
                            ,"X_cv_Eastern_Europe"              
                            ,"X_cv_High.income_Asia_Pacific"    
                            ,"X_cv_Western_Europe"              
                            ,"X_cv_Southern_Latin_America"      
                            ,"X_cv_High.income_North_America"   
                            ,"X_cv_Caribbean"                   
                            ,"X_cv_Andean_Latin_America"        
                            ,"X_cv_Central_Latin_America"       
                            ,"X_cv_Tropical_Latin_America"      
                            ,"X_cv_North_Africa_and_Middle_East"
                            ,"X_cv_South_Asia"                  
                            ,"X_cv_Eastern_Sub.Saharan_Africa"  
                            ,"X_cv_Southern_Sub.Saharan_Africa" 
                            ,"X_cv_Western_Sub.Saharan_Africa"  
                            ,"X_cv_Oceania")
         , new = c("cv_East_Asia"
                   , "cv_Southeast_Asia"
                   , "cv_Central_Asia"
                   , "cv_Central_Europe"
                   , "cv_Eastern_Europe"
                   , "cv_High-income_Asia_Pacific"
                   , "cv_Western_Europe"
                   , "cv_Southern_Latin_America"
                   , "cv_High-income_North_America"
                   , "cv_Caribbean"
                   , "cv_Andean_Latin_America"
                   , "cv_Central_Latin_America"
                   , "cv_Tropical_Latin_America"
                   , "cv_North_Africa_and_Middle_East"
                   , "cv_South_Asia"
                   , "cv_Eastern_Sub-Saharan_Africa"
                   , "cv_Southern_Sub-Saharan_Africa"
                   , "cv_Western_Sub-Saharan_Africa"
                   , "cv_Oceania"))      

draws <- names(sex_draws)[grepl("draw", names(sex_draws))]
sex_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]

# Pull out population data. We need age- sex- location-specific population.
pop <- get_population(age_group_id = "all", sex_id = "all", decomp_step = "step2", year_id = unique(floor(both_sex$year_mid)),
                      location_id=unique(both_sex$location_id), single_year_age = T)

#Get age group IDs
ids <- get_ids("age_group") ## age group IDs
head(ids)
pop <- merge(pop, ids, by="age_group_id", all.x=T, all.y=F)
head(pop)
table(pop$age_group_name)

pop_backup <- copy(pop)

#Transform age group name to numeric so it can marge to mid age
pop$age_group_name <- as.numeric(pop$age_group_name)
table(pop$age_group_name)
pop$age_group_id <- NULL
pop$run_id <- NULL

## Merge in populations for both-sex and each sex. Turning age bins into the midpoint - because the population ratios, not population itself, is what's important.
both_sex <- merge(both_sex, pop[sex_id==3,], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
setnames(both_sex, "population", "population_both")
both_sex <- merge(both_sex, pop[sex_id==1,], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
setnames(both_sex, "population", "population_male")
both_sex <- merge(both_sex, pop[sex_id==2,], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
setnames(both_sex, "population", "population_female")

## There may be some rows where population is NA - get rid of them
both_sex <- both_sex[!is.na(population_both)] #No missing population
sex_draws[, merge := 1] #Creating merge id
both_sex[, merge := 1] #Creating merge id


#create region covariates 
both_sex$sex_id.x <- NULL
both_sex$sex_id.y <- NULL
both_sex$sex_id.x <- NULL
both_sex$sex_id.y <- NULL

both_sex$sex_id <- 3

both_sex <- merge(both_sex, locs, by=c("location_name", "location_id"), all.x = T)



for (k in unique(mrbrt_sex_dt$region_name)){
  
  both_sex$region_name <- gsub(" ", "_",both_sex$region_name)
  k <- gsub(" ", "_",k)
  k <- gsub("-", ".",k)

  both_sex <- both_sex[region_name == k,paste0("cv_",k):= 1]
  both_sex <- both_sex[region_name != k,paste0("cv_",k):= 0]
  
}


setnames(both_sex, old = c("cv_High.income_Asia_Pacific",
                           "cv_High.income_North_America",
                           "cv_Eastern_Sub.Saharan_Africa",
                           "cv_Southern_Sub.Saharan_Africa",
                           "cv_Western_Sub.Saharan_Africa"), 
                           new = c("cv_High-income_Asia_Pacific",
                                   "cv_High-income_North_America",
                                   "cv_Eastern_Sub-Saharan_Africa",
                                   "cv_Southern_Sub-Saharan_Africa",
                                   "cv_Western_Sub-Saharan_Africa"))

#---------------## SEX SPLITTING
# Calculate means that will be reported in the note_modeler column
ratio_mean <- round(sex_draws[, rowMeans(.SD), .SDcols = draws], 2)
ratio_se <- round(sex_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)

# Apply predictions of the ratio

both_sex <- merge(both_sex, sex_draws, by= c("merge", "youth", "cv_East_Asia",
                                         "cv_Southeast_Asia",
                                         "cv_Central_Asia",
                                         "cv_Central_Europe",
                                         "cv_Eastern_Europe",
                                         "cv_High-income_Asia_Pacific",
                                         "cv_Western_Europe",
                                         "cv_Southern_Latin_America",
                                         "cv_High-income_North_America",
                                         "cv_Caribbean",
                                         "cv_Andean_Latin_America",
                                         "cv_Central_Latin_America" ,      
                                         "cv_Tropical_Latin_America",
                                         "cv_North_Africa_and_Middle_East",
                                         "cv_South_Asia",
                                         "cv_Eastern_Sub-Saharan_Africa",
                                         "cv_Southern_Sub-Saharan_Africa" ,
                                         "cv_Western_Sub-Saharan_Africa",
                                         "cv_Oceania"), allow.cartesian = T)



# below is the algebra that makes the adjustments 
both_sex[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (population_both/(population_male + (get(paste0("draw_", x)) * population_female))))]
both_sex[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
both_sex[, m_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
both_sex[, f_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
both_sex[, m_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
both_sex[, f_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]

# get rid of draw columns
both_sex[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]


z <- qnorm(0.975)
both_sex[mean == 0, sample_sizem := sample_size * population_male/population_both]
both_sex[mean == 0 & (measure == "prevalence" | measure == "proportion"), m_standard_error := sqrt(mean*(1-mean)/sample_sizem + z^2/(4*sample_sizem^2))]
both_sex[mean == 0 & measure == "incidence", m_standard_error := ((5-mean*sample_sizem)/sample_sizem+mean*sample_sizem*sqrt(5/sample_sizem^2))/5]
both_sex[mean == 0, sample_sizef := sample_size * population_female/population_both]
both_sex[mean == 0 &(measure == "prevalence" | measure == "proportion"), f_standard_error := sqrt(mean*(1-mean)/sample_sizef + z^2/(4*sample_sizef^2))]
both_sex[mean == 0 & measure == "incidence", f_standard_error := ((5-mean*sample_sizef)/sample_sizef+mean*sample_sizef*sqrt(5/sample_sizef^2))/5]
both_sex[, c("sample_sizem", "sample_sizef") := NULL]


## Make male- and female-specific dts
male_dt <- copy(both_sex)
male_dt[, `:=` (mean = m_mean, standard_error = m_standard_error, upper = "", lower = "",
                cases = "", sample_size = "", uncertainty_type_value = "", sex = "Male",
                note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                      ratio_se, ")"))]
male_dt <- dplyr::select(male_dt, n)
female_dt <- copy(both_sex)
female_dt[, `:=` (mean = f_mean, standard_error = f_standard_error, upper = "", lower = "",
                  cases = "", sample_size = "", uncertainty_type_value = "", sex = "Female",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                        ratio_se, ")"))]

female_dt <- dplyr::select(female_dt, n) ## original names of data

total_dt <- rbindlist(list(sex_specific, female_dt, male_dt), fill = T)

message("Done with sex-splitting.")


#---------------## PLOT SEX SPLIT DATA
## Plot sex split data
graph_dt <- copy(both_sex[measure == "proportion", .(age_start, age_end, mean, m_mean, m_standard_error, f_mean, f_standard_error, nid, location_id)])


graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean", "nid", "location_id"), measure.vars = c("m_mean", "f_mean"))
graph_dt_means[variable == "f_mean", variable := "Female"][variable == "m_mean", variable := "Male"]
graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean", "nid", "location_id"), measure.vars = c("m_standard_error", "f_standard_error"))
graph_dt_error[variable == "f_standard_error", variable := "Female"][variable == "m_standard_error", variable := "Male"]
setnames(graph_dt_error, "value", "error")
graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable", "nid", "location_id"))
graph_dt[, N := (mean*(1-mean)/error^2)]
wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
graph_dt[, midage := (age_end + age_start)/2]
ages <- c(10, 20, 30, 40, 50, 60, 70, 80, 90)
graph_dt[, age_group := cut2(midage, ages)]

gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) +
  geom_point() +
  geom_errorbar(aes(ymin = lower, ymax = upper)) +
  #facet_wrap(~age_group) +
  labs(x = "Both Sex Mean", y = " Sex Split Means") +
  geom_abline(slope = 1, intercept = 0) +
  ggtitle("Sex Split Means Compared to Both Sex Mean") +
  scale_color_manual(name = "Sex", values = c("Male" = "midnightblue", "Female" = "purple")) +
  theme_classic()
gg_sex



#---------------## EXPORT FINAL SEX SPLIT DATA

write.csv(total_dt, 'FILEPATH', row.names = F)

