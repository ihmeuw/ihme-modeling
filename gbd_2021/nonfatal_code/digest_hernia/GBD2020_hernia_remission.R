#################################################################################
#################################################################################
################# TO ESTIMATE REMISSION FOR Hernia
#################################################################################
#################################################################################
rm(list=ls())


## Load functions, objects, and packages
home_dir <-  FILEPATH
pacman::p_load(data.table, ggplot2, dplyr, stringr, DBI, openxlsx, gtools, plyr, readxl)
library(splines)
functions_dir <-FILEPATH
functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids", "get_covariate_estimates", "get_bundle_data", "upload_bundle_data", "get_bundle_version",
            "save_bundle_version", "get_crosswalk_version", "save_crosswalk_version", "save_bulk_outlier")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))
age_predict <- c(0,10,20,30,40,50,60,70,80,90,100) #ages you want to predict remission 

date <- gsub("-", "_", Sys.Date())
date <- Sys.Date()

plot_output_path <- FILEPATH

######################################################################################################## 
## Step 1: AGGREGATE PROC/ALL DATA IN PREPARATION FOR REGRESSION
######################################################################################################## 
dt_proc <- as.data.table(read.csv(FILEPATH))
dt_proc <- subset(dt_proc, sex_id!=0)

setnames(dt_proc, c("year", "num_enrolid_with_proc", "num_enrolid_with_dx"), c("year_id", "cases", "sample_size"))
dt_proc$cf <- NULL

merge_cv <- c( "sex_id", "age_start", "age_end", "location_id", "location_name")
dt_proc[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by = merge_cv]
dt_proc[, c("type")] <- NULL
dt_proc <- unique(dt_proc, by = merge_cv)

dt_proc[, mean := cases/sample_size]
dt_proc[(sex_id ==1), sex := "Male"]
dt_proc[(sex_id ==2), sex := "Female"]

dt_proc$measure <- "remission"
dt_proc$year_id <- 2013

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  dt[is.na(standard_error) & measure == "remission" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "remission" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  
  return(dt)
}
dt_proc$standard_error <- NA
dt_proc$standard_error <- as.numeric(dt_proc$standard_error)
dt_proc <- get_se(dt_proc)


######################################################################################################## 
## Step 2: SET UP COVARIATES (HAQi, SEX, AGE)
######################################################################################################## 
# HAQi
haq <- as.data.table(get_covariate_estimates( covariate_id=1099,
                                              gbd_round_id=7,
                                              decomp_step='iterative'))

haq2 <- haq[, c("location_id", "year_id", "mean_value", "lower_value", "upper_value")]
setnames(haq2, "mean_value", "haqi_mean")
setnames(haq2, "lower_value", "haqi_lower")
setnames(haq2, "upper_value", "haqi_upper")

haq2[, `:=` (haqi_mean = mean(haqi_mean) ), by = c("location_id", "year_id")]

dt_proc_haqi <- merge(dt_proc, haq2, by = c("location_id", "year_id"))

# SEX
dt_proc_haqi$sex_binary[dt_proc_haqi$sex_id==2] <- 1
dt_proc_haqi$sex_binary[dt_proc_haqi$sex_id==1] <- 0

# Subset sex-spsecific dataset
dt_proc_haqi_male <- subset(dt_proc_haqi, sex_binary==0)
dt_proc_haqi_female <- subset(dt_proc_haqi, sex_binary==1)

#add dummy variables              
dt_proc_haqi_orig <- copy(dt_proc_haqi)
dummy_dt <- as.data.table(read.xlsx(FILEPATH))
dt_proc_haqi <- rbind.fill(dt_proc_haqi, dummy_dt)


######################################################################################################## 
## Step 3: PLOTS
######################################################################################################## 
dt_proc_haqi$location_name <- as.character(dt_proc_haqi$location_name)
dt_proc_haqi <- as.data.table(dt_proc_haqi)

# diagnostic create plots 
pdf(paste0(plot_output_path, "Total_hernia_raw_proAall", date, ".pdf"),  width=20, height=8) # this will be the name of the file
p <- 1
locs <-(unique(dt_proc_haqi[, location_id])) 
years <- (unique(dt_proc_haqi[, year_id])) 


for (loc in locs) {
  message(p)
  data <- copy(dt_proc_haqi[location_id == loc, ])
  locs_name <- data[,location_name]
  
  for (year in years) {
    p <- p+1
    plotting_data <- copy(data[year_id == year, ])
    
    data_plot <- ggplot() +
      geom_point(data, mapping = aes(x = age_start, y = mean)) +
      facet_wrap(~sex) +
      theme_bw() + 
      ggtitle(paste0("Hernia, ", locs_name, " in ", year)) +
      xlab("Age start") + ylab("remission")  + ylim(0,1)
    print(data_plot)
  }
}
dev.off()



######################################################################################################## 
## Step 4: exploratory analysis to see if HAQi and remission have a positive association
######################################################################################################## 
reg_a=lm(mean~ haqi_mean +sex_binary, data=dt_proc_haqi_orig)
summary(reg_a)
reg_b=lm(mean~ 0 + haqi_mean +sex_binary, data=dt_proc_haqi_orig)
summary(reg_b)

anova(reg_a, reg_b)
#with and without 0 intercept, haqi coeff is positive; including zero intercept improves the model fit 
#(F and R2 (adjusted and unadjusted), but not the standard error by 0.001)

ggplot() +
  geom_point(dt_proc_haqi, mapping = aes(x = haqi_mean, y = mean)) +
  stat_smooth(aes(dt_proc_haqi$haqi_mean, dt_proc_haqi$mean), method='lm', formula = y~0+x) +xlim(0,90) 



######################################################################################################## 
## Step 4: ADD (0,0) for when HAQi is zero
######################################################################################################## 
#some visualizations - exploratory
scatter.smooth(x=dt_proc_haqi$haqi_mean, y=dt_proc_haqi$mean)
ggplot() +
  geom_point(dt_proc_haqi_orig, mapping = aes(x = haqi_mean, y = mean)) +
  facet_wrap(~sex)

ggplot() +
  geom_point(dt_proc_haqi_female, mapping = aes(x = age_start, y = mean)) +
  stat_smooth(aes(dt_proc_haqi_female$age_start, dt_proc_haqi_female$mean), method='lm', formula = y~poly(x,6)) +
ggtitle("Female") 

ggplot() +
  geom_point(dt_proc_haqi_male, mapping = aes(x = age_start, y = mean)) +
  stat_smooth(aes(dt_proc_haqi_male$age_start, dt_proc_haqi_male$mean), method='lm', formula = y~poly(x,6)) +
  ggtitle("Male") 


ggplot() +
  geom_point(dt_proc_haqi_female, mapping = aes(x = age_start, y = mean)) +
  stat_smooth(aes(dt_proc_haqi_female$age_start, dt_proc_haqi_female$mean), method='lm', formula = y~bs(x, knots=c(5, 25, 35, 55,  75))) +
  ggtitle("Female") 

ggplot() +
  geom_point(dt_proc_haqi_male, mapping = aes(x = age_start, y = mean)) +
  stat_smooth(aes(dt_proc_haqi_male$age_start, dt_proc_haqi_male$mean), method='lm', formula = y~bs(x, knots=c(5, 25, 35, 55, 75))) +
  ggtitle("Male") 



## test different models 
reg1=glm(mean~ haqi_mean+ sex_binary + age_start, data=dt_proc_haqi, binomial(link = "logit") )
reg2=glm(mean~ haqi_mean+ sex_binary + bs(age_start, knots=c(5, 25, 35, 55,  75)), data=dt_proc_haqi,  binomial(link = "logit") )
reg3=glm(mean~ haqi_mean+ sex_binary + poly(age_start, 6, raw=TRUE), data=dt_proc_haqi,  binomial(link = "logit")) #bs looks better than poly

reg4=glm(mean~haqi_mean+ sex_binary + bs(age_start, knots=c(5, 25, 35, 55,  75)) +sex_binary*bs(age_start, knots=c(5, 25, 35, 55,  75)), data=dt_proc_haqi, binomial(link = "logit") ) # best!!!!
reg5=glm(mean~ haqi_mean+ sex_binary +poly(age_start, 6, raw=TRUE)+ sex_binary*poly(age_start, 6, raw=TRUE), data=dt_proc_haqi ,binomial(link = "logit")) #bs looks better than poly

reg4_nodummy=glm(mean~haqi_mean+ sex_binary + bs(age_start, knots=c(5, 25, 35, 55,  75))+sex_binary*bs(age_start, knots=c(5, 25, 35, 55,  75)), data=dt_proc_haqi_orig, binomial(link = "logit") ) 


#reg5 FINAL

summary(reg1)
summary(reg2)
summary(reg3) 
summary(reg4) 
summary(reg5) 
summary(reg6) 
summary(reg4_nodummy) 



######################################################################################################## 
## Step 5: Create metadata
######################################################################################################## 

## Get meta data
loc_dt <- as.data.table(get_location_metadata(35, gbd_round_id=7))
loc_dt <- subset(loc_dt, level == 3 | level == 4)
loc_dt[, c("location_set_id", "level", "is_estimate", "location_set_version_id", "parent_id", "path_to_top_parent", "most_detailed", "sort_order", "lancet_label", "location_ascii_name", "location_name_short", "location_type_id", "location_type", "ihme_loc_id", "local_id", "developed", "start_date", "end_date", "date_inserted", "last_updated", "last_updated_by", "last_updated_action" ) := NULL]
age_dt <- read.csv(paste0(FILEPATH, "/age_table.csv"))
sex_dt <- read.csv(paste0(FILEPATH, "/sex_names.csv"))
sex_dt <- subset(sex_dt, sex_id==c(1, 2))
sex_dt$sex_binary[sex_dt$sex_id==2] <- 1
sex_dt$sex_binary[sex_dt$sex_id==1] <- 0
year_dt <- read.csv(paste0(FILEPATH, "/year.csv"))


## Create master data frame for predictions
meta_dt <- tidyr::crossing(loc_dt, age_dt)
meta_dt <- tidyr::crossing(meta_dt, sex_dt)
meta_dt <- tidyr::crossing(meta_dt, year_dt)

##create sex binary
meta_dt <- as.data.table(meta_dt)

## Add HAQi
meta_dt <- merge(meta_dt, haq2, by = c("location_id", "year_id"))



######################################################################################################## 
## Step 6: predict out using the best regression model
######################################################################################################## 
predictions <- as.data.table(predict(reg4, se.fit = TRUE, newdata = meta_dt, type="response")) 
meta_dt_pred <- cbind(meta_dt, predictions)
max(meta_dt_pred$fit)
min(meta_dt_pred$fit)

predictions_nodummy <- as.data.table(predict(reg4_nodummy, se.fit = TRUE, newdata = meta_dt, type="response")) 
meta_dt_pred_nodummy <- cbind(meta_dt, predictions_nodummy)
max(predictions_nodummy$fit)
min(predictions_nodummy$fit)



graph_results <- function(raw_data, predicts, title){
  data_dt <- as.data.table(raw_data)
  model_dt <- as.data.table(predicts)
  quant_haq <- round(quantile(model_dt$haqi_mean, probs = seq(0,1, by = 0.50)), digits = 2)
  model_dt <- model_dt[, haqi_mean := round(haqi_mean, digits =2)]
  model_dt <- model_dt[model_dt$haqi_mean %in% quant_haq, ]
  model_dt$haqi_mean <- as.character(model_dt$haqi_mean)
  model_dt$sex <- ifelse(model_dt$sex_binary == 1, "Female", "Male")

  gg_subset<- ggplot() + geom_jitter(data = data_dt, aes(x = age_start, y = (mean)), color = "dark blue", width=0.6, alpha=0.2, size=2)+facet_wrap(~sex) + labs(x = "Age start ", y = "Remission") +
    geom_line(data = model_dt, aes(x = age_start, y = fit, color = as.character(haqi_mean), linetype = sex), size=1.5)  +
    scale_color_manual(name = "", values = c( "green","purple",  "red"), 
                       labels = c("50th Percentile HAQ", "Min HAQ", "Max HAQ")) +ggtitle(title) +
      theme_classic() + theme(text = element_text(size = 15, color = "black")) 
      
    return(gg_subset)
    }  
graph_rem <- graph_results(raw_data = dt_proc_haqi_orig, predicts = meta_dt_pred_nodummy, title = "Predictions overlay on remission input data: reg4 with dummy zero")
graph_rem    
graph_rem1 <- graph_results(raw_data = dt_proc_haqi, predicts = meta_dt_pred, title = "Predictions overlay on remission input data: reg4")
graph_rem1
ggsave(graph_rem, filename = paste0(plot_output_path, "Hernia_reg4_", date,"_no_dummyzero.pdf"), width = 10, height = 7)
ggsave(graph_rem1, filename = paste0(plot_output_path, "Hernia_reg4_", date,"_include_dummyzero.pdf"), width = 10, height = 7)

    

ggplot() + geom_jitter(data = dt_proc_haqi, aes(x = haqi_mean, y = (mean)), color = "dark blue", width=0.6, alpha=0.2, size=2)+facet_wrap(~sex) + labs(x = "HAQi", y = "Remission") +
  geom_smooth(data = meta_dt_pred, aes(x = haqi_mean, y = V1,  linetype = sex), size=1.5)  + xlim(0, 90)


ggplot() + geom_jitter(data = dt_proc_haqi_orig, aes(x = haqi_mean, y = (mean)), color = "dark blue", width=0.6, alpha=0.2, size=2)+facet_wrap(~sex) + labs(x = "HAQi", y = "Remission") +
  geom_smooth(data = meta_dt_pred_nodummy, aes(x = haqi_mean, y = fit,  linetype = sex), size=1.5)  + xlim(0, 90)



######################################################################################################## 
## Step 7: PREP META DATASET FOR EPI UPLOAD
######################################################################################################## 
final_rem <- copy(meta_dt_pred)

## For uploader validation - add in needed columns
final_rem$seq <- NA
final_rem$crosswalk_parent_seq <- NA
final_rem$sex <- ifelse(final_rem$sex_binary == 1, "Male", "Female")
final_rem$year_start <- final_rem$year_id
final_rem$year_end <- final_rem$year_id
final_rem[,c("sex_binary", "year_id", "haqi_mean")] <- NULL
final_rem$nid <- 419449
final_rem$year_issue <- 0
final_rem$age_issue <- 0
final_rem$sex_issue <- 0
final_rem$measure <- "remission"
final_rem$source_type <- "Facility - inpatient"
final_rem$extractor <- USERNAME
final_rem$is_outlier <- 0
final_rem$sample_size <- 1000
setnames(final_rem, c("fit", "se.fit"), c("mean", "standard_error"))
final_rem$cases <- final_rem$sample_size * final_rem$mean
final_rem$measure_adjustment <- 1
final_rem$uncertainty_type <- "Confidence interval"
final_rem$lower <- NA
final_rem$upper <- NA
final_rem$uncertainty_type_value <- NA

final_rem$recall_type <- "Point"
final_rem$urbanicity_type <- "Mixed/both"
final_rem$unit_type <- "Person"
final_rem$representative_name <- "Nationally and subnationally representative"
final_rem$note_modeler <- "These data are modeled from the US data using the number of people with procedures codes among those with ICD code of interest, fake data at (0, 0) for HAQi"
final_rem$unit_value_as_published <- 1
final_rem$step2_location_year <- "This is modeled remisison that uses the NID for one dummy row of remisison data added to the bundle data"

final_rem <- subset(final_rem, location_id!=95)

write.csv(final_rem, paste0(plot_output_path, "Total_hernia_modeled_remission_", date, ".csv"))


######################################################################################################## 
## Step 8: APPEND INPUT DATA
######################################################################################################## 
emr <- as.data.table(read.xlsx(FILEPATH))
dta20 <- subset(emr, measure != "remission")
combined <- rbind.fill(final_rem, dta20)

write.xlsx(combined, FILEPATH,  sheetName = "extraction", col.names=TRUE)

description <- DESCRIPTION
result <- save_crosswalk_version( BUNDLE_VERSION_ID, FILEPATH, description=description)

