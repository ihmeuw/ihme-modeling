################################################################################
## Description: Run the first stage of the prediction model for adult mortality
##              and calculate inputs to GPR
################################################################################

rm(list=ls())
username <- Sys.getenv("USER")

# Import installed libraries

library(pacman)
pacman::p_load(foreign, zoo, nlme, plyr, data.table, readr, devtools, methods, argparse, lme4)

# Get arguments
if (!interactive()) {
  parser <- ArgumentParser()
  parser$add_argument('--version_id', type="integer", required=TRUE,
                      help='The version_id for this run of 5q0')
  parser$add_argument('--gbd_round_id', type="integer", required=TRUE,
                      help='The gbd_round_id for this run of 5q0')
  parser$add_argument('--start_year', type="integer", required=TRUE,
                      help='The starting year for this run of 5q0')
  parser$add_argument('--end_year', type="integer", required=TRUE,
                      help='The ending year for this run of 5q0')
  parser$add_argument('--st_loess', type="integer", required=TRUE,
                      help='Whether to use the st_loess')
  parser$add_argument('--code_dir', type="character", required=TRUE,
                      help='Directory where child-mortality code is cloned')
  args <- parser$parse_args()
  list2env(args, .GlobalEnv)
} else {

  version_id <-
  gbd_round_id <-
  start_year <-
  end_year <-
  st_loess <-
  code_dir <- "FILEPATH"
}

# get release_id
yml_dir <- gsub("FILEPATH")
yml <- readLines(paste0("FILEPATH"))
yml <- yml[grepl("release_id", yml)]
release_id <- as.numeric(gsub("\\D", "", yml))

# Set core directories
output_dir <- paste0("FILEPATH")

# Load the GBD specific libraries
source(paste0("FILEPATH"))
source(paste0("FILEPATH"))
source(paste0("FILEPATH"))

# read in standard locations
model_target_locations <- fread(paste0("FILEPATH"))
input_location_ids <- model_target_locations[primary == T, location_id]

# Get data
dc = DataContainer$new(release_id = release_id,
                       start_year = start_year,
                       end_year = end_year,
                       output_dir = output_dir,
                       version_id = version_id)
data <- dc$get('model_input')

# Merge on regions
location_data <- dc$get('location')
data <- merge(data, location_data[, c('location_id', 'region_name')],
              by = 'location_id')

# Make sure that source.type is NA for places where there is no data
data[data == 0, source.type := NA]

#############################
# Choose reference categories
#############################

data$reference <- 0
data = refcats.setRefs(input = data)

# assert statement to make sure we have a reference for every location
test <- data[data$data==1,]
test <- as.data.table(test)
test[, nrefs := sum(reference), by = "ihme_loc_id"]
test <- unique(test, by = "ihme_loc_id")
if(nrow(test[nrefs == 0]) > 0){
  print(head(test[nrefs == 0], n = 20))
  stop("Missing reference for these locations")
}

# Use mix of VR for national
data <- as.data.table(data)
data[ihme_loc_id == "BRA" & source == "bra_sim" & year < 2010,
     reference := 0]
data[ihme_loc_id %in% c("MEX_4643", "MEX_4646", "MEX_4647", "MEX_4649",
                        "MEX_4650", "MEX_4652", "MEX_4655", "MEX_4656",
                        "MEX_4659", "MEX_4660", "MEX_4661", "MEX_4663",
                        "MEX_4664", "MEX_4665", "MEX_4666", "MEX_4667",
                        "MEX_4668", "MEX_4669", "MEX_4670", "MEX_4671",
                        "MEX_4672", "MEX_4673", "MEX_4674") &
       year < 2013 & grepl("vr", source, ignore.case = TRUE), reference := 0]
data <- as.data.frame(data)

dir.create(paste0("FILEPATH"), showWarnings = FALSE)
fwrite(data, paste0("FILEPATH"), na = "")

#######################
# Fit first stage model
#######################

print("Fitting first stage model")
data <- setDT(data)

# Convert qx to mx
data$mx <- log(1-data$mort)/-5

# Convert LDI to log space
data$log_ldi <- log(data$ldi)

# Create a dummy variable for source type
data$dummy <- 1

# create a variable for pandemic years
data[, pandemic_year := ifelse(year >= 2020, 1, 0)]

# Convert location and source type to a categorical factor
data$ihme_loc_id <- as.factor(data$ihme_loc_id)
data$source.type <- as.factor(data$source.type)

# Set dhs cbh as the first (and therefore reference) category for source.types
data$source.type <- relevel(data$source.type, "Standard_DHS_CBH")

# We shouldn't have 5q0 data greater than 1
data <- data[!(!is.na(mort) & mort>1),]

#######################
# Step 1. Run stage 1 mixed effects model for standard locations only
#######################
df <- data[location_id %in% input_location_ids, ]

# Create model input
grouped_input_data <- groupedData(mx ~ 1 | ihme_loc_id/source1, data = df[!is.na(df$mort),])

# Model: fixed intercept, survey.type, random ihme_loc_id/survey
fm1start <- c(rep(0, length(unique(df$source.type[df$data == 1]))+3))

# Stage 1 formula: fixed effect on source.type
fm1form <- as.formula("mx ~ exp(beta1*log_ldi + beta2*maternal_edu + beta5*dummy + beta4) + beta3*hiv")

# Run nlme with nested RE on ihme_loc_id/survey, FE on source.type
model <- nlme(
  fm1form,
  data = grouped_input_data,
  fixed = list(beta1 + beta2 + beta3 ~1, beta5 ~ source.type),
  random = list(ihme_loc_id = beta1 + beta2 + beta4 ~ 1, source1 = beta4 ~ 1),
  start = fm1start,
  verbose = F
)

# Write data
model_file_output <- paste0("FILEPATH")
save(model, file=model_file_output)

print("First stage model complete")

#######################
#Step 2. Take predicted fixed effect coefficients, apply to all location-year-sex combinations
#######################

stage_1_coefficients <- data.table(fixed_effect_beta_log_ldi = model$coefficients$fixed[[1]],
                                   fixed_effect_beta_maternal_edu = model$coefficients$fixed[[2]],
                                   fixed_effect_beta_hiv = model$coefficients$fixed[[3]],
                                   fixed_effect_beta_intercept = model$coefficients$fixed[[4]]
)

# source.type fixed effects coefficients
st.fe <- fixef(model)[grep("(Intercept)", names(fixef(model))):length(fixef(model))]
names(st.fe) <- levels(df$source.type)
st.fe[1] <- 0
st.fe <- as.data.frame(st.fe)
st.fe$source.type <- row.names(st.fe)
data <- merge(data,st.fe, by = "source.type", all.x = TRUE)


#######################
#Step 3. Subtract FE predictions from all 5q0 data values to generate FE-residual
#######################
data[, fixed_effect_beta_log_ldi := stage_1_coefficients$fixed_effect_beta_log_ldi]
data[, fixed_effect_beta_maternal_edu := stage_1_coefficients$fixed_effect_beta_maternal_edu]
data[, fixed_effect_beta_hiv := stage_1_coefficients$fixed_effect_beta_hiv]
data[, fixed_effect_beta_intercept := stage_1_coefficients$fixed_effect_beta_intercept]

data[, fe_resid := log(mx - fixed_effect_beta_hiv*hiv) - ((fixed_effect_beta_log_ldi * log_ldi) + (fixed_effect_beta_maternal_edu *maternal_edu) + fixed_effect_beta_intercept + st.fe)]

data[is.na(fe_resid)&(data==1),data:=0]
data[is.na(fe_resid)&(reference==1),reference:=0]

fwrite(data,paste0("FILEPATH"), na = "")

#######################
#Step 4. Then run a random effect only model on the residuals
#######################
# Create model input
grouped_data <- groupedData(fe_resid ~ 1 | ihme_loc_id/source1, data = data[!is.na(fe_resid),])
stage1_re_models <- lmer(fe_resid ~ 0 + (log_ldi + maternal_edu|ihme_loc_id) + (1|ihme_loc_id:source1), data = grouped_data, REML = FALSE)
save(stage1_re_models, file = paste0("FILEPATH"))


##Merge ihme_loc_id random effects into data
re_coef <- ranef(stage1_re_models)
b.re <- as.data.frame(re_coef[["ihme_loc_id"]])
colnames(b.re) <- c("ctr_re", "b1.re", "b2.re")
b.re$ihme_loc_id = rownames(b.re)
data <- merge(data, b.re, by="ihme_loc_id", all.x = T)

##Merge ihme_loc_id:survey (nested) Random Effects into data
data$src.ihme_loc_id <- paste(data$ihme_loc_id,":",data$source1, sep="")
src.re <- as.data.frame(re_coef[["ihme_loc_id:source1"]])
colnames(src.re) <- c("re2")
src.re$src.ihme_loc_id <- row.names(src.re)
data <- merge(data, src.re, by="src.ihme_loc_id", all.x = T)

data$pred.y <-  exp((data$fixed_effect_beta_log_ldi + data$b1.re) * data$log_ldi + (data$fixed_effect_beta_maternal_edu + data$b2.re) * data$maternal_edu + data$fixed_effect_beta_intercept + data$ctr_re + data$re2 + data$st.fe) + data$fixed_effect_beta_hiv * data$hiv

data$residual_covariate_model = data$mx - data$pred.y


#Get data back in order
data <- as.data.frame(data)
data <- data[order(data$ihme_loc_id, data$year),]
fwrite(data, paste0("FILEPATH"), na = "")

########################
#Get reference value of FE+RE and adjust data
########################
dat3 <- data[data$reference == 1,]
dat3 <- ddply(dat3[!duplicated(dat3[,c("ihme_loc_id", "source1")]),],
              .(ihme_loc_id),
              function(x){
                data.frame(ihme_loc_id = x$ihme_loc_id[1],
                           mre2 = mean(x$re2[x$reference == 1]),
                           mfe = mean(x$st.fe[x$reference ==1]))
              })

dat3$summe <- dat3$mre2 + dat3$mfe

#merge ref sum re/fe into data
data <- merge(data, dat3, all=T)

#get adjusted re + fe into data
data$adjre_fe <- data$re2 + data$st.fe - data$summe


#####################
#Get predictions
####################

# Predict w/o random effects or source-specific fixed effects
data$pred.mx <- exp((data$fixed_effect_beta_log_ldi * data$log_ldi) + (data$fixed_effect_beta_maternal_edu * data$maternal_edu) + data$fixed_effect_beta_intercept) + (data$fixed_effect_beta_hiv * data$hiv)

# Convert to qx space
data$pred.1b <- 1-exp(-5*data$pred.mx)

keep_cols <- c("ihme_loc_id", "source.type", "location_id", "year", "ldi",
               "maternal_edu", "hiv", "mort", "category", "corr_code_bias",
               "to_correct", "source", "source_year", "source1", "vr", "data",
               "ptid", "log10.sd.q5", "type", "method", "graphing.source",
               "region_name", "reference", "vr_no_overlap", "mx", "log_ldi",
               "dummy", "re2", "ctr_re", "b2.re",
               "b1.re", "st.fe", "mre2", "mfe", "summe", "adjre_fe","residual_covariate_model","fe_resid",
               "fixed_effect_beta_log_ldi", "fixed_effect_beta_maternal_edu",
               "fixed_effect_beta_hiv", "fixed_effect_beta_intercept", "pred.1b")
data <- data[,keep_cols]
fwrite(data, paste0("FILEPATH"), na = "")

