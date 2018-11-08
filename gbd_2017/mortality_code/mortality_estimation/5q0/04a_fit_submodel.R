################################################################################
## Description: Run the first stage of the prediction model for adult mortality
##              and calculate inputs to GPR
################################################################################

rm(list=ls())

# Import installed libraries
library(foreign)
library(zoo)
library(nlme)
library(plyr)
library(data.table)
library(devtools)
library(methods)
library(argparse)


# Get arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of 5q0')
parser$add_argument('--submodel_id', type="integer", required=TRUE,
                    help='The submodel_id for this submodel of 5q0')
parser$add_argument('--input_location_ids', type="integer", required=TRUE,
                    nargs='+', help='The input ids for this submodel of 5q0')
parser$add_argument('--output_location_ids', type="integer", required=TRUE,
                    nargs='+', help='The output ids for this submodel of 5q0')
parser$add_argument('--gbd_round_id', type="integer", required=TRUE,
                    help='The gbd_round_id for this run of 5q0')
parser$add_argument('--start_year', type="integer", required=TRUE,
                    help='The starting year for this run of 5q0')
parser$add_argument('--end_year', type="integer", required=TRUE,
                    help='The ending year for this run of 5q0')
parser$add_argument('--st_loess', type="integer", required=TRUE,
                    help='Whether to use the st_loess')
args <- parser$parse_args()

# Get arguments
version_id <- args$version_id
submodel_id <- args$submodel_id
input_location_ids <- args$input_location_ids
output_location_ids <- args$output_location_ids
gbd_round_id <- args$gbd_round_id
start_year <- args$start_year
end_year <- args$end_year
st_loess <- args$st_loess


# Set core directories
code_dir <- "FILEPATH"
output_dir <- "FILEPATH"


# Get data
dc = DataContainer$new(gbd_round_id = gbd_round_id, start_year = start_year,
                       end_year = end_year, output_dir = output_dir)
data <- dc$get('model_input')
data <- data[location_id %in% input_location_ids, ]

# Merge on regions
location_data <- dc$get('location')
data <- merge(data, location_data[, c('location_id', 'region_name')],
              by = 'location_id')

# Make sure that source.type is NA for places where there is no data
# NOTE: This is here because to be a bug originating when reading in the
#       CSV/saving from the previous step.
data[data == 0, source.type := NA]

#############################
# Choose reference categories
#############################
data$reference <- 0
data = refcats.setRefs(input = data)

# set vr_no_overlap to vr_biased, but mark it so that it isn't run through the vr bias adjustment
data$vr_no_overlap <- 0
data[data$category %in% c("vr_no_overlap"), 'vr_no_overlap'] <- 1
data[data$category %in% c("vr_no_overlap"), 'corr_code_bias'] <- TRUE
data[data$category %in% c("vr_no_overlap"), 'to_correct'] <- T
data[data$category %in% c("vr_no_overlap"), 'source.type'] <- "vr_biased_VR/SRS/DSP"
data[data$category %in% c("vr_no_overlap"), 'graphing.source'] <- "vr_biased"
data[data$category %in% c("vr_no_overlap"), 'category'] <- "vr_biased"

dc$save_submodel(data, submodel_id, 'references_applied')

#######################
# Fit first stage model
#######################
print("Fitting first stage model")
# Convert qx to mx
data$mx <- log(1-data$mort)/-5

# Convert LDI to log space
data$log_ldi <- log(data$ldi)

# Create a dummy variable for source type
data$dummy <- 1

# Convert location and source type to a categorical factor
data$ihme_loc_id <- as.factor(data$ihme_loc_id)
data$source.type <- as.factor(data$source.type)

# Set dhs cbh as the first (and therefore reference) category for source.types
data$source.type <- relevel(data$source.type, "Standard_DHS_CBH")


# Create model input
grouped_input_data <- groupedData(mx ~ 1 | ihme_loc_id/source1, data = data[!is.na(data$mort),])

# Model: fixed intercept, survey.type, random ihme_loc_id/survey
# NOTE: have tested - model not sensitive to start values
fm1start <- c(rep(0, length(unique(data$source.type[data$data == 1]))+3))

# Stage 1 formula: fixed effect on source.type
fm1form <- as.formula("mx ~ exp(beta1*log_ldi + beta2*maternal_edu + beta5*dummy + beta4) + beta3*hiv")

# Run nlme with nested RE on ihme_loc_id/survey, FE on source.type
model <- nlme(fm1form,
              data = grouped_input_data,
              fixed = list(beta1 + beta2 + beta3 ~1, beta5 ~ source.type),
              random = list(ihme_loc_id = beta1 + beta2 + beta4 ~ 1, source1 = beta4 ~ 1),
              start = fm1start,
              verbose = F)

# Write data
model_file_output <- dc$save_submodel(model, submodel_id, 'stage_1_model')
save(model, file=model_file_output)

print("First stage model complete")

##########################
#Merge residuals, fixed effects, random effects into data
#########################
##Merge residuals into data
data$residual_covariate_model <- rep("NA", length(data$data))
data$residual_covariate_model[!is.na(data$mort)] <- model$residuals[, "source1"]
data$residual_covariate_model <- as.numeric(data$residual_covariate_model)

##Merge ihme_loc_id:survey (nested) Random Effects into data
data$src.ihme_loc_id <- paste(data$ihme_loc_id, "/",data$source1, sep="")

src.re <- as.data.frame(model$coefficients$random$source1[, 1])
colnames(src.re) <- "re2"
src.re$src.ihme_loc_id <- row.names(src.re)


data <- merge(data, src.re, by="src.ihme_loc_id", all.x = T)

##Merge ihme_loc_id random effects into data
data$b1.re <- data$b2.re <- data$ctr_re <- NA
for (ii in rownames(model$coefficients$random$ihme_loc_id)){
  data$b1.re[data$ihme_loc_id == ii] <- model$coefficients$random$ihme_loc_id[ii,1]
  data$b2.re[data$ihme_loc_id == ii] <- model$coefficients$random$ihme_loc_id[ii,2]
  data$ctr_re[data$ihme_loc_id == ii] <- model$coefficients$random$ihme_loc_id[ii,3]
}

# Merge source.type fixed effects into data
# Intercept/reference category is assigned to be Standard_DHS_CBH right now
st.fe <- fixef(model)[grep("(Intercept)", names(fixef(model))):length(fixef(model))]
names(st.fe) <- levels(data$source.type)
st.fe[1] <- 0

st.fe <- as.data.frame(st.fe)
st.fe$source.type <- row.names(st.fe)
st.fe <- data.table(st.fe)
data <- merge(data, st.fe, by="source.type", all.x = T)


#Get data back in order
data <- data[order(data$ihme_loc_id, data$year),]

########################
#Get reference value of FE+RE and adjust data
########################
dat3 <- ddply(data[!duplicated(data[,c("ihme_loc_id", "source1")]),],
              .(ihme_loc_id),
              function(x){
                data.frame(ihme_loc_id = x$ihme_loc_id[1],
                           mre2 = mean(x$re2[x$reference == 1]),
                           mfe = mean(x$st.fe[x$reference ==1]))
              })

dat3$summe <- dat3$mre2 + dat3$mfe

#merge ref sum re/fe into data
data <- merge(data, dat3, all=T)

data <- data[,names(data) != "src.ihme_loc_id"]

#get adjusted re + fe into data
data$adjre_fe <- data$re2 + data$st.fe - data$summe

# Save fixed effects
data <- data.table(data)
data[, fixed_effect_beta_log_ldi := model$coefficients$fixed[[1]]]
data[, fixed_effect_beta_maternal_edu := model$coefficients$fixed[[2]]]
data[, fixed_effect_beta_hiv := model$coefficients$fixed[[3]]]
data[, fixed_effect_beta_intercept := model$coefficients$fixed[[4]]]

#####################
#Get predictions
####################

# Predict w/o random effects or source-specific fixed effects
pred.mx <- exp((data$fixed_effect_beta_log_ldi * data$log_ldi) + (data$fixed_effect_beta_maternal_edu * data$maternal_edu) + data$fixed_effect_beta_intercept) + (data$fixed_effect_beta_hiv * data$hiv)

# Convert to qx space
data$pred.1b <- 1-exp(-5*pred.mx)


dc$save_submodel(data, submodel_id, 'stage_1_prediction')
