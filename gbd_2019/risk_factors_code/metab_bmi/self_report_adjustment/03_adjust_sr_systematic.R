################################################################################
## DESCRIPTION: Applies self-report adjustment (systematic error) for anthropometric data (height, weight, BMI, and derivatives) ##
## INPUTS: Age-sex split bundle version data ##
## OUTPUTS: Adjusted bundle version data ready for upload to crosswalk version ##
## AUTHOR: ##
## DATE CREATED: 21 June 2019 ##
################################################################################

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"

# Base filepaths
work_dir <- paste0(j, 'FILEPATH')
share_dir <- 'FILEPATH' # only accessible from the cluster
code_dir <- if (os == "Linux") "FILEPATH" else if (os == "Windows") paste0("FILEPATH")

## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH/primer.R'))
library(msm)

## PARSE ARGS
parser <- ArgumentParser()
parser$add_argument("--data_dir", help = "parent directory where intermediate data will be stored",
                    default = 'FILEPATH', type = "character")
parser$add_argument("--processing_version_id", help = "Internal version ID of data processing run",
                    default = 2, type = "integer")
parser$add_argument("--ow_bundle_version_id", help = "Prevalence overweight bundle version id being processed",
                    default = 5498, type = "integer")
parser$add_argument("--ob_bundle_version_id", help = "Prevalence obese bundle version id being processed",
                    default = 5501, type = "integer")

args <- parser$parse_args()
list2env(args, environment()); rm(args)


## LOAD RELEVANT DATA
loc_map <- get_locations(gbd_type = 'gbd')


## Read in adjustment factors (differences in logit space for prev ow and prev ob)
## Model 1 refers to the sex-specific model fit with the results from the both sex MR-BRT model as priors and only untrimmed data from the both sex model
ow_adj <- fread('FILEPATH/coeffs_metab_overweight_1.csv')[model == 1, .(region_id, age_group_id, sex_id, pred, pred_var)]
ob_adj <- fread('FILEPATH/coeffs_metab_obese_1.csv')[model == 1, .(region_id, age_group_id, sex_id, pred, pred_var)]

ow_data <- fread(sprintf('%s/FILEPATH/bundle_version_%d.csv', data_dir, ow_bundle_version_id))
ob_data <- fread(sprintf('%s/FILEPATH/bundle_version_%d.csv', data_dir, ob_bundle_version_id))

ow_data <- merge(ow_data, loc_map[, .(location_id, region_id)], by = 'location_id')
ob_data <- merge(ob_data, loc_map[, .(location_id, region_id)], by = 'location_id')

# Adjustment applied to age 15+ even though fit on only data from 20+ (given switch in definition of ow/ob for under 20). Current adjustment doesn't vary by age so copy adjustment factors for age 20

ow_adj <- rbind(ow_adj,
                ow_adj[age_group_id == 9, .(age_group_id = 8, sex_id, region_id, pred, pred_var)], use.names = T, fill = T)

ob_adj <- rbind(ob_adj,
                ob_adj[age_group_id == 9, .(age_group_id = 8, sex_id, region_id, pred, pred_var)], use.names = T, fill = T)

merge_ids <- c('age_group_id', 'sex_id', 'region_id')
ow_data <- merge(ow_data, ow_adj, by = merge_ids, all.x = T)
ob_data <- merge(ob_data, ob_adj, by = merge_ids, all.x = T)

# Assume data w/o info on self-report is self report
# Outlier any self-report data under 15 for now
ow_data[cv_diagnostic == ''|is.na(cv_diagnostic), cv_diagnostic := 'self-report']
ob_data[cv_diagnostic == ''|is.na(cv_diagnostic), cv_diagnostic := 'self-report']

ow_data[age_group_id < 8 & cv_diagnostic == 'self-report', is_outlier := 1]
ob_data[age_group_id < 8 & cv_diagnostic == 'self-report', is_outlier := 1]

# Apply adjustment and uncertainty to data. Needs to be done in logit space. Variance needs to be converted to and from logit space using delta method
ow_data[, logit_data := logit(data)]
lapply(1:nrow(ow_data), function(row) ow_data[row, logit_se := deltamethod(~log(x1/(1-x1)), data, variance)]) %>% invisible
ow_data[, logit_adj_data := ifelse(cv_diagnostic == 'self-report', logit_data - pred, logit_data)]
ow_data[, logit_adj_var := ifelse(cv_diagnostic == 'self-report', logit_se^2 + pred_var, logit_se^2)]
ow_data[, adj_data := inv.logit(logit_adj_data)]
lapply(1:nrow(ow_data), function(row) ow_data[row, adj_var := deltamethod(~exp(x1)/(1+exp(x1)), logit_adj_data, logit_adj_var)^2]) %>% invisible


ob_data[, logit_data := logit(data)]
lapply(1:nrow(ob_data), function(row) ob_data[row, logit_se := deltamethod(~log(x1/(1-x1)), data, variance)]) %>% invisible
ob_data[, logit_adj_data := ifelse(cv_diagnostic == 'self-report', logit_data - pred, logit_data)]
ob_data[, logit_adj_var := ifelse(cv_diagnostic == 'self-report', logit_se^2 + pred_var, logit_se^2)]
ob_data[, adj_data := inv.logit(logit_adj_data)]
lapply(1:nrow(ob_data), function(row) ob_data[row, adj_var := deltamethod(~exp(x1)/(1+exp(x1)), logit_adj_data, logit_adj_var)^2]) %>% invisible

### Produce adjustment diagnostics and run appropriate validations

ggplot(data = ow_data[is_outlier == 0], aes(data, adj_data)) + geom_point(shape = 1, alpha = .2) + 
  geom_abline(slope = 1, intercept = 0, color = 'red') + 
  coord_cartesian(xlim = c(0,1), ylim = c(0,1)) +
  facet_wrap(~sex_id) +
  theme_bw() + labs(title = "Impact of adjustment on prevalence overweight data")

ggplot(data = ow_data[is_outlier == 0], aes(data, adj_data)) + geom_point(shape = 1, alpha = .2) + coord_fixed(ratio = 1) + 
  geom_abline(slope = 1, intercept = 0, color = 'red') +
  coord_cartesian(xlim = c(0,1), ylim = c(0,1)) +
  facet_wrap(~sex_id) +
  theme_bw() + labs(title = "Impact of adjustment on proportion overweight that are obese data")

### SAVE OUTPUTS
write.csv(ow_data, sprintf('%s/FILEPATH/bundle_version_%d.csv', data_dir, ow_bundle_version_id), row.names = F)
write.csv(ob_data, sprintf('%s/FILEPATH/bundle_version_%d.csv', data_dir, ob_bundle_version_id), row.names = F)