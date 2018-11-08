## Intimate Partner Violence, HIV PAF calculation, save_results script

## get arguments
print(commandArgs())
spectrum.name <- as.numeric(commandArgs()[4])
main_dir <- as.numeric(commandArgs()[5])
ver <- as.numeric(commandArgs()[6])

# source shared functions
shared_functions_dir <- 'FILEPATH'
source(paste0(shared_functions_dir,'/get_location_metadata.R'))
source(paste0(shared_functions_dir,'/get_best_model_versions.R'))
source(paste0(shared_functions_dir,'/save_results_risk.R'))

# create description
ipv_mvid <- get_best_model_versions(entity='modelable_entity', ids=2452, status='best')$model_version_id
description <- paste('HIV model', spectrum.name,'& IPV exposure model',ipv_mvid,sep=" ")

# check locations all present
loc_table <- get_location_metadata(35)
loc_table <- loc_table[most_detailed==1,]
locations <- unique(loc_table$location_id)
missing <- c()
files <- list.files(paste0(main_dir,'/paf_draws_',ver),full.names=F)
for(loc in locations){
  if(!paste0(loc,"_2.csv")%in%files) missing <- c(missing, loc)
}
print("missing these locations:")
print(missing)

# save results
save_results_risk(
  modelable_entity_id=8824,
  description=description,
  input_dir=paste0(main_dir,'/paf_draws_',ver),
  input_file_pattern="{location_id}_{sex_id}.csv",
  risk_type='paf',
  mark_best='True',
  sex_id=2,
  year_id=c(1990:2017)
)