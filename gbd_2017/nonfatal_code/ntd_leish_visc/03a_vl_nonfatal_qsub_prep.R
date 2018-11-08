#pair st-gpr outputs with age-sex curves to produce NF results

rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}


source(sprintf("FILEPATH/get_demographics.R",prefix))
source(sprintf("FILEPATH/get_covariate_estimates.R",prefix))
source(sprintf("FILEPATH/get_location_metadata.R",prefix))
source(sprintf("FILEPATH/get_population.R",prefix))
source(sprintf("FILEPATH/get_draws.R",prefix))
source(sprintf("FILEPATH/save_results_cod.R",prefix))
source(sprintf("FILEPATH/save_results_epi.R",prefix))
source(sprintf("FILEPATH/get_model_results.R",prefix))
source(sprintf("FILEPATH/get_ids.R",prefix))

#define gbdmodel
gbdmodel<-"ver9"






#define geographic restrictions list
leish_geo<-read.csv(sprintf("FILEPATH/geo_restrict_vl.csv",prefix), stringsAsFactors = FALSE)
dataset<-leish_geo[,1:48]


#convert to long
melt_data<-melt(dataset, id.vars = c('loc_id',
                                     'parent_id',
                                     'level',
                                     'type',
                                     'loc_nm_sh',
                                     'loc_name',
                                     'spr_reg_id',
                                     'region_id',
                                     'ihme_lc_id',
                                     'GAUL_CODE'))

melt_data$value<-as.character(melt_data$value)

status_list<-subset(melt_data, type=='status')

#drop out pre1980 data
status_list<-subset(status_list, variable != 'pre1980')

presence_list<-subset(status_list,value=='p' |value=='pp')

unique_vl_locations<-unique(presence_list$loc_id)

#define epi required locations
tmp<-get_demographics("ADDRESS")
full_loc_set<-tmp$location_id
full_age_set<-tmp$age_group_id
#full_year_id<-tmp$year_id
full_year_id<-seq(1980,2017,1)
full_sex_id<-tmp$sex_id

#import the 2010 global estimate - is best for all-time estimate
draws_global<-get_draws("modelable_entity_id", 11645, source = 'ADDRESS', gbd_round=5,
                        location_id=1, year_id=2010)

draws_master<-draws_global

#drop the birth prevalence row
draws_global<-subset(draws_global, age_group_id!=164)

for (beta in 1:nrow(draws_global)){
  if(draws_global$age_group_id[beta] %in% c(2,3)){
    draws_global[beta,5:1004]<-draws_global[beta,5:1004]*0
  }
}


draws_global<-draws_global[,-1:-4]
draws_global<-draws_global[,-1001:-1004]


#implement this in ratio space
draws_ratio<-draws_global
for(k in 1:ncol(draws_global)){
  draw_string<-data.frame(draws_global[,..k])
  draw_string<-subset(draw_string, draw_string!=0)
  draws_ratio[,k]<-draws_global[,..k]/min(draw_string)
}
#this gives a global proportion model at draw level
setDF(draws_ratio)

#create a blank epi template that would need to be filled - aim to produce a csv for each location year
#sex_id order matches the draws_master sex_id order
empty_df<-data.frame(location_id=rep(NA,(2*length(full_age_set))),
                     year_id=rep(NA,(2*length(full_age_set))),
                     age_group_id=full_age_set,
                     sex_id=c(rep(1,length(full_age_set)),rep(2,length(full_age_set))),
                     measure_id=rep(NA,(2*length(full_age_set))),
                     modelable_entity_id=rep(NA,(2*length(full_age_set)))
)

zero_matrix<-draws_global*0
zero_df<-cbind(empty_df, zero_matrix)

#load in the subnational proportions
subnat_prop<-read.csv(paste0(prefix,'FILEPATH/subnational_proportions.csv'))
unique_subnat<-unique(subnat_prop$loc_id)

#define model_id from st-gpr
model_id<-42536


#save as an RData for qsub loop to reference
save.image(file = paste0(prefix,'FILEPATH/qsub_workspace.RData'))
