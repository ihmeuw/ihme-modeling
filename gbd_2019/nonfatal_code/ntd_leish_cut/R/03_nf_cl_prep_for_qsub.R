#pair st-gpr outputs with age-sex curves to produce NF results
#for cl

rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}


source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH.R",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))

#define geographic restrictions list
leish_geo<-read.csv(sprintf("FILEPATH",prefix), stringsAsFactors = FALSE)
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

unique_cl_locations<-unique(presence_list$loc_id)

#define epi required locations
tmp<-get_demographics("epi")
full_loc_set<-tmp$location_id
full_age_set<-tmp$age_group_id
#full_year_id<-tmp$year_id
full_year_id<-seq(1990,2017,1)
full_sex_id<-tmp$sex_id

#import the 2010 global estimate 
draws_global<-get_draws("modelable_entity_id", ADDRESS, source = 'epi', gbd_round=ADDRESS,
                        location_id=1, year_id=2010)
draws_master<-draws_global
#drop the birth prevalence row
draws_global<-subset(draws_global, age_group_id!=164)
#drop to incidence
draws_global<-subset(draws_global, measure_id==ADDRESS)

for (beta in 1:nrow(draws_global)){
  if(draws_global$age_group_id[beta] %in% c(2,3)){
    draws_global[beta,2:1001]<-draws_global[beta,2:1001]*0
  }
}

draws_global<-draws_global[,-1]
draws_global<-draws_global[,-1001:-1007]

#global_ratio<-draws_global$draw_0/min(draws_global$draw_0)
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

subnat_prop<-read.csv(paste0(prefix,'FILEPATH'))
unique_subnat<-unique(subnat_prop$loc_id)

#define model_id from st-gpr
model_id<-ADDRESS

#save workspace so can be loaded in by qsubbed scripts

save.image(paste0(prefix, "FILEPATH"))
