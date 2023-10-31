#################################################################################
#### Pull and convert Clostridium DisMod model, create and save PAFs by draw ####
### By location, this pulls the diarrhea
## envelope model of incidence, prevalence, and mortality, divides the
## Clostridium difficile model by those parents to produce a PAF, and saves
## the PAFs as CSVs for upload. ##
#################################################################################

##########################################################################################

args <- commandArgs(trailingOnly = TRUE) 
location <- args[1]

library(plyr)
library(data.table)
source("/PATH/get_draws.R")
source("/PATH/get_age_metadata.R")

pop_dir <- "/PATH/pop/"
cod_dir <- "/PATH/cod/"
pop <- fread(paste0(pop_dir, "pop_", location, ".csv"))
year_id <- c(1990,1995,2000,2005,2010,2015,2019,2020, 2021, 2022)
sex_id <- c(1,2)
age_map <- get_age_metadata(age_group_set = 19, gbd_round_id = 7)
age_group_id <- age_map$age_group_id


##########################################################################################

# To avoid overwhelming central resources, use flat files to load codcorrect and population.
cod_draws <- fread(paste0(cod_dir, "cod_", location, ".csv"))
pop <- fread(paste0(pop_dir, "pop_", location, ".csv"))
cod_draws <- join(cod_draws, pop, by=c("year_id","age_group_id","sex_id"))

cod_draws <- cod_draws[, paste0("draw_",0:999) := lapply(0:999, function(x){ get(paste0("draw_",x))/population})]
setnames(cod_draws, paste0("draw_",0:999), paste0("dmort_",0:999))
#-----


# Get Diarrhea Incidence draws#
inc_draws <- data.frame(get_draws(source="epi"
                                  , gbd_id_type="modelable_entity_id"
                                  , gbd_id=1181
                                  , sex_id=sex_id
                                  , location_id=location
                                  , year_id=year_id
                                  , age_group_id=age_group_id
                                  , decomp_step="iterative"
))
inc_draws <- subset(inc_draws, measure_id==6)
setnames(inc_draws, paste0("draw_",0:999), paste0("dinc_",0:999))

# Get C. difficile draws for DisMod model #
cdiff_draws <- data.frame(get_draws(source="epi"
                                    , gbd_id_type="modelable_entity_id"
                                    , gbd_id=1227
                                    , sex_id=sex_id
                                    , location_id=location
                                    , year_id=year_id
                                    , age_group_id=age_group_id
                                    , decomp_step="iterative"
))

############################################################################################
# Subset to cause-specific mortality, calculate fatal PAFs #
cdiff_mtsp <- subset(cdiff_draws, measure_id==15)
cdiff_mtsp <- join(cdiff_mtsp, cod_draws, by=c("location_id","year_id","sex_id","age_group_id"))


mort_df <- cdiff_mtsp[,c("location_id","year_id","age_group_id","sex_id")]
for(i in 0:999){
  draw <- cdiff_mtsp[,paste0("draw_",i)] / cdiff_mtsp[,paste0("dmort_",i)]
  # make sure it doesn't go outside bounds
  draw <- ifelse(draw > 1, 0.99, draw)
  mort_df[,paste0("draw_",i)] <- draw
}

mort_df$rei_id <- 183
mort_df$paf_modelable_entity <- 9334
mort_df$cause_id <- 302
mort_df[is.na(mort_df)] <- 0
mort_df[mort_df == ""] <- 0
write.csv(mort_df, paste0("/PATH/yll_",location,".csv"), row.names=F)

##############################################################################################
# Subset to incidence, calculate non-fatal PAFs #
cdiff_inc <- subset(cdiff_draws, measure_id==6)
cdiff_inc <- join(cdiff_inc, inc_draws, by=c("location_id","year_id","sex_id","age_group_id"))

inc_df <- cdiff_inc[,c("location_id","year_id","age_group_id","sex_id")]
for(i in 0:999){
  draw <- cdiff_inc[,paste0("draw_",i)] / cdiff_inc[,paste0("dinc_",i)]
  # make sure it doesn't go outside bounds
  draw <- ifelse(draw > 1, 1, draw)
  inc_df[,paste0("draw_",i)] <- draw
}

inc_df$rei_id <- 183
inc_df$paf_modelable_entity <- 9334
inc_df$cause_id <- 302
inc_df[is.na(inc_df)] <- 0
inc_df[inc_df == ""] <- 0
write.csv(inc_df, paste0("/PATH/yld_",location,".csv"), row.names=F)

##############################################################################################

print("Finished!")
