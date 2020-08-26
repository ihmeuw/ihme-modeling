#################################################################################
#### Pull and convert Clostridium DisMod model, create and save PAFs by draw ####
### This file is straightforward. By location, it pulls the diarrhea
## envelope model of incidence, prevalence, and mortality, divides the
## Clostridium difficile model by those parents to produce a PAF, and saves
## the PAFs as CSVs for upload. ##
#################################################################################

##########################################################################################
## Do some set up and housekeeping ##
print(commandArgs()[1])
print(commandArgs()[2])
print(commandArgs()[3])
print(commandArgs()[4])
location <- commandArgs()[4]

library(plyr)
source("/filepath/get_draws.R")
source("/filepath/get_population.R")
age_map <- read.csv("filepath/age_mapping.csv")

## Define these here so they are written out just once.
age_group_id <- age_map$age_group_id[!is.na(age_map$id)]
sex_id <- c(1,2)
year_id <- c(1990,1995,2000,2005,2010,2015,2017,2019)

##########################################################################################
# Get population to go from deaths to mortality rate #
  pop <- get_population(location_id=location, age_group_id=age_group_id, year_id=year_id, sex_id=sex_id, decomp_step="step3")

# Get Cod draws, calculate rate, rename #
source_draws <- "codcorrect"
version_id <- 94

  cod_draws <- data.frame(get_draws(source=source_draws, gbd_id_type="cause_id", gbd_id=302, sex_id=sex_id, version_id=version_id, gbd_round_id=6, location_id=location, year_id=year_id, age_group_id=age_group_id, decomp_step="step3"))
  cod_draws <- subset(cod_draws, measure_id==1)
  cod_draws <- join(cod_draws, pop, by=c("year_id","age_group_id","sex_id"))

  for(i in 0:999){
    cod_draws[,paste0("draw_",i)] <- cod_draws[,paste0("draw_",i)] / cod_draws$population
    setnames(cod_draws, paste0("draw_",i), paste0("dmort_",i))
  }

# Get Diarrhea Incidence draws, rename #
  inc_draws <- data.frame(get_draws(source="epi", gbd_id_type="modelable_entity_id", gbd_id=1181, sex_id=sex_id, location_id=location, year_id=year_id, age_group_id=age_group_id, decomp_step="step3"))
  inc_draws <- subset(inc_draws, measure_id==6)
  for(i in 0:999){
    setnames(inc_draws, paste0("draw_",i), paste0("dinc_",i))
  }

# Get C. difficile draws for DisMod model #
  cdiff_draws <- data.frame(get_draws(source="epi", gbd_id_type="modelable_entity_id", gbd_id=1227, sex_id=sex_id, location_id=location, year_id=year_id, age_group_id=age_group_id, decomp_step="step3"))

############################################################################################
# Subset to cause-specific mortality, calculate fatal PAFs #
  cdiff_mtsp <- subset(cdiff_draws, measure_id==15)
  cdiff_mtsp <- join(cdiff_mtsp, cod_draws, by=c("location_id","year_id","sex_id","age_group_id"))

  mort_df <- cdiff_mtsp[,c("location_id","year_id","age_group_id","sex_id")]
  for(i in 0:999){
    draw <- cdiff_mtsp[,paste0("draw_",i)] / cdiff_mtsp[,paste0("dmort_",i)]
    # make sure it doesn't go outside bounds
      draw <- ifelse(draw > 1, 1, draw)
    mort_df[,paste0("draw_",i)] <- draw
  }
  mort_df$rei_id <- 183
  mort_df$paf_modelable_entity <- 9334
  mort_df$cause_id <- 302
  write.csv(mort_df, paste0("/filepath/yll_",location,".csv"), row.names=F)

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
  write.csv(inc_df, paste0("/filepath/yld_",location,".csv"), row.names=F)

##############################################################################################

print("Finished!")
