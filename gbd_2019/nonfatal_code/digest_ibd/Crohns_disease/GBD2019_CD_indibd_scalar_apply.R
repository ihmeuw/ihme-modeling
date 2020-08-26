###################################################################################################################
## This script generates redistribution scalar of indeterminate colitis, applies to ulcerative colitis and crohn's 
## disease, and save scaled draws to custom model save_results
###################################################################################################################

source("FILEPATH/get_location_metadata.R")

##MAKE DIRECTORIES TO SAVE DRAWS BEFORE UPLOADED TO THE CUSTOM MODEL
date <- file.path(scratch, format(Sys.Date(), format = "%Y_%m_%d"))
dir.create(date, showWarnings = F)
dir.create(file.path(date, "01_draws"), showWarnings = F)
draws <- file.path(date, "01_draws")

##GET GBD LOCATIONS
epi_locations <- get_location_metadata(location_set_id=9, gbd_round_id=6, decomp_step="step4")
location_ids <- epi_locations[is_estimate==1 & most_detailed==1]
location_ids <- location_ids$location_id


causes<- "crohns_disease"
cause<-"crohns_disease"
location <- location_ids
cause_draws<-file.path("FILEPATH")
age_group_ids<-c(2:20, 30:32, 235)


##PARENT CAUSE DISMOD MODEL IDs
crohns_disease_adj <- CUSTOM_MODEL_ID
crohns_disease_unadj <- PARENT_MODEL_ID


##GENERATE 1000 DRAWS OF SCALAR AND CREATE A VECTOR
##values 1+mean/1+lower/1+upper results from GBD2019_Indeterminantibd_scalar.R
mean <- 1.0624 
lower <- 1.0549
upper <- 1.0699
sd <- (upper-lower)/(2*1.96)  

#GENERATE VECTOR OF 1000 DRAWS OF DISTRIBUTION CENTERED AROUND SCALAR WITH STANDARD ERRORS
ic_scalars<-rnorm(1000,mean, sd)

##GET DRAWS FOR EACH CAUSE LOCATION
source("FILEPATH/get_draws.R")

#DATAFRAME OBJECTS, GEETTING DRAWS FROM PARENT DISMOD MODEL 
unadj_draws <- get_draws(gbd_id_type="modelable_entity_id", 
                         gbd_id=crohns_disease_unadj,
                         source="epi", 
                         status="best",
                         location_id=location_ids, 
                         measure_id=c(5,6), 
                         age_group_id=age_group_ids, 
                         gbd_round_id=6, 
                         decomp_step="step4")
unadj_draws[,grep("mod", colnames(unadj_draws)) := NULL]

##APPLY SCALARS TO DRAWS
adj_draws <- copy(unadj_draws) 
adj_draws[,"modelable_entity_id"] <- crohns_disease_adj


for(i in 0:999) { 
  draw <- paste0("draw_", i)
    adj_draws[, draw := adj_draws[, draw,with=F]*ic_scalars[i+1], with=F]
  }

##SAVE DRAWS; PRODUCTS OF TWO SETS OF DRAWS AND SAVE THEN IN CSV. 

dir.create(file.path(cause_draws, "adjusted_draws"), showWarnings = F)
write.csv(adj_draws, file.path(cause_draws, "adjusted_draws", paste0(location, ".csv")), row.names = F)
