###################################################################################################################
## Author: 
## Description: gets proportion asympotmatic gallbladder draws from total and symptomatic
## Output: asymptomatic gallbladder draw files ready for save results 
###################################################################################################################


rm(list=ls())

##working environment
os <- .Platform$OS.type
if (os=="windows") {
  j<- "J:/"
  h<-"H:/"
} else {
  j<- "/home/j/"
  h<-"/homes/USERNAME/"
}


##load in arguments from parent script (must start index at 3 because of shell)
  cause<-commandArgs()[3]
  location<-commandArgs()[4]
  cause_draws<-commandArgs()[5]

#set objects
age_group_ids<-c(2:20, 30:32, 235)
measure <- 5
functions_dir <- paste0(j, FILEPATH)

##ME_ids
bile_symp <-  1940
bile_total <-  9760
bile_asymp <-  9535

##create directories
dir.create(file.path(cause_draws, "asymptomatic"), showWarnings = T)
dir.create(file.path(cause_draws, "proportion_symp"), showWarnings = T)
dir.create(file.path(cause_draws, "proportion_asymp"), showWarnings = T)

##create list of draws
draws <- paste0("draw_", 0:999)


##############################################################################################
##CODE STARTS HERE: Get draws of asymptomatic by doing total - symptomatic 
##############################################################################################

##get draws
source(paste0(functions_dir, "get_draws.R"))

total_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=get(paste0(cause,"_total")), source="epi", location_id=location, measure_id=5)
total_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=get(paste0(cause,"_total")), source="epi", location_id=location, measure_id=5)
total_draws <- total_draws[age_group_id %in% age_group_ids,]
total_draws[,grep("mod", colnames(total_draws)) := NULL]


symp_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=get(paste0(cause,"_symp")), source="epi", location_id=location, measure_id=5)
symp_draws <- symp_draws[age_group_id %in% age_group_ids,]
symp_draws[,grep("mod", colnames(symp_draws)) := NULL]


##calculate asymptomatic from total - symptomatic, replace negative draws with 0, and save draws in a new folder 
asymp_draws <- copy(total_draws)

for (draw in draws) {
  asymp_draws[,draw := get(draw) - symp_draws[,get(draw)],with=F]
  asymp_draws[[draw]][asymp_draws[[draw]]<0]=0
}

write.csv(asymp_draws, file.path(cause_draws, "asymptomatic", paste0(measure,"_", location, ".csv")), row.names = F)
