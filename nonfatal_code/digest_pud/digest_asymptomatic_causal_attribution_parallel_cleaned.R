###################################################################################################################
## Author: USERNAME
## Description: parallelize getting asymptomatic and proportion draws, remove negative, greater than 1, and NA draws 
## Output:  asymptomatic and proportion draw files ready for save results 
## Notes: currently done for PUD, gastritis, hernia, gallbladder -- needs to incorporate pancreatitis in GBD 2017
###################################################################################################################


rm(list=ls())

##working environment

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- #FILEPATH 
  j<- #FILEPATH 
  h<-#FILEPATH 
} else {
  lib_path <- #FILEPATH 
  j<- #FILEPATH 
  h<-#FILEPATH 
}

##load in arguments from parent script (must start index at 3 because of shell)

cause<-commandArgs()[3]
location<-commandArgs()[4]
cause_draws<-commandArgs()[5]

##update if age_group_ids change, or if changing measure of draws, measure 5 is prevalence only 

age_group_ids<-c(2:20, 30:32, 235)

measure <- 5

##ME_ids
pud_symp <- 1924
pud_total <- 9759
pud_asymp <-  9314
gastritis_symp  <- 1928
gastritis_total <- 9761
gastritis_asymp <-9528
bile_symp <-  1940
bile_total <-  9760
bile_asymp <-  9535
hernia_symp <-  1934
hernia_total <-  9794
hernia_asymp <-  9542
mild_anemia_pud <- 1925
mod_anemia_pud <- 1926
sev_anemia_pud <- 1927
mild_anemia_gastritis <- 1929
mod_anemia_gastritis <- 1930
sev_anemia_gastritis <- 1931


##create directories

dir.create(FILEPATH)
dir.create(FILEPATH)
dir.create(FILEPATH)

##create list of draws

draws <- paste0("draw_", 0:999)


##############################################################################################
##CODE STARTS HERE: Get draws of asymptomatic by doing total - symptomatic 
##############################################################################################

##get draws

source(paste0(j, FILEPATH))

total_draws <- get_draws(gbd_id_field="modelable_entity_id", gbd_id=get(paste0(cause,"_total")), source="epi", location_id=location, measure_ids=5)
total_draws <- total_draws[age_group_id %in% age_group_ids,]
total_draws[,grep("mod", colnames(total_draws)) := NULL]


symp_draws <- get_draws(gbd_id_field="modelable_entity_id", gbd_id=get(paste0(cause,"_symp")), source="epi", location_id=location, measure_ids=5)
symp_draws <- symp_draws[age_group_id %in% age_group_ids,]
symp_draws[,grep("mod", colnames(symp_draws)) := NULL]


##calculate asymptomatic from total - symptomatic, replace negative draws with 0, and save draws in a new folder 

asymp_draws <- copy(total_draws)

for (draw in draws) {
  asymp_draws[,draw := get(draw) - symp_draws[,get(draw)],with=F]
  asymp_draws[[draw]][asymp_draws[[draw]]<0]=0
}

write.csv(asymp_draws, FILEPATH)), row.names = F)

##############################################################################################
##exclusivity adjustments with anemia results if causes are pud and gastritis 
##############################################################################################


if (cause=="pud" | cause=="gastritis") {
  
  ##create proportion draws of symptomatic and asymptomatic, force any proportinos above 1 to be 1  
  
  symp_prop_draws <- copy(symp_draws)
  asymp_prop_draws <- copy(asymp_draws)
  
  for (draw in draws) {
    symp_prop_draws[,draw := get(draw)/total_draws[,get(draw)],with=F]
    symp_prop_draws[[draw]][symp_prop_draws[[draw]]>1]=1
    asymp_prop_draws[,draw := get(draw)/total_draws[,get(draw)],with=F]
  }
  
  ##multiply each symptomatic and asymptomatic proportion draws by mild/mod/sev anemia 
    
  for(severity in c("mild", "mod", "sev")) {
    
    ##create directories
    
    dir.create(FILEPATH)
    dir.create(FILEPATH)
    
    ##get anemia draws for mild/mod/severe anemia due to gastritis and pud
    
    anemia_draws <- get_draws(gbd_id_field="modelable_entity_id", gbd_id=get(paste0(severity,"_anemia_",cause)), source="epi", location_id=location, measure_ids=5)
    anemia_draws <- anemia_draws[age_group_id %in% age_group_ids,]
    anemia_draws[,grep("mod", colnames(anemia_draws)) := NULL]
    
    ##create copy data tables to manipulate and multiply draws
    
    symp_cause_anemia_draws <- copy(anemia_draws)
    asymp_cause_anemia_draws <- copy(anemia_draws)
    
    for(draw in draws) {
    symp_cause_anemia_draws[,draw := get(draw)*symp_prop_draws[,get(draw)],with=F]
    asymp_cause_anemia_draws[,draw := get(draw)*asymp_prop_draws[,get(draw)],with=F]
    }
    
    ##save files
    
    write.csv(symp_prop_draws, FILEPATH), row.names = F)
    write.csv(asymp_prop_draws, FILEPATH)), row.names = F)
    write.csv(symp_cause_anemia_draws, FILEPATH), row.names = F)
    write.csv(asymp_cause_anemia_draws, FILEPATH), row.names = F)
  }
}





