############################################################################
## Chidlhood Sexual Abuse, HIV PAF calculation, child job
## PURPOSE: (1) get CSA exposure
##          (2) pull in proportion HIV incidence due to sex
##          (3) pull in HIV incidence
##          (4) calculate PAF for one location
## NOTE: only works on the cluster
############################################################################

time_start <- Sys.time()
print(commandArgs())

# SETUP -------------------------------------------------------------------

#libraries and functions
library(data.table)
library(stringr)
require(dplyr)
library(svMisc, lib.loc = paste0("FILEPATH", Sys.getenv(x='USER'), "/rlibs/"))
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_draws.R")

#if interactively testing: 
if (interactive()) {
  
} else { #otherwise, get arguments
  print(commandArgs())
  print("\n --- break --- ")
  print(paste("8 ", commandArgs()[8]))
  print(paste("9 ", commandArgs()[9]))
  loc_id <- as.numeric(commandArgs()[6])
  main_dir <- commandArgs()[7]
  ver <- commandArgs()[8]
  spectrum.name <- commandArgs()[9]
  paf_draws <- commandArgs()[10]
  paf_draws <- as.numeric(paf_draws)
  print("\n --- break --- ")
  print(paste(loc_id, main_dir, ver, spectrum.name,paf_draws,sep=" -- "))
  date <- Sys.Date()
  
}

#set file paths
sex <- paste0(main_dir,'/FILEPATH/sex_paf_gbd2023_iter.csv')
hiv_dir <- paste0(main_dir,'/hiv_inc_draws_',spectrum.name)
age_group_mapping <- paste0(main_dir,'/FILEPATH/age_mapping.csv')

#modeling years and ages
years <- c(1980:2024)
ages <- c(8:20,30:32,235)

#get hiv rr draws from the bop analysis
relative_risk <- get_draws("rei_id", 134, "rr", release_id = 16, downsample = T, n_draws = paf_draws)[cause_id==298 & age_group_id %in% ages] #csa rei is 134

 
# GET CSA EXPOSURE --------------------------------------------------------
print("getting draws!!!")
print(paste0("for loc ", loc_id))

csa_exp <- get_draws(gbd_id_type='modelable_entity_id', gbd_id=28695, location_id=loc_id,
                     sex_id=c(1,2), status='best', source='epi', age_group_id=ages,
                     year_id=years, measure_id=18, release_id=16, downsample = T, n_draws = paf_draws)
csa_exp <- csa_exp[,c('measure_id','metric_id','model_version_id','modelable_entity_id'):=NULL]

# melt to get draws long
csa_exp <- melt(csa_exp, id.vars=c('location_id','year_id','age_group_id','sex_id'))
setnames(csa_exp,'value','csa_exp')
csa_exp[,draw_num:=as.numeric(substr(variable,6,10))]
csa_exp[,variable:=NULL]

# GET PROPORTION HIV DUE TO SEX -------------------------------------------

print("getting hiv from sex")

#read and subset sex-only
sex_only <- read.csv(sex) %>% as.data.table
sex_only <- sex_only[location_id==loc_id,]

#interpolate prop_sex_only to get all years
for(age in ages) {
  for (s in c(1,2)) {
    subset <- sex_only[age_group_id==age & sex_id==s,c('year_id','sex_id','prop_sex_only')]
    int <- approx(y=subset$prop_sex_only, x=subset$year_id, xout=years, method='linear', rule=2)
    new <- data.table(location_id=loc_id, year_id=int[[1]], age_group_id=age, sex_id=s, prop_sex_only=int[[2]])
    
    if (unique(new$sex_id) != s) {
      print('error')
    }
    
    sex_only <- rbind(sex_only,new,fill=T)
    
  }
}
sex_only <- unique(sex_only, by=c('year_id','sex_id','age_group_id'))

#merge onto csa_exposure
dt <- merge(csa_exp, sex_only, by=c('location_id','year_id','age_group_id','sex_id'))


# GET HIV INCIDENT DRAWS --------------------------------------------------

print("getting hiv incident draws")

hiv_incidence <- read.csv(paste0(hiv_dir,'/',loc_id,'_hiv_incidence_draws.csv')) %>% as.data.table
hiv_incidence <- hiv_incidence[age_group_id >= 8,] # people 15 years or older
setnames(hiv_incidence,"inc250","inc0") # incidence draws were 1-1000 instead of 0-999

# melt to get draws long
hiv_incidence <- melt(hiv_incidence, id.vars=c('location_id','year_id','age_group_id','sex_id'))
setnames(hiv_incidence,'value','hiv_inc')
hiv_incidence[,draw_num:=as.numeric(substr(variable,4,10))]
hiv_incidence[,variable:=NULL]
hiv_incidence[is.na(hiv_inc), hiv_inc:=0]

# CALCULATE PAF -----------------------------------------------------------

print("paf and rr!")

#subset to rr_draws among exposed
#get rr among exposed; our risks are not age- or sex-specific, so just grab 1 of each
keep_cols <- c("location_id","year_id", paste0("draw_",0:(paf_draws-1)))
rr_draws <- copy(relative_risk[parameter == "cat1" & age_group_id == 8 & sex_id == 1][, ..keep_cols]) 

#make long
rr_draws <- melt(rr_draws, id.vars = c("location_id","year_id")) %>% 
  setnames("value", "rr") %>% 
  setnames("variable", "draw_num") %>%
  mutate(draw_num = as.numeric(stringr::str_replace_all(draw_num, "draw_", "")))

#drop extra columns (rrs not location or year specific)
rr_draws[, c("location_id", "year_id") := NULL]

# merge ipv exposure with relative risk
dt <- merge(dt, rr_draws, by='draw_num')

# calculate PAF on incidence  [Prevalence of CSA * (RR - 1)] / [Prevalence of CSA * (RR - 1) + 1]
dt[, paf:=csa_exp*(rr-1)/(csa_exp*(rr-1)+1)]

# merge PAF on HIV incidence
dt <- merge(dt, hiv_incidence, by=c('location_id','year_id','age_group_id','sex_id','draw_num'))

# apply proportion HIV incidence that is sexually transmitted and not from CSW to total HIV incidence
dt[, sexnoncsw_incidence := hiv_inc*prop_sex_only]

# merge on age_table to get age_start
age_table <- read.csv(age_group_mapping) %>% as.data.table
dt <- merge(dt,age_table,by='age_group_id')

# expand to get one year age groups
expand_age <- function(i){
  copy <- copy(dt)
  copy[,age_start:=age_start+i]
  return(copy)
}
dt <- rbindlist(lapply(0:4,expand_age))
dt[,age_group_id:=NULL]


# Denominator: cumulative incidence of all HIV
dt[,birth_yr:=year_id-age_start]
dt <- dt[order(draw_num,birth_yr,age_start)]
dt[,denominator:=1-cumprod(1-hiv_inc), by=c('draw_num','birth_yr','sex_id')]

# Numerator: cumulative incidence of HIV due to IPV
dt[,numerator:=1-cumprod(1-paf*sexnoncsw_incidence), by=c('draw_num','birth_yr','sex_id')]

# calculate PAF on prevalence of HIV due to IPV
dt[denominator>0,paf_prev:=numerator/denominator]
dt[denominator==0, paf_prev:=0]

# take averages over age to get 5 year age groups
round5 <- function(x) return(5*floor(x/5))
dt[,age_start:=round5(age_start)]
dt[,paf_prev:=mean(paf_prev),by=c('age_start','year_id','draw_num','sex_id')]
dt <- unique(dt, by=c('age_start','year_id','draw_num','sex_id'))

# reshape wide on draw
dt[,draw:=paste0("draw_",draw_num)]
dt <- dcast(dt, year_id+age_start+sex_id~draw, value.var='paf_prev')

# merge on information about age_group_id
dt <- merge(dt,age_table,by='age_start')
dt[,age_start:=NULL]

# duplicate, same paf for ylds (3) and ylls (4)
dt[,measure_id:=3]
dt_yll <- copy(dt)
dt_yll[,measure_id:=4]
dt <- rbind(dt,dt_yll)

# expand and add cause_id to prepare for save_results_risk (948-950 & 300)
expand_cause <- function(i){
  copy <- copy(dt)
  copy[,cause_id:=i]
  return(copy)
}
dt <- rbindlist(lapply(c(300,948:950),expand_cause))

print("saving!")

# save in country and sex specific files ready for upload
write.csv(dt, paste0(main_dir,"/paf_draws_",ver,"/",loc_id,".csv"),row.names=F)

# note time
time_end <- Sys.time()
print(time_end - time_start)

# END