#----HEADER-------------------------------------------------------------------------------------------------------------

# Project: RF: occ
# Purpose: Calculate PAFs from occ injuries

#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- commandArgs()[-(1:3)]  # First args are for unix use only
  
  if (length(arg)==0) {
    #toggle for targeted run on cluster
    arg <- c("133", #location
             23, #squeeze version
             7, #output version
             1000, #draws required
             20) #number of cores to provide to parallel functions
  }
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
  arg <- c("492", #location
           19, #squeeze version
           6, #output version
           1000, #draws required
           20) #number of cores to provide to parallel functions
  
}

# load packages, install if missing
pacman::p_load(data.table, fst, ggplot2, magrittr, readxl)

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

# set project values
location_set_version_id <- 149

# Set parameters from input args
country <- arg[1]
squeeze.version <- arg[2]
output.version <- arg[3]
draws.required <- as.numeric(arg[4])
cores.provided <- as.numeric(arg[5])

#settings
years <- c(1990, 1995, 2000, 2005, 2010, 2013, 2015, 2016) #removing unnecessary years, takes too long
relevant.ages <- c(8:20, 30) #only ages 15-80 (85+ = 0 participation)

#colnames
draw.cols <- paste0('draw_', 0:(draws.required-1))
death.cols <- paste0('cod_', 0:(draws.required-1))
paf.cols <- paste0('paf_', 0:(draws.required-1))

##in##
doc.dir <- file.path(home.dir, 'FILEPATH')
injury.causes <- file.path(doc.dir, 'FILEPATH') %>% fread
injury.causes <- injury.causes[keep==1] #drop pairs that have been IDed as nonsensical
injury.causes[, isic_code := major]
isic.map <- file.path(doc.dir, 'FILEPATH')
isic.3.map <- read_xlsx(isic.map, sheet = "ISIC_REV_3_1") %>% as.data.table
exp.dir <- file.path('FILEPATH', squeeze.version)


##out##
summary.dir <- file.path(home.dir, "FILEPATH", output.version)
paf.dir <- file.path('FILEPATH', output.version)
lapply(c(paf.dir, summary.dir), dir.create, recursive=T)
#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
central.function.dir <- file.path(h_root, "FILEPATH")
ubcov.function.dir <- file.path(j_root, 'FILEPATH')
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source
# other tools created by covs team for querying db (personal version)
file.path(central.function.dir, "db_tools.R") %>% source
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "FILEPATH") %>% source
# central functions
file.path(j_root, 'FILEPATH') %>% source
file.path(j_root, 'FILEPATH') %>% source
#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

#create a function and use it to follow just N draw(s) through the whole process
drawTracker <- function(varlist,
                        random.draws) {
  
  varlist[-random.draws] %>% return
  
}

subtract <- sapply(list(draw.cols, death.cols, paf.cols),
                   drawTracker,
                   random.draws=sample(draws.required, 3))
#***********************************************************************************************************************

#----CALC PAF-----------------------------------------------------------------------------------------------------------
#read in the squeezed injuries
all <- file.path(exp.dir, paste0(country, '_inj.fst')) %>% read.fst(as.data.table=T)
setkeyv(all, c('location_id', 'year_id', 'age_group_id', 'sex_id', 'isic_code'))
all <- unique(all, by=key(all)) #investigate error with 361/354 where there are identical duplicate rows

#now sum the total squeezed injuries by id variable in order to calculate the PAF
#PAF = total occ injuries / total injuries

#first get the relevant injuries
#read in this year file
relevant.causes <- unique(injury.causes$cause_id)
deaths <- get_draws(gbd_id_field=rep('cause_id', relevant.causes %>% length),
                    gbd_id=relevant.causes, source='codcorrect', location_ids=country,
                    year_ids=c(1970:2016),
                    measure_ids=1, status='best', num_workers=(cores.provided-1))

deaths <- deaths[age_group_id %in% relevant.ages] #subset post hoc as the age registriction is breaking in get draws
setnames(deaths, draw.cols, death.cols)

#combine with the selected cause pairs
deaths <- merge(deaths, injury.causes[, list(cause_id, cause_name, isic_code)], by='cause_id', allow.cartesian=TRUE)
pafs <- merge(deaths,  all[, c(key(all), draw.cols), with=F], by=key(all))

#sum over causes to get the total relevant injuries for an industry type (some have different denominator)
pafs[, (death.cols) := lapply(.SD, sum), .SDcols=death.cols, by=key(pafs)] #calculate totals

#calculate PAFs as total occ injuries / total injuries
pafs[, (paf.cols) := lapply(1:draws.required, function(draw) get(draw.cols[draw]) / get(death.cols[draw]))]

#now sum across the industries to get the cause specific PAF
setkeyv(pafs, c('location_id', 'year_id', 'sex_id', 'age_group_id', 'cause_id'))
pafs[, (paf.cols) := lapply(.SD, sum), .SDcols=paf.cols, by=key(pafs)] #calculate totals
pafs <- unique(pafs, by=key(pafs)) #no longer need industry specific values
pafs[, isic_code := NULL]

#now, cap the PAF to 85% using the average value seen in highest observed country with insurance reported data
#this is guatemala, so used the mean value for males 15-49 in 1990 as the maximum feasible PAF
#this is a fix for possible overestimation (paf>1) where the numerator (total injuries) is undercounted
#or where the denomator (total injuries based on the rates/industry composition) is being extrapolated too high
paf.cap <- .85
pafs[, paf_mean := rowMeans(.SD), .SDcols=paf.cols]
pafs[, residual := paf_mean - paf.cap]
pafs[residual > 0, shift := residual]
pafs[residual <=0, shift := 0]
pafs[, (paf.cols) := lapply(.SD, function(x) {x-shift}), .SDcols=paf.cols]

#calculate mean/CI for PAF
pafs[, paf_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=paf.cols]
pafs[, paf_mean := rowMeans(.SD), .SDcols=paf.cols]
pafs[, paf_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=paf.cols]

#now output values
for (year in c(1990, 1995, 2000, 2005, 2010, 2013, 2015, 2016)) {
  
  for (sex in c(1,2)) {
    
    out <- pafs[sex_id == sex & year_id == year, c(key(pafs), paf.cols), with=F]
    
    out[, measure_id := 18]
    out[, risk := "occ_inj"]
    
    write.csv(out, file= paste0(paf.dir, "/paf_yll_", country, "_", year, "_", sex, ".csv"))
    write.csv(out, file= paste0(paf.dir, "/paf_yld_", country, "_", year, "_", sex, ".csv"))
    
  }
  
}

summary <- pafs[, c(key(pafs),
                    names(pafs)[grep("lower|mean|upper", names(pafs))]), with=F]

write.csv(summary,
          file.path(summary.dir, paste0(country, '_paf.csv')))

#***********************************************************************************************************************