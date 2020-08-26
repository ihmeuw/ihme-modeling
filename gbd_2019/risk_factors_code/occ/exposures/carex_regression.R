# ---HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Prep carex database and regress using SDI to predict for all countries
#********************************************************************************************************************************

# ---CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# load packages
pacman::p_load(data.table, gridExtra, ggplot2, lme4, magrittr, parallel, stringr, zoo, ini, openxlsx)
library(fst, lib.loc = "FILEPATH")

# set working directories
home.dir <- "FILEPATH"
  setwd(home.dir)

#set values for project
location_set_version_id <- 443
year_start <- 1970
year_end <- 2019
by_sex <- 0
by_age <- 0
relevant.ages <- c(8:18) #only ages 15-69
decomp <- "step4"

# versioning
carex.version <- 4 # gbd 19 final

#list of carcinogens currently in GBD
#formatted to match the  names in the CAREX db
carc.list <- c("Arsenic and arsenic compounds",
               'Bis(chloromethyl)ether and chloromethyl methyl ether (techn. grade)',
               'Benzene',
               "Beryllium and beryllium compounds ",
               '1,3-Butadiene',
               "Cadmium and cadmium compounds ",
               'Chromium VI compounds',
               'Cobalt and its compounds',
               'Diesel engine exhaust',
               'Ethylene oxide',
               "Formaldehyde",
               'Ionizing radiation',
               'Lead and lead compounds, inorganic',
               'Nickel compounds ',
               'Polychlorinated biphenyls (PCB)',
               "Polycyclic aromatic hydrocarbons (excl. environmental tobacco smoke)",
               "Silica, crystalline",
               "Strong-inorganic-acid mists containing sulfuric acid (occup. exp. to)",
               'Tetrachloroethylene',
               'Trichloroethylene')

##in##
data.dir <- file.path(home.dir, "FILEPATH")
cov.dir <- file.path(home.dir, "FILEPATH")
cw.dir <- file.path(home.dir, "FILEPATH")
doc.dir <- file.path(home.dir, "FILEPATH")
  isic.map <- file.path(doc.dir, "FILEPATH")
    isic.3.map <- read.xlsx(isic.map, sheet = 2) %>% as.data.table
exp.dir <- file.path(home.dir, "FILEPATH")
in.dir <- "FILEPATH"

##out##
out.dir <- file.path("FILEPATH", carex.version)
if(!dir.exists(out.dir)) dir.create(out.dir, recursive = TRUE)
#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
ubcov.function.dir <- "FILEPATH"
file.path(ubcov.function.dir, "FUNCTION") %>% source
# central functions
file.path("FUNCTION") %>% source
file.path("FUNCTION") %>% source

#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

inv.logit <- function(x) {
  return(exp(x)/(exp(x)+1))
}

logit_offset <- function(x, offset) {

  x_len = length(x)

  value <- vector(mode="numeric", length=x_len)

  for (i in 1:x_len) {

    if (x[i]==1) {
      value[i] <- x[i] - offset
    } else if (x[i]==0)  {
      value[i] <- x[i] + offset
    } else value[i] <- x[i]

  }

  return(log(value/(1-value)))
}
#***********************************************************************************************************************

#***********************************************************************************************************************

# ---PREP CAREX---------------------------------------------------------------------------------------------------------
if (Sys.info()["sysname"] != "Linux") { #linux has trouble reading the odbc file due to dependancy
  #working with carex
  carex <- file.path(data.dir, 'cxv2.mdb') %>% odbcConnectAccess2007 #reading in the db
  #getting names of all tables
  table.list <- sqlTables(carex, tableType = 'TABLE')$TABLE_NAME

  #start prepping the table for regression
  #prep carc database and subset to the carcinogens of concern
  carc.dt <- sqlFetch(carex, 'carcinogen') %>% as.data.table
    carc.dt <- carc.dt[carcinogen %in% carc.list] #subset to the carcs relevant to GBD

  #prep countries
  country.dt <- sqlFetch(carex, 'country') %>% as.data.table
    country.dt[country %like% "1990-93", year_id := 1992]
    country.dt[country %like% "1997", year_id := 1997]
    country.dt[country %like% "1981-83", year_id := 1982]
    country.dt[, ihme_loc_id := cocode]
    country.dt[ihme_loc_id=="US", ihme_loc_id := "USA"] #removing 92 USA data as it has been unchecked
    country.dt[ihme_loc_id=="LAT", ihme_loc_id := "LVA"]
    country.dt[ihme_loc_id=="LIT", ihme_loc_id := "LTU"]

  #prep industries and create aggregates at ISIC2 level
  ind.dt <- sqlFetch(carex, 'cindustry') %>% as.data.table
  ind.dt[, industry := substr(cicode, 1, 1)]

  #prep employment (denominator)
  #note that it is missing for a couple of USA years/industries
  #will use second USA estimate where available
  #for two that are missing both, use DEU estimate (closest in size to USA)
  denom.dt <- sqlFetch(carex, 'ciemployment') %>% as.data.table
    denom.dt[, cicode := trimws(cicode)] #remove whitespace issues
    denom.dt[cocode %like% 'US', impute := max(employment, na.rm=T) %>% as.integer, by='cicode']
    denom.dt[cicode %in% c("91", "96") & is.na(employment) & is.na(impute),
             impute := denom.dt[cicode %in% c("91", "96") & cocode=="DEU", employment]]
    denom.dt[is.na(employment), employment := impute]


  #pull the estimates of exposed workers
  estimate <- sqlFetch(carex, 'estimate') %>% as.data.table

  #merge all tables together
  dt <- merge(estimate, carc.dt[, list(ccode, carcinogen, unit)], by='ccode')
  dt <- merge(dt, ind.dt[, list(cicode, cindustry, industry)], by='cicode')
  dt <- merge(dt, denom.dt[, list(cocode, cicode, employment)], by=c('cocode', 'cicode'))
  dt <- merge(dt, country.dt[, list(cocode, ihme_loc_id, year_id, country)], by='cocode')

  #take the mean of the 3
  dt[, exposed := rowMeans(.SD, na.rm=T), .SDcols=c('est1', 'est2', 'est3')]

  #convert to rates using employment
  dt[, industry_total := sum(employment), by=c('cocode', 'year_id', 'ccode', 'industry')]
  dt[, prop := employment/industry_total]
  dt[, data := est3/employment] #using average rate
  dt[is.na(data), data := est2/employment]
  dt[is.na(data), data := est1/employment]
  dt[, agg_data := weighted.mean(data, weight=prop, na.rm=T), by=c('cocode', 'year_id', 'ccode', 'industry')]

  write.fst(dt, path=file.path(in.dir, 'carex_prepped.fst'))
  write.fst(carc.dt, path=file.path(in.dir, 'carex_carc_dt.fst'))

} else {

  dt <- file.path(in.dir, 'carex_prepped.fst') %>% read.fst(as.data.table=TRUE,old_format = T)
  carc.dt <- file.path(in.dir, 'carex_carc_dt.fst') %>% read.fst(as.data.table=TRUE,old_format = T)

}
#***********************************************************************************************************************

# ---PREP COVS----------------------------------------------------------------------------------------------------------
#prep covariates from ihme db
#get levels
locs <- get_location_hierarchy(location_set_version_id)
levels <- locs[,grep("level|location_id|ihme_loc_id", names(locs)), with=F]

# get the covariates to test
sdi <- get_covariate_estimates(881, decomp_step = decomp)
sdi <- sdi[, .(location_id, year_id, mean_value)]
setnames(sdi, "mean_value", "sdi")

prop_urban <- get_covariate_estimates(854, decomp_step = decomp)
prop_urban <- prop_urban[, .(location_id, year_id, mean_value)]
setnames(prop_urban, "mean_value", "prop_urban")

covs <- merge(sdi, prop_urban, by=c("location_id", "year_id"))

secondhand_smoke <- get_covariate_estimates(1183, decomp_step = decomp)
secondhand_smoke <- secondhand_smoke[, .(location_id, year_id, age_group_id, sex_id, mean_value)]
setnames(secondhand_smoke, "mean_value", "secondhand_smoke")

covs <- merge(secondhand_smoke, covs, by=c("location_id", "year_id"))

#popweight secondhand smoke for workers
## then get pop
#read in population using central fx
pops <- get_population(location_set_version_id=location_set_version_id,
                       location_id = unique(covs$location_id),
                       decomp_step = decomp,
                       year_id = unique(covs$year_id),
                       sex_id = c(1,2,3), #pull all sexes
                       age_group_id = c(8:18)) #pull only 15-69

covs <- merge(covs, pops, by=c('location_id', 'year_id', 'age_group_id', 'sex_id'))
covs[, shs := weighted.mean(secondhand_smoke, w=population), by=c('location_id', 'year_id')]
covs <- unique(covs, by=c('location_id', 'year_id')) #collapse back to country year
covs[, c('age_group_id', 'sex_id') := list(201, 3)]

#merge covs
dt <- merge(dt, levels, by="ihme_loc_id")
dt <- merge(dt, covs, by=c('location_id', 'year_id'))

#ETS modelled separately
ets.dt <- dt[ccode=="ETS"]
dt <- dt[ccode!="ETS"] #ETS modelled separately

duplicateCarc <- function(carc, dt) {

  message('duplicating for carcinogen=', carc)
  out <- copy(dt)
  out[, carcinogen := carc]
  return(out)

}

all <- lapply(carc.list, duplicateCarc, dt=covs) %>% rbindlist
all <- merge(all, carc.dt[, list(carcinogen, ccode)], by="carcinogen")
all[, ccode := as.factor(ccode)] %>% setkey(location_id)
all <- merge(all, levels, by="location_id")

#***********************************************************************************************************************
# ---REGRESS------------------------------------------------------------------------------------------------------------
indLoop <- function(this.industry, this.cov, mod.dt, pred.dt) {

  message('modelling relationship of exposed workers to ', this.cov, ' for industry #',
          this.industry,
          "\n ~> ",
          mod.dt[industry==this.industry, cindustry] %>% unique)

  if (this.industry=="5") { #borrow strength from similar occs for construction/retail

    industry.pair <- "2"

  } else if (this.industry=="6") {

    industry.pair <- "7"

  } else industry.pair <- NA

  model.data <- mod.dt[industry %in% c(this.industry, industry.pair)]


  mod <- lmer(log(agg_data+0.001) ~ get(this.cov) + as.factor(ccode) + (1|level_2), data=model.data)
    summary(mod)


  out <- copy(pred.dt)
  out[, isic_code := this.industry]

  coefficients <- as.data.table(coef(summary(mod)), keep.rownames = T)
  main.effect <- coefficients[rn=="get(this.cov)", Estimate]

  message('predict based on a ', this.cov, ' effect of: ', main.effect)

  out[, value := predict(mod, out[.BY], allow.new.levels=T, re.form=NA) %>% exp, by=c('location_id')]
  out[value>1, value := 1] #cap values at 100% exposure

  return(out)

}

predictions <- mclapply(unique(dt$industry), indLoop,
                        this.cov="sdi", mod.dt=dt, pred.dt=all, mc.cores=cores) %>% rbindlist(use.names=T)

write.fst(predictions, path=file.path(out.dir, 'exposure_rates_raw.fst'))
#***********************************************************************************************************************

# ---OUTPUT-------------------------------------------------------------------------------------------------------------
#crosswalk to isic v3 and then cleanup/output the values
setnames(predictions, 'value', 'exposure_rate')

#crosswalk to isic3
#crosswalk ISIC 2 to ISIC 3
predictions[, major_isic_2 := as.numeric(isic_code)]
#calculate the count of categories to inform uncertainty around splits
isic2.map <- file.path(cw.dir, "ISIC_2_TO_3_LVL_1_CW.csv") %>% fread
predictions <- merge(predictions, isic2.map[, list(major_isic_2, major_isic_3, weight)],
                     by="major_isic_2", allow.cartesian = T)

#now use the proportion to xwalk each ISIC2 category to ISIC3, then collapse
predictions[, split_rate:= weighted.mean(exposure_rate, weight),
            by=c('major_isic_3', 'location_id', 'year_id', 'sex_id', 'age_group_id', 'ccode')]
predictions <- unique(predictions, by=c('major_isic_3', 'location_id', 'year_id', 'sex_id', 'age_group_id', 'ccode'))
predictions[, isic_code := major_isic_3]

predictions[!is.na(split_rate), exposure_rate := split_rate]
predictions[, isic_version := "ISIC3"]

#save predictions
write.fst(predictions[, list(location_id, ihme_loc_id, year_id, age_group_id, sex_id, ccode, carcinogen, isic_version, isic_code, exposure_rate)],
       path=file.path(out.dir, 'exposure_rates.fst'))
message("DONE! results saved to FILEPATH", carex.version)
