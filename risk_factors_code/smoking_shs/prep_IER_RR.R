#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Estimate cigarettes per smoker, then secondhand PM2.5 per smoker (run by 01_launch_IER_RR.R whenever smoking inputs change)
# Run:     source("FILEPATH/prep_IER_RR.R", echo=T)
#***********************************************************************************************************************
 

#----PREP---------------------------------------------------------------------------------------------------------------
# bring in locations
locations <- get_location_metadata(gbd_round_id=4, version_id=149)
location.ids <- unique(locations[, location_id]) 

# set ages 
ages <- c(8:20, 30:32, 235)

# results of RR curve fitting analysis
# parameters that define these curves are used to generate age/cause specific RRs for a given exposure level
rr.dir <- file.path(j_root, "FILEPATH", paste0(rr.data.version, rr.model.version))

out.environment <- file.path(home.dir, "GBD2016/RR/data", paste0(output.version, "_clean.Rdata")) # this file will be read in by each parallelized run in order to preserve draw covariance
# objects exported:
# SHS.global.exp = file pulled from covariate database and used to estimate the secondhand smoke exposure (in PM2.5) by country/year
# age.cause - list of all age-cause pairs currently being calculated
# all.rr - compiled list of all the RR curves for the ages/causes of interest

# make a list of all cause-age pairs that we have.
age.cause <- ageCauseLister(full.age.range=T, gbd.version) 

# prep the RR curves into a single object, so that we can loop through different years without hitting the files extra times.
all.rr <- lapply(1:nrow(age.cause), prepRR, rr.dir=rr.dir)

# first - save the variables you will be iterating over as vectors
id.varnames <- c("location_id", "year_id", "age_group_id", "sex_id")
draw.colnames <- paste0("draw_", 0:(draws.required - 1))
cig.colnames <- paste0("cigs_", 0:(draws.required - 1))
#***********************************************************************************************************************
 

#----POPULATION---------------------------------------------------------------------------------------------------------
# INPUT 1: Population
# pull in population (used to weighted collapse smoking prev to country for merge with cig_pc)
pop <- get_population(location_id=location.ids, age_group_id=ages, sex_id=1:2, year_id=1980:2016, gbd_round_id=4)
# estimate the population above 10 years old (smokers)
pop.smoking <- pop[age_group_id %in% ages, possible_smokers := sum(population), by=c("location_id", "year_id")]
# collapse to country year to generate total smoking pop at country year
setkey(pop.smoking, "possible_smokers")
pop.smoking <- unique(pop.smoking[age_group_id %in% ages, c(id.varnames[1:2], "possible_smokers"), with=F])
#***********************************************************************************************************************
 

#----SMOKER PREV--------------------------------------------------------------------------------------------------------
# INPUT 2: Smoking Prevalence
# pull in most updated smoking prevalence
smoker.prev1 <- get_draws("modelable_entity_id", 8941, "epi", location_ids=location.ids, year_ids=1980:2016, gbd_round_id=4, sex_id=1:2, 
          age_group_id=ages, status="best")
# then collapse it down to all age/sex

# merge to population in order to calculate the number of smokers in each age/sex group
smoker.prev <- merge(smoker.prev1, pop, by=id.varnames)

# calculate the number of smokers in each age group using population and smoker prevalence
smoker.prev[, (draw.colnames) := lapply(.SD, function(x) x * population), 
            .SDcols=draw.colnames]

# now collapse this to the country/year level, which is where we will know total cigs
smoker.prev[, (draw.colnames) := lapply(.SD, function(x) sum(x)), 
            .SDcols=draw.colnames, by=c(id.varnames[1:2])]
#***********************************************************************************************************************
 

#----CIGS PER CAPITA----------------------------------------------------------------------------------------------------
# INPUT 3: Cigarettes per capita
# pull in most updated cigs_pc
cig.pc <- get_covariate_estimates(covariate_name_short="cigarettes_pc", location_id=location.ids)
cig.pc <- cig.pc[, list(location_id, year_id, mean_value)]
cig.pc[, (draw.colnames) := mean_value]
cig.pc[, mean_value := NULL]

# merge the number of possible smokers to the cigarettes per capita @ country year level
cig.pc <- merge(cig.pc, pop.smoking, by=id.varnames[1:2])

# calculate the total number of cigarettes in the country using cigs_pc and # possible smokers
cig.pc[, (draw.colnames) := lapply(.SD, function(x) x * possible_smokers), 
       .SDcols=draw.colnames]
# change name to prep for merge
setnames(cig.pc, draw.colnames, cig.colnames)
#***********************************************************************************************************************
 

#----CIGS PER SMOKER----------------------------------------------------------------------------------------------------
# merge the total number of smokers in each country year to total cigarettes in the same
all.data <- merge(smoker.prev[, c(id.varnames[1:2], draw.colnames), with=F], 
                  cig.pc[, c(id.varnames[1:2], cig.colnames), with=F], 
                  by=id.varnames[1:2])

# now collapse to country year, as there are still extra rows for age/sex
setkeyv(all.data, id.varnames[1:2])
all.data <- all.data %>% unique

# calculate the daily cigarettes per smoker as total cigarettes / total smokers / 365
all.data[, (draw.colnames) := lapply(draws.required, 
                                     function(draw) get(cig.colnames[draw]) / get(draw.colnames[draw]) / 365)]
#***********************************************************************************************************************
 

#----PM PER SMOKER------------------------------------------------------------------------------------------------------
# set study cigarettes per smoker from which to derive ratios
study.cigs.ps.semple <- (13.65175+13.74218+13.74416+13.64030+13.52084)/5 # using the average 2009-2013 Scotland cigarettes per smoker, as these are the years of the studies used in Semple's 2014 paper where we have drawn the distribution of PM2.5 from SHS)

# calculate draws of the PM2.5 per cigarette using information from Semple et al (2014)
log.sd <- (log(111)-log(31))/qnorm(.75) # formula calculates the log SD from Q3 and Median reported by Semple and assumption of lognormal
log.se <- log.sd/sqrt(93) #divide by the sqrt of reported sample size to get the SE
log.median <- log(31)
pm.cig.semple <- exp(rnorm(draws.required, mean = log.median, sd = log.se)) / study.cigs.ps.semple # using a lognormal distribution to convert draws based on median into mean, then divide by cigarettes per smoker in the study to get pm per cigarette

# now apply draws of sidestream PM per cigarette to draws of cigarettes per smoker
all.data[, (draw.colnames) := lapply(1:draws.required, 
                                     function(draw) get(draw.colnames[draw]) * pm.cig.semple[draw]), with=F]

# rename for clarity when importing to the next script (also cut out intermediate vars)
SHS.global.exp <- all.data[, c(id.varnames[1:2], draw.colnames), with=F] 

# write this csv to the shared drive for later use
fwrite(SHS.global.exp, file.path(home.dir, "FILEPATH/cigs_ps_db.csv"))
#***********************************************************************************************************************
 

#----OUTPUT-------------------------------------------------------------------------------------------------------------
# now melt it so draws are long
SHS.global.exp <- melt(SHS.global.exp, 
                       id              = id.varnames[1:2], 
                       variable.name   = "draw",
                       value.name      = "exposure",
                       variable.factor = F)

# leave draw variable as just the number
SHS.global.exp[, draw := as.data.table(str_split_fixed(draw, fixed("_"), 2)[,2])]
SHS.global.exp[, draw := as.numeric(draw) + 1] # this step is done to convert from the weird draw numbering of 0-999 vs 1-1000

# finally create draws of the non-IER (i.e. binary) RRs using the literature values
set.seed(0311)
ot.genesis <- rnorm(draws.required, mean=1.37, sd=((1.50-1.25)/3.92))
breast.cancer.draws <- rnorm(draws.required, mean=1.07, sd=((1.13-1.02)/3.92))
diabetes.draws <- rnorm(draws.required, mean=1.34, sd=((1.56-1.15)/3.92))

# save the clean environment with all necessary files
save(SHS.global.exp,
     ot.genesis,
     breast.cancer.draws,
     diabetes.draws,
     all.rr,
     age.cause,
     file=out.environment)
#***********************************************************************************************************************