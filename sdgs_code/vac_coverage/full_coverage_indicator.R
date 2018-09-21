###########################################################
### Purpose: Generate full coverage indicator for SDGs indicator 3.b.1
###
###
###		SDGs Indicator 3.b.1 corresponds to full vaccination coverage, or the 
###		percent of the population covered by all vaccines in the national schedule.
###		We calculate this as the geometric mean of 8 vaccines (dpt, mcv, bcg, polio, 
###		hepb, hib, pcv, rota), given their respective introduction for that country-year.
###########################################################

###################
### Setting up ####
###################
rm(list=ls())
pacman::p_load(data.table, dplyr)

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  user <- Sys.info()[["user"]]
  h <- paste0("FILEPATH", user)
}

## Initialize
source(paste0(j, 'FILEPATH/init.r'))

###########################################################################################################################
# 														setup
###########################################################################################################################

####################################################################
# Set up introduction frame to merge on to the draw frame; used to 
# set coverage to NA if the particular vaccine is not in the national
# schedule at for that country year. This is most relevant for 
# the new vaccines (HepB, Hib, PCV, Rota) prior to their introduction
# and for BCG where countries have removed it from the national schedule.
# For newer vaccines, we allow for 2 years of delay (following the year of 
# introduction) such that countries have time to ramp up the vaccination
# the geometric mean isn't too jagged.

## Set up intro frame
mes <- c("vacc_dpt3", "vacc_mcv1", "vacc_bcg", "vacc_polio3", "vacc_hib3", "vacc_hepb3", "vacc_pcv3", "vacc_rotac")
intro <- readRDS(paste0(data_root, "FILEPATH/vaccine_intro.rds"))
intro <- intro[me_name %in% mes]
intro <- intro[, me_name := paste0(me_name, "_intro")]
intro <- intro[grepl("CHN", ihme_loc_id) & grepl("hib|hepb|pcv|rota", me_name), cv_intro := 9999]
## Create cv_intro_years frame to represent the number of years
intro <- intro[, cv_intro_years := ifelse((year_id-(cv_intro-1))>=4, year_id-(cv_intro-1), 0)] ## 2 years of delay for vaccine intro
intro <- intro[!is.na(cv_outro), cv_intro_years := ifelse((cv_outro - year_id)>0, year_id - 1980 + 1, 0)]
## Reshape wide
intro.w <- dcast(intro, location_id + year_id ~ me_name, value.var='cv_intro_years')


#########################################################
# Function to calculate the geometric mean by location.
# Pulls draws from the draws_root folder, merges and sets
# introduction dates, then takes the geometric mean
# depending on the number of non-NA vaccinations.
# Function returns a summary mean, lower, upper
# or draws.

calc.geo_mean <- function(loc, draws=FALSE) {
## Load
mes <- c("vacc_dpt3", "vacc_mcv1", "vacc_bcg", "vacc_polio3", "vacc_hib3", "vacc_hepb3", "vacc_pcv3", "vacc_rotac")
    df.list <- lapply(mes, function(me) {
        path <- paste0(draws_root, "FILEPATH", me, "/", loc, ".csv")
        df <- fread(path)
        key <- c("location_id", "year_id", "age_group_id", "sex_id")
        df <- melt(df, id.vars=key, measure=patterns("^draw"), variable.name="draw",  value.name=me)
    })
## Merge
for (i in 1:length(mes)) {
    if (i == 1) {
       df <- df.list[[i]]  
    } else {
       df <- merge(df, df.list[[i]], by=c("location_id", "year_id", "age_group_id", "sex_id", "draw"))
    }
}
## Set intros
df <- merge(df, intro.w, by=c("location_id", "year_id"))
for (me in c("vacc_hepb3", "vacc_pcv3", "vacc_hib3", "vacc_rotac", "vacc_bcg")) {
    df <- df[, (me) := ifelse(get(paste0(me, "_intro")) < 1, NA , get(me))]
}
## Take geometric mean
df <- df[, n := Reduce(`+`, lapply(.SD,function(x) !is.na(x))), .SDcols = mes]
df <- df[, vacc_full := Reduce(`*`, lapply(.SD, function(x) ifelse(is.na(x), 1, x))), .SDcols = mes]
df <- df[, vacc_full := vacc_full ^ (1/n)]
## Reshape
if (draws) {
df.w <- dcast(df, location_id + year_id + age_group_id + sex_id ~ draw, value.var="vacc_full")
return(df.w) 
}
## Summary
if (!draws) {
key <- c("location_id", "year_id", "age_group_id", "sex_id")
df <- df[, mean := mean(vacc_full), by=key]
df <- df[, lower := quantile(vacc_full, 0.025), by=key]
df <- df[, upper := quantile(vacc_full, 0.975), by=key]
df.s <- df[, .(location_id, year_id, age_group_id, sex_id, mean, lower, upper)] %>% unique
return(df.s)
}
}

###########################################################################################################################
# 														main
###########################################################################################################################

## Calculate geometric mean and save
path <- paste0(draws_root, "FILEPATH")
unlink(path, recursive=TRUE)
dir.create(path)
locs <- get_location_hierarchy(location_set_version_id)[level>=3]$location_id
mclapply(locs, function(x) {
    df <- calc.geo_mean(x, draws=FALSE)
    write.csv(df, paste0(path, "/", x, ".csv"), na="", row.names=F)
}, mc.cores=10)