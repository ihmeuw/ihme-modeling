################################################################################
## Purpose: Ages forward births implied by vital statistics data, compares to census, to adjust data for completeness.
################################################################################

################################################################################
### Setup
################################################################################

rm(list=ls())

# load packages 
if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, magrittr, haven, KernSmooth, reshape) # load packages and install if not installed
 

username <- ifelse(Sys.info()[1]=="Windows","[username]",Sys.getenv("USER"))
j <- "FILEPATH"
h <- "FILEPATH"

################################################################################
### Arguments 
################################################################################
locsetid <- 21
max_age <- 110

direct_from_births <- NA
asfr_available <- NA
asfr_unavailable <- NA
  
################################################################################ 
### Paths for input and output 
################################################################################

mortdir <- paste0("FILEPATH") #most recent results

tfrdir <- paste0("FILEPATH")

birthdir <- paste0("FILEPATH")

unwppdir <- paste0("FILEPATH")



################################################################################
### Functions 
################################################################################
source("FILEPATH")

setwd("FILEPATH")
source("FILEPATH")
source("FILEPATH")



################################################################################
### Data 
################################################################################
ages <- as.data.table(get_age_map(type="lifetable"))
ages_all <-  as.data.table(get_age_map(type="all"))
locs <- get_location_metadata(location_set_id = locsetid)

census <- fread("FILEPATH")

tfr <- fread("FILEPATH") # pre data adjustment predictions of TFR
who_excluded <- fread("FILEPATH")
who_excluded <- merge(who_excluded, locs[, .(location_id, ihme_loc_id)], by = "location_id")
who_excluded <- who_excluded[.(location_id, ihme_loc_id, year_id = year, id, tfr)]

wpp <- read_dta("FILEPATH") %>% as.data.table


temp_mort <- fread("FILEPATH")
mort <- copy(temp_mort)

temp_births <- read_dta("FILEPATH") %>% as.data.table
births <- copy(temp_births)


################################################################################
### Code 
################################################################################

#######################
## Scale UN POP ASFR for national data to TFR
#######################

tfr <- tfr[!is.na(tfr) & vital == T, .(ihme_loc_id, location_id, year_id = year, id, tfr)]
tfr <- rbindlist(list(tfr, who_excluded), use.names = T, fill = T)

tfr <- tfr[!ihme_loc_id %like% "_"] 

wpp <- wpp[year <= 2016, .(year_id = year, age_group_id = (age/5)+5, ihme_loc_id = iso3, asfr)]
wpp[ihme_loc_id == "CHN", ihme_loc_id := "CHN_44533"]
wpp[ihme_loc_id == "HKG", ihme_loc_id := "CHN_354"]
wpp[ihme_loc_id == "MAC", ihme_loc_id := "CHN_361"]

tfr <- merge(wpp, tfr, by = c("ihme_loc_id", "year_id"), all.y = T)

no_wpp_asfr <- tfr[is.na(asfr), .(ihme_loc_id, year_id, location_id, id, tfr)] #separating out Taiwan, Greenland, and Bermuda, which are not estimated by UNPOP

tfr <- tfr[!is.na(asfr)]

tfr[, wpp_tfr := 5*sum(asfr), by = .(ihme_loc_id, location_id, year_id, id)]
tfr[, scaling_factor := tfr/wpp_tfr]
tfr[, asfr := asfr*scaling_factor]
tfr[, c("scaling_factor", "wpp_tfr", "tfr") := NULL]

tfr <- tfr[, .(asfr = mean(asfr)), by = .(location_id, year_id, age_group_id)] 
mex <- copy(tfr)

#######################
##  Pare down fertility to appropriate location-age-sex aggregates
#######################

mort <- mort[location_id %in% unique(mex$location_id) & sex_id != 3, .(location_id, year_id = year, age_group_id, sex_id, mx, lx)]

#######################
##  Calculate Single-age Mortality Rates
#######################
ages[,age:=as.numeric(age_group_name_short)]
mort <- merge(mort, ages[,list(age, age_group_id)], by="age_group_id")

# store mort before interpolation for later use
mort_store <- mort


# interpolate to get lx at each single age 
bw <- 3

## create local polynomial function that deals with all years having NA
smooth <- function(p,a) {
    sp <- splinefun(x=a[!is.na(p)],y=p[!is.na(p)], method="hyman")
    output <- sp(seq(0,(max(1:(max(a[!is.na(p)]))))))
    return(output)
}

mort <- mort[order(location_id, year_id, sex_id, age)]
setkey(mort, location_id, sex_id, year_id)


mort <- mort[location_id != 135]

for (loc in unique(mort$location_id)) {
    
    loc <- 135
    ctry <- mort[location_id == loc]
    print(loc)
    
    for (yr in unique(ctry$year_id)) {
        
        print(yr)
        cyear <- ctry[year_id == yr]
        cyear <- cyear[,list(age= seq(0,max_age),     
                           lx=smooth(lx,age)),
                     key(ctry)]
        
    }

}

mort <- mort[,list(age= seq(0,max_age),     
                   lx=smooth(lx,age)),
             key(mort)]

# convert from lx to qx
mort <- mort[order(location_id, year_id, sex_id, age)]
setkey(mort,location_id, sex_id,year_id)
mort <- mort[, px:= 1-c(-diff(lx), -1*lx[length(lx)])/lx, by=key(mort)] 
mort[age==max_age,px:=0]

# keeping only ages of interest
mort <- mort[age <= 10]
mort[, lx := NULL]
mort[, birth_year := as.numeric(as.character(year_id)) - age]
mort <- dcast.data.table(mort, location_id + sex_id + birth_year ~ age, value.var = "px")
setnames(mort, grep("[[:digit:]]", names(mort), value = T), paste0("px_", grep("[[:digit:]]", names(mort), value = T)))
#######################
## Step 2: Compute Implied Births from VR Data
#######################

pops <- get_population(location_id = unique(mex$location_id), age_group_id = c(8:14), sex_id = 2, location_set_id = 21, year_id = unique(mex$year_id))  

births <- births[,.(location_id, year_id = year, sex, births)]
births <- dcast.data.table(births, location_id + year_id ~ sex, value.var = "births")
sex_ratios <- births[, .(m_ratio = male/both, f_ratio = female/both), by = .(location_id, year_id)]

data <- merge(mex, pops, by = c("location_id", "age_group_id", "year_id")) 
data <- data[, .(total_births = sum(population*asfr)), by = .(location_id, year_id)]

data <- merge(data, sex_ratios, by = c("location_id", "year_id"))
data[, ':=' (male_births = total_births * m_ratio, female_births = total_births * f_ratio)]
data[, c("m_ratio", "f_ratio", "total_births") := NULL]
data <- melt.data.table(data, id.vars = c("location_id", "year_id"), variable.name = "sex_id", value.name = "pop_0")
data[, sex_id := factor(sex_id, levels = c("male_births", "female_births"), labels = c(1,2))]


#######################
## Step 3: Age births till census years using current mortality rates
#######################
#COHORT TRENDS ARE WIDE
setnames(data, "year_id", "birth_year")
data <- merge(data, mort, by = c("location_id", "birth_year", "sex_id"))

years_to_age <- 11

prob_survival <- paste0("px_", seq_len(years_to_age)-1)

survival <- paste0("pop_", seq(0,11))

for (yr in seq_len(years_to_age)) {
    
    set(data, i = NULL , survival[yr+1], data[[prob_survival[yr]]] * data[[survival[yr]]])
    print(data[[prob_survival[yr]]])
    data[, paste0("px_", yr-1):= NULL]
}

#######################
## Step 4: Reshape data set into year specific groups
#######################
data <- melt.data.table(data, id.vars = c("location_id", "birth_year", "sex_id"), variable.name = "years_aged", value.name = "num_survived")
data[, years_aged := as.numeric(gsub("pop_", "", years_aged))]
data[, year_id := birth_year + years_aged]

#######################
## Step 4: Collapse age-groups to compare with five year bins in census
#######################

data <- data[years_aged %between% c(1,9)]
full_groups <- data[!is.na(num_survived), .N, by = .(year_id, sex_id)][N == max(N), full := 1]
data <- merge(data, full_groups[, .(year_id, full, sex_id)], by = c("year_id", "sex_id"))
data <- data[full == 1]
data[years_aged <= 4, age_group_id := 5]
data[years_aged >4, age_group_id := 6]

data <- data[, .(num_survived = sum(num_survived)), by = .(location_id, year_id, age_group_id, sex_id)]

census <- melt.data.table(census, id.vars = grep("DATUM", names(census), value = T, invert = T), variable.name = "age_group_id", value.name = "census_population")
census <- census[!is.nan(census_population)]
census[, age_group_id := gsub("DATUM", "", age_group_id)]
split <-  colsplit(census$age_group_id, split = "to", names = c("age_start", "age_end"))
census <- cbind(census, split)
census <- census[sex != "both" & ((age_start == 1 & age_end == 4) | (age_start == 5 & age_end == 9))]
census[,age_group_id := (as.integer(as.character(age_end)) %/% 5) + 5]
census <- merge(census, locs[, .(ihme_loc_id, location_id)], by = "ihme_loc_id")
census[, sex_id := factor(sex, levels = c("male", "female"), labels = c(1,2))]
census <- census[, .(location_id, year_id = year, age_group_id, sex_id, census_population)]

data <- merge(data, census, by = c("location_id", "year_id", "age_group_id", "sex_id"))

#######################
## Step 5: Assess difference between implied population and VR
#######################

### Add up populations by census, and compute ratio of implied population to census population
data <- data[, .(num_survived = sum(num_survived), census_population = sum(census_population)), by = .(location_id, year_id)]
data[, implied_to_census_ratio := num_survived/census_population]

write.csv(data, "FILEPATH")

################################################################################ 
### End
################################################################################