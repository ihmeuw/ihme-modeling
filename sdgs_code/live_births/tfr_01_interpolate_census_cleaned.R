################################################################################
## Purpose: Interpolate Single Year Age groups from census data for Intercensal Survival Process
################################################################################

################################################################################
### Setup
################################################################################

rm(list=ls())

# load packages 
if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, magrittr) # load packages and install if not installed


username <- ifelse(Sys.info()[1]=="Windows","[username]",Sys.getenv("USER"))
j <- ifelse("FILEPATH")
h <- ifelse("FILEPATH")

################################################################################
### Arguments 
################################################################################
locsetid <- 21


################################################################################
### Functions 
################################################################################
setwd("FILEPATH")
source("FILEPATH")


    
################################################################################
### Data 
################################################################################

census <- read_dta("FILEPATH") %>% as.data.table 
locs <- get_location_metadata(location_set_id = locsetid)



################################################################################
### Code 
################################################################################

#######################
## Step 1: Prep Census
#######################

census <- census[source_type == "CENSUS" & sex != "both"]
census <- melt.data.table(census, id.vars = grep("DATUM", names(census), value = T, invert = T), variable.name = "age_group_id", value.name = "census_population")
census[, age_group_id := gsub("DATUM", "", age_group_id)]
split <-  colsplit(census$age_group_id, split = "to", names = c("age_start", "age_end"))
census <- cbind(census, split)
census[, ':=' (age_start = as.numeric(as.character(age_start)), age_end = as.numeric((as.character(age_end))))]
census[, single_year := (age_start == age_end)]
census[!is.nan(census_population), covered := 1]
census[is.nan(census_population), covered := 0]

storage_locs <- copy(census[age_start == age_end & age_end %between% c(1,9), .(N = sum(covered)), by = .(ihme_loc_id, year, sex)][N ==9 , .(ihme_loc_id, year, no_interpolate = 1)])
storage_locs <- unique(storage_locs, by = c('ihme_loc_id', 'year'))

census <- census[!is.nan(census_population)]
census <- census[!is.na(age_start)]
census <- census[age_end > 0]
census <- census[, .(ihme_loc_id, year_id = year, year, sex, pop = census_population, age_group_name = age_group_id, age_start, age_end)]
census[, age := ((age_end + 1) + age_start)/2]

census <- merge(census, storage_locs, by = c("ihme_loc_id", "year"), all.x = T)
census[is.na(no_interpolate), no_interpolate := 0]
interpolate <- census[no_interpolate == 0]
five_yr_groups <- copy(interpolate)
interpolate[, pop := pop/((age_end+1)-age_start)] # setting pop of age to be interpolated to average value across five year age group

no_interpolate <- census[no_interpolate == 1]

## set bandwidth
bw <- 3

## create local polynomial function that deals with all years having NA
smooth <- function(p,a,b) {
    if (length(p) == sum(is.na(p))) {
        out <- list(x=as.numeric(1:79),y=as.numeric(rep(NA,79)))
        return(out)
    } else {
        locpoly(x=a[!is.na(p)],y=p[!is.na(p)],degree=1,bandwidth=b,
                gridsize=9,range.x=c(1,9)) 
    }
}


## do the local polynomial interpolation
setkey(interpolate,ihme_loc_id,year,sex)
c <- interpolate[,list(age=smooth(pop,age,3)$x,
                       interp_pop=smooth(pop,age,3)$y),
                 key(interpolate)]

## fix zeroes 
comp <- c(0.001,c$interp_pop[-length(c$interp_pop)])
comp2 <- c(0.001,comp[-length(comp)])
comp[comp <=0] <- 0.001
comp2[comp2 <=0] <- 0.001
c$interp_pop[c$interp_pop <= 0 & !is.na(c$interp_pop)] <- comp[c$interp_pop <= 0 & !is.na(c$interp_pop)] * (comp[c$interp_pop <= 0 & !is.na(c$interp_pop)]/comp2[c$interp_pop <= 0 & !is.na(c$interp_pop)])

## make the populations add up to the original 5-year age groups
c[, age_group_id := (age%/%5) + 5]
c[, interpolated_five_yr_pop := sum(interp_pop), by = .(ihme_loc_id, year, sex, age_group_id)]
c <- c[age_group_id <= 6]

five_yr_groups[age_start == 1 & age_end == 4, age_group_id := 5]
five_yr_groups[age_start == 5 & age_end == 9, age_group_id := 6]
five_yr_groups <- five_yr_groups[!is.na(age_group_id)]
five_yr_groups <- five_yr_groups[, .(ihme_loc_id, year, sex, pop, age_group_id)]

interpolated <- merge(c, five_yr_groups, by = c("ihme_loc_id", "year", "sex", "age_group_id"), all = T)
interpolated[, scaling_ratio:= pop/interpolated_five_yr_pop, by = .(ihme_loc_id, year, sex, age_group_id)]
interpolated[is.na(scaling_ratio), scaling_ratio := mean(scaling_ratio, na.rm = T), by = .(ihme_loc_id, year, sex)] 

interpolated[!is.na(scaling_ratio), interp_pop := interp_pop*scaling_ratio]

no_interpolate <- no_interpolate[age_start == age_end & age_start >= 1 & age_end <= 9]
no_interpolate[, age:= age_start]

census_interpolated <- rbindlist(list(interpolated[, .(ihme_loc_id, year_id = year, sex, age, pop = interp_pop)],
                                      no_interpolate[, .(ihme_loc_id, year_id = year, sex, age, pop)]), use.names = T, fill = T)


census <- merge(census_interpolated, locs[, .(ihme_loc_id, location_id)], by = "ihme_loc_id")
census[, sex_id := factor(sex, levels = c("male", "female"), labels = c(1,2))]
census <- census[, .(location_id, year_id, age, sex_id, census_population = pop)]

write.csv(census, "FILEPATH", row.names = F)

################################################################################ 
### End
################################################################################