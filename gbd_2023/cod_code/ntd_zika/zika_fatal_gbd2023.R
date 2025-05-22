#### Zika fatal
# Produced fatal estimates for Zika for GBD 2023
# Use death data and incidence from gbd2020 to calculate the global CFR 
# CFR is then applied to the age-sex specific incidence model

### ----------------------- Set-Up ------------------------------
rm(list=ls())

source("FILEPATH")

user <- Sys.info()[["user"]] ## Get current user name

library(data.table)
library(ggplot2)
library(dplyr)
library(data.table)

release_id = ADDRESS

zika_nf_mvid = ADDRESS # model_version_id for the zika incidence/non-fatal model 

### ----------------------- Consolidate Data ------------------------------
# import death data 
zika_deaths_input_new<- get_bundle_version(ADDRESS)

setnames(zika_deaths_input_new, "cases", "deaths")

zika_deaths_input_new <- subset(zika_deaths_input_new, seq!=39)  # need to double check this

# Calculating at the most granular level
grs <- fread("FILEPATH")

# Identify endemic locations (those that are considered endemic for at least one year)
endemic_locs <- unique(grs[value_endemicity == 1 & most_detailed == 1, location_id]) 

zika_incidence <- get_model_results('epi', 
                                    gbd_id = ADDRESS,
                                    location_set_id = 35,
                                    location_id = endemic_locs,
                                    sex_id = c(1,2),
                                    age_group_id = 22,
                                    measure = 6,
                                    model_version_id = zika_nf_mvid,
                                    year_id = c(2014:2022),
                                    release_id = release_id)

pops <- get_population(age_group_id = 22,
                       location_id = endemic_locs,
                       year_id = c(2014:2024),
                       sex_id = c(1,2),
                       release_id = release_id)

zika_incidence <- merge(zika_incidence, 
                        subset(pops, select = c("age_group_id", "location_id", "year_id", "sex_id", "population")), 
                        by= c(c("age_group_id", "location_id", "year_id", "sex_id")),
                        all.x = TRUE,
                        all.y = FALSE)

setnames(zika_incidence, "mean", "incidence")
zika_incidence[, inc_cases := incidence * population]

# aggregate to both sexes
zika_incidence <- zika_incidence[, .(inc_cases = sum(inc_cases), population = sum(population)), 
                                 by = .(location_id, year_id, age_group_id, modelable_entity_id, measure, measure_id, model_version_id)]
zika_incidence[, sex_id := 3]

# drop Brazil national data
zika_deaths <-   subset(zika_deaths_input_new, location_id !=135)

zika_deaths[, year_id := year_start]
zika_deaths[, year_start := NULL]
zika_deaths[, year_end := NULL]

# merge death data with incidence data (i.e., non-fatal data)
draws_deaths<-merge(zika_incidence,zika_deaths, by=c("location_id","year_id"), all.x=T)

# ad hoc adding 2015 deaths to Rio grande do Norte, Maranhao, and Para 
draws_deaths[(location_id == 4763 | location_id == 4769 | location_id == 4759) & year_id ==2015, deaths :=1]

draws_deaths <- subset(draws_deaths, inc_cases > 0)
draws_deaths[ is.na(deaths), deaths :=0]

locations_zika <- unique(draws_deaths$location_id)

### ----------------------- Calculate global CFR ------------------------------
# for now subsetting only year 2015 -2017
cfr_df <- subset(draws_deaths, year_id >= 2015 & year_id <= 2017 )

cfr_df2 <- cfr_df[, .(inc_cases = sum(inc_cases), deaths = sum(deaths))]
cfr_df2[, cfr_global := deaths/inc_cases]
cfr_global <- cfr_df2$cfr_global

draws.required <- 1000
draw.cols_cfr <- paste0("cfr_draw_", 0:999)

# set a seed
set.seed(222)
size2 <- round(sum(cfr_df$inc_cases))

cfr_df2[, (draw.cols_cfr) := as.list((rbinom(draws.required, prob= cfr_global, size = size2 )) /size2 )]

# check the mean
# returns the number of deaths
mean_ui3 <- function(x){
  y <- select(x, starts_with("cfr_draw_"))
  y$mean <- apply(y, 1, mean)
  y$lower <- apply(y, 1, function(x) quantile(x, c(.025)))
  y$upper <- apply(y, 1, function(x) quantile(x, c(.975)))
  w <- y[, c('mean', 'lower', 'upper')]
  z <- select(x, -contains("cfr_draw_"))
  v <- cbind(z, w)
  v <- as.data.table(v)
  return(v)
}

cfr_df2_test <- mean_ui3(cfr_df2)

### ----------------------- Apply global CFR to incidence ------------------------------
# Pull incidence draws
zika_incidence_draws <- get_draws(gbd_id_type= "modelable_entity_id", 
                                  gbd_id = ADDRESS,
                                  source = 'epi', 
                                  location_id = locations_zika,
                                  measure = 6,
                                  year_id = c(2014:2022),
                                  version_id = zika_nf_mvid,
                                  release_id = release_id) 

zika_incidence_draws23 <- subset(zika_incidence_draws, year_id == 2022)
zika_incidence_draws23[, year_id := 2023]

zika_incidence_draws24 <- subset(zika_incidence_draws, year_id == 2022)
zika_incidence_draws24[, year_id := 2024]

zika_incidence_draws <- rbind(zika_incidence_draws, zika_incidence_draws23,  zika_incidence_draws24)

age_ids <- unique(zika_incidence_draws$age_group_id)

table(zika_incidence_draws$age_group_id)

pops2 <- get_population(location_id = locations_zika,
                        age_group_id = age_ids ,
                        year_id = c(2014:2024),
                        sex_id = c(1,2),
                        release_id = release_id)

zika_incidence_draws2 <- merge(zika_incidence_draws, 
                               subset(pops2, select = c("age_group_id", "location_id", "year_id", "sex_id", "population")),
                               by=c("age_group_id", "location_id", "year_id", "sex_id"),
                               all.x = TRUE,
                               all.y = FALSE)

table(zika_incidence_draws2$age_group_id)
table(zika_incidence_draws2$year_id)

#drop years < 2014
zika_incidence_draws3 <- copy(zika_incidence_draws2)
zika_incidence_draws3 <- subset(zika_incidence_draws3, year_id > 2014)
zika_incidence_draws3 <- as.data.frame(zika_incidence_draws3)

draws_var_indices <- grep("draw_",colnames(zika_incidence_draws3), fixed=T)
for (i in draws_var_indices){
  zika_incidence_draws3[i] <- zika_incidence_draws3[i]*zika_incidence_draws3$population
}

# test
teste3 <- subset(zika_incidence_draws3, location_id == 125)
teste2 <- subset(zika_incidence_draws2, location_id == 125 & year_id > 2014)

teste3a <- subset(teste3, select=c(location_id, year_id, age_group_id, sex_id, draw_1, population))
teste2a <- subset(teste2, select=c(location_id, year_id, age_group_id, sex_id, draw_1, population))

teste2a3a <- merge(x=teste2a, y=teste3a, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=T, all.y =T )

### multiply population
death_function <- function(x){
  inc = select(x, starts_with("draw_"))
  cfr = select(x, starts_with("cfr_draw_"))
  
  deaths <-  inc * cfr
  
  z<- subset(x, select =c(location_id, age_group_id,sex_id, year_id))
  
  v <- cbind(z, deaths)
  v <- as.data.table(v)
  return(v)
  
}

cfr_df2_inc <- cbind(cfr_df2, zika_incidence_draws3)

cfr_df2_inc2 <- death_function(cfr_df2_inc)

mean_ui2 <- function(x){
  y <- select(x, starts_with("draw_"))
  y$mean <- apply(y, 1, mean)
  y$lower <- apply(y, 1, function(x) quantile(x, c(.025)))
  y$upper <- apply(y, 1, function(x) quantile(x, c(.975)))
  w <- y[, c('mean', 'lower', 'upper')]
  z <- select(x, -contains("draw_"))
  v <- cbind(z, w)
  v <- as.data.table(v)
  return(v)
}

cfr_df2_inc2m <- mean_ui2(cfr_df2_inc2)

cfr_df2_inc2m <- subset(cfr_df2_inc2m, mean >0)

draws.cols <- paste0("draw_", 0:999)
cfr_df2_inc2a <- cfr_df2_inc2[, lapply(.SD, sum, na.rm=TRUE), by=.(location_id,  year_id), .SDcols= draws.cols ]

cfr_df2_inc2am <- mean_ui2(cfr_df2_inc2a)
cfr_df2_inc2am <- subset(cfr_df2_inc2am, mean >0)

zika_deaths_cfr <- merge(x=cfr_df2_inc2am, y=draws_deaths, by= c("location_id", "year_id"), all.x =T )

# check locations where mean < observed deaths
locdeaths_issue <- subset(zika_deaths_cfr, mean.x < deaths)

locdeaths_issue <- subset(locdeaths_issue, select=c(location_id, year_id))

locdeaths_issue <- as.data.frame(locdeaths_issue)

zika_cfr_country <- data.table()
for (i in 1:nrow(locdeaths_issue)) {
  
  location_issue <- locdeaths_issue[i, 1]
  year_issue <- locdeaths_issue[i, 2]

  for(l in location_issue){
    for(y in year_issue){
      
      aa <-  subset(cfr_df, location_id == l & year_id ==y)
      aa[, cfr_country := deaths/inc_cases]
      cfr_country <- aa$cfr_country
      
      #set a seed
      set.seed(222)
      size2a <- round(sum(aa$inc_cases))
      
      aa <- subset(aa, select = c(deaths, inc_cases, cfr_country ))
      aa[, (draw.cols_cfr) := as.list( (rbinom(draws.required, prob= cfr_country, size = size2a )) /size2a )]
      
      bb <- subset(zika_incidence_draws3, location_id == l & year_id ==y)
      
      aa3 <- cbind(aa, bb)
      
      aa3a <- death_function(aa3)
      
    }
  }
  
  zika_cfr_country <- rbind(zika_cfr_country, aa3a)
}

draws.cols <- paste0("draw_", 0:999)
zika_cfr_country2 <- zika_cfr_country[, lapply(.SD, sum, na.rm=TRUE), by=.(location_id,  year_id), .SDcols= draws.cols ]

zika_cfr_country2m<- mean_ui2(zika_cfr_country2)

zika_cfr_all <- copy(cfr_df2_inc2)
zika_cfr_all[, dropcol := paste0(location_id, year_id)]

zika_cfr_all <- subset(zika_cfr_all, dropcol !=1182015 & dropcol !=3852016 &
                         dropcol !=47572016 & dropcol !=47592015 &
                         dropcol !=47592016 & dropcol !=47632015 &
                         dropcol !=47642016 &
                         dropcol !=47682016 &
                         dropcol !=47692015 &
                         dropcol !=47702017 & dropcol != 47752017
)

zika_cfr_all <- rbind(zika_cfr_all, zika_cfr_country, fill=T )

table(zika_cfr_all$location_id)
table(zika_cfr_all$age_group_id)
table(zika_cfr_all$year_id)
table(zika_cfr_all$sex_id)

zika_cfr_all[, dropcol := NULL]
zika_cfr_all[, cause_id := 935]
zika_cfr_all[, metric_id := 1]
zika_cfr_all[, measure_id := 1]

# final model
write.csv(zika_cfr_all, "FILEPATH", row.names=F)
