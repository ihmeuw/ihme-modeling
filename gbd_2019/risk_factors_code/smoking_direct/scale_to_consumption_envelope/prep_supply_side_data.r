# this script preps the supply side and sales data, outliers, and smooths the points

######################### CUSTOM FUNCTIONS USED IN THE SCRIPT ################################
split_ussr_usda <- function(usda_data, pop){
  library(zoo)
  
  #Set how many years you want to use to inform the disaggregation proportions
  prop_years <- 5
  usda <- usda_data
  pop721 <- pop
  
  location_list2 <- c("EU-15","French Guiana","Reunion","Yemen (Aden)","Yemen (Sanaa)","Belgium-Luxembourg","Former Yugoslavia","Netherlands Antilles","Yugoslavia (>05/92)")

  usda <- usda[!(location_name %in% location_list2)]
  usda <- usda[!is.na(value)]
  usda[, count:=sequence(.N), by=ihme_loc_id]
  
  #Disaggregate aggregated geographies
  usda <- disaggregate_usda(aggregate = "Former Czechoslovakia", disaggregates = c("Czech Republic", "Slovakia"), prop_years = prop_years, usda = usda)
  
  usda <- disaggregate_usda(aggregate = "Union of Soviet Socialist Repu", disaggregates = c("Tajikistan, Republic of","Russian Federation","Georgia, Republic of","Ukraine", "Moldova, Republic of","Belarus",
                                                                                            "Armenia, Republic of","Azerbaijan, Republic of","Kazakhstan, Republic of","Uzbekistan, Republic of","Turkmenistan",
                                                                                            "Kyrgyzstan, Republic of","Estonia","Latvia","Lithuania"), prop_years = prop_years, usda = usda)
  
  #####Deal with some final details
  setkeyv(usda, id_vars)
  
  rm(country_set, df, usda_failures, usda_sub, sudan_pop,aggregate, country6, country5, country4, country3, country2, country1, location_list2, location_list, 
     prop_country6, prop_country5, prop_country4, prop_country3, prop_country2, prop_country1)
  
  usda <- usda[, c("ihme_loc_id", "location_name", "year_id", "value"), with=F]
  
  ddply(usda, .(ihme_loc_id), nrow)

  return(usda)
}


## used to disaggregate the former USSR locations in the FAO data
disaggregate <- function(aggregate, 
                         disaggregates, prop_years, fao) {
  
  prop_years <- prop_years
  df <- fao
  id_vars <- c("location_name", "year_id")
  
  country_set <- df[(location_name %in% disaggregates),]
  
  if (aggregate == "USSR"){
    country_set[, approx := na.approx(value), by=c("location_name")]
    country_set[is.na(value), value:= approx]  
    country_set[, approx:=NULL]
    country_set[location_name=="Latvia" & year_id==1992, value:= 349]
    country_set[location_name=="Latvia" & year_id==1995 ,value:= 1713 ]
  }
  
  #Create proportion of sales by country for each of the four years following disaggregation
  setkeyv(country_set, id_vars)
  country_set[, count:= sequence(.N), by=location_name]
  country_set[count<=prop_years, total_vals := as.double(sum(value, na.rm = TRUE))
              , by = year_id]
  country_set[count <= prop_years,
              agg_prop := value/total_vals, by = year_id]
  
  country_set[, prop:=mean(agg_prop, na.rm=T), by=location_name]
  country_props <- unique(country_set, by="prop")
  
  ###Now to duplicate rows of aggregate for each country, change the name, and make the value the USSR value * prop for that country
  aggregate_df <- df[location_name==aggregate]
  aggregate_df <- aggregate_df[, count:=NULL]
  aggregate_df <- aggregate_df[rep(seq(.N), length(disaggregates)), !"flag", with=F]
  
  location_keeps <- rep(disaggregates, nrow(df[location_name==aggregate]))
  location_keeps <- sort(location_keeps)
  
  aggregate_df <- cbind(aggregate_df, location_keeps)
  aggregate_df[, location_name:=location_keeps]
  aggregate_df[, location_keeps:=NULL]
  
  aggregate_df <- merge(aggregate_df, country_props[, c("location_name", "prop"), with=F], by="location_name")
  aggregate_df[, value:=value*prop]
  
  ###Now, change ihme_loc_id to match disaggregated country ids
  aggregate_df[ ,ihme_loc_id:=NULL]
  #aggregate_df <- merge(aggregate_df, location_reference[, c("location_name", "ihme_loc_id"), with=F]
  #                     , by="location_name" )
  # by parkes because cannot find location_reference
  aggregate_df <- merge(aggregate_df, locs[, c("location_name", "ihme_loc_id"), with=F]
                        , by="location_name" )
  aggregate_df[, prop:=NULL]
  
  setkeyv(aggregate_df, id_vars)
  setkeyv(df, id_vars)
  
  #og_df <- copy(df)
  #Now just drop the aggregate columns from df and bind_rows of new disaggregates in!
  df <- bind_rows(df, aggregate_df)
  setkeyv(df, id_vars)
  setkeyv(fao, id_vars)
  fao <- df[!location_name==aggregate]
  return(fao)
  
}

## used to disaggregate the former USSR locations in the USDA data
disaggregate_usda <- function(aggregate, 
                              disaggregates, prop_years, usda) {
  
  prop_years <- prop_years
  df <- usda
  id_vars <- c("location_name", "year_id")
  
  country_set <- df[(location_name %in% disaggregates),]
  
  if (aggregate == "Union of Soviet Socialist Repu"){
    country_set[, approx := na.approx(value), by=c("location_name")]
    country_set[is.na(value), value:= approx]  
    country_set[, approx:=NULL]
  }
  
  #Create proportion of sales by country for each of the four years following disaggregation
  setkeyv(country_set, id_vars)
  country_set[, count:= sequence(.N), by=location_name]
  country_set[count<=prop_years, total_vals := as.double(sum(value, na.rm = TRUE))
              , by = year_id]
  country_set[count <= prop_years,
              agg_prop := value/total_vals, by = year_id]
  
  country_set[, prop:=mean(agg_prop, na.rm=T), by=location_name]
  country_props <- unique(country_set, by="prop")
  
  ###Now to duplicate rows of aggregate for each country, change the name, and make the value the USSR value * prop for that country
  aggregate_df <- df[location_name==aggregate]
  aggregate_df <- aggregate_df[, count:=NULL]
  
  location_keeps <- rep(disaggregates, nrow(df[location_name==aggregate]))
  location_keeps <- sort(location_keeps)
  
  aggregate_df <- cbind(aggregate_df, location_keeps)
  aggregate_df[, location_name:=location_keeps]
  aggregate_df[, location_keeps:=NULL]
  
  aggregate_df <- merge(aggregate_df, country_props[, c("location_name", "prop"), with=F], by="location_name")
  aggregate_df[, value:=value*prop]
  
  ###Now, change ihme_loc_id to match disaggregated country ids
  aggregate_df[ ,ihme_loc_id:=NULL]
  aggregate_df[ ,new_ihme_loc_id:=NULL]
  aggregate_df <- merge(aggregate_df, locs[, c("location_name", "ihme_loc_id"), with=F]
                        , by="location_name",all.x=T)
  
  
  fix_locs <- fread('FILEPATH') # list of location names that translate location names into IHME standard location names
  aggregate_df <- merge(aggregate_df,fix_locs,all.x=T)
  aggregate_df[,ihme_loc_id := ifelse(is.na(ihme_loc_id),new_ihme_loc_id,ihme_loc_id)]
  aggregate_df[, prop:=NULL]
  
  setkeyv(aggregate_df, id_vars)
  setkeyv(df, id_vars)
  
  #Now just drop the aggregate columns from df and bind_rows of new disaggregates in
  df <- bind_rows(df, aggregate_df)
  setkeyv(df, id_vars)
  setkeyv(fao, id_vars)
  fao <- df[!location_name==aggregate]
  return(fao)
  
}

## splits former USSR countries for FAO data
split_ussr <- function(fao_data, pop){
  library(zoo)
  
  #Set how many years you want to use to inform the disaggregation proportions
  prop_years <- 5
  fao <- fao_data
  pop721 <- pop
  #bring in fao data
  fao$Year <- as.numeric(fao$Year)
  names(fao) <- tolower(names(fao))
  
  #Create country-specific population reference

  sudan_pop <- pop721[(ihme_loc_id == "SSD" | ihme_loc_id == "SDN"),]
  
  sudan_pop <- sudan_pop[,c("ihme_loc_id", "year_id", "pop"), with=F]
  setkeyv(sudan_pop, c("ihme_loc_id", "year_id"))
  
  #create population aggregate sums and proportions
  sudan_pop[, agg_pop := sum(pop), by=year_id] #total pop by year and ihme_loc_id
  
  #aggregate pop for SSN and SDN combined across years
  sudan_pop[, prop := pop/agg_pop, by = ihme_loc_id] #proportion of population (SDN and SSN) for that year
  setkeyv(sudan_pop, c("year_id", "ihme_loc_id"))
  sudan_pop <- sudan_pop[, c("ihme_loc_id", "year_id", "prop"), with=F]
  
  #now, bring in fao data and adjust it for location/column naming inconsistencies with IHME standards
  fao <- fao[, c("areaname", "year", "value", "flag"), with = FALSE]
  setnames(fao, "areaname", "location_name")
  setnames(fao, "year", "year_id")
  fao <- fao[!is.na(value)] # doesn't look like there are any NA values present
  fao <- fao[!(flag == "A")] # 1756 rows with this flag - but what does this mean?
  setkeyv(fao, c("location_name", "year_id"))
  
  #merge fao with location_reference
  fao <- merge(fao, locs[,c("location_name", "ihme_loc_id"), with=F], by = "location_name", all=TRUE)
  
  #Change unrecognized country names in FAO data
  fao_failures <- data.table(location_name = c("Bahamas", "Bolivia (Plurinational State of)",
                                               "Brunei Darussalam", "Cabo Verde", "China, Hong Kong SAR",
                                               "China, Macao SAR", "China, Taiwan Province of", "China, mainland",
                                               "Côte d'Ivoire", "Democratic People's Republic of Korea",
                                               "Ethiopia PDR", "Gambia", "Iran (Islamic Republic of)", 
                                               "Lao People's Democratic Republic", "Republic of Korea",
                                               "Republic of Moldova", "Russian Federation", 
                                               "North Macedonia", "United Republic of Tanzania",
                                               "United States of America", "Venezuela (Bolivarian Republic of)",
                                               "Viet Nam", "Eswatini", "Czechia"), ihme_loc_id = c("BHS", "BOL", "BRN", "CPV", "CHN_354", "CHN_361", "TWN",
                                                                                                   "CHN", "CIV", "PRK", "ETH", "GMB", "IRN", "LAO", "KOR", 
                                                                                                   "MDA", "RUS", "MKD", "TZA", "USA", "VEN", "VNM", "SWZ","CZE"))
  
  location_list <- c("Bahamas", "Bolivia (Plurinational State of)",
                     "Brunei Darussalam", "Cabo Verde", "China, Hong Kong SAR",
                     "China, Macao SAR", "China, Taiwan Province of", "China, mainland",
                     "Côte d'Ivoire", "Democratic People's Republic of Korea",
                     "Ethiopia PDR", "Gambia", "Iran (Islamic Republic of)", 
                     "Lao People's Democratic Republic", "Republic of Korea",
                     "Republic of Moldova", "Russian Federation", 
                     "North Macedonia", "United Republic of Tanzania",
                     "United States of America", "Venezuela (Bolivarian Republic of)",
                     "Viet Nam", "Eswatini", "Czechia")
  
  fao_sub <- fao[location_name %in% location_list, ] # for those locations not recognized by IHME standards, set the ihme_loc_id to NULL
  fao_sub[, ihme_loc_id:=NULL]
  
  fao_sub <- merge(fao_sub, fao_failures, by="location_name") # so then once this merge happens, these entries will have the correct ihme_loc_id
  
  fao <- fao[!(location_name %in% location_list),]
  fao <- bind_rows(fao, fao_sub) # then bind back together
  setkeyv(fao, c("location_name", "year_id"))
  fao <- fao[!is.na(value)]
  
  fao[grep("Ivoir", fao$location_name), location_name:="Cote d'Ivoire"]
  fao[location_name == "Cote d'Ivoire", ihme_loc_id:= "CIV"]
  
  # correct Sudan
  fao <- bind_rows(one = fao, two = fao[location_name == "Sudan (former)"], .id = "id")
  fao[location_name == "Sudan (former)" & id == "one", ihme_loc_id:= "SSD"]
  fao[location_name == "Sudan (former)" & id == "two", ihme_loc_id := "SDN"] 
  
  #final formatting
  fao$year_id <- as.numeric(fao$year_id)
  fao <- merge(fao, sudan_pop, by=c("ihme_loc_id", "year_id"), all = TRUE)
  
  #Multiply value for each Sudan by yearly prop
  fao$value <- as.double(fao$value)
  fao[ihme_loc_id == "SSD", value:=value*prop, by=year_id]
  fao[ihme_loc_id == "SDN", value:=value*prop, by = year_id]
  
  #Drop some countries we don't use in GBD
  location_list2 <- list("French Polynesia", "Netherlands Antilles (former)", "New Caledonia")
  
  fao <- fao[!(location_name %in% location_list2)]
  fao <- fao[!is.na(value)]
  fao <- fao[, c("prop", "id"):=NULL]
  fao[, count:=sequence(.N), by=ihme_loc_id]
  
  #Disaggregate aggregated geographies
  fao[flag=="SD", value:=NA_character_]
  
  fao <- disaggregate(aggregate = "Belgium-Luxembourg", disaggregates = c("Belgium", "Luxembourg"), prop_years = prop_years, fao = fao)
  fao <- disaggregate(aggregate = "Czechoslovakia", disaggregates = c("Czech Republic", "Slovakia"), prop_years = prop_years, fao = fao)
  fao <- disaggregate(aggregate = "Serbia and Montenegro", disaggregates = c("Serbia", "Montenegro"), prop_years = prop_years, fao = fao)
  
  #Extra prep for USSR issues
  fao[location_name=="Republic of Moldova", location_name:="Moldova"]
  location_drop <- c("Croatia", "Bosnia and Herzegovina", "Slovenia", "Serbia and Montenegro", "North Macedonia")
  keep_list <- fao[(count==1 & year_id==1992 & !(location_name %in% location_drop)), location_name ]
  
  fao <- disaggregate(aggregate = "USSR", disaggregates = keep_list, prop_years = prop_years, fao = fao)
  
  #And lastly, Yugoslav SFR
  fao[location_name=="North Macedonia", location_name:= "Macedonia"]
  fao <- fao[!(flag=="SD"& location_name=="Macedonia")]
  
  fao <- disaggregate(aggregate = "Yugoslav SFR", disaggregates = c("Serbia", "Montenegro", "Slovenia",
                                                                    "Croatia", "Bosnia and Herzegovina", "Macedonia"), prop_years = prop_years, fao = fao)
  
  
  #####Deal with some final details
  setkeyv(fao, id_vars)
  
  rm(country_set, df, fao_failures, fao_sub, sudan_pop,aggregate, country6, country5, country4, country3, country2, country1, location_list2, location_list, 
     prop_country6, prop_country5, prop_country4, prop_country3, prop_country2, prop_country1)
  
  fao <- fao[, c("ihme_loc_id", "location_name", "year_id", "value"), with=F]
  
  ddply(fao, .(ihme_loc_id), nrow)
  return(fao)
}

##############################################################################################

##############################################################################################
################# BEGINNING OF ACTUAL SCRIPT ################

# load libraries
library(readstata13)
library(data.table)
library(ggplot2)
library(gridExtra)
library(dplyr)

# central function to get locations
# we only have supply side data for countries and 2 China subnats
locs <- locs[level==3 | ihme_loc_id == "CHN_354" | ihme_loc_id=="CHN_361"] # just countries, plus Hong Kong and Macao

##Useful things!
ages <- c(7:20, 30:32, 235)
id_vars <- c("ihme_loc_id", "year_id")

# central function to pull standard age groups
age_meta <- get_age_metadata(age_group_set_id = 12, gbd_round_id = 6)
age_meta[,age_range := paste0(age_group_years_start,"-",age_group_years_end)]

#output directories
code_dir <- paste0('FILEPATH') # code location
raw_dir <- paste0('FILEPATH') # location of raw data
prepped_dir <- paste0('FILEPATH') # location of prepped data
output_dir <- paste0('FILEPATH') # output directory


# get IHME population data for all locations, ages, years, and sexes
all_pops <- fread('FILEPATH')

# pop721 used to split former USSR locations
pop721 <- copy(all_pops) %>% as.data.table()
pop721 <- pop721[sex_id == 3] # males and females separately
pop721 <- pop721[year_id > 1959, ]
pop721 <- pop721[age_group_id %in% ages]
pop721 <-merge(pop721, locs[, c("location_id", "ihme_loc_id"), with = F], by = "location_id")
pop721 <-pop721[, c("ihme_loc_id", "year_id", "population"), with = F]
pop721[, total_pop := sum(population), by = c("ihme_loc_id", "year_id")]
pop721[, pop := total_pop]
pop721[, total_pop := NULL]
pop721[, population := NULL]
pop721 <- unique(pop721, by = c("ihme_loc_id", "year_id"))

# both sexes, and only 10+ values
all_pops <- all_pops[sex_id != 3]
all_pops <- all_pops[age_group_id %in% ages]


#Bring in data sources
fao_new <- fread('FILEPATH') # FAO data
usda_raw <- fread("FILEPATH") # USDA data
usda <- usda_raw[Commodity_Description == "Tobacco, Unmfg., Total" &  Attribute_Description == "Domestic Consumption"] # subset USDA data
em_placeholder <- fread('FILEPATH') # newly download Euromonitor data
em_placeholder_old <- fread('FILEPATH') # old Euromonitor data (some past years do not continue to be published)



######### Format the FAO data ####################
fao_new$`Year Code` <- NULL
fao_new$Unit <- NULL

# read in file that standardizes some of the location names in the FAO data to match IHME location names
new_name_fix <- as.data.table(read_excel('FILEPATH'))

# general formatting
setnames(new_name_fix, old = "old", new = "Country")
fao_new <- merge(fao_new, new_name_fix, by = "Country", all = T)
fao_new[,Country := ifelse(!is.na(new), new, Country)]
fao_new$new <- NULL
setnames(fao_new, old = c("Country Code","Country", "Element Code", "Element", "Item Code", "Item", "Flag Description"), 
         new = c("AreaCode","AreaName","ElementCode", "ElementName", "ItemCode", "ItemName", "FlagD"))

# turn negative values to NA to ensure that they are not messing up the split USSR function
fao_new[Value < 0, Value := NA]

# split the USSR conglomerates (as well as other country conglomerates) using population values
fao <- split_ussr(fao_data = fao_new, pop = pop721)
###################################################
  
########## Format USA Data ##########################
## merge on locations for USDA
setnames(usda, old=c("Country_Name","Calendar_Year","Value"),new=c("location_name","year_id","value"))
usda <- usda[,.(location_name,year_id,value)]
usda[,location_name := trimws(location_name)]
usda <- merge(usda,locs[,.(location_name,ihme_loc_id)],by="location_name",all.x=T)
fix_locs <- fread('FILEPATH') # location names that need to be fixed to match with IHME names
usda <- merge(usda,fix_locs,all=T)
usda[,ihme_loc_id := ifelse(is.na(ihme_loc_id),new_ihme_loc_id,ihme_loc_id)]

# split out country conglomerates, like with FAO
# split_ussr(): custom function (see top of file)
usda <- split_ussr_usda(usda_data=usda,pop=pop721)


# then sum the values by ihme_loc_id and year (this is just to add all of the Germany components together)
usda[,value := sum(value,na.rm=T),by=c("ihme_loc_id","year_id")]
# formatted
usda[,c("new_ihme_loc_id","location_name") := NULL]
# note: these units are in millions of grams, and they will be converted to grams further down in the code

#####################################################

###Set id vars
id_vars <- c("ihme_loc_id", "year_id")
value_vars <- c("fao", "usda", "sales", "em_placeholder_data")
loc_vars <- c("location_id","super_region_name", "super_region_id","region_name", "region_id")


#####Minor data adjustments############################
fao <- fao[, c("ihme_loc_id", "year_id", "value"), with=F]

###############Euromonitor
em_placeholder[, V1:=NULL]
##################

setnames(fao, "value", "fao")
setnames(usda, "value", "usda")


#Check for any negatives from FAO that made it past the flag
fao <- fao[!fao<0]
fao_original <- copy(fao)
fao_original[, fao:= fao*1000000]

#Merge all data together
setkeyv(fao, id_vars)
setkeyv(usda, id_vars)
setkeyv(em_placeholder, id_vars)

df <- merge(fao, usda, all=T)
df <- merge(df, em_placeholder, all=T)

df <- merge(df, locs[, c("ihme_loc_id", "location_id", "region_name", "region_id", "super_region_name", 
                         "super_region_id"), with=F], by = "ihme_loc_id")

###Multiply consumption data by 1 million to reflect true units of each data source (million tonnes/sticks)
df[ , (value_vars) := lapply(.SD, function(x) x*1000000), .SDcols = value_vars]

# get the minimum year - used as a cutoff further down
min_year <- min(df$year_id)

setkeyv(df, id_vars)

###### CREATE AGE-SEX SPLIT DATA ################

  
# mclapply to deal with each location separately:
locs_6 <- get_location_metadata(location_set_id = 22)$location_id
run_id1 <- 'RUN ID' # old current smoking run ID
run_idtob <- 'RUN ID' # old amount smoked run ID

# function to split supply side into cigs/smoker/dau
summarize_to_mean <- function(loc, run_id = run_id1, run_id_tob = run_idtob) {
  
  
  fp <- 'FILEPATH'  # file path for current smoking draws for the location of interest
  fp_tob <- 'FILEPATH' # file path for amount smoked draws for the location of interest
  
  if(file.exists(fp)){
    
    dt <- fread(fp)
    
    idvars<-c("location_id", "year_id", "age_group_id", "sex_id")
    drawvars<-c(paste0("draw_", 0:999))
    
    dt<-melt(dt, id.vars = idvars, measure.vars = drawvars)
    setnames(dt, "value", "prevalence")
    # repeat the prevalence values for the older age groups
    temp<-dt[age_group_id==21]
    for(i in c(30, 31, 32, 235)) {
      temp<-temp[, age_group_id:=i]
      dt<-rbind(dt, temp)
    }
    # and also repeat for the earlier years
    temp<-dt[year_id==1980]
    for(i in seq(1961,1979,1)) {
      temp<-temp[, year_id:=i]
      dt<-rbind(dt, temp)
    }
    
    # read in the amt_consumed draws, and merge them onto dt
    dt_tob <- fread(fp_tob)
    dt_tob<-melt(dt_tob, id.vars = idvars, measure.vars = drawvars)
    setnames(dt_tob,old="value",new="cigssmokday")
    # also assume that the same amount of tobacco/per consumed in the older age groups:
    temp<-dt_tob[age_group_id==21]
    for(i in c(30, 31, 32, 235)) {
      temp<-temp[, age_group_id:=i]
      dt_tob<-rbind(dt_tob, temp)
    }
 
    # merge on population values
    dt_intermediate <- merge(dt, all_pops[location_id == loc], by=c("year_id", "age_group_id", "sex_id", "location_id"), all.y=T) # merge on population for each location, year, sex, and age group
    dt_combined <- merge(dt_tob,dt_intermediate, by=c("variable",idvars), all=T)
    
    # take everything above the minimum year available in the supply side data
    dt_combined <- dt_combined[year_id >= min_year]
    dt <- dt_combined
    
    # multiple each draw by the population to get draws for the number of smokers for each age, year, sex, and location
    dt[,num_smokers_drawn := prevalence * population]
    
    
    # sum to get draws of the total number of smokers for each location and year
    dt[, total_num_smokers:= sum(num_smokers_drawn,na.rm=T), by=c("location_id", "year_id","variable")]
    
    # sum to get the total population in each year for each location; used in a later step where the tobacco data are divided by the population value to get tob per capita
    dt[, total_pop := sum(population,na.rm=T), by=c("location_id", "year_id","variable")] 
    
    # calculate the number of cigaretes consumed per year, for each age-sex-loc-year
    dt[,cigsyr := cigssmokday * 365 * num_smokers_drawn]
    dt[,cigsday := cigssmokday * num_smokers_drawn] # total number of cigarettes consumed per day, for each age-sex-loc-year
    
    # then sum the values derived from the survey-level data for each location and year, because right now they are also age and sex specific
    dt[,total_cigsyr := sum(cigsyr,na.rm=T),by=c("location_id","year_id","variable")]
    
    # then divide total number of cigarettes consumed per loc/year by the total number of smokers, and then 365, to get cig/smoker/day
    dt[,total_cigsmokday := total_cigsyr/(total_num_smokers*365)]
    
    # make draws of the age-sex-year-loc ratio
    dt[,asp := cigsday/sum(cigsday,na.rm=T),by=c("location_id", "year_id", "variable")]
    
    # merge onto the df for the location of interest. then summarize the results
    df_temp <- merge(df[location_id == loc], dt,by=c("location_id","year_id"))
    
    # create tob per smoker indicators using the draws, that we will then summarize to get mean, upper, and lower
    
    # multiply fao values by the age-sex ratio, and then divide by the draws of the number of smokers
    # and then divide by 365 -> draws of supply side estimates by age/sex/loc/year
    df_temp[,fao_tob_per_smoker := (fao * asp)/(num_smokers_drawn * 365)]
    df_temp[,usda_tob_per_smoker := (usda * asp)/(num_smokers_drawn * 365)]
    df_temp[,em_tob_per_smoker := (em_placeholder_data * asp)/(num_smokers_drawn * 365)]
    
    # repeat the age-sex pattern for the years < 1980
    dt_old <- df_temp[year_id < 1980]
    dt_old[,asp:=NULL]
    # need to replicate this 1000 times so that I can add in draws of asp
    dt_1980 <- df_temp[year_id == 1980,.(location_id,age_group_id, sex_id,asp,variable)]
    
    # assume that the same age-sex pattern should be applied to all earlier years, distinguished by age and sex
    # why is this age-sex pattern non-existent?
    dt_old_combined <- merge(dt_old,dt_1980,by=c("location_id","age_group_id","sex_id","variable"))
    
    dt_old_combined[,fao_tob_per_smoker := (fao * asp)/(num_smokers_drawn * 365)]
    dt_old_combined[,usda_tob_per_smoker := (usda * asp)/(num_smokers_drawn * 365)]
    dt_old_combined[,em_tob_per_smoker := (em_placeholder_data * asp)/(num_smokers_drawn * 365)]
    
    # replace the old past years stuff with the new stuff
    df_temp_new <- df_temp[year_id >= 1980]
    df_temp <- rbind(df_temp_new,dt_old_combined)
    
    
    # then calculate the same statistics for # cigs/smok/day, # cigs/year, and cigs pc
    df_temp <- df_temp[,c("cig_smok_day_mean","cig_smok_day_lower","cig_smok_day_upper"):=as.list(c(mean(get("total_cigsmokday")), quantile(get("total_cigsmokday"), c(0.025, 0.975),na.rm=T) )), by=c("location_id","year_id")]
    df_temp <- df_temp[,c("cigsyr_mean","cigsyr_lower","cigsyr_upper"):=as.list(c(mean(get("total_cigsyr")), quantile(get("total_cigsyr"), c(0.025, 0.975),na.rm=T) )), by=c("location_id","year_id")]
    df_temp <- df_temp[,c("asp_mean","asp_lower","asp_upper"):=as.list(c(mean(get("asp")), quantile(get("asp"), c(0.025,0.975),na.rm=T))), by=c("location_id","year_id","age_group_id","sex_id")]
    
    
    # then create mean, upper, and lower of the draws of number of smokers for each location and year
    df_temp <- df_temp[, c("fao_tob_per_smoker_mean", "fao_tob_per_smoker_low", "fao_tob_per_smoker_up"):=as.list(c(mean(get("fao_tob_per_smoker")), quantile(get("fao_tob_per_smoker"), c(0.025, 0.975),na.rm=T) )), by=c("location_id","year_id")]
    df_temp <- df_temp[, c("usda_tob_per_smoker_mean", "usda_tob_per_smoker_low", "usda_tob_per_smoker_up"):=as.list(c(mean(get("usda_tob_per_smoker")), quantile(get("usda_tob_per_smoker"), c(0.025, 0.975),na.rm=T) )), by=c("location_id","year_id")]
    df_temp <- df_temp[, c("em_tob_per_smoker_mean", "em_tob_per_smoker_low", "em_tob_per_smoker_up"):=as.list(c(mean(get("em_tob_per_smoker")), quantile(get("em_tob_per_smoker"), c(0.025, 0.975),na.rm=T) )), by=c("location_id","year_id")]
    
    
    # multiply the draws of supply side estimates of cig/smoker/day (not age-sex specific) by the draws of the age-sex ratio
    # then we get draws of the supply side estimates of cig/smoker/day broken up by age and sex
    ss_vars <- c("fao_tob_per_smoker","usda_tob_per_smoker","em_tob_per_smoker") # age sex split cig/smoker/day values
    ss_draws_as <- c("fao_tob_per_smoker_as","usda_tob_per_smoker_as","em_tob_per_smoker_as")
    df_temp[, (ss_draws_as) := lapply(.SD, function(x) x), .SDcols = ss_vars]
    
    # then create summaries of these
    for (k in 1:4){
      df_temp <- df_temp[,paste0(ss_draws_as[k],c("_mean","_lower","_upper")):=as.list(c(mean(get(ss_draws_as[k])), quantile(get(ss_draws_as[k]), c(0.025,0.975),na.rm=T))), by=c("location_id","year_id","age_group_id","sex_id")]
    }
    
    df_temp_unique <- unique(df_temp[,c("location_id", "year_id","age_group_id","sex_id","cig_smok_day_mean","cig_smok_day_lower","cig_smok_day_upper",
                                        "fao","usda","em_placeholder_data", "total_pop",
                                        c(paste0("fao_tob_per_smoker_as",c("_mean","_lower","_upper")),
                                          paste0("usda_tob_per_smoker_as",c("_mean","_lower","_upper")),
                                          paste0("em_tob_per_smoker_as",c("_mean","_lower","_upper"))))])
    
    
    return(df_temp_unique)
    
  }
  
  
}


est1 <- data.table()
for (loc in unique(df$location_id)){
  message(loc)
  est1_temp <- summarize_to_mean(loc, run_id = run_id1, run_id_tob = run_idtob)
  est1 <- rbind(est1,est1_temp)
}
  

df <- copy(est1)
# merge on ihme_loc_id:
df <- merge(df,locs[,.(ihme_loc_id,location_id)],by="location_id")
data_input <- copy(df)
setkeyv(df, id_vars)


###### OUTLIERING ##############
##Outliers being defined by tobacco persmoker > 40grams/day til 1980, 30g/day after 1979

outliered_pc <- copy(df)

setnames(outliered_pc, "em_placeholder_data", "em")
vars <- c("fao", "usda", "em", "sales")

initial_fao <- nrow(outliered_pc[!is.na(fao)])
initial_em <- nrow(outliered_pc[!is.na(em)])
initial_usda <- nrow(outliered_pc[!is.na(usda)])
initial_sales <- nrow(outliered_pc[!is.na(sales)])

#### GENERATE PER CAPITA ESTIMATES ####
#divide by population aggregated over location and year, since the tobacco data are not age-sex specific:
vars_pc <- paste0(vars,"_pc")
outliered_pc[ ,(vars_pc) := lapply(.SD, '/', outliered_pc$total_pop), .SDcols = vars ] # divides each of the sales data by the population, creating draws of each of the supply side variables
outliered_pc[, (vars) := lapply(.SD, function(x) x), .SDcols = vars_pc]
outliered_pc[,(vars_pc) := NULL]

# create a more informative dataset
plotting_df <- copy(outliered_pc)
plotting_df <- merge(plotting_df,locs,by=c("location_id","ihme_loc_id"))
plotting_df <- merge(plotting_df,age_meta,by="age_group_id")
plotting_df[,sex:=ifelse(sex_id == 1, "Male","Female")]

############### SET THRESHOLDS by age, sex, location, year in order to flag points #############
# ages 10-15 for males and females - over 5 cigs; don't want to outlier at the lower range because of smoking being introduced in some places during the time series
# ages 15-20 for males - over 20; don't want to outlier at the lower range because of smoking being introduced in some places during the time series
# ages 15-20 for females - over 18; don't want to outlier at the lower range because of smoking being introduced in some places during the time series
# the rest of the ages for both sexes - over 3
# the rest of the ages for males - over 38
# the rest of the ages for females - over 35

# load the estimates for each age-sex-loc-year for survey-level data, to merge onto the data
# we will use these to compare what might be "realistic" for the cigs/smoker/day values generated from the supply side data
tob_as <- model_load('RUN ID',obj="raked") # estimated from amount smoked ST-GPR model
tob_as <- tob_as[age_group_id %in% ages]
tob_as <- tob_as[location_id %in% unique(plotting_df$location_id)]

# create a "filled out" dataset with all years, sexes, etc
cig_smok_day_thresh <- expand.grid(age_group_id = unique(plotting_df$age_group_id),
                                   sex_id = c(1,2), super_region_id = unique(locs$super_region_id))
cig_smok_day_thresh <- merge(cig_smok_day_thresh,age_meta[,.(age_range,age_group_id)],by="age_group_id")
cig_smok_day_thresh <- as.data.table(cig_smok_day_thresh)
cig_smok_day_thresh[,lower_thresh := ifelse(age_group_id > 9, 3, 0)]
cig_smok_day_thresh[,upper_thresh := ifelse(age_group_id == 7, 10, ifelse(age_group_id == 8 & sex_id == 1, 25,
                                    ifelse(age_group_id == 8 & sex_id == 2, 20, ifelse(sex_id == 1, 38, 35))))]


plotting_df <- merge(plotting_df,cig_smok_day_thresh,by=c("age_group_id","sex_id","age_range","super_region_id"))
# create indicators to tell us if a value is over or under the threshold
plotting_df[,fao_outlier := ifelse(fao_tob_per_smoker_as_mean <= lower_thresh | fao_tob_per_smoker_as_mean >= upper_thresh, 1, 0)]
plotting_df[,usda_outlier := ifelse(usda_tob_per_smoker_as_mean <= lower_thresh | usda_tob_per_smoker_as_mean >= upper_thresh, 1, 0)]
plotting_df[,em_outlier := ifelse(em_tob_per_smoker_as_mean <= lower_thresh | em_tob_per_smoker_as_mean >= upper_thresh, 1, 0)]

# merge the survey level ST-GPR results onto the data:
plotting_df <- merge(plotting_df,tob_as,by=c("location_id","year_id","age_group_id","sex_id"),all=T)
### use the above dataset to plot if you want...

#continue on with the processing
outliered_pc <- copy(plotting_df)

# change names to be more explicit about what we are modeling
setnames(outliered_pc, vars, paste0(vars, "_pc"))
pc_vars <- paste0(vars, "_pc")

df <- outliered_pc %>% as.data.table

# sum the outlier values over location and year so that we know which location-year pairs have an outlier for any age/sex
df[,fao_outlier_sum := sum(fao_outlier,na.rm=T),by=c("location_id","year_id")]
df[,em_outlier_sum := sum(em_outlier,na.rm=T),by=c("location_id","year_id")]
df[,usda_outlier_sum := sum(usda_outlier,na.rm=T),by=c("location_id","year_id")]

################## outlier based on mean values ####################

### temporarily set the outliers to NA so that I can calculate mean values without them
## save the values somewhere
df[, original_fao:=fao_pc]
df[, original_em:=em_pc]
df[, original_usda:=usda_pc]


########### MANUAL OUTLIERING # 1 #############
#read in the outliers:
manual_outliers <- as.data.table(read.csv('FILE PATH'))
#deal with the manual outliers for the mean-extreme outliers. these should be fixed first before doing the mean-extreme outliering
manual_outliers$outlier_value <- 1
manual_outliers[,series := gsub("$","_out",series)]
manual_outliers_wide <- dcast.data.table(manual_outliers, location_id + year_id ~ series, value.var = "outlier_value")

manual_outliers_wide[manual_outliers_wide == 1] <- 13

df <- merge(df,manual_outliers_wide, by = c("location_id","year_id"), all = T)

df[!is.na(em_pc_out),em_outlier_sum := em_pc_out]
df[!is.na(fao_pc_out),fao_outlier_sum := fao_pc_out]
df[!is.na(usda_pc_out),usda_outlier_sum := usda_pc_out]

#outlier some points for FAO in Mauritania because they are creating a weird trend
df[location_id == 212 & year_id >= 1990, fao_outlier_sum := fao_outlier_sum+13]
###### end manual outliering ######


##### now that we are sure about which outliers we want to exclude at this step, we can set the outliered values to be NA #####
df[fao_outlier_sum > 12, fao_pc := NA]
df[em_outlier_sum > 12, em_pc := NA]
df[usda_outlier_sum > 12, usda_pc := NA]

# get rid of the age and sex specific stuff because we don't need that anymore
df_year_loc <- copy(df)
df_year_loc[,c("age_group_id","sex_id","cig_smok_day_mean","cig_smok_day_lower","cig_smok_day_upper","gpr_mean","gpr_lower","gpr_upper","age_range","sex",
               "age_group_years_start","age_group_years_end","age_group_weight_value","fao_tob_per_smoker_as_mean","fao_tob_per_smoker_as_lower", 
               "fao_tob_per_smoker_as_upper","usda_tob_per_smoker_as_mean","usda_tob_per_smoker_as_lower","usda_tob_per_smoker_as_upper","em_tob_per_smoker_as_mean",
               "em_tob_per_smoker_as_lower","em_tob_per_smoker_as_upper",
               "lower_thresh","upper_thresh","fao_outlier","usda_outlier","em_outlier") := NULL]
df_year_loc$V1 <- NULL
df_year_loc <- unique(df_year_loc)
df <- copy(df_year_loc)

# take the rolling means for each 10 year time period (centered)
df[, fao_mean := rollapply(.SD$fao_pc, 10, mean, na.rm=T, fill = NA, partial = T), by="location_id"]
df[, em_mean := rollapply(.SD$em_pc, 10, mean, na.rm=T, fill = NA, partial = T), by="location_id"]
df[, usda_mean := rollapply(.SD$usda_pc, 10, mean, na.rm=T, fill = NA, partial = T), by="location_id"]

# now deal with the terminal years
########### fao #################
min_val_fao <- min(df[!is.na(fao_pc), year_id], na.rm = T)
max_val_fao <- max(df[!is.na(fao_pc), year_id], na.rm = T)
for (years in c(min_val_fao:(min_val_fao+5))){
  for (loc in unique(df[year_id == years & !is.na(fao_pc), ihme_loc_id])){ # only measure for locations that have non-NA FAO values
    df[year_id == years & ihme_loc_id == loc,fao_mean := mean(df[year_id %in% years:(years+9) & ihme_loc_id == loc,fao_pc], na.rm = T)] # for the smaller terminal years, use 10 years (including the year itself) in the mean
  }
  
}
for (years in c(max_val_fao:(max_val_fao-5))){
  for (loc in unique(df[year_id == years & !is.na(fao_pc), ihme_loc_id])){ # only measure for locations that have non-NA FAO values
    df[year_id == years & ihme_loc_id == loc,fao_mean := mean(df[year_id %in% years:(years-9) & ihme_loc_id == loc,fao_pc], na.rm = T)] # for the smaller terminal years, use 10 years (including the year itself) in the mean
  }
  
}

################ EM ##############
min_val_em <- min(df[!is.na(em_pc), year_id], na.rm = T)
max_val_em <- max(df[!is.na(em_pc), year_id], na.rm = T)
for (years in c(min_val_em:(min_val_em+5))){
  for (loc in unique(df[year_id == years & !is.na(em_pc), ihme_loc_id])){ # only measure for locations that have non-NA EM values
    df[year_id == years & ihme_loc_id == loc,em_mean := mean(df[year_id %in% years:(years+9) & ihme_loc_id == loc,em_pc], na.rm = T, by = "location_id")] # for the smaller terminal years, use 10 years (including the year itself) in the mean
  }
  
}
for (years in c(max_val_em:(max_val_em-5))){
  for (loc in unique(df[year_id == years & !is.na(em_pc), ihme_loc_id])){ # only measure for locations that have non-NA EM values
    df[year_id == years & ihme_loc_id == loc,em_mean := mean(df[year_id %in% years:(years-9) & ihme_loc_id == loc,em_pc], na.rm = T, by = "location_id")] # for the smaller terminal years, use 10 years (including the year itself) in the mean
  }
  
}
############ USDA ####################
min_val_usda <- min(df[!is.na(usda_pc), year_id], na.rm = T)
max_val_usda <- max(df[!is.na(usda_pc), year_id], na.rm = T)
for (years in c(min_val_usda:(min_val_usda+5))){
  for (loc in unique(df[year_id == years & !is.na(usda_pc), ihme_loc_id])){ # only measure for locations that have non-NA usda values
    df[year_id == years & ihme_loc_id == loc,usda_mean := mean(df[year_id %in% years:(years+9) & ihme_loc_id == loc,usda_pc], na.rm = T, by = "location_id")] # for the smaller terminal years, use 10 years (including the year itself) in the mean
  }
  
}
for (years in c(max_val_usda:(max_val_usda-5))){
  for (loc in unique(df[year_id == years & !is.na(usda_pc), ihme_loc_id])){ # only measure for locations that have non-NA usda values
    df[year_id == years & ihme_loc_id == loc,usda_mean := mean(df[year_id %in% years:(years-9) & ihme_loc_id == loc,usda_pc], na.rm = T, by = "location_id")] # for the smaller terminal years, use 10 years (including the year itself) in the mean
  }
  
}

############ finished creating the mean values for the terminal years ################
######################################################################################

### set thresholds for how different a point can be from the rolling mean
df[,diff_fao := fao_mean - fao_pc]
df[,diff_em := em_mean - em_pc]
df[,diff_usda := usda_mean - usda_pc]

df[,r_fao := abs(fao_pc - fao_mean)/fao_mean]
df[,r_em := abs(em_pc - em_mean)/em_mean]
df[,r_usda := abs(usda_pc - usda_mean)/usda_mean]
test_val_up_r <- 0.7

#########################################################################################################
# determine which points are above/below threshold
df[,usda_high := ifelse(r_usda > test_val_up_r, 1, 0)]
df[,fao_high := ifelse(r_fao > test_val_up_r, 1, 0)]
df[,em_high := ifelse(r_em > test_val_up_r, 1, 0)]

df[is.na(usda_high), usda_high := 2]
df[is.na(fao_high), fao_high := 2]
df[is.na(em_high), em_high := 2]

df[,usda_out_lab := ifelse(usda_high > 0, ifelse(usda_high == 1, "Too extreme", "Already outliered"), "Good")]
df[,fao_out_lab := ifelse(fao_high > 0, ifelse(fao_high == 1, "Too extreme", "Already outliered"), "Good")]
df[,em_out_lab := ifelse(em_high > 0, ifelse(em_high == 1, "Too extreme", "Already outliered"), "Good")]
########################################################################
#######################################################################


### make a count of the number of sources as an error-checking device to make sure the number of rows being outliers is reasonable
before_more_outliering <- nrow(df[fao_outlier_sum > 12 | em_outlier_sum > 12 | usda_outlier_sum > 12]) 

df[, fao_outlier_sum := ifelse(fao_outlier_sum > 12 | fao_high == 1, fao_outlier_sum+13, fao_outlier_sum+0)]
df[, em_outlier_sum := ifelse(em_outlier_sum > 12 | em_high == 1, em_outlier_sum+13, em_outlier_sum+0)]
df[, usda_outlier_sum := ifelse(usda_outlier_sum > 12 | usda_high == 1, usda_outlier_sum+13, usda_outlier_sum+0)]

after_more_outliering <- nrow(df[fao_outlier_sum > 12 | em_outlier_sum > 12 | usda_outlier_sum > 12])

########### MANUAL OUTLIERING # 2 #############
# Read in the outliers:
df[,c("em_pc_out","usda_pc_out","fao_pc_out") := NULL]
manual_outliers <- as.data.table(read.csv('FILEPATH'))
# Deal with the manual outliers for the mean-extreme outliers. these should be fixed first before doing the mean-extreme outliering
manual_outliers$outlier_value <- 1
manual_outliers[,series := gsub("$","_out",series)]
manual_outliers_wide <- dcast.data.table(manual_outliers, location_id + year_id ~ series, value.var = "outlier_value")

manual_outliers_wide[manual_outliers_wide == 1] <- 13
df <- merge(df,manual_outliers_wide, by = c("location_id","year_id"), all = T)
df[!is.na(em_pc_out),em_outlier_sum := em_pc_out] # might err here is there are no EM changes, which is fine!
df[!is.na(fao_pc_out),fao_outlier_sum := fao_pc_out]
df[!is.na(usda_pc_out),usda_outlier_sum := usda_pc_out] # same for fao

# set these new outliers to NA as well so that they are not included in the smoothing
df[fao_outlier_sum > 12, fao_pc := NA]
df[em_outlier_sum > 12, em_pc := NA]
df[usda_outlier_sum > 12, usda_pc := NA]

#############################################################################
########### perform some smoothing on all sources #########################
#Apply fao noise reduction
df <- df[, fao_smooth := rollapply(.SD$fao_pc, 3, mean, na.rm=T, fill = NA), by="location_id"] %>%
  .[is.na(fao_pc), fao_smooth := NA] # cannot figure out to ignore the NA values without doing fill = .SD$fao_pc, but there must be a way


df <- df[, em_smooth := rollapply(.SD$em_pc, 3, mean, na.rm=T, fill = NA), by="location_id"] %>%
  .[is.na(em_pc), em_smooth := NA]

df <- df[, usda_smooth := rollapply(.SD$usda_pc, 3, mean, na.rm=T, fill = NA), by="location_id"] %>%
  .[is.na(usda_pc), usda_smooth := NA]

df[,fao_pc := ifelse(!is.na(fao_smooth), fao_smooth,fao_pc)]
df[,em_pc := ifelse(!is.na(em_smooth), em_smooth,em_pc)]
df[,usda_pc := ifelse(!is.na(usda_smooth), usda_smooth,usda_pc)]

###############################################################################################
####### set the NA values back to what they were originally (in the original_ columns) ########

# if it is an outlier, assign it the original fao value; otherwise, keep the value that has been smoothed
df[,fao_pc := ifelse(fao_outlier_sum > 12, original_fao, fao_pc)]
df[,usda_pc := ifelse(usda_outlier_sum > 12, original_usda, usda_pc)]
df[,em_pc := ifelse(em_outlier_sum > 12, original_em, em_pc)]

df$V1 <- NULL
df <- unique(df)
############## WRITE FINAL FILE ! #####################
# this file then needs to go through the imputation step
write.csv(df,'FILEPATH')




