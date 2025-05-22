################################################################################
## DESCRIPTION ##  This preps the SUA to be used for food groups availability estimates and combines it with the sales, nutrients, and energy data!
#                        A replacement for 04_prep_FAO_sales_grams.do
## INPUTS ##
## OUTPUTS ##
## AUTHOR ##   
## DATE ##    
################################################################################

rm(list = ls()) 
library(data.table)
library(tidyverse)
library(dplyr)
library(haven) # for reading in dta files
library(reshape2)
library(xlsx)
library(FAOSTAT, lib.loc = "FILEPATH/R_packages") 
source("/FIELPATH/get_location_metadata.R")

# System info & drives
os <- Sys.info()[1]
user <- Sys.info()[7]
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "H:/"


if (interactive()) {
  save_components <- TRUE 
  version <- 2022
  gbd_round <- 'gbd2022' 
  release_id = 16
} else {
  save_components <- TRUE 
  args = commandArgs(trailingOnly=TRUE)
  gbd_round <- args[1]
  release_id <- args[2]
  version <- args[3] 
}

# Path to files
data_path <- paste0("FILEPATH")  
input_path <- paste0("FILEPATH")
output_path <- paste0("FILEPATH")
dir.create(output_path)

################################################################
###### 1) aggregate the SUA data to food groups
################################################################
include_refuse_var <- TRUE
energy_prep <- FALSE

raw_sua_data <- read_csv(paste0("FILEPATH", "simple_sua.csv")) 

output_file <- paste0(output_path, sprintf("SUA_foodgroups_data_%s.csv", version)) 

FBS_and_SUA_list <- read.xlsx("FILEPATH/FBS_and_SUA_list.xlsx", sheetName  = "SUA FBS") 
food_ids <- fread(paste0("FILEPATH/SUA_foodgroup_codebook.csv"))
food_ids <- food_ids[!is.na(food_group)]



sua_data <- as.data.table(raw_sua_data)

sua_data[, data:=data*100] # converts data back to g/person/day
sua_data[, ele_name:="g/person/day"]

sua_data <- sua_data[!is.na(data),]

#merge food ids and data so we can aggregate for each food group -- internal merge
df_gram <- merge(x=food_ids, y=sua_data, by.x="com", by.y="product_codes", all.x = FALSE, all.y=FALSE)
df_gram <- as.data.table(df_gram)
df_gram$comn.x <- NULL
setnames(df_gram, "comn.y", "comn")

milk_rows <- df_gram[food_group == "Milk",]
milk_rows$food_group <- "Dairy"
df_gram <- rbind(df_gram, milk_rows)


if(!include_refuse_var){ df_gram$data <- df_gram$old_data
print("using old_data column because you don't want to use refuse")}
prepped_grams <- df_gram[,sum(data, na.rm=TRUE), by=list(food_group, countries, location_id, year, ele_name)]  #only keeps totals
df_gram <- prepped_grams
setnames(df_gram, "countries", "location_name")

setnames(df_gram, "food_group", "covariate")
setnames(df_gram, "V1", "value")

df_gram$covariate <- tolower(df_gram$covariate)
df_gram$ele_name <- NULL

#fix some covariate names
df_gram[covariate=="egg", covariate:="eggs"]
df_gram[covariate=="fruit", covariate:="fruits"]
df_gram[covariate=="starchy vegetables", covariate:="starchy_veg"]
df_gram[covariate=="red meat", covariate:="red_meats"]
df_gram[covariate=="legumes", covariate:="pulses_legumes"]
df_gram[covariate=="nuts and seeds", covariate:="nuts_seeds"]
df_gram[covariate=="oil", covariate:="total_oil"]
df_gram[covariate=="processed meat", covariate:="procmeat"]
df_gram[covariate=="grains", covariate:="all_grains"]
df_gram[covariate=="dairy", covariate:="all_dairy"]    #this is dairy (including milk) exlcuding butter   
#keep butter as butter  

df_gram[,cv_fao_data:=1]
df_gram[,case_definition:="g/day"]
df_gram[,svy:="FAO SUA USDA"]
df_gram[,nid:=238501]


#Seychelles are not in FBS at all 
df_gram <- df_gram[location_id!=186,]

## droping unrealstic data points 

#legumes
df_gram <- df_gram[!(covariate=="pulses_legumes" & location_id %in% c(105,33,34,305,83,23,59,27,393,40,30)),]

#nuts and seeds
df_gram <- df_gram[!(covariate=="nuts_seeds" & location_id %in% c(105,305,203,110,173,112,23,194,185,27,215,393,117,118,19,30)),]
df_gram <- df_gram[!(covariate=="nuts_seeds" & location_id %in% c(28,26,22)),]

#oil
df_gram <- df_gram[!(covariate=="total_oil" & location_id %in% c(43,105,33,107,57,108,121,45,204,98,354,77,110,141,127,58,22,35,112,83,115,36,23,37,12,59,146,194,60,87,49,14,88,61,50,148,195,53,54,17,393,116,117,118,39,119,40,99,41,20)),]

#total dairy 
df_gram <- df_gram[!(covariate=="all_dairy" & location_id==12 ),]



finished_food_groups <- df_gram
if(save_components){write.csv(finished_food_groups, output_file, row.names = FALSE)}


###############################################
#####  2) Energy prep using SUAs
###############################################


if(energy_prep){
  #use the already imported raw_sua_data here
  output_file <- paste0(output_path, sprintf("SUA_energy_data_%s.csv", version)) 
  
  raw_sua_data <- as.data.table(raw_sua_data)
  energy_prep <- raw_sua_data[!is.na(data),]
  
  
  df_gram <- merge(x=food_ids, y=energy_prep, by.x="com", by.y="product_codes", all.x = FALSE, all.y=FALSE)
  df_gram <- as.data.table(df_gram)
  df_gram$comn.x <- NULL
  setnames(df_gram, "comn.y", "comn")
  df_gram[, kcals_per_day:= usda_kcal_100_grams*data]
  prepped_kcals <- df_gram[,sum(kcals_per_day, na.rm=TRUE), by=list(countries, location_id, year, ele_name)]  #only keeps totals
  setnames(prepped_kcals, "V1", "value")
  
  #dropp unrealistic data points
  
  prepped_kcals <- prepped_kcals[!(countries=="Palest; O.T." & year < 1996),] 
  prepped_kcals <- prepped_kcals[!(countries=="Sudan" & year < 2012),] 
  prepped_kcals <- prepped_kcals[!(countries=="Montenegro" & year < 2006),] 
  prepped_kcals <- prepped_kcals[value > 1200] 
  prepped_kcals <- prepped_kcals[!(countries=="Burundi" | location_id==175),]  #
  prepped_kcals[,cv_fao_data:=1]
  prepped_kcals[,case_definition:="kcal/day pc"]
  prepped_kcals[,svy:="FAO SUA USDA"]
  prepped_kcals[,nid:=238501]
  
    
  setnames(prepped_kcals, "countries", "location_name")
  prepped_kcals$covariate <- "energy_kcal_unadj"
  finished_energy <- prepped_kcals 
  if(save_components){write.csv(prepped_kcals, output_file, row.names = FALSE)}
}

######################################################
### 3) Refined grains prep using FBS
######################################################

# prepare codes to link old and new data

FBS_and_SUA_list_clean <- FBS_and_SUA_list %>%
  fill("II", .direction = "down") %>%
  fill("NA.", .direction = "down") %>%
  fill("III", .direction = "down") %>%
  fill("NA..1", .direction = "down") %>%
  fill("FBS.group.name", .direction = "down") %>%
  fill("IV", .direction = "down") %>%
  dplyr::select(-c("I"))

FBS_and_SUA_list_clean <- FBS_and_SUA_list_clean[!is.na(FBS_and_SUA_list_clean$FCL),]

FBS_and_SUA_list_clean_codes <- FBS_and_SUA_list_clean %>% dplyr::select(c("IV", "FCL"))

FBS_code_link <- merge(x=FBS_and_SUA_list_clean_codes, y=food_ids, by.x="FCL", by.y="com", all.x = TRUE, all.y=TRUE) %>% 
  dplyr::select(c("IV", 'food_group')) %>%
  unique() %>%
  drop_na()
FBS_code_link <- FBS_code_link %>%
  add_row(`IV` = 2807, food_group = "Grains")

# prepare FAOSTAT metadata for location links

FAOSTAT_get <- FAOcountryProfile
FAOSTAT_countries <- FAOSTAT_get %>% 
  dplyr::select(c(FAOST_CODE, ISO3_CODE, FAO_TABLE_NAME, OFFICIAL_FAO_NAME, SHORT_NAME)) %>%
  mutate(ISO3_CODE = replace(ISO3_CODE, ISO3_CODE == "HKG", "CHN_354"),
         ISO3_CODE = replace(ISO3_CODE, ISO3_CODE == "MAC", "CHN_361"))
# South Sudan doesn't have a FAO code, need to add manually...
FAOSTAT_countries$ISO3_CODE[is.na(FAOSTAT_countries$ISO3_CODE) & FAOSTAT_countries$FAOST_CODE == 277] <- 'SSD'

locs <- get_location_metadata(location_set_id = 35, release_id=release_id) %>% 
  dplyr::select(c(location_id, location_name, ihme_loc_id))

## new FBS data that needs to be reformatted 
new_fbs_data <- fread('FILEPATH/FoodBalanceSheets_E_All_Data.csv')

# reformat new data to match old data
new_fbs_data_reshape <- new_fbs_data %>% 
  dplyr::select(-c('Y2010F','Y2011F','Y2012F','Y2013F','Y2014F', 'Y2015F','Y2016F','Y2017F','Y2018F','Y2019F', 'Y2020F', 'Y2021F', "Area Code (M49)", "Item Code (FBS)")) %>%
  pivot_longer(cols = -c("Area Code", "Area", "Item Code", "Item", "Element Code", "Element", "Unit"),
               names_to = "year", names_prefix = '', values_to = "data")

new_fbs_data_reshape$year <- as.numeric(substr(new_fbs_data_reshape$year, 2, 7)) # removes Y prefix on year values

setnames(new_fbs_data_reshape, old = c('Element Code', 'Area Code', 'Area', 'Item Code', 'Item', 'Element'),
         new = c('ele_codes', 'country_codes', 'countries', 'product_codes', 'products', 'elements'))

new_fbs_data_reshape <- filter(new_fbs_data_reshape, ele_codes == 645) 

new_fbs_data_reshape_locs <- merge(x=new_fbs_data_reshape, y=FAOSTAT_countries, by.x="country_codes", by.y="FAOST_CODE", all.x = TRUE)
new_fbs_data_reshape_locs <- merge(x=new_fbs_data_reshape_locs, y=locs, by.x="ISO3_CODE", by.y="ihme_loc_id", all.x = TRUE)

new_grains_all <- merge(x=new_fbs_data_reshape_locs, y=FBS_code_link, by.x="product_codes", by.y="IV", all.x = TRUE, all.y=TRUE) %>%
  filter(`food_group` == "Grains") %>%
  filter(`products` != "Rice and products" & `products` != "Maize and products") %>% 
  group_by(ISO3_CODE, countries, year, food_group) %>%
  summarize(Total = sum(data, na.rm = TRUE)) %>%
  ungroup() %>%
  drop_na() %>%
  # special characters in country names cause issues
  mutate(countries = replace(countries, countries == "C\xf4te d'Ivoire", "Côte d'Ivoire")) %>%
  mutate(countries = replace(countries, countries == "T\xfcrkiye", "Türkiye"))

setnames(new_grains_all, c("countries", "food_group", "Total"), c("countryname", "covariate", "value"))

#bring in fbs data, which is still in kg/year space!
old_fbs_dta <- as.data.table(read_dta("FILEPATH/FAOSTAT_1961_2013_FOOD_BALANCE_SHEETS_PRELIM_NORM_Y2016M02D24.dta")) 
old_fbs_dta_grains_only <- filter(old_fbs_dta, `covariate` == 'refined_grains') 
old_grains_before_2010 <- filter(old_fbs_dta_grains_only, year < 2010) 

# rename Sudan, Laos, China mainland, Taiwan
old_grains_before_2010 <- old_grains_before_2010 %>%
  mutate(countryname = replace(countryname, countryname == "Sudan (former)", "Sudan")) %>%
  mutate(countryname = replace(countryname, countryname == "Lao People's Democratic Republic", "the Lao People's Democratic Republic")) %>%
  mutate(countryname = replace(countryname, countryname == "China, mainland", "China mainland")) %>%
  mutate(countryname = replace(countryname, countryname == "China, Taiwan Province of", "Taiwan"))

# Create ISO3_CODE column to fill
old_grains_before_2010$ISO3_CODE <- NA 

# combine old and new data
fbs_data <- rbind(old_grains_before_2010, new_grains_all)

FAOSTAT_names <- FAOSTAT_get %>% 
  mutate(ISO3_CODE = replace(ISO3_CODE, ISO3_CODE == "HKG", "CHN_354"),
         ISO3_CODE = replace(ISO3_CODE, ISO3_CODE == "MAC", "CHN_361"))

# Group by country name, check different columns from FAOSTAT for NA values, and fill them with ISO3 code
fbs_data_add_iso3 <- fbs_data %>%
  group_by(countryname) %>%
  mutate(!!'ISO3_CODE' := ifelse(is.na(!!sym('ISO3_CODE')),
                               filter(FAOSTAT_names, SHORT_NAME == countryname)[, 'ISO3_CODE'][1],
                               !!sym('ISO3_CODE'))) %>%
  mutate(!!'ISO3_CODE' := ifelse(is.na(!!sym('ISO3_CODE')),
                                 filter(FAOSTAT_names, UNOFFICIAL1_NAME == countryname)[, 'ISO3_CODE'][1],
                                 !!sym('ISO3_CODE'))) %>%
  mutate(!!'ISO3_CODE' := ifelse(is.na(!!sym('ISO3_CODE')),
                                 filter(FAOSTAT_names, FAO_TABLE_NAME == countryname)[, 'ISO3_CODE'][1],
                                 !!sym('ISO3_CODE')))

refined_grains <- fbs_data_add_iso3 %>%
  mutate(covariate = replace(covariate, covariate == "Grains", "refined_grains"))

refined_grains <- as.data.table(refined_grains) %>%
  drop_na(ISO3_CODE) # this drops super regions and other aggregates

refined_grains[,ihme_risk:="diet_refined_grains"]
refined_grains[, value:=value*1000/365]

refined_grains <- merge(refined_grains, locs, by.x="ISO3_CODE", by.y="ihme_loc_id", all.x=TRUE) %>%
  drop_na(location_id) 


refined_grains$countryname <- NULL
refined_grains[, c("year_start", "year_end"):=year]

refined_grains[,svy:="FAO_Food_Balance_Sheets"]
refined_grains[,nid:=203327] 
refined_grains[,cv_fao_data:=1]
finished_refined_grains <- refined_grains
output_file <- paste0(output_path, "FBS_grains_data_", version, ".csv") 
if(save_components){write.csv(finished_refined_grains, output_file, row.names = FALSE)}

######################################################
### 4) import nutrient data prepped in do files
######################################################

nutrient_data <- read_csv(paste0(input_path, "FBS_USDA_nutrients_",version,"_data.csv"))
nutrient_data <- as.data.table(nutrient_data)

nutrient_data[, covariate:=gsub("diet_","", ihme_risk)]
nutrient_data[covariate=="satfat", covariate:="saturated_fats"]
nutrient_data$nutrients_data <- 1
nutrient_data$risk <- NULL
setnames(nutrient_data, "mean", "value")
nutrient_data[,nid:=238501]



finished_nutrients <- nutrient_data
fwrite(finished_nutrients, paste0(output_path, "finished_nutrients.csv"))

###################################################################
#### 4) import sales data from the last time it was prepped
###################################################################

sales_data <- read_dta("FILEPATH") 
sales_data <- as.data.table(sales_data)
setnames(sales_data, "mean", "value")
sales_data[, covariate:=gsub("diet_","", ihme_risk)]
sales_data$sales_data <- 1
sales_data$risk <- NULL
sales_data[, nid:=282698]

#sales data already has an energy column. We will use that. 
sales_data[, energy:=energy*2459/3281]    # //this is a global scalar that will be applied to FAO energy estimates to make them comparable to household budget survey energy estimates 
                                          #(3281 = FAO energy estimate, 2459 = HHBS energy estimate); extracted from Serra-Majem et al. 2003,
                                          ## Comparative analysis of nutrition data from national, household, and individual levels: results from a WHO-CINDI collaborative project in Canada, Finland, Poland, and Spain
sales_data[ihme_risk=="diet_hvo_sales", value:= value*9/energy]
sales_data[ihme_risk=="diet_hvo_sales", case_definition:="% of total dietary energy"]
sales_data[, c("energy","level","super_region_name","region_name", "energy_adj_scalar","mean_adj","data_status"):=NULL]


output_file <- paste0(output_path, "sales_data_", version, ".csv")
finished_sales <- sales_data
if(save_components){write.csv(finished_sales, output_file, row.names=FALSE)}

##################################################################
###### compile it all and save it 
##################################################################
finished_sales <- fread(paste0(output_path, "sales_data_2022.csv"))
finished_food_groups <- fread(paste0(output_path, "SUA_foodgroups_data_2022.csv"))
finished_nutrients <- fread(paste0(output_path, "finished_nutrients.csv"))
finished_refined_grains <- fread(paste0(output_path, "FBS_grains_data_2022.csv"))
finished_energy <- fread("FILEPATH")

full_data <- rbind(finished_food_groups, finished_sales, fill=TRUE)
full_data <- rbind(full_data, finished_nutrients, fill=TRUE)
full_data <- rbind(full_data, finished_energy, fill=TRUE)
full_data <- rbind(full_data, finished_refined_grains, fill=TRUE)


full_data[is.na(nutrients_data), nutrients_data:=0]
full_data[is.na(cv_fao_data), cv_fao_data:=0]
full_data[is.na(sales_data), sales_data:=0]


setnames(full_data, c("covariate","value"), c("gbd_cause","grams_daily_unadj"))
full_data[, location_id:=as.numeric(location_id)]

full_data <- as.data.frame(full_data)
write.csv(full_data,paste0(output_path, "FAO_all_",version,".csv"))

sub_data <- data.frame("location_id"= unique(full_data$location_id))


write.csv(sub_data,paste0(output_path, "FAO_locyears.csv")) 


