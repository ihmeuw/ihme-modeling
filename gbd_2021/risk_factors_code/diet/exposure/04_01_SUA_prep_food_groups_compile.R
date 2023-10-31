################################################################################
## DESCRIPTION ##  This preps the SUA to be used for food groups availability estimates and combines it with the sales, nutrients, and energy data!
#                        A replacement for 04_prep_FAO_sales_grams.do
################################################################################

rm(list = ls())

# System info & drives
os <- Sys.info()[1]
user <- Sys.info()[7]
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"
code_dir <- if (os == "Linux") paste0("FILEPATH/", user, "/") else if (os == "Windows") ""
source(paste0(code_dir, 'FILEPATH/.R'))
library("readstata13")

save_components <- FALSE
version <- "GBD2021"

################################################################
###### 1) aggregate the SUA data to food groups
################################################################
include_refuse_var <- TRUE
energy_prep <- TRUE
raw_sua_data <- read.dta13("FILEPATH/data.dta")
output_file <- paste0("FILEPATH/SUA_foodgroups_data_GBD2021.csv")
food_ids <- fread("FILEPATH/SUA_foodgroup_codebook.csv")
food_ids <- food_ids[!is.na(food_group)]
sua_data <- as.data.table(raw_sua_data)
sua_data[, data:=data*100]
sua_data[, ele_name:="g/person/day"]
sua_data <- sua_data[!is.na(data),]

#merge food ids and data so we can aggregate for each food group
df_gram <- merge(x=food_ids, y=sua_data, by.x="com", by.y="product_codes", all.x = FALSE, all.y=FALSE)
df_gram <- as.data.table(df_gram)
df_gram$comn.x <- NULL
setnames(df_gram, "comn.y", "comn")
milk_rows <- df_gram[food_group == "Milk",]
milk_rows$food_group <- "Dairy"
df_gram <- rbind(df_gram, milk_rows)

#aggregate for each food group

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


#Seychelles are not in FBS at all so lets drop the country entirely
df_gram <- df_gram[location_id!=186,]
#legumes
df_gram <- df_gram[!(covariate=="pulses_legumes" & location_id %in% c(105,33,34,305,83,23,59,27,393,40,30)),]
#nuts and seeds
df_gram <- df_gram[!(covariate=="nuts_seeds" & location_id %in% c(105,305,203,110,173,112,23,194,185,27,215,393,117,118,19,30)),]
#removing three oceania countries just because they are outliers 
df_gram <- df_gram[!(covariate=="nuts_seeds" & location_id %in% c(28,26,22)),]
#oil
df_gram <- df_gram[!(covariate=="total_oil" & location_id %in% c(43,105,33,107,57,108,121,45,204,98,354,77,110,141,127,58,22,35,112,83,115,36,23,37,12,59,146,194,60,87,49,14,88,61,50,148,195,53,54,17,393,116,117,118,39,119,40,99,41,20)),]
#total dairy (this is just a choice outlier because Laos has values ~1 compared to hundreds in other countries)
df_gram <- df_gram[!(covariate=="all_dairy" & location_id==12 ),]

finished_food_groups <- df_gram
if(save_components){write.csv(finished_food_groups, output_file, row.names = FALSE)}


###############################################
#####  2) Energy prep using SUAs
###############################################


if( energy_prep){
 output_file <- paste0("FILEPATH/SUA_foodgroups_data_GBD2021.csv")
   raw_sua_data <- as.data.table(raw_sua_data)
  energy_prep <- raw_sua_data[!is.na(data),]
  df_gram <- merge(x=food_ids, y=energy_prep, by.x="com", by.y="product_codes", all.x = FALSE, all.y=FALSE)
  df_gram <- as.data.table(df_gram)
  df_gram$comn.x <- NULL
  setnames(df_gram, "comn.y", "comn")
  df_gram[, kcals_per_day:= usda_kcal_100_grams*data]
  prepped_kcals <- df_gram[,sum(kcals_per_day, na.rm=TRUE), by=list(countries, location_id, year, ele_name)]  
  setnames(prepped_kcals, "V1", "value")
  prepped_kcals <- prepped_kcals[!(countries=="Palestine_occupied" & year < 1996),]
  prepped_kcals <- prepped_kcals[!(countries=="Sudan" & year < 2012),]
  prepped_kcals <- prepped_kcals[!(countries=="Montenegro" & year < 2006),]
  prepped_kcals <- prepped_kcals[value > 1200]
  prepped_kcals <- prepped_kcals[!(countries=="Burundi" | location_id==175),]    
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
fbs_data <- as.data.table(read.dta13("FILEPATH/FAOSTAT_1961_2013_FOOD_BALANCE_SHEETS_PRELIM_NORM_Y2016M02D24.dta")) 
refined_grains <- fbs_data[covariate=="refined_grains"]
refined_grains[,ihme_risk:="diet_refined_grains"]
refined_grains[, value:=value*1000/365]
fao_name_codebook <- fread("/FILEPATH/fao_name_codebook_long.csv")
refined_grains[countryname=="China, mainland", countryname:="China_main"]
refined_grains[countryname=="Côte d'Ivoire" , countryname:="Ivory_Coast"]
refined_grains <- merge(refined_grains, fao_name_codebook, by.x="countryname", by.y="fao_name", all.x=TRUE)
refined_grains[, c("year_start", "year_end"):=year]

setnames(refined_grains, "countryname", "location_name")
refined_grains[,ihme_countryname:=NULL]
refined_grains[,svy:="FAO_Food_Balance_Sheets"]
refined_grains[,nid:=203327]  #
refined_grains[,cv_fao_data:=1]
finished_refined_grains <- refined_grains
output_file <- paste0("FILEPATH",version,"/FBS_grains_data_", version, ".csv")
if(save_components){write.csv(finished_refined_grains, output_file, row.names = FALSE)}

######################################################
### 4) import nutrient data prepped in do files
######################################################

nutrient_data <- read.dta13(paste0("FILEPATH",version,"/FBS_USDA_nutrients_",version,".dta"))
nutrient_data <- as.data.table(nutrient_data)
nutrient_data[, covariate:=gsub("diet_","", ihme_risk)]
nutrient_data[covariate=="satfat", covariate:="saturated_fats"]
nutrient_data$nutrients_data <- 1
nutrient_data$risk <- NULL
setnames(nutrient_data, "mean", "value")
nutrient_data[,nid:=238501]
finished_nutrients <- nutrient_data

###################################################################
#### 4) import sales data from the last time it was prepped
###################################################################

sales_data <- read.dta13("/FILEPATH/sales_data_outliered_2017_1.dta")
sales_data <- as.data.table(sales_data)
setnames(sales_data, "mean", "value")
sales_data[, covariate:=gsub("diet_","", ihme_risk)]
sales_data$sales_data <- 1
sales_data$risk <- NULL
sales_data[, nid:=282698]
 # //this is a global scalar that will be applied to FAO energy estimates to make them comparable to household budget survey energy estimates (3281 = FAO energy estimate, 2459 = HHBS energy estimate); extracted from Serra-Majem et al. 2003, Comparative analysis of nutrition data from national, household, and individual levels: results from a WHO-CINDI collaborative project in Canada, Finland, Poland, and Spain
sales_data[, energy:=energy*2459/3281]   
sales_data[ihme_risk=="diet_hvo_sales", value:= value*9/energy]
sales_data[ihme_risk=="diet_hvo_sales", case_definition:="% of total dietary energy"]
sales_data[, c("energy","level","super_region_name","region_name", "energy_adj_scalar","mean_adj","data_status"):=NULL]
output_file <- paste0("FILEPATH",version,"/sales_data_", version, ".csv")
finished_sales <- sales_data
if(save_components){write.csv(finished_sales, output_file, row.names=FALSE)}

##################################################################
###### compile it all and save it 
##################################################################


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
save.dta13(full_data,paste0("FILEPATH",version,"/FAO_all_",version,".dta"))
sub_data <- data.frame("location_id"= unique(full_data$location_id))
save.dta13(sub_data,paste0("FILEPATH",version,"/FAO_locyears.dta"))

###
if(FALSE){
  raw_sua_data <- read.dta13("FILEPATH/data.dta")
    #fbs_data 
  sua_data <- as.data.table(read.dta13("FILEPATH/reshaped_josef_sua_2017.dta")) 
  #keep if ele codes =11 or 141
  sua_data <- sua_data[ele_codes==11 | ele_codes==141]
  codes <- fread("FILEPATH/USDA-FAOSTAT-codes3.csv")
  setnames(codes, "com","product_codes")
    #test which product codes are not in there
  food_ids <- fread("FILEPATH/SUA_foodgroup_codebook.csv")
  fish_ids <- food_ids[food_group=="Fish"]
  fish_codes <- codes[product_codes %in% fish_ids$com]
  fish_codes$product_codes %in% sua_data$product_codes  
  fish_ids$com %in% sua_data$product_codes              
  fish_ids$com %in% codes$product_codes          
  
}




