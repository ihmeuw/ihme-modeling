#################################################################################
#################################################################################
### Purpose: To model custom  CSMR  using 
###          proportions from star 4 and 5 star CoD
### Measure: mtspecific (CSMR)
#################################################################################
#################################################################################

rm(list=ls())


### Load functions and packages
library(msm, lib.loc=FILEPATH)
library(metafor, lib.loc=FILEPATH)
pacman::p_load(data.table, ggplot2, dplyr, stringr, DBI, openxlsx, gtools, plyr, readxl)
base_dir <- FILEPATH
functions <- c("get_age_metadata", "get_location_metadata")
lapply(paste0(base_dir, functions, ".R"), source)


date <- gsub("-", "_", Sys.Date())
home_dir <-  FILEPATH

## Set parameters
cause_id = CAUSE_ID
age_group_ids<-c(2:3, 6:20, 30:34, 235, 238,388, 389) 

##get locations 
loc_table <- get_location_metadata(gbd_round_id = 7, location_set_id=35)
super_region_dt <- loc_table[, .(location_id, location_name, super_region_name, super_region_id, region_name, region_id)]
loc_table <- loc_table[most_detailed == 1, ]
locs <- loc_table[, location_id]


## INPUT DATA -------------------------------------------------------------------------------------------------------------------------------------------------
prop_dt <- as.data.table(read.xlsx(paste0(FILEPATH, "/thyroid_disorders_4_5_star_COD_data_without775.3.xlsx")))
all_dt <- as.data.table(read.xlsx(paste0(FILEPATH,"/thyroid_disorders_4_5_star_COD_data_all.xlsx")))


## Aggregate number of deaths for each source
prop_dt <- prop_dt[, prop_deaths := sum(deaths), by = c("location_id", "sex_id", "year_id")]
prop_dt[, c("cause_id", "icd_code", "deaths","nid", "underlying_nid","age_group_id")] <- NULL
prop_dt <- unique(prop_dt)

all_dt <- all_dt[, all_deaths := sum(deaths), by = c( "location_id", "sex_id",  "year_id")]
all_dt[, c("cause_id", "icd_code", "deaths","nid", "underlying_nid","age_group_id")] <- NULL
all_dt <- unique(all_dt)


#Merge prop_dt and all_dt
combined <- merge(prop_dt, all_dt, by = c( "location_id", "sex_id",  "year_id"), all=TRUE) 
combined[(is.na(prop_deaths)), prop_deaths:=0]

#calculate disease proportion without nontoxic goiter
combined <- combined[, disease_prop := prop_deaths/all_deaths]

#remove where death is zero for both sources
combined <- subset(combined, !(prop_deaths==0 & all_deaths==0))
test <- subset(combined, disease_prop==0) 

## Calculate se: SEp = sqrt [ p(1 - p) / n ] from https://stattrek.com/estimation/standard-error.aspx
combined[,  standard_error := sqrt((disease_prop*(1-disease_prop))/all_deaths)]
test <- subset(combined, standard_error==0) 


#merge combined with region
combined <- merge(super_region_dt, combined, by = "location_id")
unique(combined$super_region_id) 
sort(unique(combined$year_id))


# DIAGNOSTIC PLOTS -----------------------------------------------------------------------------------------------------------------------------------
pdf(paste0(home_dir, "4_5_star_", date, "_super_region.pdf"),  width=20, height=8) # this will be the name of the file
p <- 1
locs <-(unique(combined[, super_region_id])) 
combined$sex[combined$sex_id==1] <- "Male"
combined$sex[combined$sex_id==2] <- "Female"
group.colors <- c("Male" = "blue", "Female" = "red")

for (loc in locs) {
  message(p)
  p <- p+1
  data <- copy(combined[super_region_id == loc, ])
  locs_name <- data$super_region_name
  data_plot <- ggplot() +
    geom_point(data, mapping = aes(x = year_id, y = disease_prop)) +    facet_wrap(~sex) +
    theme_bw() + 
    ggtitle(paste0("Thyroid proportion comparison in ",  locs_name, "; total vs. excluding nontoxic goiter")) +
    scale_color_manual( values = group.colors) + xlab("Year") + ylab("Proportion") 
  print(data_plot)
}
dev.off()



ggplot() +
  geom_point(combined, mapping = aes(x = year_id, y = disease_prop, color = super_region_name.x)) +    facet_wrap(~sex) +
  theme_bw() + 
  ggtitle(paste0("Thyroid proportion comparison; total vs. excluding nontoxic goiter")) +
 xlab("Year") + ylab("Proportion") 



#########################################################################################################
## Proportion 
#########################################################################################################
#create dummy variables for sex
combined <- within(combined, {sex_binary=ifelse(sex_id ==2, 1, 0)})

#run linear regression model
prop_fit <- glm(disease_prop ~ year_id + sex_binary + factor(super_region_name), data=combined, binomial(link = "logit") )

summary(prop_fit)
unique(combined$super_region_name)


######################################################################################################## 
## Create metadata
######################################################################################################## 

## Get meta data
loc_table <- loc_table[, c("location_id", "super_region_name")] 
super_region_dt <- subset(loc_table, (super_region_name!=""))
super_region_dt$super_region_name1 <- super_region_dt$super_region_name
super_region_dt[(super_region_name=="South Asia" | super_region_name=="Sub-Saharan Africa"), super_region_name := "North Africa and Middle East"]

sex_dt <- read.csv(FILEPATH)
sex_dt <- subset(sex_dt, sex_id==c(1, 2))
sex_dt$sex_binary[sex_dt$sex_id==2] <- 1
sex_dt$sex_binary[sex_dt$sex_id==1] <- 0
year_id <- c(seq(1985, 2015, 5), 2019, 2020, 2021, 2022)

## Create master data frame for predictions
meta_dt <- tidyr::crossing(super_region_dt, sex_dt)
meta_dt <- tidyr::crossing(meta_dt, year_id)
meta_dt <- as.data.table(meta_dt)



######################################################################################################## 
## Create metadata
######################################################################################################## 
predictions <- as.data.table(predict(prop_fit, se.fit = TRUE, newdata = meta_dt)) 

write.xlsx(meta_dt_pred, FILEPATH, sheetName = "extraction")

