##### Chagas disease_
# Function: Age split Chagas data using pydisagg

################################################################################
# If you are using the Scicomp beta image with R 4.4.0 then load the reticulate library like this:
# library(reticulate)
source("FILEPATH/get_draws.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_bundle_version.R")

# If you are using the default image (R 4.2.2) then you need to load the reticulate library like this: 
library(reticulate, lib.loc = "FILEPATH")
reticulate::use_python("FILEPATH")
splitter <- import("pydisagg.ihme.splitter")

library(data.table)
library(dplyr)

################################################################################

ages <- get_age_metadata(release_id = ADDRESS, age_group_set_id =ADDRESS )

# data to be split
data <- fread("FILEPATH")
data[sex == "Male", sex_id :=1]
data[sex == "Female", sex_id :=2]
data[, year_id := round((year_start + year_end)/2,0)]
data[, age_range := age_end - age_start]

pre_split_age_specific <- subset(data, age_range < 25)
pre_split <- subset(data, age_range >= 25)

pre_split[mean == 0, mean:= 0.00000000001]
pre_split[mean == 1, mean:= 0.9999999999]
 

# LAC for 2010
pattern <- get_draws(
  gbd_id_type ="modelable_entity_id",
  source = "epi",
  gbd_id =  c(ADDRESS),
  location_id=103,
  year_id=c(ADDRESS),
  sex_id=c(1,2),
  #  measure_id=c(ADDRESS),
  # metric_id=c(ADDRESS),
  age_group_id=c(ages$age_group_id),  
  version_id =ADDRESS,
  release_id=ADDRESS
)

pattern <- subset(pattern, select = -c(year_id, location_id ))
pattern <- merge(x=pattern, y= ages, by="age_group_id", all.x=T)

# get pop
locs_pop <- unique(pre_split$location_id)

pop <- get_population(location_id = locs_pop, year_id= c(ADDRESS), sex_id = c(1,2), release_id = ADDRESS, age_group_id = c(ages$age_group_id))

# All are example column names
data_config <- splitter$DataConfig(
  index=c("nid","origin_seq", "location_id", "year_id", "sex_id"),
  age_lwr="age_start",
  age_upr="age_end",
  val="mean",
  val_sd="standard_error"
)

draw_cols <- grep("^draw_", names(pattern), value = TRUE)

pattern_config <- splitter$PatternConfig(
  by=list("sex_id"),
  age_key="age_group_id",
  age_lwr="age_group_years_start",
  age_upr="age_group_years_end",
  draws=draw_cols,
  val="mean_draw",
  val_sd="var_draw"
)

pop_config <- splitter$PopulationConfig(
  index=c("age_group_id", "location_id", "year_id", "sex_id"),
  val="population"
)

age_splitter <- splitter$AgeSplitter(
  data=data_config, pattern=pattern_config, population=pop_config
)

result <- age_splitter$split(
  data=pre_split,
  pattern=pattern,
  population=pop,
  model="logodds",
  output_type="rate"
)

result <- as.data.table(result)

###### format the result
result1 <- subset(result, select =c("nid","origin_seq", "location_id","year_id", "sex_id" , "pat_age_group_years_start", "pat_age_group_years_end", "split_result", "split_result_se"))
result2 <- merge(x= pre_split, y= result1, by = c("nid","origin_seq", "location_id", "year_id", "sex_id"), all.x=T)
result2[, age_start := pat_age_group_years_start]
result2[, age_end:= pat_age_group_years_end]
result2[, mean := split_result]
result2[, stadard_error:= split_result_se]

result2[,cases := NULL]
result2[,sample_size := NULL]
result2[,upper := NULL]
result2[,lower := NULL]
result2[, note_modeler := paste0(ADDRESS)]
#result2[male_mean ==0 & female_mean ==0, mean := 0]
#result2[male_mean ==0 & female_mean ==0, lower := 0]

### Combine with the non-age-split data
finaldata <- rbind(pre_split_age_specific, result2, fill=T)

#### delete seq , and prepare to save crosswalk version
finaldata[, crosswalk_parent_seq := origin_seq]
finaldata[is.na(upper), uncertainty_type_value := NA]
finaldata[!(is.na(upper)), uncertainty_type_value :=95]

finaldata[, c("group", "group_review", "specificity") := NA]
finaldata[,seq:=NULL]
finaldata[, seq := ""]

finaldata[tracking_id == ADDRESS , field_citation_value := "Lima MM, Sarquis O, de Oliveira TG, Gomes TF, Coutinho C, Daflon-Teixeira NF, Toma HK, Britto C, Teixeira BR, D'Andrea PS, Jansen AM, Bóia MN, Carvalho-Costa FA. Investigation of Chagas disease in four periurban areas in northeastern Brazil: epidemiologic survey in man, vectors, non-human hosts and reservoirs. Transactions of the Royal Society of Tropical Medicine and Hygiene. 2012 Mar 1;106(3):143-9." ]

#### save crosswalk version
# saving it as a flat file
openxlsx::write.xlsx(finaldata, sheetName = "extraction", file = paste0("FILEPATH"))

# This is for bundle_id ADDRESS
cwsave <- save_crosswalk_version(bundle_version_id = ADDRESS , 
                                 data_filepath = "FILEPATH" , 
                                 description = "Chagas_among_PaR_age_sex_split") 