################################################################
## Make population weights ##
################################################################
library(data.table)

source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")

hierarchy <- get_location_metadata(location_set_id = 115, location_set_version_id = 746)
population <- get_population(location_id = hierarchy$location_id, age_group_id = c(8:21), sex_id=c(1,2), year_id = 2019, gbd_round_id = 6, decomp_step = "step4")

# this map isn't right, but use to find facebook age groups
age_map <- fread('FILEPATH/fb_gbd_age_mapping.csv')
age_map[, age_year := NULL]
age_map <- unique(age_map)
age_map <- age_map[!(age_fb %in% c("Under 18"))]
age_map <- age_map[!(age_gbd_bin %in% c("80 to 84"))]

population <- merge(population, age_map, by="age_group_id")

loc_pop <- population[, lapply(.SD, function(x) sum(x)), by="location_id", .SDcols = "population"]
setnames(loc_pop, "population", "location_population")

age_gp_pop <- population[, lapply(.SD, function(x) sum(x)), by=c("location_id","sex_id","age_fb"), .SDcols = "population"]

sample_pop <- merge(age_gp_pop, loc_pop, by="location_id")
sample_pop[, population_probability := population / location_population]
sample_pop[, population_weight := 1 / population_probability]

ggplot(sample_pop, aes(x=age_fb, y=population_weight, col=factor(sex_id))) + geom_boxplot() +
  scale_y_log10("Weight")

setnames(sample_pop, c("sex_id","age_fb"), c("sex","age"))
write.csv(sample_pop, 'FILEPATH/regression_weights_gbd.csv', row.names=F)

###################################################################
## Make weights for Global Facebook ##
# Tabulate age/sex/location (date?)
  glb <- fread('FILEPATH/summary_individual_responses_global.gz')
    setnames(glb, "age_group", "age")
    glb[, sex := ifelse(gender == "male", 1, ifelse(gender == "female", 2, NA))]
    glb <- glb[!is.na(sex)]
    glb[, age := ifelse(age == "> 75", "75+", as.character(age))]
    
  glb[, row := 1]
  glb_t <- glb[, lapply(.SD, function(x) sum(x)), by=c("age","sex","location_id"), .SDcols = "row"]
  glb_c <- glb[, lapply(.SD, function(x) sum(x)), by=c("location_id"), .SDcols = "row"]
    setnames(glb_c, "row", "total_n")
  glb_t <- merge(glb_t, glb_c, by="location_id")
  glb_t <- glb_t[!is.na(location_id)]
  glb_t[, sample_probability := row / total_n]
  
  write.csv(glb_t, 'FILEPATH/tabulated_global.csv', row.names = F)
  
  glb_t <- merge(glb_t, sample_pop, by=c("location_id","age","sex"))
  glb_t$weight <- glb_t$population_probability / glb_t$sample_probability
  glb_t <- merge(glb_t, hierarchy[,c("location_id","location_name","super_region_name","region_name")], by="location_id")
  head(glb_t)
  
  ggplot(glb_t, aes(x=age, y=weight, col=factor(sex))) + geom_boxplot() +
    scale_y_log10("Weight")
  
  ggplot(glb_t, aes(x=age, y=weight, col=factor(sex))) + geom_boxplot() +
    scale_y_log10("Weight") + facet_wrap(~super_region_name)
  
  setdiff(unique(glb$location_id), unique(glb_t$location_id))
  setdiff(unique(glb_t$location_id), unique(glb$location_id))

## Make weights for US Facebook ##
  # Tabulate age/sex/location (date?)
  usa <- fread('FILEPATH/summary_individual_responses_us.gz')
  usa[, age := NULL]
  setnames(usa, "age_group", "age")
  unique(usa$age)
  unique(sample_pop$age)
  usa[, sex := gender]
  usa <- usa[!is.na(sex)]
  usa[, age := ifelse(age == "24-34", "25-34", as.character(age))]
  usa <- usa[age != ""]
  
  usa[, row := 1]
  usa_t <- usa[, lapply(.SD, function(x) sum(x)), by=c("age","sex","location_id"), .SDcols = "row"]
  usa_c <- usa[, lapply(.SD, function(x) sum(x)), by=c("location_id"), .SDcols = "row"]
  setnames(usa_c, "row", "total_n")
  usa_t <- merge(usa_t, usa_c, by="location_id")
  usa_t <- usa_t[!is.na(location_id)]
  usa_t[, sample_probability := row / total_n]
  
  write.csv(usa_t, 'FILEPATH/tabulated_us.csv', row.names = F)
  
  usa_t <- merge(usa_t, sample_pop, by=c("location_id","age","sex"))
  usa_t$weight <- usa_t$population_probability / usa_t$sample_probability
  head(usa_t)
  
  ggplot(usa_t, aes(x=age, y=weight, col=factor(sex))) + geom_boxplot() +
    scale_y_log10("Weight")
  
  setdiff(unique(usa$location_id), unique(usa_t$location_id))
  setdiff(unique(usa_t$location_id), unique(usa$location_id))
  