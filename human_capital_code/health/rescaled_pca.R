# PCA on and applied to rescaled prevalences

rm(list = ls())

pacman::p_load(data.table, magrittr)

# helpers
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/sql_query.R")

age_std <- sql_query(dbname="",
                     host="",
                     query=paste0("")
)

# age groups 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235
# all nats and subnats
prev_data <- fread("/FILEPATH/prev_data_05092018.csv")

prev_data[,health_score:=NULL]
names(prev_data)[6:10] <- c("anemia", "blindness", "dev_dis", "hearing", "inf_yld") # , "stunt", "waste")

# rescale components to 0 to 1
prev_data[,anemia:= scales::rescale_max(x = anemia, to = c(0, 1), from = (quantile(anemia, c(0.01, 0.99))))]
prev_data[anemia > 1, anemia:= 1]

prev_data[,blindness:= scales::rescale_max(x = blindness, to = c(0, 1), from = (quantile(blindness, c(0.01, 0.99))))]
prev_data[blindness > 1, blindness:= 1]

prev_data[,dev_dis:= scales::rescale_max(x = dev_dis, to = c(0, 1), from = (quantile(dev_dis, c(0.01, 0.99))))]
prev_data[dev_dis > 1, dev_dis:= 1]

prev_data[,hearing:= scales::rescale_max(x = hearing, to = c(0, 1), from = (quantile(hearing, c(0.01, 0.99))))]
prev_data[hearing > 1, hearing:= 1]

prev_data[,inf_yld:= scales::rescale_max(x = inf_yld, to = c(0, 1), from = (quantile(inf_yld, c(0.01, 0.99))))]
prev_data[inf_yld > 1, inf_yld:= 1]

prev_data[,stunt:= scales::rescale_max(x = stunt, to = c(0, 1), from = (quantile(stunt, c(0.01, 0.99))))]
prev_data[stunt > 1, stunt:= 1]

prev_data[,waste:= scales::rescale_max(x = waste, to = c(0, 1), from = (quantile(waste, c(0.01, 0.99))))]
prev_data[waste > 1, waste:= 1]

fwrite(prev_data[,1:12], 'FILEPATH/rescaled_prevs_05092018.csv')

# select only groups 20 to 64 
pca_data <- prev_data[age_group_id %in% c(9:17)]

# get populations and merge onto prevalences
loc_meta <- get_location_metadata(location_set_id = 35, gbd_round_id = 4)

pops <- get_population(location_id = loc_meta[,c(location_id)], age_group_id = c(9:17), 
                       sex_id = 1:3, year_id = 1990:2016)

pca_data <- merge(pca_data, pops, by = c("age_group_id", "year_id", "location_id", "sex_id"))

# standardize sexes
pca_data <- pca_data[, .(anemia = sum(anemia * population) / sum(population),
                      blindness = sum(blindness * population) / sum(population),
                      dev_dis = sum(dev_dis * population) / sum(population), 
                      hearing = sum(hearing * population) / sum(population),
                      inf_yld = sum(inf_yld * population) / sum(population),
                      stunt = sum(stunt * population) / sum(population),
                      waste = sum(waste * population) / sum(population)), by = .(location_name, location_id, year_id, age_group_id)]

# standardize ages
pca_data <- merge(pca_data, age_std, by = "age_group_id")
pca_data[,anemia := anemia * age_group_weight_value]
pca_data[,blindness := blindness * age_group_weight_value]
pca_data[,dev_dis := dev_dis * age_group_weight_value]
pca_data[,hearing := hearing * age_group_weight_value]
pca_data[,inf_yld := inf_yld * age_group_weight_value]
pca_data[,stunt := stunt * age_group_weight_value]
pca_data[,waste := waste * age_group_weight_value]

pca_data <- pca_data[,.(anemia = mean(anemia),
                        blindness = mean(blindness),
                        dev_dis = mean(dev_dis),
                        hearing = mean(hearing),
                        inf_yld = mean(inf_yld), 
                        stunt = mean(stunt), 
                        waste = mean(waste)), by = .(location_name, location_id, year_id)]



##### run analysis ####
pca <- prcomp(pca_data[,c(4:10)])

# table of weights
weights <- as.data.frame(pca$rotation[,1:7])
weights[,8] <- rownames(weights)
setDT(weights)
names(weights) <- c("PC1", "PC2", "PC3", "PC4", "PC5", "PC6", "PC7", "cause")
weights <- weights[,.(cause, PC1, PC2, PC3, PC4, PC5, PC6, PC7)]

fwrite(weights, "/FILEPATH/pca_weights_05092018.csv")


# merge GDPpc
setDT(pca_data)
cordata <- pca_data[,.(location_name, location_id, year_id)]

for (i in c(1:7)) {
  var <- paste0("pc", as.character(i))
  cordata[,c(var) := pca$x[,i]]
}

ns_gdp <- read_feather("/FILEPATH.feather")
ns_gdp <- as.data.table(ns_gdp)
ns_gdp <- ns_gdp[year %in% c(1990:2016) & scenario == "reference"]
ns_gdp <- ns_gdp[, mean := rowMeans(.SD, na.rm=T), .SDcols=(grep("draw", names(ns_gdp), value=T))]
ns_gdp <- ns_gdp[,.(iso3, year, mean)]
names(ns_gdp) <- c("ihme_loc_id", "year_id", "gdp_pc")
ns_gdp <- merge(ns_gdp, loc_meta[,.(ihme_loc_id, location_name)], by = "ihme_loc_id")

cordata <- merge(cordata, ns_gdp, by = c("location_name", "year_id"))

results <- data.table(var_share = summary(pca)$importance[2, 1:7]) # proportion of variance
rownames(results) <- c("PC1", "PC2", "PC3", "PC4", "PC5", "PC6", "PC7")

results[, cor_gdp := cor(cordata[,c(4:10, 12)])[c(1:7), 8]] # correlation to GDPpc

fwrite(results, "/FILEPATH/pca7_rescaled_results.csv")

weights.used <- weights$PC1 / sum(weights$PC1)

prev_data[, paste0('wght_', weights$cause) := lapply(c(1:7), function(x) weights.used[x]) ]

prev_data[, health_stuff:= eval(parse(text = paste(paste0(  weights$cause,  "*",  paste0('wght_', weights$cause), collapse = '+')) ) )]

fwrite(prev_data, paste0('/FILEPATH/rescaled_prevs_w_score_', 
                     format(Sys.Date(), format="%m%d%Y"),
                     '.csv'))