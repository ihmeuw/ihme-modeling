#####################################################################
## Plot PREMISE in Italy and compare with Facebook ##
#####################################################################
library(ggplot2)
source(file.path("/ihme/cc_resources/libraries/current/r/get_location_metadata.R"))
source(file.path("/ihme/cc_resources/libraries/current/r/get_population.R"))

## Tables (merge together a covid and gbd hierarchy to get needed information for aggregation)
hierarchy <- get_location_metadata(location_set_id = 115, location_set_version_id = 746)

dt <- fread("/ihme/covid-19/mask-use-outputs/best/used_data.csv")

it <- dt[location_id %in% hierarchy[parent_id == 86, location_id]]

# compare against Facebook
it <- it[source == "Facebook"]

# Aggregate to national
it[, mask_count := N * prop_always]
it <- it[, lapply(.SD, function(x) sum(x)), by="date", .SDcols = c("N","mask_count")]
it[, mask_use := mask_count / N]
it[, source := "Facebook"]
it[, date := as.Date(date)]

premise <- fread("/home/j/Project/covid/data_intake/Premise_data/2020_09_28/italian_premise_data.csv")

premise[, date := as.Date(created)]
premise[, N := 1]
premise[, mask_use := when_you_leave_your_home_do_you_typically_wear_a_face_mask]
premise[, mask_count := ifelse(mask_use == "yes_always", 1, 0)]

premise <- premise[, lapply(.SD, function(x) sum(x)), by="date", .SDcols = c("N","mask_count")]
premise[, mask_use := mask_count / N]
premise[, source := "Premise"]

dt <- rbind(it, premise)

ss <- dt[, lapply(.SD, function(x) mean(x)), by="source", .SDcols = "N"]
ss

crosswalk <- merge(premise, it, by="date")
mean(crosswalk$mask_use.y / crosswalk$mask_use.x)

ggplot(dt, aes(x=date, y = mask_use, col = source)) + geom_point(alpha = 0.5, aes(size = N)) + theme_bw()


