
rm(list = ls())

source("FILEPATH")

# Read the location as an argument
args <- commandArgs(trailingOnly = TRUE)
l <- ifelse(!is.na(args[1]),args[1], 101) # take location_id
ages      <- c(2:20, 30:32, 235)
sexes     <- c(1,2)

df<-get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 24811, source = "epi",
            location_id = l, gbd_round_id = 6, year_id = 1990:2019,
            decomp_step = "step4", age_group_id = ages, sex_id = sexes)

draw_cols<-paste0("draw_", 0:999)
df<-as.data.table(df)
df<-df[, (draw_cols):=lapply(.SD, function(x) {1-x}), .SDcols = draw_cols]

df<-df[, modelable_entity_id:=24812]

write.csv(df, paste0("FILEPATH", l, ".csv"), na = "", row.names = F)

