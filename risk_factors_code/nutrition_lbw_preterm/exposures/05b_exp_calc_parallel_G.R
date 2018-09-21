## Calculate 0-6 prevalence, 7-27 days prevalence from birth prevalence


rm(list=ls())

print(Sys.time())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "FILEPATH"
  h <- "FILEPATH"
  
} else {
  j<- "FILEPATH"
  h<- "FILEPATH"
  
}

library(data.table)

args <- commandArgs(trailingOnly = TRUE)

loc <- args[1]

distn <- args[2]

print(paste(distn, loc))


########################
## Load functions

set_NAs_to_zero <- function(DT) {
  
  for (j in names(DT))
    set(DT,which(is.na(DT[[j]])),j,0)
  
}

save_me_file <- function(dt, age){
  #print(age)
  dt <- merge(dt, me_map[, c("modelable_entity_name", "modelable_entity_id")], by = "modelable_entity_name", all.x = T)
  
  modelable_entity_id <- unique(dt$modelable_entity_id)
  
  dt <- dt[, -c("modelable_entity_id", "modelable_entity_name")]
  
  write.csv(dt, "FILEPATH", row.names = F)
  
}


########################
## Load in exposures, me_map & save_template


file_long <- data.table(readRDS("FILEPATH"))  
file_long[, draw := as.integer(draw)]

me_map <- fread("FILEPATH")
setnames(me_map, "categorical_parameter", "parameter")

save_template <- data.table(expand.grid(location_id = loc, sex_id = 1:2, year_id = c(1990, 1995, 2000, 2005, 2010, 2016), age_group_id = 2, 
                                        measure_id = 5, modelable_entity_name = me_map[["modelable_entity_name"]], draw = 0:999, stringsAsFactors = F))

save_template[, location_id := as.integer(location_id)]

########################
## Load in mortality data and subset to loc

mort <- fread("FILEPATH")
mort <- mort[location_id == loc, -c("location_name")]

mort <- melt(mort, id.vars = c("location_id", "year_id", "sex_id"), variable.name = "draw", variable.factor = F,
             value.name = c("births", "pop_surv_enn", "pop_surv_lnn", "qx_enn", "qx_lnn", "ly_enn", "ly_lnn", "mort_rate_enn", "mort_rate_lnn"),
             measure = patterns("births_", "pop_surv_enn_", "pop_surv_lnn_", "qx_enn_", "qx_lnn_", "ly_enn_", "ly_lnn_", "mort_rate_enn_", "mort_rate_lnn_"))

mort[, draw := as.integer(draw)]

## change draw 1000 to draw 0
mort[draw == 1000, draw := 0]




########################
## Load in the Relative Risk draws by sex
## Need to replace flat file with shared function

rr_1 <- fread("FILEPATH")
rr_1[, sex_id := 1]
rr_2 <- fread("FILEPATH")
rr_2[, sex_id := 2]

rr_draws <- rbindlist(list(rr_1, rr_2), use.names = T)
rr_draws <- rr_draws[cause_id == 686, -c("cause_id", "mortality", "morbidity")]
rr_draws <- melt(rr_draws, id.vars = c("sex_id", "age_group_id", "parameter"), value.name = "rr", variable.name = "draw")
rr_draws[, draw := as.integer(substr(draw, 4, 8))]
tmrels <- data.table(expand.grid(parameter = "cat56", sex_id = 1:2, age_group_id = 2:3, draw = 0:999, rr = 1, stringsAsFactors = F))

rr_draws <- rbindlist(list(rr_draws, tmrels), use.names = T)

rr_draws <- merge(rr_draws, me_map[, c("modelable_entity_name", "parameter")], all.y = T)
rr_draws[, sex_id := as.integer(sex_id)]



#######################
## Load & Save Birth Prevalence data

birth_long <- copy(file_long)

correct_NA <- birth_long[grep(x = modelable_entity_name, pattern = "NA g"), c("location_id", "sex_id", "age_group_id", "year_id", "draw", "ME_prop")]
setnames(correct_NA, "ME_prop", "add_ME_prop")

birth_long <- merge(correct_NA, birth_long, by = c("location_id", "sex_id", "age_group_id", "year_id", "draw"), all = T)
birth_long[is.na(add_ME_prop), add_ME_prop := 0]
birth_long[, ME_prop := ME_prop + add_ME_prop]
birth_long <- birth_long[!(grep(x = modelable_entity_name, pattern = "NA g")), ]
birth_long <- birth_long[, -c("add_ME_prop")]

print("at birth_long")

## Aggregate under 24

birth_long[, ga_start := lapply(strsplit(x = modelable_entity_name, ","), '[[', 1)]
birth_long[, ga_start := lapply(strsplit(x = unlist(ga_start), "\\["), '[[', 2)]

birth_long[, bw_start := lapply(strsplit(x = modelable_entity_name, ","), '[[', 3)]
birth_long[, bw_start := lapply(strsplit(x = unlist(bw_start), "\\["), '[[', 2)]

birth_long[bw_start == "5000 ) g", bw_start := "5000"]
birth_long[, bw_start := as.integer(bw_start)]
birth_long[, ga_start := as.integer(ga_start)]

birth_long[ga_start < 22, ga_start := 22]
birth_long[, ME_prop := lapply(.SD, sum, na.rm = T), by = list(sex_id, year_id, draw, bw_start, ga_start), .SDcols = "ME_prop"]

birth_long <- birth_long[!(grepl(x = modelable_entity_name, pattern = "\\[0, 20") | grepl(x = modelable_entity_name, pattern = "\\[20, 22)")), ]

birth_long <- birth_long[, -c("ga_start", "bw_start")]

birth_long <- merge(save_template, birth_long, by = c("modelable_entity_name", "location_id", "sex_id", "year_id", "age_group_id", "draw"), all.x = T)
set_NAs_to_zero(birth_long) 

birth_long <- unique(birth_long)

## Reshape wide
birth_wide <- dcast(birth_long, location_id + sex_id + age_group_id + year_id + modelable_entity_name + measure_id ~ paste0("draw_", draw), value.var = "ME_prop", variable.var = "draw_")
birth_wide[, age_group_id := 164]

## Split into individual MEs
birth_file_list <- split(x = birth_wide, by = "modelable_entity_name")

lapply(birth_file_list, save_me_file, age = "0_0")



#######################
## Calculate 0-6 SMR Grid
## Calculate 0-6 prevalence & 7 days

## Get birth exposures
smr_calc <- copy(birth_long)

## Merge with rr_draws -- only keep age_group_id = 2
print("at SMR_calc 0-6")

smr_calc <- merge(smr_calc, rr_draws, by = c("modelable_entity_name", "sex_id", "age_group_id", "draw"), all.x = T)

## Find sum of all ME's exp * rr by sex_id, age_group_id, "year_id", draw
smr_calc[, sum_all_exp_x_rr := sum(ME_prop * rr) + (1 - sum(ME_prop)), by = c("sex_id", "age_group_id", "year_id", "draw") ] ## ASSUMPTION HERE IS ALL RELATIVE RISK AFTER THE TMREL = 1;;

## Find sum by sex_id, age_group_id, "year_id", draw
smr_calc[, exp_x_rr := ME_prop * rr]
smr_calc[, smr := exp_x_rr / sum_all_exp_x_rr]

# Bin-Specific Mortality Rate per l/y/s between 0-6 days = Total All-Cause Mortality Rate per l/y/s between 0-6 days * Bin-specific Standardized Mortality Ratio per l/y/s between 0-6 days 

smr_calc <- merge(smr_calc, mort, by = c("location_id", "year_id", "sex_id", "draw"), all.x = T)

smr_calc[, bin_mort_rate_enn := mort_rate_enn * smr]

# Bin-Specific Number of births per l/y/s = "Total number of births per l/y/s" * "Prevalence of bin at birth per l/y/s"

smr_calc[, bin_births := births * ME_prop]

# Bin-Specific Number of deaths per l/y/s between 0-6 days = Bin-Specific Mortality Rate per l/y/s between 0-6 days * "Total Life Years between 0-6 days per l/y/s"                       

smr_calc[, bin_deaths_enn := bin_mort_rate_enn * ly_enn]

# Bin-Specific Number of survivors at 7 days = Bin-specific number of births per l/y/s  -  Bin-Specific Number of deaths per l/y/s between 0-6 days                                          

smr_calc[, bin_surv_7_days := bin_births - bin_deaths_enn]

smr_calc[bin_surv_7_days < 0, bin_surv_7_days := 0]  

# Prevalence at bin at 7 days per l/y/s = Bin-Specific number of survivors at 7 days / "Total Number of Survivors at 7 days per l/y/s"

smr_calc[,  ME_prop_7_days := bin_surv_7_days / pop_surv_enn ]


#######################
## SAVE PREV AT 7 DAYS

prev_7_7_long <- copy(smr_calc)
prev_7_7_long[, ME_prop := ME_prop_7_days]

## Reshape wide
prev_7_7_wide <- dcast(prev_7_7_long, location_id + sex_id + age_group_id + year_id + modelable_entity_name + measure_id ~ paste0("draw_", draw), value.var = "ME_prop", variable.var = "draw_")
prev_7_7_wide[, age_group_id := -7]
set_NAs_to_zero(prev_7_7_wide) 

## Split into individual MEs
prev_7_7_file_list <- split(x = prev_7_7_wide, by = "modelable_entity_name")

lapply(prev_7_7_file_list, save_me_file, age = "7_7")

# Bin-Specific Life Years between 0-6 days per l/y/s = Bin-Specific number of survivors in l/y/s at 7 days * 7 days  + Bin-Specific number of Deaths between 0-6 days in l/y/s * (0.6 + 0.4*3.5) / 365

smr_calc[,  bin_ly_enn := ( bin_surv_7_days * 7 / 365 ) + ( bin_deaths_enn * ((0.6 + 0.4*3.5) / 365) )  ]

# Bin-Specific Prevalence between 0-6 days per l/y/s = Bin-Specific Life Years between 0-6 days per l/y/s  /  "Total Life Years per l/y/s between 0-6 days"
smr_calc[, ME_prop_enn := bin_ly_enn / ly_enn ]



#######################
## SAVE PREV AT 0-6 DAYS

prev_0_6_long <- copy(smr_calc)
prev_0_6_long[, ME_prop_orig := ME_prop]
prev_0_6_long[, ME_prop := ME_prop_enn]
prev_0_6_long[, age_group_id := 2]

## Reshape wide
prev_0_6_wide <- dcast(prev_0_6_long, location_id + sex_id + age_group_id + year_id + modelable_entity_name + measure_id ~ paste0("draw_", draw), value.var = "ME_prop", variable.var = "draw_")
set_NAs_to_zero(prev_0_6_wide) 

## Split into individual MEs
prev_0_6_file_list <- split(x = prev_0_6_wide, by = "modelable_entity_name")

lapply(prev_0_6_file_list, save_me_file, age = "0_6")


#######################
## Calculate 7-27 SMR Grid
## Calculate 7-27 prevalence & 28 days

smr_calc <- smr_calc[, list(location_id, year_id, sex_id, draw, modelable_entity_name, age_group_id, measure_id, bin_surv_7_days, ME_prop_7_days, bin_births, bin_deaths_enn)]
smr_calc[, age_group_id := 3]

## Merge with rr_draws -- only keep age_group_id = 2

smr_calc <- merge(smr_calc, rr_draws, by = c("modelable_entity_name", "sex_id", "age_group_id", "draw"), all.x = T)

## Find sum of all ME's exp * rr by sex_id, age_group_id, "year_id", draw
smr_calc[, sum_all_exp_x_rr := sum(ME_prop_7_days * rr), by = c("sex_id", "age_group_id", "year_id", "draw") ]

## Find sum by sex_id, age_group_id, "year_id", draw
smr_calc[, exp_x_rr := ME_prop_7_days * rr]
smr_calc[, smr := exp_x_rr / sum_all_exp_x_rr]

# Bin-Specific Mortality Rate per l/y/s between 0-6 days = Total All-Cause Mortality Rate per l/y/s between 0-6 days * Bin-specific Standardized Mortality Ratio per l/y/s between 0-6 days 

smr_calc <- merge(smr_calc, mort, by = c("location_id", "year_id", "sex_id", "draw"), all.x = T)

smr_calc[, bin_mort_rate_lnn := mort_rate_lnn * smr]

# Bin-Specific Number of births per l/y/s = "Total number of births per l/y/s" * "Prevalence of bin at birth per l/y/s"



# Bin-Specific Number of deaths per l/y/s between 0-6 days = Bin-Specific Mortality Rate per l/y/s between 0-6 days * "Total Life Years between 0-6 days per l/y/s"                       

smr_calc[, bin_deaths_lnn := bin_mort_rate_lnn * ly_lnn]

# Bin-Specific Number of survivors at 28 days = Bin-specific number of births per l/y/s  -  Bin-Specific Number of deaths per l/y/s between 0-6 days                                          

smr_calc[, bin_surv_28_days := bin_surv_7_days - bin_deaths_lnn]

smr_calc[bin_surv_28_days < 0, bin_surv_28_days := 0]  

# Prevalence at bin at 28 days per l/y/s = Bin-Specific number of survivors at 7 days / "Total Number of Survivors at 7 days per l/y/s"

smr_calc[,  ME_prop_28_days := bin_surv_28_days / pop_surv_lnn ]





#######################
## SAVE PREV AT 28 DAYS

prev_28_28_long <- copy(smr_calc)
prev_28_28_long[, ME_prop := ME_prop_28_days]

## Reshape wide
prev_28_28_wide <- dcast(prev_28_28_long, location_id + sex_id + age_group_id + year_id + modelable_entity_name + measure_id ~ paste0("draw_", draw), value.var = "ME_prop", variable.var = "draw_")
prev_28_28_wide[, age_group_id := -28]
set_NAs_to_zero(prev_28_28_wide) 

## Split into individual MEs
prev_28_28_file_list <- split(x = prev_28_28_wide, by = "modelable_entity_name")

lapply(prev_28_28_file_list, save_me_file, age = "28_28")




# Bin-Specific Life Years between 7-27 days per l/y/s = Bin-Specific number of survivors in l/y/s at 7 days * 7 days  + Bin-Specific number of Deaths between 0-6 days in l/y/s * 7/365 
smr_calc[,  bin_ly_lnn := ( bin_surv_28_days * 7 / 365 ) + ( bin_deaths_lnn * 7/365 )   ]


# Bin-Specific Prevalence between 0-6 days per l/y/s = Bin-Specific Life Years between 0-6 days per l/y/s  /  "Total Life Years per l/y/s between 0-6 days"
smr_calc[, ME_prop_lnn := bin_ly_lnn / ly_lnn ]



#######################
## SAVE PREV AT 0-6 DAYS

prev_7_27_long <- copy(smr_calc)
prev_7_27_long[, ME_prop := ME_prop_lnn]

## Reshape wide
prev_7_27_wide <- dcast(prev_7_27_long, location_id + sex_id + age_group_id + year_id + modelable_entity_name + measure_id ~ paste0("draw_", draw), value.var = "ME_prop", variable.var = "draw_")
prev_7_27_wide[, age_group_id := 3] ## already age_group_3
set_NAs_to_zero(prev_7_27_wide) 


## Split into individual MEs
prev_7_27_file_list <- split(x = prev_7_27_wide, by = "modelable_entity_name")

lapply(prev_7_27_file_list, save_me_file, age = "7_27")





#######################
## SAVE FOR SAVE_RESULTS FORMAT

prev_0_27_wide <- rbindlist(list(prev_0_6_wide, prev_7_27_wide), use.names = T)
prev_0_27_file_list <- split(x = prev_0_27_wide, by = "modelable_entity_name")
lapply(prev_0_27_file_list, save_me_file, age = "save_results")

print(Sys.time())