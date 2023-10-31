# Title: Compile data for age-sex model
# Description:
# (1) compiles population data (with exceptions for India SRS and China WHO data)
# (2) calculates risks for VR
# (3) remove obs if no qx values generated
# (4) marks data types (i.e. age format)
# (5) makes transformations (i.e. calculate conditional probabilities)

# set-up ------------------------------------------------------------------

rm(list=ls())

library(data.table)
library(readstata13)
library(foreign)
library(assertable)
library(argparse)
library(assertable)
library(plyr)
library(mortdb, lib.loc = "FILEPATH")

# For interactive testing
if(interactive()){
   
   version_id <- x
   version_ddm_id <- x
   gbd_year <- x 
   
}else{

   parser <- argparse::ArgumentParser()
   parser$add_argument("--version_id", type = "integer", required = TRUE,
                       help = "age sex version id")
   parser$add_argument("--ddm_version_id", type = "integer", required = TRUE,
                       help = "ddm version id")
   parser$add_argument("--gbd_year", type = "integer", required = TRUE,
                       help = "GBD year")

   args <- parser$parse_args()
   list2env(args, .GlobalEnv)

   version_ddm_id <- ddm_version_id
}

# directories
input_dir   = "FILEPATH"
output_dir  = "FILEPATH"
ddm_dir			= "FILEPATH"

# VR files
vr_file = paste0("FILEPATH")

# population files
gbd_pop_file = paste0("FILEPATH") # GBD estimates, has all age groups
data_pop_file = paste0("FILEPATH") # Includes data, has abridged life table age groups

# births file
births_file <- paste0("FILEPATH")

# age map
age_map <- c("enn" = 2,
             "lnn" = 3,
             "pnn" = 4,
             "pna" = 388,
             "pnb" = 389,
             "nn" = 42,
             "inf" = 28,
             "ch" = 5,
             "cha" = 238,
             # "cha" = 49,
             "chb" = 34,
             "u5" = 1)
age_map <- as.data.table(age_map, keep.rownames=T)
setnames(age_map, c("rn", "age_map"), c("age", "age_group_id"))

# location map
loc_map <- mortdb::get_locations(gbd_year = gbd_year, level = "all", gbd_type = "ap_old")[, c("ihme_loc_id", "location_id")]
loc_map <- loc_map[ihme_loc_id != "CHN"]

# Denominators ------------------------------------------------------------

gbd_pop <- fread(gbd_pop_file)
pop <- haven::read_dta(data_pop_file)
births <- fread(births_file)

# reshape gbd pop file
gbd_pop <- merge(gbd_pop, age_map, by = c("age_group_id"))
gbd_pop <- gbd_pop[age_group_id != 49]
gbd_pop <- merge(gbd_pop, loc_map, by = c("location_id"))
gbd_pop[sex_id == 1, sex := "male"]
gbd_pop[sex_id == 2, sex := "female"]
gbd_pop[sex_id == 3, sex := "both"]
setnames(gbd_pop, "year_id", "year")
gbd_pop[, age := paste0("pop_", age)]
gbd_pop <- dcast(gbd_pop, sex + ihme_loc_id + year ~ age, value.var = "population")
gbd_pop <- as.data.table(gbd_pop)
gbd_pop[, `:=` (source_type = "VR", pop_source = "gbd")]

# reshape ddm pop file (for DSP and SRS sources)
pop <- setDT(pop)
idvars <- c("ihme_loc_id", "year", "sex", "source_type", "pop_source")
pop <- pop[, .SD, .SDcols = c(idvars, "c1_0to0", "c1_1to4")]
setnames(pop, c("c1_0to0", "c1_1to4"), c("pop_inf", "pop_ch"))

# subset for non-VR source types (i.e. DSP and SRS)
pop <- pop[source_type == "SRS" | source_type == "DSP"]

# prep births
births <- births[, .SD, .SDcols = c("ihme_loc_id", "year", "sex", "births")]

# combine all together
denominators <- rbind(pop, gbd_pop, fill = T)
denominators <- merge(denominators, births, by = c("ihme_loc_id", "year", "sex"), all.x = T)

# Calculate rates (VR) -----------------------------------------------------

# read in vr
vr <- fread(paste0("FILEPATH"))

# ensure all vr source types are consistent
vr[grepl("VR", source_type), source_type := "VR"]
vr[grepl("SRS", source_type), source_type := "SRS"]
vr[grepl("DSP", source_type), source_type := "DSP"]

# merge pop and deaths - only keep sources that can merge with population
vr <- merge(vr, denominators, by = c("source_type", "ihme_loc_id", "sex", "year"))

# calculate qx directly for neonatal using births and deaths
vr[, q_enn := deaths_enn/births]
vr[, q_lnn := deaths_lnn/(births - deaths_enn)]
vr[, q_nn := 1 - (1 - q_enn) * (1 - q_lnn)]

# calculate mx with deaths/pop for older age groups
vr[, m_inf := deaths_inf/pop_inf]
vr[, m_ch := deaths_ch/pop_ch]
vr[, m_cha := deaths_cha/pop_cha]
vr[, m_chb := deaths_chb/pop_chb]

# set ax for mx -> qx conversion:

# 1a0 
vr[sex == "male" & m_inf >= 0.107, ax_1a0 := 0.330]
vr[sex == "female" & m_inf >= 0.107, ax_1a0 := 0.350]
vr[sex == "both" & m_inf >= 0.107, ax_1a0 := (0.330 + 0.350)/2]
vr[sex == "male" & m_inf < 0.107, ax_1a0 := 0.045 + 2.684 * m_inf]
vr[sex == "female" & m_inf < 0.107, ax_1a0 := 0.053 + 2.800 * m_inf]
vr[sex == "both" & m_inf < 0.107, ax_1a0 := (0.045 + 2.684 * m_inf + 0.053 + 2.800 * m_inf)/2]

# 4a1 
vr[sex == "male" & m_inf >= 0.107, ax_4a1 := 1.352]
vr[sex == "female" & m_inf >= 0.107, ax_4a1 := 1.361]
vr[sex == "both" & m_inf >= 0.107, ax_4a1 := (1.352 + 1.361)/2]
vr[sex == "male" & m_inf < 0.107, ax_4a1 := 1.651 - 2.816 * m_inf]
vr[sex == "female" & m_inf < 0.107, ax_4a1 := 1.522 - 1.518 * m_inf]
vr[sex == "both" & m_inf < 0.107, ax_4a1 := (1.651 - 2.816 * m_inf + 1.522 - 1.518 * m_inf)/2]

# 1a1 
vr[, ax_1a1 := 1 + (1/m_cha) - (1 / (1 - exp(-1*m_cha)))]

# 3a2 
vr[, ax_3a2 := 3 + (1/m_chb) - (3 / (1 - exp(-1*m_chb)))]

# mx --> qx
vr[, q_inf := 1 * m_inf/(1 + (1 - ax_1a0) * m_inf)]
vr[, q_ch := 4 * m_ch/(1 + (4 - ax_4a1) * m_ch)]
vr[, q_cha := 1 * m_cha/(1 + (1 - ax_1a1) * m_cha)]
vr[, q_chb := 3 * m_chb/(1 + (3 - ax_3a2) * m_chb)]
vr[, q_u5 := 1 - (1 - q_inf) * (1 - q_ch)]

# calculate qx for post-neonatal
vr[, q_pnn := 1 - (1 - q_inf)/(1 - q_nn)]
vr[, q_pna := deaths_pna / (births - deaths_nn)]
vr[, q_pnb := 1 - (1 - q_inf)/((1 - q_nn)*(1 - q_pna))]

vr[is.na(q_u5), q_u5 := 1 - (1 - q_inf)*(1 - q_cha)*(1 - q_chb)]

vr[is.na(q_u5), q_u5 := 1 - (1 - q_nn)*(1-q_pnn)*(1 - q_ch)]
vr[is.na(q_u5), q_u5 := 1 - (1 - q_nn)*(1-q_pna)*(1-q_pnb)*(1 - q_ch)]
vr[is.na(q_u5), q_u5 := 1 - (1 - q_nn)*(1-q_pnn)*(1 - q_cha)*(1 - q_chb)]
vr[is.na(q_u5), q_u5 := 1 - (1 - q_nn)*(1-q_pna)*(1-q_pnb)*(1 - q_cha)*(1 - q_chb)]

vr[is.na(q_u5), q_u5 := 1 - (1 - q_enn)*(1 - q_lnn)*(1-q_pnn)*(1 - q_ch)]
vr[is.na(q_u5), q_u5 := 1 - (1 - q_enn)*(1 - q_lnn)*(1-q_pna)*(1-q_pnb)*(1 - q_ch)]
vr[is.na(q_u5), q_u5 := 1 - (1 - q_enn)*(1 - q_lnn)*(1-q_pnn)*(1 - q_cha)*(1 - q_chb)]
vr[is.na(q_u5), q_u5 := 1 - (1 - q_enn)*(1 - q_lnn)*(1-q_pna)*(1-q_pnb)*(1 - q_cha)*(1 - q_chb)]

# create conditional probabilities
vr[, prob_pnn := (1-q_enn)*(1-q_lnn)*q_pnn/q_u5]
vr[, prob_pna := (1-q_enn)*(1-q_lnn)*q_pna/q_u5]
vr[, prob_pnb := (1-q_enn)*(1-q_lnn)*(1-q_pna)*q_pnb/q_u5]
vr[, prob_cha := (1-q_enn)*(1-q_lnn)*(1-q_pnn)*q_cha/q_u5]
vr[, prob_chb := (1-q_enn)*(1-q_lnn)*(1-q_pnn)*(1-q_cha)*q_chb/q_u5]
vr[, prob_ch := (1-q_enn)*(1-q_lnn)*(1-q_pnn)*q_ch/q_u5]

# rescale pna and pnb
vr[, scale := (prob_pna + prob_pnb)/prob_pnn]
vr[, prob_pna := prob_pna/scale]
vr[, prob_pnb := prob_pnb/scale]

# rescale cha and chb
vr[, scale := (prob_cha + prob_chb)/prob_ch]
vr[, prob_cha := prob_cha/scale]
vr[, prob_chb := prob_chb/scale]

# replace qx with rescaled values
vr[!is.na(q_enn) & !is.na(q_lnn) & !is.na(q_pnn) & !is.na(q_pna),
   q_pna := q_u5*prob_pna / ((1-q_enn)*(1-q_lnn))]
vr[!is.na(q_enn) & !is.na(q_lnn) & !is.na(q_pnn) & !is.na(q_pna) & !is.na(q_pnb),
   q_pnb := q_u5*prob_pnb / ((1-q_enn)*(1-q_lnn)*(1-q_pna))]
vr[!is.na(q_enn) & !is.na(q_lnn) & !is.na(q_pnn) & !is.na(q_cha),
   q_cha := q_u5*prob_cha / ((1-q_enn)*(1-q_lnn)*(1-q_pnn))]
vr[!is.na(q_enn) & !is.na(q_lnn) & !is.na(q_pnn) & !is.na(q_cha) & !is.na(q_chb),
   q_chb := q_u5*prob_chb / ((1-q_enn)*(1-q_lnn)*(1-q_pnn)*(1-q_cha))]

# remove conditional probabilities
vr[, c("prob_pnn", "prob_pna", "prob_pnb", "prob_ch", "prob_cha", "prob_chb", "scale") := NULL]

vr[, data_subset := "vr/dsp/srs"] # adding data_subset to identify data set in outliering

# Data with no denominators (SRS) ------------------------------------------

add_agg_ests <- read.dta13("FILEPATH")
add_agg_ests <- as.data.table(add_agg_ests)
add_agg_ests[, year := floor(year)]

vr <- vr[!(grepl("IND", ihme_loc_id) & grepl("srs", source, ignore.case = T) &
              (year >= 2008 & year <= 2019))]
add_agg_ests <- add_agg_ests[year < 2008]

replacement_agg_ests <- fread("FILEPATH")

setnames(replacement_agg_ests, c("iso3", "t", "q01", "q14", "q5", "nid"),
         c("ihme_loc_id", "year", "q_inf", "q_ch", "q_u5", "NID"))
replacement_agg_ests[sex_id == 1, sex := "male"]
replacement_agg_ests[sex_id == 2, sex := "female"]
replacement_agg_ests[sex_id == 3, sex := "both"]

replacement_agg_ests <- replacement_agg_ests[, c("ihme_loc_id", "year", "sex", "source", "NID",
                                                 "q_inf", "q_ch", "q_u5")]

add_agg_ests <- rbind(add_agg_ests, replacement_agg_ests)

add_agg_ests[, source_type := "SRS"]
add_agg_ests[, data_subset := "srs no denom"]

# Combine all sources -----------------------------------------------------
cbh <- fread(paste0("FILEPATH"))
cbh[, data_subset := "cbh"]

dt <- rbind(cbh, vr, fill = T)
dt <- rbind(dt, add_agg_ests, fill = T)

assertable::assert_values(dt, colnames = c("q_u5"), test = "not_na", warn_only = T)

# Conditional probability -------------------------------------------------

# probability of dying in the early neonatal period; conditional on dying in the first five years
dt[, prob_enn := q_enn/q_u5]

# probability of dying in the late neonatal period; conditional on dying in the first five years
dt[, prob_lnn := (1-q_enn)*q_lnn/q_u5]

# probability of dying in the post-neonatal period; conditional on dying in the first five years
dt[, prob_pnn := (1-q_nn)*q_pnn/q_u5]

# probability of dying in the post-neonatal (28-182 days) period; conditional on dying in the first five years
dt[, prob_pna := (1-q_nn)*q_pna/q_u5]

# probability of dying in the post-neonatal (183-364 days) period; conditional on dying in the first five years
dt[, prob_pnb := (1-q_nn)*(1-q_pna)*q_pnb/q_u5]

# probability of dying in the infant period; conditional on dying in the first five years
dt[, prob_inf := q_inf/q_u5]

# probability of dying in the child period; conditional on dying in the first five years
dt[, prob_ch := (1-q_inf)*q_ch/q_u5]

# probability of dying in the 1 year period; conditional on dying in the first five years
dt[, prob_cha := (1-q_inf)*q_cha/q_u5]

# probability of dying in the 2-4 year period; conditional on dying in the first five years
dt[, prob_chb := (1-q_inf)*(1-q_cha)*q_chb/q_u5]

# Remove obs if missing all qx -----------------------------------------

dt <- dt[!(is.na(q_enn) & is.na(q_lnn) & is.na(q_nn) & is.na(q_pnn) & is.na(q_pna) & is.na(q_pnb)
           & is.na(q_inf) & is.na(q_ch) & is.na(q_cha) & is.na(q_chb) & is.na(q_u5))]


# Mark data types ------------------------------------------------------

# i.e. identify which age groups have non-missing qx values by loc/year/sex/source
dt[, age_type := ""]

for(age in age_map$age){
   dt[!is.na( get(paste0("q_", age)) ), age_type :=
         ifelse(age_type == "", paste0(age_type, age), paste0(age_type, "/", age)),
      by = c("ihme_loc_id", "year", "sex", "source", "source_y", "NID", "underlying_nid")]
}

# check that there are no missing age_types
assert_values(dt, "age_type", test_val = "", test = 'not_equal')

# output file for outliering and exclusions
write.csv(dt, paste0("FILEPATH"), row.names = F)
