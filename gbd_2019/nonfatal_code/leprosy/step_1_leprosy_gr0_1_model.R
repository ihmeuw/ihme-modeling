#' [Title: Leprosy Grade 0 Grade 1 Proportion Model
#' [Notes: Logistic regression using Sinan Grade 0 and Grade 1 data 

#' [ Set-Up ]
data_root <- 'FILEPATH'

# packages
library(data.table)
library(stringr)
library(ggplot2)
library(viridis)
library(dplyr)
library(Zelig, lib.loc = "FILEPATH")
source("FILEPATH")

params_dir <- "FILEPATH"

# data
data  <- fread("FILEPATH"))

#'[ Clean Data ]

data  <- data[location_name == "Brazil" & sex != 3]
data  <- data[age_end != 99 & age_start != 0.1]
data  <- data[, .(nid, year_start, year_end, age_start, age_end, location_name, numerator, denominator, healthstate, sex)]

# (denominator year_start age_start) uniquely separates grade 1 and grade 2 except for below data where four rows present and not possible to decipher which grade 1 matches with grade 2(exclude for now)
data  <- data[denominator != 48 & year_start != 2005 & age_start != 0.1]

data  <- dcast(data, ... ~ healthstate, value.var = "numerator")
setnames(data, "denominator", "all_leprosy")

# denominator is all leprosy
data[, disfigure_0 := all_leprosy - disfigure_1 - disfigure_2]
data[, rat_1_0 := disfigure_1 / disfigure_0]
data[, age_midpoint := (age_start + age_end) / 2]

#add proportion
data[, total_0_1 := disfigure_1+disfigure_0]
data[, prop_1 := disfigure_1 /(total_0_1) ]

data[, age_midpoint2:=age_midpoint^2]

#'[ Model - GEE]

data_m <- data[sex == 1]
data_f <- data[sex == 2]

# Get ages needed
age_md <- get_age_metadata(19, gbd_round_id = gbd_round_id)
age_md <- age_md[, .(age_group_years_start, age_group_years_end, age_group_id)]
setnames(age_md, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_md[, age_midpoint := (age_start + age_end)/2 ]
ages <- age_md[, age_midpoint]

# Male
#logistic regression
#outcome is the proportion of cases that are disfig 1 out of total disfig 1+ disfig 0, weights = total cases grade 0 and 1
z.mod_m <- zelig(prop_1 ~age_midpoint+I(age_midpoint^2), model ="logit.gee", weights=data_m$total_0_1, id = "year_start", data = data_m,corstr = "ar1")
x.mod_m <- setx(z.mod_m, age_midpoint = ages)
s.mod_m <- sim(z.mod_m, x = x.mod_m)
draws_m <- as.data.table(zelig_qi_to_df(s.mod_m))
draws_m[, sex_id := 1]

# Female
#logistic regression
#outcome is the proportion of cases that are disfig 1 out of total disfig 1+ disfig 0, weights = total cases grade 0 and 1
z.mod_f <- zelig(prop_1 ~age_midpoint+I(age_midpoint^2), model ="logit.gee", weights=data_f$total_0_1, id = "year_start", data = data_f,corstr = "ar1")
x.mod_f <- setx(z.mod_f, age_midpoint = ages)
s.mod_f <- sim(z.mod_f, x = x.mod_f)
draws_f <- as.data.table(zelig_qi_to_df(s.mod_f))
draws_f[, sex_id := 2]


# Bind and Calculate 2.5%, 50%, 97.5%
all <- rbind(draws_m, draws_f)
props <- all[, .("lower" = quantile(expected_value, 0.025) , "mean" = quantile(expected_value, 0.5) , "upper" = quantile(expected_value, 0.975)), by = c("age_midpoint", "sex_id")]
write.csv(props, "FILEPATH", row.names = FALSE))


# format all draws
draws <- all[, .(age_midpoint, expected_value, sex_id)]
draws[, draw_num := rep(paste0("draw_", 0:999), nrow(draws)/1000)]
draws <- dcast(draws, ... ~ draw_num, value.var = "expected_value")
draws <- merge(draws, age_md, by = "age_midpoint")

write.csv(draws, "FILEPATH", row.names = FALSE)
