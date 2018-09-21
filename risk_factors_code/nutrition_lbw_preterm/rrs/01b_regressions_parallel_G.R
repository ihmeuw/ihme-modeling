
rm(list=ls())

args <- commandArgs(trailingOnly = TRUE)
sss <- args[1]
aaa <- args[2] ## ager5

os <- .Platform$OS.type

if (os=="windows") {
  j<- ## FILEPATH
  h <- ## FILEPATH
  my_libs <- NULL
  
  
} else {
  j<- "FILEPATH"
  h<- "FILEPATH"
  my_libs <- "FILEPATH"
}

library(reshape2)
library(data.table)

dt_reg <- readRDS("FILEPATH")

dt_reg <- dt_reg[, c("sex", "bw", "gestweek", "ager5")]
setnames(dt_reg, "gestweek", "orig_gestweek")

## Convert birthweight into numerics

bwlabels <-  c("[0, 500)", "[500, 1000)", "[1000, 1500)",
               "[1500, 2000)", "[2000, 2500)", "[2500, 3000)", "[3000, 3500)",
               "[3500, 4000)", "[4000, 4500)", "[4500, 5000)","[5000 )")

bwlevels <- c("0", "500", "1000", "1500", "2000", "2500", "3000", "3500", "4000", "4500", "5000", "10000")

dt_reg[, bw := cut(bw, breaks = bwlevels, labels = bwlabels, include.lowest = T)]
dt_reg[, bw := as.integer(bw)]
dt_reg[, bw := 500 * bw]

dt_reg[orig_gestweek <= 23, orig_gestweek := 23]
dt_reg[orig_gestweek == 24 | orig_gestweek == 25, orig_gestweek := 24]
dt_reg[orig_gestweek == 26 | orig_gestweek == 27, orig_gestweek := 26]
dt_reg[orig_gestweek == 28 | orig_gestweek == 29, orig_gestweek := 28]
dt_reg[orig_gestweek == 30 | orig_gestweek == 31, orig_gestweek := 30]
dt_reg[orig_gestweek == 32 | orig_gestweek == 33, orig_gestweek := 32]
dt_reg[orig_gestweek == 34 | orig_gestweek == 35, orig_gestweek := 34]

dt_reg[orig_gestweek == 36, orig_gestweek := 36]
dt_reg[orig_gestweek == 37, orig_gestweek := 37]

dt_reg[orig_gestweek == 38 | orig_gestweek == 39, orig_gestweek := 38]
dt_reg[orig_gestweek == 40 | orig_gestweek == 41, orig_gestweek := 40]
dt_reg[orig_gestweek >=42, orig_gestweek := 42]

setorder(dt_reg, sex, orig_gestweek)

## Set reference as first factor: gestweek_40
dt_reg[, orig_gestweek := as.character(orig_gestweek)]
dt_reg[, orig_gestweek := paste0("gestweek_", orig_gestweek)]
ga_reference <- "gestweek_40"
dt_reg[, orig_gestweek := factor(orig_gestweek, levels = c(ga_reference, "gestweek_23", "gestweek_24", "gestweek_26", "gestweek_28", "gestweek_30", "gestweek_32", "gestweek_34", "gestweek_36", "gestweek_37", "gestweek_38", "gestweek_42"))]

## Set reference as first factor: birthweight_4500
dt_reg[, bw := as.character(bw)]
dt_reg[, bw := paste0("birthweight_", bw)]
bw_reference <- "birthweight_4500"
dt_reg[bw == "birthweight_500", "bw"] <- "birthweight_0500"
dt_reg[, bw := factor(bw, levels = c(bw_reference, "birthweight_0500", "birthweight_1000", "birthweight_1500", "birthweight_2000", "birthweight_2500", "birthweight_3000", "birthweight_3500", "birthweight_4000", "birthweight_5000", "birthweight_5500"))]

## Create bivariate column
dt_reg[, ga_bw := paste0(orig_gestweek, "_", bw)]

## Set age of death to integers from factors. Change NAs to 0s. 
## Any value 1-5 represents a death, but at different times (<1hr, <1days, etc)
## 1 = Under 1 Hour
## 2 = 1-23 hours
## 3 = 1-6 days
## 4 = 7-27 days
## 5 = 28 days and over
## aaa will be 3, 4, 5
## For each row, remove past deaths

dt_reg[, ager5 := as.integer(ager5)]

## Clean so that all deaths between 0-6days are included in aaa 3
dt_reg[ager5 <= 3, ager5 := 3]

dt_reg[, "ager5"][is.na(dt_reg[, "ager5"])] <- 0

dt_reg <- dt_reg[ager5 >= aaa | ager5 == 0, ] 


## Set all other recodes to 0 (no deaths, as well as deaths past this neonatal period)
## Remaining is all aaa = 1, everything else is 0, previous deaths are removed
dt_reg[ager5 != aaa, ager5 := 0]
dt_reg[ager5 == aaa, ager5 := 1]

dt_reg <- dt_reg[, list(sex, orig_gestweek, bw, ga_bw, ager5)]

ga_sample_size <- dt_reg[, list(sample = .N), keyby = "orig_gestweek,sex"]
bw_sample_size <- dt_reg[, list(sample = .N), keyby = "bw,sex"]
ga_bw_sample_size <- dt_reg[, list(sample = .N), keyby = "ga_bw,sex"]


##################################################################################################

### Prior Logistic Regression

prior_model <- glm(formula = ager5~orig_gestweek + bw, family = binomial(link = "logit"), data = dt_reg[sex == sss, ])

prior_store <- data.table(names(coef(prior_model)), coef(prior_model), coef(summary(prior_model))[, "Std. Error"])

setnames(prior_store, c("V1", "V2", "V3"), c("ga_bw_dummies", "ln_oddsratio", "ln_odds_std_error"))

## Rename intercept to reference 4500
prior_store[ga_bw_dummies == "(Intercept)", ][["ga_bw_dummies"]] <- "bwbirthweight_4500"

reference <- "bwbirthweight_4500"

prior_store[, intercept := prior_store[ga_bw_dummies == "bwbirthweight_4500", ][["ln_oddsratio"]] ]

prior_store[ga_bw_dummies == reference, ln_oddsratio := 0]

extra_reference_row <- data.table(ga_bw_dummies = "orig_gestweekgestweek_40", ln_oddsratio = 0,
                                  ln_odds_std_error = prior_store[ga_bw_dummies == "bwbirthweight_4500", ][["ln_odds_std_error"]],
                                  intercept = prior_store[ga_bw_dummies == "bwbirthweight_4500", ][["intercept"]])

l = list(extra_reference_row, prior_store)

prior_store = rbindlist(l)

prior_store[, ln_mortality_odds := intercept + ln_oddsratio]

write.csv(prior_store, "FILEPATH", row.names = F)

ga_dummies <- copy(prior_store[grep(x = ga_bw_dummies, pattern = "gestweek"), ])
ga_dummies[, ga := substr(ga_bw_dummies, 23, 26)]
ga_dummies[, sex := sss]
ga_dummies <- ga_dummies[, -"ga_bw_dummies"]

write.csv(ga_dummies, file = "FILEPATH", row.names = F, na = "")

bw_dummies <- copy(prior_store[grep(x = ga_bw_dummies, pattern = "birthweight"), ])
bw_dummies[, bw := substr(ga_bw_dummies, 15, 20)]
ga_dummies[, sex := sss]
bw_dummies <- bw_dummies[, -"ga_bw_dummies"]

write.csv(bw_dummies, file = "FILEPATH", row.names = F, na = "")


# ##################################################################################################
# 
# ##### Univariate Logistic Regression - Gestational Week

ga_model <- glm(formula = ager5~orig_gestweek, family = binomial(link = "logit"), data = dt_reg[sex == sss, ])

## Store the variable names, coefficients, ln_std_errors
## Note that the reference is stored as "Intercept"
## Note that variable names are stored as orig_gestweekgestweek_22
ga_store <- data.table(names(coef(ga_model)), coef(ga_model), coef(summary(ga_model))[, "Std. Error"])

setnames(ga_store, c("V1", "V2", "V3"), c("orig_gestweek", "oddsratio", "ln_odds_std_error"))

## Rename intercept to reference 40

ga_store[orig_gestweek == "(Intercept)", ][["orig_gestweek"]] <- "orig_gestweekgestweek_40"

## Remove redundancy in variable name
ga_store[, orig_gestweek := sapply(strsplit(ga_store[["orig_gestweek"]], split='orig_gestweek', fixed=TRUE), function(x) (x[2]))]

ga_sample_size <- as.data.table(aggregate(sample ~ orig_gestweek, ga_sample_size, sum))

ga_store <- merge(ga_sample_size, ga_store, by = 'orig_gestweek')

## Set reference as "gestweek_40"
reference <- "gestweek_40"

## Change value in reference "oddsratio" column to 0 and add separate column for intercept
ga_store[, intercept := ga_store[orig_gestweek == reference, "oddsratio"]]
ga_store[orig_gestweek == reference, oddsratio := 0]

## Create columns for values of interest
ga_store[, ln_mortality_odds := intercept + oddsratio]

ga_store[, mortality_odds := exp(ln_mortality_odds)]

ga_store[, mortality_risk := mortality_odds / (1+mortality_odds)]

reference_risk <- ga_store[orig_gestweek == reference, mortality_risk]

ga_store[, relative_risk := mortality_risk / reference_risk]

write.csv(ga_store, file = "FILEPATH", row.names = F, na = "")

 
# ##################################################################################################
# ##### Univariate Logistic Regression - Birthweight
 
bw_model <- glm(formula = ager5~bw, family = binomial(link = "logit"), data = dt_reg[sex == sss, ])

## Store the variable names, coefficients, ln_std_errors
## Note that the reference is stored as "Intercept"
## Note that variable names are stored as bwbirthweight_500

bw_store <- data.table(names(coef(bw_model)), coef(bw_model), coef(summary(bw_model))[, "Std. Error"])

setnames(bw_store, c("V1", "V2", "V3"), c("bw", "oddsratio", "std_error"))

## Rename intercept to reference 4500
bw_store[bw == "(Intercept)", ][["bw"]] <- "bwbirthweight_4500"

## Remove redundancy in variable name
bw_store[, bw := sapply(strsplit(bw_store[["bw"]], split='bw', fixed=TRUE), function(x) (x[2]))]

## Add sample size
bw_sample_size <- as.data.table(aggregate(sample ~ bw, bw_sample_size, sum))
bw_store <- merge(bw_sample_size, bw_store, by = 'bw')

## Set birthweight reference
reference <- "birthweight_4500"

## Change value in reference "oddsratio" column to 0 and add separate column for intercept
bw_store[, intercept := bw_store[bw == reference, "oddsratio"]]
bw_store[bw == reference, oddsratio := 0]

## Create columns for values of interest
bw_store[, ln_mortality_odds := intercept + oddsratio]

bw_store[, mortality_odds := exp(ln_mortality_odds)]

bw_store[, mortality_risk := mortality_odds / (1+mortality_odds)]

reference_risk <- bw_store[bw == reference, mortality_risk]

bw_store[, relative_risk := mortality_risk / reference_risk]

write.csv(bw_store, file = "FILEPATH", row.names = F, na = "")



##################################################################################################


##### Bivariate Logistic Regression - Birthweight & Gestational Age

all_gestweek <- unique(dt_reg$orig_gestweek)
all_birthweight <- unique(dt_reg$bw)

dt_reg_full <- data.table(expand.grid(all_gestweek, all_birthweight, sex = c(1, 2)))

dt_reg_full[, ga_bw := paste0(Var1, "_", Var2)]
dt_reg_full <- dt_reg_full[, list(ga_bw, sex)]

setkey(dt_reg_full)

dt_reg_full <- merge(dt_reg_full, dt_reg, all = T)

dt_reg_full <- dt_reg_full[, list(ga_bw, sex, ager5)]

## Data Prep
ga_bw_levels <- list()
index <- 1

allgestweeks <- c(23, seq(24, 36, 2), 37, seq(38, 42, 2))


for(i in allgestweeks){
  for(j in 1:11){
    
    a <- paste0("gestweek_", i, "_birthweight_", j*500)
    
    if(j == 1){a <- paste0("gestweek_", i, "_birthweight_0500") }
    
    ga_bw_levels[index] <- a
    index = index + 1
  }
}


ga_bw_levels <- as.character(ga_bw_levels)
reference <- "gestweek_40_birthweight_4500"
ga_bw_levels_minus_reference <- ga_bw_levels[!ga_bw_levels %in% reference]

dt_reg_full[, ga_bw := factor(ga_bw, levels = c(reference, ga_bw_levels_minus_reference))]

## Bivariate model - by sex
ga_bw_model <- glm(formula = ager5~ga_bw, family = binomial(link = "logit"), data = dt_reg_full[sex == sss, ])

ga_bw_store <- data.table(names(coef(ga_bw_model)), coef(ga_bw_model), coef(summary(ga_bw_model))[, "Std. Error"])

setnames(ga_bw_store, c("V1", "V2", "V3"), c("ga_bw", "oddsratio", "std_error"))

## Rename intercept to reference 40
ga_bw_store[ga_bw == "(Intercept)", ][["ga_bw"]] <- "ga_bwgestweek_40_birthweight_4500"

ga_bw_store[, ga_bw := sapply(strsplit(ga_bw_store[["ga_bw"]], split='ga_bw', fixed=TRUE), function(x) (x[2]))]

## keep & merge only males

ga_bw_store <- merge(ga_bw_sample_size[sex == sss, ], ga_bw_store, by = 'ga_bw')

reference <- "gestweek_40_birthweight_4500"

ga_bw_store[, intercept := ga_bw_store[ga_bw == reference, "oddsratio"]]

ga_bw_store[ga_bw == reference, oddsratio := 0]

ga_bw_store[, ln_mortality_odds := intercept + oddsratio]

ga_bw_store[, mortality_odds := exp(ln_mortality_odds)]

ga_bw_store[, mortality_risk := mortality_odds / (1+mortality_odds)]

reference_risk <- ga_bw_store[ga_bw == reference, mortality_risk]

ga_bw_store[, relative_risk := mortality_risk / reference_risk]

## Add formatting to feed into 2D GPR
ga_bw_store[, ga := as.integer(substr(ga_bw, 10, 11))]
ga_bw_store[, bw := as.integer(substr(ga_bw, 25, 28))]

ga_bw_store[, oddsratio_intercept := oddsratio]
ga_bw_store[ga_bw == reference, "oddsratio_intercept"] <- ga_bw_store[ga_bw == reference, "intercept"]


write.csv(ga_bw_store, file = "FILEPATH", row.names = F, na = "")

