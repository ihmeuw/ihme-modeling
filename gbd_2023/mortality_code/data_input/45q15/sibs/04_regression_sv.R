######################################################################################################################################################################
## 	Adult Mortality through Sibling Histories: #4. REGRESSION & GENERATING UNCERTAINTY TO CALCULATE 5 YEAR Q's & 45q15
##
## 	Description: This do-file runs a regression model on sibling-period data for each survey, by sex.
## 					It saves regression output and calculates age-specific q's, 45q15's (not fully adjusted yet), and confidence intervals.
## 		Steps:
## 				1. Put existing variable names into command lines that can be automatically put into code to run regression so that each variable doesn't have to be typed in manually
## 				   These are grouped into categories (all of the age dummies, the survey-period dummies)
## 				2. Run regression, save coefficient and variance-covariance in matrix
## 				3. Save regression output for graphing age coefficients
## 				4. Generate uncertainty: create a matrix with each unique 0/1 combination representing each survey-period-age category.
## 				5. Match the regression variance-covariance matrix and the possible CY age categories matrix to draw 1000 predictions for each category
## 				6. Convert coefficients to yearly q's, then convert the yearly q's to 5 year q's for each age
## 				7. Calculate 45q15 from the 5 year q's
## 				8. Use the 2.5-97.5 percentile to compute lower bound and upper bound for uncertainty
## 				9. Reshape results so that each observation yields 45q15, 5 year q's, and corresponding uncertainty bounds for each country-year (CY)
##
##  NOTE: IHME OWNS THE COPYRIGHT


rm(list = ls())
library(readr)
library(data.table)
library(haven)
library(plyr)
library(glue)
library(fastDummies , lib='FILEPATH')
library(survey)
library(MASS)

user <- "USERNAME"
if (Sys.info()[1] == "Windows") {
  root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  root <- "FILEPATH"
  h_root <- "FILEPATH"
}

gbd_year <- 2023
sib_dir <- "FILEPATH"
input_dir <- "FILEPATH"
source_list <- glue("FILEPATH/source_list.csv") |> fread()

input <- fread(paste0(input_dir, "/allsibs_sibhistlist.csv"))

acs <- setDT(read_csv(paste0(input_dir, "/allsibs_surveys.csv")))
acs <- acs[sex != 0]
acs[yr_interview <= 1950, yr_interview := yr_interview + 100]
acs <- unique(acs)

# Drop sibs with missing data
missing_drop <- acs[alive %in% c(0, 1) & !(is.na(yod) & alive == 0) & !is.na(yob)]

# Create GK weights ------------------------------------------------------
# GK weight = births/survivors in each family

missing_drop[sex == 2 & between(yr_interview - yob, 15, 49),
  si := sum(alive, na.rm = T),
  by = id_sm
]
missing_drop[, tot_si := sum(si, na.rm = T), by = id_sm]
missing_drop[is.na(tot_si) | tot_si == 0, si := 1]
missing_drop <- missing_drop[order(id_sm, si)]
missing_drop[, si := si[1], by = id_sm]

missing_drop[, gkwt := 1 / si]
missing_drop[is.na(gkwt) | missing_sib == 1, gkwt := 1]

# Sample weight:
missing_drop[, samplewt := v005]
missing_drop[missing_sib == 1, samplewt := 1]
missing_drop[, totalwt := samplewt * gkwt]

## Fix, running multiple surveys, make sure this is specific
missing_drop[, totalwt_total := sum(totalwt, na.rm = T), by = "svy"]
missing_drop[, totalwt := totalwt / totalwt_total]
missing_drop[, c("v005", "alive", "si", "totalwt_total") := NULL]

# Remove records where events occurred after survey year
final_years <- missing_drop[yob <= surveyyear]
final_years <- final_years[!(!is.na(yod) & (yod > surveyyear))]

# Create survey survival record ------------------------------------------
## Expand each sibling by 15 years ----
expanded <- final_years
for (i in 1:14) {
  expanded <- rbind(expanded, final_years)
}

expanded <- expanded[order(id_sm, sibid)]
expanded[, order := 1:.N, by = c("id_sm", "sibid")]
setnames(order, "surveyyear", "calcyear")
expanded[, year := calcyear - order + 1]
expanded[, svy_yr := paste0(iso3, "_", as.character(year))]

## Generate TPS variable, CY, Age blocks ----

tpsvar <- copy(expanded)
tpsvar[, age := year - yob]
tpsvar[age < 0, age := NA]

tpsvar <- tpsvar[between(age, 15, 59) & !is.na(age), ]
tpsvar[, ageblock := (age %/% 5) * 5]

tpsvar[ageblock > 60 | is.na(ageblock), ageblock := 60]

## Create outcome variable ----

# Outcome variable dead values: missing if not born yet, 0 once born until year before dead, 1 on the year they die, missing after they die

# For dead people:
tpsvar[!is.na(yod) & yod == year, dead := 1]
tpsvar[!is.na(yod) & yod > year & yob <= year, dead := 0]

# For alive people:
tpsvar[is.na(yod) & yob <= year, dead := 0]

step6 <- tpsvar[!is.na(dead), ]

## drop varaibles that are no longer needed ----
step6[, c("v008", "yod", "yob", "year", "calcyear", "yr_interview") := NULL]

step6 <- step6[order(sex, psu, samplewt, totalwt)]
step6[, dead := as.integer(dead)]
for (i in unique(step6$svy)) {
  step6_svy <- step6[svy == i]
  output_dir <- source_list[svy == i, output_dir]
  step6_svy |>
    fwrite(glue("{output_dir}/finalmodel_{unique(step6_svy$iso3)}_svy.csv"))
}

# Survey regression to adjust for age sparsity -------------------------------

# loop through every survey + sex combination
for (sx in unique(step6$sex)) {
  print(sx)

  for (survey in unique(step6$svy)) {
    print(survey)
    output_dir <- source_list[svy == survey, output_dir]

    sex_spec <- step6[sex == sx & svy == survey]
    if (nrow(sex_spec) == 0) {
      sex_name <- ifelse(sx == 1, "Male", "Female")
      next(paste0("No ", sex_name, " data for ", survey))
    }

    # run survey regression
    design <- svydesign(id = ~psu, weights = ~totalwt, data = sex_spec)
    mylogit <- svyglm(
      formula = dead ~ factor(ageblock) + factor(svy_yr),
      design = design, family = "binomial"
    )
    summary(mylogit)

    model_coefs <- mylogit$coefficients
    model_vcov <- vcov(mylogit)

    # Saves regression output for graphing age coefficients
    var <- diag(model_vcov)
    model_table <- data.table(varname = names(model_coefs), coefficients = model_coefs, variance = var)

    # save coefficients
    model_table |>
      fwrite(glue("{output_dir}/logit_regression_coefficients_{sx}_{survey}.csv"))

    # Generate Uncertainty: create X matrix with each unique 0/1 combination representing each of the CY age categories
    sex_spec[, `:=`(tpsz = 0, cons = 1)]

    # Contract the dataset so there is one unique observation for every unique combination of X variables.
    matrix_x <- sex_spec[, .N, by = c("svy_yr", "ageblock", "cons")]
    matrix_x <- matrix_x[order(svy_yr, ageblock, cons)]
    matrix_x[, N := NULL]
    matrix_x <- matrix_x[between(ageblock, 15, 55)]
    matrix_x <- matrix_x |>
      fastDummies::dummy_cols(select_columns = "ageblock", remove_first_dummy = T)
    blocks <- grep("ageblock_", names(matrix_x), value = T)
    matrix_x <- matrix_x[, .SD, .SDcols = c("cons", blocks, "svy_yr")]

    matrix_x <- matrix_x |>
      fastDummies::dummy_cols(select_columns = "svy_yr", remove_first_dummy = T)
    matrix_x[, svy_yr := NULL]
    matrix_x <- as.matrix(matrix_x) # unique combinations of independent dummy variables

    #  Match the regression variance-covariance matrix and the possible CY age categories
    #  matrix to draw 1000 predictions for each category

    # Set seed for reproducibility
    set.seed(5)
    pred_data <- MASS::mvrnorm(n = 1000, mu = model_coefs, Sigma = model_vcov) # random draws
    # set column order
    pred_data <- as.matrix(pred_data)
    pred_data <- matrix_x %*% t(pred_data)

    data <- sex_spec[, .N, by = c("svy_yr", "ageblock", "cons")]
    data <- data[order(svy_yr, ageblock, cons)]
    data[, N := NULL]

    data[, x := .GRP, by = "svy_yr"]

    # cbind pred_data matrix to data
    data <- cbind(data, pred_data)

    # Convert coefficients to yearly q's, then convert the yearly q's to 5 year q's for each age
    # e = 1 year probability of death
    # q = 5 year age probability of death
    # minusq = 5year prob of survival

    # convert to long table to avoid column-wise operations
    data <- data |>
      data.table::melt(
        id.vars = c("svy_yr", "ageblock", "cons", "x"),
        variable.name = "draw_no",
        value.name = "probability"
      )
    data[, draw_no := as.numeric(substring(draw_no, 2))]

    # assign e, q, minusq
    data[, e := ltcore::invlogit(probability)]
    data[, minusq := (1 - e)^5]
    data[, q := 1 - minusq]


    # Calculates 45q15 from the 5 year q's
    ## cumulative product of 5qx

    if (survey == "NPL_2016_2017") {
      data[, value := 1 - prod(minusq[1:7]), by = c("draw_no", "x")]
    } else {
      data[, value := 1 - prod(minusq[1:9]), by = c("draw_no", "x")]
    }
    data[, lgt := logit(value), by = c("draw_no", "x")]

    labels <- data[, c("svy_yr", "ageblock")]
    labels <- labels[, .N, by = names(labels)]
    labels <- labels[, N := NULL]

    # matrix for agegroup-specific q's
    agegrp <- data[, .(svy_yr, x, ageblock, draw_no, q, value, lgt)]

    cy_specific <- data[, lapply(.SD, mean),
      by = c("svy_yr", "draw_no"),
      .SDcols = c("value", "lgt")
    ]

    # Use the 2.5-97.5 percentile to compute lower bound and upper bound for uncertainty
    agegrp[, `:=`(
      pct_lower = quantile(q, .025),
      pct_upper = quantile(q, .975),
      grp_sd = sd(q)
    ),
    by = c("ageblock", "svy_yr")
    ]

    agegrp <- agegrp[, lapply(.SD, mean),
      by = c("ageblock", "svy_yr"),
      .SDcols = c("q", "pct_lower", "pct_upper", "grp_sd")
    ]

    # Reshape results so that each observation yields 45q15, 5 year q's, and corresponding
    # uncertainty bounds for each CY
    output <- copy(cy_specific)
    output <- output[, `:=`(
      lb_45q15 = quantile(value, .025, na.rm = T),
      ub_45q15 = quantile(value, .975, na.rm = T),
      sd_45q15 = sd(value),
      mean_45q15 = mean(value),
      lb_lgt45q15 = quantile(lgt, .025, na.rm = T),
      ub_lgt45q15 = quantile(lgt, .975, na.rm = T),
      sd_lgt45q15 = sd(lgt),
      mean_lgt45q15 = mean(lgt)
    ), by = c("svy_yr")]
    col_45q15 <- grep("45q15$", names(output), value = T)
    output <- output[, lapply(.SD, mean), by = "svy_yr", .SDcols = col_45q15]

    output <- merge(agegrp, output, by = "svy_yr", all = T)
    output[, `:=`(sex = sx, svy = survey)]
    output |>
      fwrite(glue("{output_dir}/finalmodel_allcountrypool_{sx}_{survey}.csv"))
  }
}

# append male and female results for each survey --------------------------

file_list <- c()
for (input_svy in unique(source_list$svy)) {
  output_dir <- source_list[svy == input_svy, output_dir]
  file_list <- c(
    file_list,
    Sys.glob(paste0(
      output_dir,
      "/finalmodel_allcountrypool*.csv"
    ))
  )
}
outputs <- lapply(file_list, fread)
both_sexes <- do.call("rbind", outputs)

both_sexes <- merge(both_sexes, source_list[, .(svy, deaths_source)], by = "svy")

for (i in unique(both_sexes$svy)) {
  both_sexes_svy <- both_sexes[svy == i]
  output_dir <- source_list[svy == i, output_dir]
  fwrite(both_sexes_svy, fs::path(output_dir, "/both_sexes_45q15.csv"))
}
fwrite(both_sexes, paste0(input_dir, "/both_sexes_45q15.csv"))

## Destring cy into iso code and year ----
fullmodel <- both_sexes
fullmodel[, yr := as.integer(substr(svy_yr, nchar(svy_yr) - 3, nchar(svy_yr)))] #  Beginning of the 5 year period (ex. 1990: 1990-1994)

fullmodel <- fullmodel[order(sex, -yr)]
for (i in unique(fullmodel$svy)) {
  fullmodel[svy == i, period := .GRP, by = c("svy_yr")]
}
fullmodel[, female := ifelse(sex == 1, 0, 1)]
fullmodel <- merge(fullmodel, source_list[, .(svy, NID = nid)], by = "svy")
setnames(fullmodel, "mean_45q15", "q45q15")
fullmodel_cols <- c("svy", "female", "period", "q45q15", "NID", "deaths_source")
fullmodel <- fullmodel[, .SD, .SDcols = fullmodel_cols] |> unique()
setorder(fullmodel, -female, period)
fwrite(fullmodel, paste0(input_dir, "/fullmodel_svy.csv"))

for (i in unique(acs$svy)) {
  fullmodel_svy <- fullmodel[svy == i]
  output_dir <- source_list[svy == i, output_dir]
  fwrite(fullmodel_svy, fs::path(output_dir, "fullmodel_svy.csv"))
}
