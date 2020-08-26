##
##

rm(list=ls())
j <- "FILEPATH"
h <- "FILEPATH"
l <- "FILEPATH"


date <- gsub("-", "_", Sys.Date())

library(data.table)
library(ggplot2)

set.seed(826)


###### Paths, args
central <- "FILEPATH"
main_dir <- "FILEPATH"
scratch <- "FILEPATH"


#try_covs <- c("age", "edu", "marital", "bmi", "bmi_sq", "diabetes", "hlthplan", "smoke", "state")

#plot_all_models <- F

###### Read in NHANES, format to BRFSS
load(paste0(scratch, "nhanes_microdata.rdata"))
nhanes <- data[!is.na(highbp) & !is.na(bpsys) & !is.na(bpdias) & age >= 20,]
rm(data)

# identify respondents who have been diagnosed and those who have high systolic or diastolic blood pressure
nhanes[, diagnosed := as.integer(highbp == 1)]
nhanes[, high_bp := as.integer(bpsys >= 140 | bpdias >= 90)]
nhanes[, bpmeds := factor(as.numeric(bpmeds), level=0:1, labels=c("Not Using BP Meds", "Using BP Meds"))]

# format predictors
nhanes[, age := cut(age, breaks=c(20, 30, 40, 50, 60, 70, 100), right=F)]
nhanes[, bmi_sq := bmi^2]
nhanes[, smoke := factor(smoke_any, levels=0:1, labels=c("No", "Yes"))]
nhanes[, diabetes := factor(diq, levels=0:1, labels=c("Diagnosed: No", "Diagnosed: Yes"))]
nhanes[, hlthplan := factor(hlthplan, levels=0:1, labels=c("No", "Yes"))]
nhanes[, year := as.numeric(substr(svyyear, 6, 9))]
nhanes <- nhanes[, list(svyyear, year, mec_wt, sex, age, race, marital, edu, bmi, bmi_sq, smoke, diabetes, hlthplan, bpmeds, diagnosed, high_bp)]
setnames(nhanes, "mec_wt", "wt")

# rescale weights
nhanes[, wt := round(wt * length(wt)/sum(wt), 5), by='svyyear']

###### Read in BRFSS, format
## Load in BRFSS data
load(paste0(main_dir, "brfss_microdata.rdata"))
brfss <- data[year >= 2001 & age >= 20 & !is.na(highbp),
              list(year, wt, sex, age, race, edu, marital, height, weight, smoke_any, diabetes, hlthplan, bpmeds, highbp, state)]
rm(data)

## Correct BMI using BMI correction model from obesity analysis
load(paste0(main_dir, "bmi_correction_model.rdata"))
brfss[, reported.bmi := weight/(height^2)]
brfss[, bmi := correct_bmi(.SD, sex[1], correct_bmi_fit, F), by='sex']
rm(correct_bmi, correct_bmi_fit); gc()

# write function to format BRFSS data (after BMI has been corrected)
format_brfss_hypertension <- function(brfss) {

  # copy data table (we don't want to modify the original as in some cases we'll need the original variables at other times)
  fbrfss <- copy(brfss)

  # identify respondents who have been diagnosed
  fbrfss[, diagnosed := highbp]
  fbrfss[, treated := factor(as.numeric(bpmeds), level=0:1, labels=c("Not Using BP Meds", "Using BP Meds"))]
  fbrfss[, bpmeds := factor(as.numeric(bpmeds), level=0:1, labels=c("Not Using BP Meds", "Using BP Meds"))]

  # format predictors
  fbrfss[, age := cut(age, breaks=c(20, 30, 40, 50, 60, 70, 100), right=F)]
  fbrfss[, race := factor(ifelse(race == "native", "other", as.character(race)), levels=c("white", "black", "hisp", "other"))]
  fbrfss[, bmi_sq := bmi^2]
  fbrfss[, smoke := factor(smoke_any, levels=0:1, labels=c("No", "Yes"))]
  fbrfss[, diabetes := factor(diabetes, levels=0:1, labels=c("Diagnosed: No", "Diagnosed: Yes"))]
  fbrfss[, hlthplan := factor(hlthplan, levels=0:1, labels=c("No", "Yes"))]

  fbrfss <- fbrfss[, list(year, wt, sex, age, race, marital, edu, bmi, bmi_sq, smoke, diabetes, hlthplan, bpmeds, diagnosed, state)]
  fbrfss
}
brfss <- format_brfss_hypertension(brfss)

## Translate between state codes and states
fips <- fread("FILEPATH/fips_birth_codes.csv")

brfss <- merge(brfss, fips[, .(fips_code, location_name)], by.x="state", by.y="fips_code", all.x=T, all.y=F)
brfss$state <- NULL
brfss[, state := location_name]


brfss[, mean_reported_htn := mean(diagnosed), by=c("age", "state")]
ggplot(brfss, aes(mean_reported_htn, fill=age)) + geom_density(alpha=0.5) + theme_bw()


###### Specify model and fit to NHANES data
## Function that takes in covariates and makes models
prepare_model <- function(covs) {

  high_bp_fit <- NULL
  model <- as.formula(paste0("high_bp ~ ", paste(covs, collapse = " + ")))

  # fit the model for undiagnosed respondents
  mod <- model
  high_bp_fit[["undiagnosed"]] <- lapply(1:2, function(ss) {
    wt <- regards[diagnosed == 0 & sex == ss, wt]
    glm(mod, family=binomial(link="logit"), data=regards[diagnosed == 0 & sex == ss,], weights=wt)
  })

  # fit the model for diagnosed, treated respondents
  mod <- model
  high_bp_fit[["diagnosed_treated"]] <- lapply(1:2, function(ss) {
    wt <- regards[diagnosed == 1 & sex == ss & treatment == 1, wt]
    glm(mod, family=binomial(link="logit"), data=regards[diagnosed == 1 & sex == ss & treatment == 1,], weights=wt)
  })

  # fit the model for diagnosed, untreated respondents
  mod <- model
  high_bp_fit[["diagnosed_untreated"]] <- lapply(1:2, function(ss) {
    wt <- regards[diagnosed == 1 & sex == ss & treatment == 0, wt]
    glm(mod, family=binomial(link="logit"), data=regards[diagnosed == 1 & sex == ss & treatment == 0,], weights=wt)
  })

  high_bp_fit
}

# write a function to predict true blood pressure status based on this model
pred_high_bp <- function(newdata, high_bp_fit, diagnosed, treatment, sex, n.sims) {
  if (diagnosed == 1 & treatment == 1) fit <- high_bp_fit[["diagnosed_treated"]][[sex]]
  if (diagnosed == 1 & treatment == 0) fit <- high_bp_fit[["diagnosed_untreated"]][[sex]]
  if (diagnosed == 0) fit <- high_bp_fit[["undiagnosed"]][[sex]]
  model <- formula(paste("~", fit$formula[3]))
  if (n.sims == 0) fe <- as.matrix(fit$coef)
  if (n.sims > 0) fe <- t(rmvnorm(n.sims, fit$coef, vcov(fit)))

  X <- model.matrix(model, model.frame(model, newdata, na.action="na.pass"))
  X <- X[, rownames(fe)[rownames(fe) %in% dimnames(X)[[2]]]]

  fe <- fe[rownames(fe)[rownames(fe) %in% dimnames(X)[[2]]],]

  pred <- inv.logit(X %*% fe)
  if (n.sims > 0) pred <- lapply(1:n.sims, function(p) as.numeric(pred[,p] >= runif(nrow(pred), 0, 1))) 
  pred
}


###### Apply model to BRFSS data
keep_brfss <- copy(brfss)

  fit <- data.table()

  for (r in 1:length(try_covs)) {

    print(r)

    choices <- combn(try_covs, r)

    n_choices <- ncol(choices)

    for (n in 1:n_choices) {

      print(n)

      brfss <- copy(keep_brfss)
      covariates <- choices[, n]
      predvars <- paste("pred", 1:10, sep="")

      high_bp_fit <- prepare_model(covariates)

      brfss[, treatment := ifelse(bpmeds == "Using BP Meds", 1, 0)]
      brfss <- brfss[!(diagnosed==1 & is.na(treatment))]
      brfss[, (predvars) := pred_high_bp(.SD, high_bp_fit, diagnosed[1], treatment[1], sex[1], 10), by='diagnosed,treatment,sex']
      brfss <- brfss[!is.na(pred1),]
      brfss[, label := ifelse(diagnosed == 0, "Not diagnosed", ifelse(diagnosed == 1 & treatment == 1, "Diagnosed, treated", "Diagnosed, untreated"))]

      # calculate hypertension prevalence by sex for BRFSS
      brfss_est <- brfss[, c(list(source = "BRFSS",
                                  reported = weighted.mean(diagnosed, wt)),
                             lapply(predvars, function(x) weighted.mean(.SD[[x]], wt, na.rm=T))),
                         by='year,sex,label']
      setnames(brfss_est, c("svyyear", "sex", "label", "source", "reported", predvars))
    }

  }
  
  write.csv(fit, "FILEPATH/fit.csv")
