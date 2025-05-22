## Title: Fit age and sex models
## Description: Fits the age and sex models, saves model coefficients and random effects

# set-up ------------------------------------------------------------------

rm(list=ls())

library(pacman)
p_load(argparse, data.table, dplyr, foreign, lme4, splines, ggplot2, assertable)

if(interactive()){
  version_id <- x
  version_5q0_id <- x
}else{
  parser <- ArgumentParser()
  parser$add_argument("--version_id", type = "character", required = TRUE,
                      help = "The version for this run of age-sex")
  parser$add_argument("--version_5q0_id", type = "integer", required = TRUE,
                      help = "The 5q0 version for this run of age-sex")
  args <- parser$parse_args()
  
  version_id <- args$version_id
  version_5q0_id <- args$version_5q0_id
}

child_data <- paste0("FILEPATH")
output_dir <- paste0("FILEPATH")

if(dir.exists(output_dir) == F) dir.create(output_dir)
if(dir.exists(paste0("FILEPATH")) == F) {
  dir.create(paste0("FILEPATH"))
}
if(dir.exists(paste0("FILEPATH")) == F) {
  dir.create(paste0("FILEPATH"))
}

input_data <- fread(paste0("FILEPATH"))

# get locations
locs <- fread(paste0("FILEPATH"))
locs <- locs[level_all == 1, .(ihme_loc_id, standard)]
fake_regions <- fread(paste0(output_dir, "/st_locs.csv"))
fake_regions <- fake_regions[keep == 1]
setnames(fake_regions, "region_name", "gbdregion")
fake_regions[, c("location_id","keep") := NULL]
locs <- merge(locs, fake_regions, by = "ihme_loc_id")

# fit sex model -----------------------------------------------------------

dt <- copy(input_data)
dt <- dt[sex != "" & exclude_sex_mod == 0,]

dt[, dup := .N, by = c("ihme_loc_id", "region_name", "year", "source", "sex")]
assert_values(dt, "dup", test = "lte", test_val = 1)
dt[, dup := NULL]

dt <- dcast(dt, ihme_loc_id + region_name + year + source ~ sex, value.var = "q_u5")
dt <- as.data.table(dt)

dt[, q5_sex_ratio := male / female]
setnames(dt, "both", "q5_both")
dt[, region_name := NULL]
dt <- dt[,.(ihme_loc_id, year, q5_both, q5_sex_ratio)]

# exclude data for ratio >1.5 or ratio <0.8
dt <- dt[q5_sex_ratio > 0.8 & q5_sex_ratio < 1.5 & !is.na(q5_sex_ratio)]

# scale ratio to between 0 and 1 and calculate logit
logit <- function(x) {log(x/(1-x))}
dt[,logit_sex_ratio := (q5_sex_ratio - 0.8)/(1.5 - 0.8)]
dt[,logit_sex_ratio := logit(logit_sex_ratio)]

# subset to standard locations
dt <- merge(dt, locs, by = c("ihme_loc_id"), all.x = T)
assertable::assert_values(dt, c("gbdregion", "standard"), test = "not_na", warn_only = T)
dt <- dt[standard == 1]

# fit mixed model on standard locations data with spline
sex_model <- lme4::lmer(
  logit_sex_ratio ~ 1 + ns(q5_both, df = 4) + (1|gbdregion),
  data = dt
)

# plot for diagnostics
plot_temp <- copy(dt)
plot_temp$predict <- predict(sex_model, newdata = plot_temp)
gg_fit<- ggplot(data = plot_temp) +
         geom_point(aes(x = q5_both, y = logit_sex_ratio)) +
         geom_line(aes(x = q5_both, y = predict), color = "red")+
         facet_wrap("gbdregion") +
         ggtitle("Sex model")
pdf(paste0("FILEPATH"), width = 11, height = 8.5)
plot(gg_fit)
print(plot(sex_model))
dev.off()

# save model object for prediction later
saveRDS(sex_model, paste0("FILEPATH"))

# fit age model -----------------------------------------------------------

dt <- copy(input_data)
dt <- dt[exclude == 0]
dt[, log_q_u5 := log(q_u5)]
dt[, region_name := NULL]

# subset to standard locations
dt <- merge(dt, locs, by = c("ihme_loc_id"), all.x = T)
assertable::assert_values(dt, c("gbdregion", "standard"), test = "not_na",
                          warn_only = T)
dt <- dt[standard==1]

dt[exclude_enn == 0 & prob_enn > 0, log_prob_enn := log(prob_enn)]
dt[exclude_lnn == 0 & prob_lnn > 0, log_prob_lnn := log(prob_lnn)]
dt[exclude_pnn == 0 & prob_pnn > 0, log_prob_pnn := log(prob_pnn)]
dt[exclude_pna == 0 & prob_pna > 0, log_prob_pna := log(prob_pna)]
dt[exclude_pna == 0 & prob_pnb > 0, log_prob_pnb := log(prob_pnb)]
dt[exclude_inf == 0 & prob_inf > 0, log_prob_inf := log(prob_inf)]
dt[exclude_ch == 0 & prob_ch > 0, log_prob_ch := log(prob_ch)]
dt[exclude_cha == 0 & prob_cha > 0, log_prob_cha := log(prob_cha)]
dt[exclude_chb == 0 & prob_chb > 0, log_prob_chb := log(prob_chb)]

dt[, yearmerge := round(year)]

# fetch covariates from 5q0 process
covs <- fread(paste0("FILEPATH"))
covs <- covs[, .(ihme_loc_id, year, hiv, maternal_edu, ldi)]
covs[, yearmerge := ceiling(year)]
covs[, year := NULL]
covs[, ldi := log(ldi)]
covs[, hiv := log(hiv + 0.000000001)]
covs <- unique(covs)

# merge on covariates and reshape
dt <- merge(dt, covs, by = c("ihme_loc_id", "yearmerge"), all.x = T, all.y = F,
            allow.cartesian = T)
dt <- data.table::dcast(
  dt,
  gbdregion + ihme_loc_id + year + source + age_type + hiv + maternal_edu +
    ldi + s_comp ~ sex,
  value.var = c("log_prob_enn","log_prob_lnn","log_prob_pnn", "log_prob_pna",
                "log_prob_pnb", "log_prob_inf","log_prob_ch", "log_prob_cha",
                "log_prob_chb", "log_q_u5")
)

# loop through sex and age to fit model
pdf(paste0("FILEPATH"), height = 8.5, width = 11)
for(sex in c("male", "female")){
  for(age in c("enn", "lnn", "pnn", "pna", "pnb", "inf", "ch", "cha", "chb")){
    
    print(paste0("Running model for ", age," ", sex))
    
    temp <- copy(dt)
    temp <- temp[(!is.na(get(paste0("log_prob_", age, "_", sex)))) &
                   (exp(get(paste0("log_q_u5_", sex))) > 0),]
    setnames(temp,paste0("log_q_u5_", sex), "log_q5_var")
    
    # define model
    if(age == "pnn" | age == "inf" ) betas <- "1 + hiv"
    if(age == "ch") betas <- "1 + hiv + maternal_edu + s_comp"
    if(age == "cha" | age == "chb") betas <- "1 + maternal_edu + s_comp"
    if(age == "enn" | age == "lnn" | age == "pna" | age == "pnb") betas <- "1"
    outcome <- paste0("log_prob_", age, "_", sex)
    formula <- paste0(outcome, " ~ ", betas, " + ns(log_q5_var, df=4) + (1|gbdregion)")
    
    # for pna, pnb, cha, and chb, use parent HIV fixed effect
    setnames(temp, outcome, "outcome")
    if(age %in% c("pna", "pnb")) temp[, outcome := outcome - hiv_fe_pnn * hiv]
    if(age %in% c("cha", "chb")) temp[, outcome := outcome - hiv_fe_ch * hiv]
    setnames(temp, "outcome", outcome)
    
    # fit model
    age_model <- lme4::lmer(formula = formula, data = temp)
    
    # save HIV fixed effects for ch & pnn
    if(age == "pnn") hiv_fe_pnn <- fixef(age_model)[["hiv"]]
    if(age == "ch") hiv_fe_ch <- fixef(age_model)[["hiv"]]
    
    # plot for diagnostics
    plot_temp <- copy(temp)
    plot_temp$predict <- predict(age_model, newdata = plot_temp)
    gg_fit<- ggplot(data = plot_temp) +
              geom_point(aes(x = log_q5_var, y = get(outcome))) +
              geom_line(aes(x = log_q5_var, y = predict, group = 1), color = "red") +
              facet_wrap("gbdregion") +
              ggtitle(paste0("Age model: ", age, " ", sex)) +
              ylab("log_prob")
    plot(gg_fit)
    print(plot(age_model))
    
    # save model object for predict later
    saveRDS(age_model, paste0("FILEPATH"))
    
  } # end loop on age
} # end loop on sex
dev.off()

# END