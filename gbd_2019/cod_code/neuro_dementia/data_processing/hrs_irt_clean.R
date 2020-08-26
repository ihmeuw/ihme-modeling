##########################################################################
### Author: USERNAME
### Date: 16/6/18
### Project: GBD Nonfatal Estimation
### Purpose: Item Response Theory Dementia HRS Comparison
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2, ggrepel, RColorBrewer, MASS, lmtest, nnet)
library("mirt", lib.loc = paste0("FILEPATH"))
library("gplots", lib.loc = paste0("FILEPATH"))
library("ROCR", lib.loc = paste0("FILEPATH"))
library("carData", lib.loc = paste0("FILEPATH"))
library("car", lib.loc = paste0("FILEPATH"))
date <- gsub("-", "_", Sys.Date())

# SET UP OBJECTS ----------------------------------------------------------

repo_dir <- paste0("FILEPATH")
hrs_dir <- paste0("FILEPATH")
adams_dir <- paste0("FILEPATH")
graph_dir <- paste0("FILEPATH")
model_dir <- paste0("FILEPATH")
draws <- paste0("draw_", 0:999)
mean_draws <- paste0("mean_", 0:999)

# SET FUNCTIONS -----------------------------------------------------------

## THIS IS FROM THE ARM PACKAGE BUT LIFTED SO THAT IT WORKS WITH SVYGLM OBJECTS
sims <- function(object, n.sims){
  object.class <- class(object)[[1]]
  summ <- summary (object, correlation=TRUE, dispersion = object$dispersion)
  coef <- summ$coef[,1:2,drop=FALSE]
  dimnames(coef)[[2]] <- c("coef.est","coef.sd")
  beta.hat <- coef[,1,drop=FALSE]
  sd.beta <- coef[,2,drop=FALSE]
  corr.beta <- summ$corr
  n <- summ$df[1] + summ$df[2]
  k <- summ$df[1]
  V.beta <- corr.beta * array(sd.beta,c(k,k)) * t(array(sd.beta,c(k,k)))
  beta <- array (NA, c(n.sims,k))
  dimnames(beta) <- list (NULL, dimnames(beta.hat)[[1]])
  for (s in 1:n.sims){
    beta[s,] <- MASS::mvrnorm (1, beta.hat, V.beta)
  }
  # Added by Masanao
  beta2 <- array (0, c(n.sims,length(coefficients(object))))
  dimnames(beta2) <- list (NULL, names(coefficients(object)))
  beta2[,dimnames(beta2)[[2]]%in%dimnames(beta)[[2]]] <- beta
  # Added by Masanao
  sigma <- rep (sqrt(summ$dispersion), n.sims)
  
  ans <- list(coef = beta2, sigma = sigma)
  return(ans)
}

# SOURCE DATA PREP CODE ---------------------------------------------------

source(paste0("FILEPATH"))
source(paste0("FILEPATH"))

# COMBINE AND SET UP ANALYSIS ---------------------------------------------

## REMOVE INDIVIDUALS IN ADAMS FROM HRS
test_dt_hrs[, unique_id := paste0(hh_id, "_", pn)]
test_dt_adams[, unique_id := paste0(gsub("^0", "", hh_id), "_", gsub("^0", "", pn))]
test_dt_hrs <- test_dt_hrs[!unique_id %in% intersect(test_dt_hrs[, unique_id], test_dt_adams[, unique_id])]

## SET GROUP AND COMBINE
test_dt_adams[, group := "adams"]
test_dt_hrs[, group := "hrs"]
test_dt <- rbind(test_dt_adams, test_dt_hrs, fill = T, use.names = T)

## SET UP MODEL
id_vars <- c("hh_id", "pn", "unique_id", "pweight", "group")
include_vars <- names(test_dt)[!names(test_dt) %in% id_vars]
test_dt <- test_dt[!rowSums(is.na(dplyr::select(test_dt, include_vars))) == ncol(dplyr::select(test_dt, include_vars))] 
test_dt <- test_dt[!pweight == 0]
model_dt <- dplyr::select(test_dt, include_vars)
length <- ncol(model_dt)
input_string <- paste0("F1 = 1-", length)
model <- mirt.model(input = input_string, itemnames = names(model_dt))

## GET MODEL TYPES AND ANCHOR ITEMS
types_dt <- data.table(question = names(model_dt), 
                       class = unlist(lapply(names(model_dt), function(x) return(model_dt[, class(get(x))]))))
model_types <- rep(NA, nrow(types_dt))
model_types[types_dt[class %in% c("numeric", "integer"), which = T]] <- "graded"
model_types[types_dt[class == "logical", which = T]] <- "2PL"
anchor_items <- intersect(names(test_dt_hrs), names(test_dt_adams))
anchor_items <- anchor_items[!anchor_items %in% id_vars]

pweight_hist <- ggplot(test_dt, aes(x = pweight)) +
  geom_histogram() +
  facet_wrap(~group, scales = "free") +
  theme_classic()

factor2_qs <- grep("adl|iqcode", names(model_dt))
factor2_qs <- paste0(factor2_qs, ", ", collapse = '')
factor2_qs <- gsub(", $", "", factor2_qs)
factor1_qs <- names(model_dt)[!grepl("adl|iqcode", names(model_dt))]
factor1_qs <- match(factor1_qs, names(model_dt))
factor1_qs <- paste0(factor1_qs, ", ", collapse = '')
factor1_qs <- gsub(", $", "", factor1_qs)

modeldesign2factor <- paste0("F1 = ", factor1_qs, "
                             F2 = ", factor2_qs, "
                             COV = F1*F2") 

## TWO FACTOR MODEL
model_equate2f <- readr::read_rds(paste0("FILEPATH"))


## GRAPHS
loadings_m <- summary(model_equate2f, verbose = F)[[1]]$rotF 
loadings <- data.table(item = row.names(loadings_m), 
                       cog_f = loadings_m[, 'F1'],
                       func_f = loadings_m[, 'F2'])
thresholds <- coef(model_equate2f)$hrs 
thresholds <- thresholds[!grepl("GroupPars", names(thresholds))]

get_thresholds <- function(n){
  par_matrix <- thresholds[[n]]
  par_matrix <- par_matrix['par',]
  i <- names(thresholds[n])
  threshold_vec <- par_matrix[names(par_matrix)[grepl("^d", names(par_matrix))]]
  disc <- par_matrix[names(par_matrix)[grepl("^a", names(par_matrix))]]
  disc <- disc[!disc == 0]
  threshold_vec <- -threshold_vec / disc
  tdt <- data.table(item = i, threshold = threshold_vec, label_name = c(i, rep('', length(threshold_vec) - 1)))
  return(tdt)
}

thresh_dt <- rbindlist(lapply(1:length(thresholds), get_thresholds))
graph_dt <- merge(thresh_dt, loadings, by = "item")
graph_dt[cog_f == 0, cog_f := NA][func_f == 0, func_f := NA]
graph_dt <- melt(graph_dt, id.vars = c("item", "threshold", "label_name"), measure.vars = c("cog_f", "func_f"), 
                 variable.name = "factor", value.name = "loading")

getPalette <- colorRampPalette(brewer.pal(8, "Dark2"))
gg_thresh <- ggplot(graph_dt, aes(x = threshold, y = loading, color = item, label = label_name)) +
  geom_point() +
  geom_path(aes(group = item)) + 
  geom_text_repel(size = 2, segment.alpha = 0.3) +
  facet_wrap(~factor) +
  theme_classic() +
  scale_color_manual(values = getPalette(graph_dt[, length(unique(item))])) +
  labs(y = "Loading", x = "Ability") +
  theme(legend.position = "none")


get_discrimination <- function(n){
  par_matrix <- thresholds[[n]]
  par_matrix <- par_matrix['par',]
  i <- names(thresholds[n])
  disc_vec <- par_matrix[names(par_matrix)[grepl("^a", names(par_matrix))]]
  tdt <- data.table(item = i, discrimination = disc_vec, label_name = i)
  tdt <- tdt[!discrimination == 0]
  return(tdt)
}

disc_dt <- rbindlist(lapply(1:length(thresholds), get_discrimination))
graphd_dt <- merge(disc_dt, loadings, by = "item")
graphd_dt[cog_f == 0, cog_f := NA][func_f == 0, func_f := NA]
graphd_dt <- melt(graphd_dt, id.vars = c("item", "discrimination", "label_name"), measure.vars = c("cog_f", "func_f"), 
                  variable.name = "factor", value.name = "loading")

gg_disc <- ggplot(graphd_dt, aes(x = discrimination, y = loading, color = item, label = label_name)) +
  geom_point() +
  geom_text_repel(size = 2, segment.alpha = 0.3) +
  facet_wrap(~factor) +
  scale_color_manual(values = getPalette(graph_dt[, length(unique(item))])) +
  theme_classic() +
  labs(y = "Loading", x = "Discrimination") +
  theme(legend.position = "none")

## MISSINGNESS
get_missing <- function(i){
  percent_missing_adams <- test_dt[group == "adams", sum(is.na(get(i)))/nrow(test_dt[group == "adams"])]
  percent_missing_hrs <- test_dt[group == "hrs", sum(is.na(get(i)))/nrow(test_dt[group == "hrs"])]
  missing_row <- data.table(item = i, missing_adams = percent_missing_adams, missing_hrs = percent_missing_hrs)
  return(missing_row)
}
missing_dt <- rbindlist(lapply(graph_dt[, unique(item)], get_missing))

## GET CONTINGENCY TABLES
get_values <- function(i){
  value_tab <- test_dt[, table(get(i))]
  if ("TRUE" %in% names(value_tab)){
    names(value_tab)[names(value_tab)=="FALSE"] <- "0"
    names(value_tab)[names(value_tab)=="TRUE"] <- "1"
  }
  tab_dt <- data.table(item = i, count = paste0(value_tab, "_", 1:length(value_tab)))
  tab_row <- dcast(tab_dt, item ~ count, value.var = "count")
  setnames(tab_row, as.character(paste0(value_tab, "_", 1:length(value_tab))), names(value_tab))
  change_names <- names(tab_row)[!names(tab_row) == "item"]
  tab_row[, (change_names) := lapply(.SD, function(x) as.numeric(gsub("_.*$", "", x))), .SDcols = change_names]
  return(tab_row) 
}

tabulate_dt <- rbindlist(lapply(types_dt[, unique(question)], get_values), use.names = T, fill = T)
setcolorder(tabulate_dt, c("item", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", '13'))
write.csv(tabulate_dt, paste0("FILEPATH"), row.names = F, na = "")


## GET FACTOR SCORES
scores2f <- as.data.table(fscores(model_equate2f))
score_dt <- data.table(group = test_dt[, group], score1 = scores2f$F1, score2 = scores2f$F2, 
                       unique_id = test_dt[, unique_id])


## GRAPH SCORES BY AGE
hrs[, unique_id := paste0(hh_id, "_", pn)]
adamsa[, unique_id := paste0(gsub("^0", "", hhid), "_", gsub("^0", "", pn))]
hrs_age <- hrs[!unique_id %in% intersect(hrs[, unique_id], adamsa[, unique_id])]
age_dt <- rbind(hrs_age[, .(unique_id, age_year)], adamsa[, .(unique_id, age_year = age)])

score_dt <- merge(score_dt, age_dt, by = "unique_id")
score_dt[, age_cat := cut(age_year, c(seq(65, 95, by = 5), 120), right = F)]

graph_dt <- copy(score_dt)
graph_dt <- melt(graph_dt, id.vars = "group", measure.vars = c("score1", "score2"), value.name = "score", 
                 variable.name = "factor")
graph_dt[, factor := ifelse(factor == "score1", "Factor 1: Cognition", "Factor 2: Functional Limitations")]
graph_dt[, group := ifelse(group == "hrs", "HRS", "ADAMS")]


gg <- ggplot(graph_dt, aes(score)) +
  geom_histogram(data = graph_dt[group == "hrs"], fill = "#C9E4B1", alpha = 0.4) +
  geom_histogram(data = graph_dt[group == "adams"], fill = "#1FB04D", alpha = 0.4) +
  scale_color_manual(name = "Sample", values = c("hrs" = "#C9E4B1", "ADAMS" = "#1FB04D")) +
  labs(x = "Count", y = "Factor Score") +
  facet_wrap(~factor) +
  theme_classic()

gg_density <- ggplot(graph_dt, aes(score)) +
  geom_density(aes(fill = group), alpha = 0.4) +
  scale_fill_manual(name = "Sample", values = c("hrs" = "#C9E4B1", "adams" = "#1FB04D")) +
  labs(x = "Factor Score", y = "Density") +
  facet_wrap(~factor) +
  theme_classic()

gg_violin <- ggplot(graph_dt, aes(as.factor(group), score)) +
  geom_violin(aes(fill = group)) +
  geom_boxplot(width = .2, aes(fill = group), show.legend = F) +
  scale_fill_manual(name = "Sample", values = c("HRS" = "#C9E4B1", "ADAMS" = "#1FB04D")) +
  labs(x = "Factor Score", y = "Density") +
  facet_wrap(~factor) +
  theme_classic() +
  theme(text = element_text(size = 20))


pdf(paste0("FILEPATH"), width = 10)
gg_violin
dev.off()

# CALCULATE PREDICTED PREVALENCE FROM HRS ---------------------------------

## SET UP FOR HRS PREDICTIONS
hrs_scores <- merge(score_dt[, .(unique_id, score1, score2, age_year, age_cat)], hrs[, .(unique_id, pweight, strata)], by = "unique_id")
hrs_scores <- merge(hrs_scores, hrs[, .(unique_id, sex_id)], by = "unique_id")
hrs_scores[sex_id == 1, x := "male"][sex_id == 2, x := "female"][, sex_id := NULL]
hrs_scores[, x := factor(x, levels = c("male", "female"))]
setnames(hrs_scores, "x", "sex_id")

## SET UP REGRESSION DATA
codebook <- as.data.table(read.xlsx(paste0("FILEPATH")))
prev_values <- c(1,3,10:11,13)
new_values <- c(1:4, 8, 10:11, 13, 15:19)
dementia <- codebook[final_varname == "consensus_diagnosis_final" & value369295 %in% new_values, category369295]
regress_dt <- dplyr::select(adamsa, c("hhid", "pn", "strata", "sweight_cross", "cluster", "unique_id","sex_id", "education_yrs", "mmse_totalscore", "consensus_diagnosis_final", "consensus_2diagnosis_final", "consensus_3diagnosis_final"))
regress_dt[consensus_diagnosis_final %in% dementia | consensus_2diagnosis_final %in% dementia | consensus_3diagnosis_final %in% dementia, dementia := 1]
regress_dt[is.na(dementia), dementia := 0]
regress_dt <- merge(regress_dt, score_dt[group == "adams"], by = "unique_id")
regress_dt[, sex_id := factor(sex_id, levels = c("male", "female"))]

## DO REGRESSION
adams_design <- survey::svydesign(ids = ~cluster, strata = ~strata, weights = ~sweight_cross, data = regress_dt, nest = T)
regression <- survey::svyglm(dementia ~ score1 + score2 + age_year + sex_id, design = adams_design, data = regress_dt, family = binomial(link = "logit"))
regress_agesex <- survey::svyglm(dementia ~ age_year + sex_id, design = adams_design, data = regress_dt, family = binomial(link = "logit"))
regress_factors <- survey::svyglm(dementia ~ score1 + score2, design = adams_design, data = regress_dt, family = binomial(link = "logit"))
regress_sex <- survey::svyglm(dementia ~ sex_id, design = adams_design, data = regress_dt, family = binomial(link = "logit"))
regress_age <- survey::svyglm(dementia ~ age_year, design = adams_design, data = regress_dt, family = binomial(link = "logit"))

anova_result <- anova(regression, regress_agesex)
anova2_result <- anova(regression, regress_factors)
anova3_result <- anova(regress_agesex, regress_age)

## CALCULATE "TRUE" PREVALENCE FROM ADAMS
true_prev <- as.data.table(survey::svyby(~dementia, by = ~sex_id+age_cat, design = adams_design, survey::svymean, vartype = "ci"))
true_prev[ci_l < 0, ci_l := 0] 

## SET UP HRS PREDICTION FRAME AND PREDICT 
hrs_pred <- copy(hrs_scores[, .(unique_id, score1, score2, age_year, sex_id, age_cat)])

X_pred <- as.matrix(cbind(rep(1, nrow(hrs_pred)), hrs_pred[, .(score1, score2, age_year)], as.numeric(hrs_pred$sex_id == "female")))
prediction_draws <- function(X_pred, model){
  n_pred <- dim(X_pred)[1]
  sim <- sims(model, 1)
  p_pred <- boot::inv.logit(X_pred %*% t(sim$coef))
  y_pred <- rbinom(n_pred, 1, p_pred)
}
pred_draws <- as.data.table(replicate(1000, prediction_draws(X_pred, regression)))
setnames(pred_draws, names(pred_draws), draws)
pred_draws <- cbind(hrs_pred, pred_draws)

## GET MEANS BY SEX AND AGE (USE HRS SURVEY DESIGN) 
hrs_designdt <- hrs[, .(unique_id = paste0(hh_id, "_", pn), strata, sampling_error_comp_unit, pweight)]
pred_draws <- merge(pred_draws, hrs_designdt, by = "unique_id")
hrs_design <- survey::svydesign(ids = ~sampling_error_comp_unit, strata = ~strata, weights = ~pweight, data = pred_draws, nest = T) 


predicted_means <- as.data.table(survey::svyby(~draw_0, by = ~sex_id+age_cat, design = hrs_design, survey::svymean))[, 1:3]
new_mean_col <- function(x){
  new_col <- c(survey::svyby(~get(paste0("draw_", x)),
                             by = ~sex_id+age_cat, 
                             design = hrs_design, 
                             survey::svymean)[3])
  return(new_col)
}
new_cols <- parallel::mclapply(1:999, new_mean_col, mc.cores = 9) 
for (n in 1:999){
  new_col <- new_cols[[n]]
  predicted_means[, paste0("draw_", n) := new_col]
}


predicted_means[, mean_predict := rowMeans(.SD), .SDcols = draws] 
predicted_means[, lower_predict := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draws]
predicted_means[, upper_predict := apply(.SD, 1, quantile, probs=0.975), .SDcols = draws]
predicted_means[, sd := apply(.SD, 1, sd), .SDcols = draws]
predicted_means[, (draws) := NULL]
epi_data <- copy(predicted_means)
predicted_means[, sd := NULL]

graph_predicted <- copy(predicted_means)
graph_predicted[, predicted := 1]
setnames(graph_predicted, c("mean_predict", "lower_predict", "upper_predict"), c("dementia", "ci_l", "ci_u"))
true_prev[, predicted := 0]
graph_data <- rbind(graph_predicted, true_prev)
graph_data[ci_u>1, ci_u := 1]

gg_prev <- ggplot(graph_data, aes(x = age_cat, y = dementia, color = as.factor(predicted))) +
  geom_point() + 
  geom_errorbar(aes(ymin = ci_l, ymax = ci_u)) +
  facet_wrap(~sex_id) +
  labs(x = "Age", y = "Prevalence") +
  scale_color_manual(name = "Data Type", values = c("#1FB04D", "blue"), labels = c("True Prevalence", "Predicted Prevalence")) +
  theme_classic()

# EPI UPLOADER DATA -------------------------------------------------------

firstup <- function(x) {
  substr(x, 1, 1) <- toupper(substr(x, 1, 1))
  return(x)
}

epi_data[,`:=` (nid = 1, location_id = 102, location_name = "United States", year_start = 2000, year_end = 2000,
                sex = firstup(as.character(sex_id)), group = 1, specificity = "age,sex", group_review = 0, cv_marketscan = 0,
                exclude_xwalk = 0, extractor = "eln1", is_outlier = 0, cv_niaaa = 0, cv_1066 = 0, cv_gp_data = 0,
                cv_clinical_record_diagnosis = 0, cv_cutoff_score_diagnosis = 1, age_start = substr(age_cat, 2, 3),
                age_end = gsub(")$", "", gsub("^.*,", "", age_cat)))]
epi_data[age_end == 120, age_end := 99]
epi_data[, c("sex_id", "age_cat") := NULL]
setnames(epi_data, c("mean_predict", "lower_predict", "upper_predict", "sd"), c("mean", "lower", "upper", "standard_error"))
write.csv(epi_data, paste0("FILEPATH"), row.names = F)

# ABSTRACT PLOT -----------------------------------------------------------

graph_data_2factor <- copy(graph_data)
graph_data_2factor[, sex_id := firstup(as.character(sex_id))]
gg_prev_2factor <- ggplot(graph_data_2factor, aes(x = age_cat, y = dementia, color = as.factor(predicted))) +
  geom_point(size = 3) + 
  geom_errorbar(aes(ymin = ci_l, ymax = ci_u), size = 1) +
  facet_wrap(~as.factor(sex_id)) +
  labs(x = "Age", y = "Prevalence") +
  scale_color_manual(name = "Data Type", values = c("#BDE785", "#1FB04D"), labels = c("ADAMS Prevalence", "Predicted Prevalence")) +
  theme_classic()


## HRS COMPARISON PREVALENCE
calcprev <- data.table(draw_0 = survey::svymean(~draw_0, design = hrs_design)[1])
calcprevsex <- as.data.table(survey::svyby(~draw_0, by = ~sex_id, design = hrs_design, survey::svymean)[2])
hrsprev <- rbind(calcprev, calcprevsex)
new_col_calcprev <- function(x){
  new_total <- c(survey::svymean(~get(paste0("draw_", x)), design = hrs_design)[1])
  new_sex <- unlist(survey::svyby(~get(paste0("draw_", x)), by = ~sex_id, design = hrs_design, survey::svymean)[2])
  new_col <- c(new_total, new_sex)
  return(new_col)
}
hrs_comparecols <- parallel::mclapply(1:999, new_col_calcprev, mc.cores = 9) 
for (n in 1:999){
  new_col <- hrs_comparecols[[n]]
  hrsprev[, paste0("draw_", n) := new_col]
}
hrsprev[, sex := c("both", "male", "female")]
hrsprev[, mean := rowMeans(.SD), .SDcols = draws]
hrsprev[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draws]
hrsprev[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draws]
hrsprev[, .(sex, mean, lower, upper)]

hrsageprev <- as.data.table(survey::svyby(~draw_0, by = ~age_cat, design = hrs_design, survey::svymean)[1:2])
new_col_ageprev <- function(x){
  new_age <- c(survey::svyby(~get(paste0("draw_", x)), by = ~age_cat, design = hrs_design, survey::svymean)[2])
  return(new_age)
}
hrs_agecols <- parallel::mclapply(1:999, new_col_ageprev, mc.cores = 9) 
for (n in 1:999){
  new_col <- hrs_agecols[[n]]
  hrsageprev[, paste0("draw_", n) := new_col]
}
hrsageprev[, mean := rowMeans(.SD), .SDcols = draws]
hrsageprev[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draws]
hrsageprev[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draws]
hrsageprev[, .(age_cat, mean, lower, upper)]


algorithm_stats_2factor <- copy(pred_draws)
algorithm_stats_2factor <- merge(regress_dt, algorithm_stats_2factor[, c("unique_id", paste0("draw_", 0:999)), with = F], by = "unique_id")
sens_funct <- function(draw, dt){
  tp <- nrow(dt[dementia == 1 & get(paste0("draw_", draw)) == 1])
  fn <- nrow(dt[dementia == 1 & get(paste0("draw_", draw)) == 0])
  sens <- tp/(tp+fn)
  return(sens)
}
spec_funct <- function(draw, dt){
  tn <- nrow(dt[dementia == 0 & get(paste0("draw_", draw)) == 0])
  fp <- nrow(dt[dementia == 0 & get(paste0("draw_", draw)) == 1])
  spec <- tn/(tn+fp)
  return(spec)
}
accur_funct <- function(draw, dt){
  correct <- nrow(dt[(dementia == 0 & get(paste0("draw_", draw)) == 0) | (dementia == 1 & get(paste0("draw_", draw)) == 1)])
  accuracy <- correct/nrow(dt)
  return(accuracy)
}


draw_num <- 0:999
sens_2factor <- unlist(parallel::mclapply(1:length(draw_num), function(x) sens_funct(draw = draw_num[x], dt = algorithm_stats_2factor), mc.cores = 9))
spec_2factor <- unlist(parallel::mclapply(1:length(draw_num), function(x) spec_funct(draw = draw_num[x], dt = algorithm_stats_2factor), mc.cores = 9))
accuracy_2factor <- unlist(parallel::mclapply(1:length(draw_num), function(x) accur_funct(draw = draw_num[x], dt = algorithm_stats_2factor), mc.cores = 9))

vector_sum <- function(x){
  print(paste0(round(mean(x), 2), " (", round(quantile(x, 0.025), 2), " - ", round(quantile(x, 0.975), 2), ")"))
}

vector_sum(sens_2factor)
vector_sum(spec_2factor)
vector_sum(accuracy_2factor)


# INVESTIGATE HEARING VARIABLE --------------------------------------------

## INVESTIGATE HEARING
hearing <- copy(score_dt)
hear_hrs <- copy(hrs)
hear_hrs[, unique_id := paste0(hh_id, "_", pn)]
hear_hrs <- hear_hrs[, .(unique_id, age_year, iwer_checkpoint_1, iwer_checkpoint_2)]
hear_var <- c("R had difficulty hearing any of the words")
hear_hrs[, hear_flag := 0]
hear_hrs[iwer_checkpoint_1 == hear_var | iwer_checkpoint_2 == hear_var, hear_flag := 1]
hearing_graph <- merge(hearing, hear_hrs, by = "unique_id")
mean_dt <- hearing_graph[group == "hrs", mean(score.F1), by = c("hear_flag", "age_cat")]
x <- dcast(mean_dt, hear_flag ~ age_cat, value.var = "V1")

gg_hear <- ggplot(hearing_graph[group == "hrs"], aes(score.F1)) +
  geom_histogram(data = hearing_graph[group == "hrs" & hear_flag == 0], fill = "red", alpha = 0.2) +
  geom_histogram(data = hearing_graph[group == "hrs" & hear_flag == 1], fill = "blue", alpha = 0.2) +
  #facet_wrap(~age_cat, scales = "free_y") +
  theme_classic()


# TEST LORDIF APPROACH ----------------------------------------------------

## GET ITEM RESPONSES
response_dt <- copy(test_dt)
funct_qs <- names(model_dt)[grepl("adl|iqcode", names(model_dt))]
cog_qs <- names(model_dt)[!names(model_dt) %in% funct_qs]
dif_dt <- merge(response_dt, score_dt, by = c("unique_id", "group"), all.x = T)

## MAKE ALL ITEMS FACTORS
items <- copy(anchor_items)
for (item in items){
  if (class(dif_dt[, get(item)]) == "logical"){
    dif_dt[, c(item) := as.factor(as.numeric(get(item)))]
  } else if (class(dif_dt[, get(item)]) == "numeric"){
    dif_dt[, c(item) := as.factor(get(item))]
  } else if (class(dif_dt[, get(item)]) == "factor"){
    next
  }
}


print(paste0(length(items)*2, " Tests: Use p-value of ", 0.05/(length(items)*2)))
get_lordif <- function(item){
  print(item)
  if (item %in% funct_qs){
    if (dif_dt[, length(levels(get(item)))] == 2){
      model1 <- glm(get(item) ~ score2 + age_year, data = dif_dt, family = binomial(link = "logit"))
      model2 <- glm(get(item) ~ score2 + age_year + group, data = dif_dt, family = binomial(link = "logit"))
      model3 <- glm(get(item) ~ score2 + age_year + group + group*score2, data = dif_dt, family = binomial(link = "logit"))
      udif_b <- round(model2$coefficients['grouphrs'], 2)
      nudif_b <- round(model3$coefficients["score2:grouphrs"], 2)
    } else if (dif_dt[, length(levels(get(item)))] > 2){
      model1 <- multinom(get(item) ~ score2 + age_year, data = dif_dt, trace = F)
      model2 <- multinom(get(item) ~ score2 + age_year + group, data = dif_dt, trace = F)
      model3 <- multinom(get(item) ~ score2 + age_year + group + group*score2, data = dif_dt, trace = F)
      udif_b <- paste(round(coef(model2)[, 'grouphrs'], 2), collapse = ", ")
      nudif_b <- paste(round(coef(model3)[, 'score2:grouphrs'], 2), collapse = ", ")
    } 
  } else if (item %in% cog_qs){
    if (dif_dt[, length(levels(get(item)))] == 2){
      model1 <- glm(get(item) ~ score1 + age_year, data = dif_dt, family = binomial(link = "logit"))
      model2 <- glm(get(item) ~ score1 + age_year + group, data = dif_dt, family = binomial(link = "logit"))
      model3 <- glm(get(item) ~ score1 + age_year + group + group*score1, data = dif_dt, family = binomial(link = "logit"))
      udif_b <- round(model2$coefficients['grouphrs'], 2)
      nudif_b <- round(model3$coefficients['score1:grouphrs'], 2)
    } else if (dif_dt[, length(levels(get(item)))] > 2){
      model1 <- multinom(get(item) ~ score1 + age_year, data = dif_dt, trace = F)
      model2 <- multinom(get(item) ~ score1 + age_year + group, data = dif_dt, trace = F)
      model3 <- multinom(get(item) ~ score1 + age_year + group + group*score1, data = dif_dt, trace = F)
      udif_b <- paste(round(coef(model2)[, 'grouphrs'], 2), collapse = ", ")
      nudif_b <- paste(round(coef(model3)[, 'score1:grouphrs'], 2), collapse = ", ")
    }
  }
  compare1 <- lrtest(model1, model2)
  compare2 <- lrtest(model2, model3)
  new_row <- data.table(item_name = item, udif_p = round(compare1$`Pr(>Chisq)`, 6), 
                        udif_beta = udif_b, 
                        nudif_p = round(compare2$`Pr(>Chisq)`, 6), 
                        nudif_beta = nudif_b)
  new_row[, u_flag := ifelse(udif_p < 0.05/(length(items)*2), 1, 0)]
  new_row[, nu_flag := ifelse(nudif_p < 0.05/(length(items)*2), 1, 0)]
  new_row <- new_row[!is.na(udif_p)]
  return(new_row)
}

lordif_table <- rbindlist(lapply(items, get_lordif))

## ABSOLUTE DIFFERENCE
adams_model <- readr::read_rds(paste0("FILEPATH"))
hrs_model <- readr::read_rds(paste0("FILEPATH"))

id_vars <- c("hh_id", "pn", "unique_id", "pweight", "group")
anchor_items <- intersect(names(test_dt_hrs), names(test_dt_adams))
anchor_items <- anchor_items[!anchor_items %in% id_vars]

get_parameters <- function(model){
  parameters <- coef(model)
  parameters <- parameters[!grepl("GroupPars", names(parameters))]
  get_row <- function(n){
    par_matrix <- parameters[[n]]
    par_matrix <- par_matrix['par',]
    i <- names(parameters[n])
    threshold_vec <- par_matrix[names(par_matrix)[grepl("^d", names(par_matrix))]]
    disc <- par_matrix[names(par_matrix)[grepl("^a", names(par_matrix))]]
    disc <- disc[!disc == 0]
    threshold_vec <- -threshold_vec / disc
    tdt <- data.table(item = i, value = c(disc, threshold_vec), parameter = c("disc", paste0("threshold_", 1:length(threshold_vec))) )
    return(tdt)
  }
  parameter_dt <- rbindlist(lapply(1:length(parameters), get_row))
  return(parameter_dt)
}

adams_pars <- get_parameters(adams_model)
adams_pars[, sample := "adams"]
hrs_pars <- get_parameters(hrs_model)
hrs_pars[, sample := "hrs"]

## COMPARE
par_dt <- rbind(adams_pars, hrs_pars)
par_dt <- par_dt[item %in% anchor_items]
par_dt <- dcast(par_dt, item + parameter ~ sample, value.var = "value")
par_dt[, par_dif := adams - hrs]

# GRAPH DIF ABSOLUTE/STATISTICAL ------------------------------------------

pval_table <- copy(lordif_table[, .(item_name, udif_p, nudif_p)])
graph_dif <- merge(par_dt, pval_table, by.x = "item", by.y = "item_name")
graph_dif[, pval := ifelse(parameter == "disc", nudif_p, udif_p)]
graph_dif[, under.01 := pval < 0.01]

disc_abs <- ggplot(graph_dif[parameter == "disc"], aes(x = as.factor(item), y = abs(par_dif), color = as.factor(under.01))) +
  geom_point() +
  labs(x = "Item", y = "Parameter Difference Absolute Value") +
  scale_color_manual(name = "P-Value", values = c("blue", "red"), labels = c("p-val over 0.01", "p-val under 0.01")) +
  theme_classic() +
  theme(axis.text.x = element_text(angle = 80, hjust = 1, size = 12))

## SCATTER AGAINST THE NUMBER OF PEOPLE WHO RESPONDED TO THE QUESTION

## GET SAMPLE SIZE PER QUESTION
get_ss <- function(i){
  responses <- model_dt[, get(i)]
  ss <- length(responses[!is.na(responses)])
  ss_dt <- data.table(item = i, num = ss)
  return(ss_dt)
}

ss_dt <- rbindlist(lapply(anchor_items, get_ss))
graph_dif <- merge(graph_dif, ss_dt, by = "item")

disc_ss <- ggplot(graph_dif[parameter == "disc"], aes(x = as.factor(item), y = num, color = as.factor(under.01))) +
  geom_point() +
  labs(x = "Item", y = "Sample Size") +
  scale_color_manual(name = "P-Value", values = c("blue", "red"), labels = c("p-val over 0.01", "p-val under 0.01")) +
  theme_classic() +
  theme(axis.text.x = element_text(angle = 80, hjust = 1, size = 12))

disc_scatter <- ggplot(graph_dif[parameter == "disc"], aes(x = abs(par_dif), y = num, color = as.factor(under.01), label = item)) +
  geom_point() +
  geom_text_repel(size = 2, segment.alpha = 0.3) +
  labs(x = "Parameter Difference Absolute Value", y = "Sample Size") +
  scale_color_manual(name = "P-Value", values = c("blue", "red"), labels = c("p-val over 0.01", "p-val under 0.01")) +
  theme_classic()

thresh_scatter <- ggplot(graph_dif[grepl("thresh", parameter)], aes(x = abs(par_dif), y = num, color = as.factor(under.01), 
                                                                    label = paste0(item, " - ", gsub("^threshold_", "", parameter)))) +
  geom_point() +
  geom_text_repel(size = 2, segment.alpha = 0.3) +
  labs(x = "Parameter Difference Absolute Value", y = "Sample Size") +
  scale_color_manual(name = "P-Value", values = c("blue", "red"), labels = c("p-val over 0.01", "p-val under 0.01")) +
  theme_classic()