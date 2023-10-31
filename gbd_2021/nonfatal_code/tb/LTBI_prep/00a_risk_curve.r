## EMPTY THE ENVIRONMENT
rm(list = ls())
## SET UP FOCAL DRIVES
os <- .Platform$OS.type
if (os=="windows") {
  ADDRESS <-"ADDRESS"
  ADDRESS <-"ADDRESS"
  ADDRESS <-"ADDRESS"
} else {
  ADDRESS <-"ADDRESS"
  ADDRESS <-paste0("ADDRESS/", Sys.info()[7], "/")
  ADDRESS <-"ADDRESS"
}

## LOAD FUNCTIONS AND PACKAGES
library(readxl)
library(data.table)
library(ggplot2)

## READ DATA
main <- read.csv(paste0(ADDRESS,"FILEPATH"))
main<-main[,-1]

library(dplyr)
library(mrbrt001) 

#############################################################################################
###                                  FIT MR-BERT MODEL                                    ###
#############################################################################################
## GET MODEL DATA

mod_data <- MRData()
mod_data$load_df(data = main,
                 col_obs = 'log_mean',
                 col_obs_se = 'log_se',
                 col_covs = c('indur_lower', 'indur_upper'),
                 col_study_id = "study" )

knots_samples <- utils$sample_knots(
  num_intervals = 3L,
  knot_bounds = rbind(c(0.0, 0.6), c(0.4, 1)),
  interval_sizes = rbind(c(0.1, 1),c(0.1, 1),  c(0.1, 1)),
  num_samples = 20L
)

log_cov_model <- LogCovModel(alt_cov           = list('indur_lower', 'indur_upper'),
                             use_re            = TRUE,
                             use_spline        = TRUE,
                             spline_r_linear   = TRUE,
                             spline_knots_type = "domain",
                             spline_knots      = array(c(0, 0.3, 0.6,  1)),

                             prior_spline_monotonicity    = 'increasing',
                             prior_spline_maxder_gaussian = array(c(0, 0.3)),
                             spline_degree = 2L)



mr <- MRBeRT(
  data = mod_data,
  ensemble_cov_model = log_cov_model,
  inlier_pct = 0.9,
  ensemble_knots = knots_samples

)



## FIT THE MODEL
mr$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
print(c('beta_soln :', mr$sub_models[[1]]$beta_soln))

## GET BETAS/SAMPLES
n_samples = 1000L
samples        <- mr$sample_soln(sample_size = n_samples)
names(samples) <- c("beta", "gamma")
mr_betas       <- samples

#############################################################################################
###                                  GET PREDICTION DRAWS                                 ###
#############################################################################################

## GET INDURATION FOR PREDICTION
exposure <- unique(c(seq(0, 1, by=0.1), seq(0, 25, by=0.1)))
exposure <- unique(c(exposure, seq(0, 25, by = 0.25)))
df_pred   <- data.frame( indur_lower=exposure, indur_upper=exposure)


df_pred3 <- data.frame(indur_lower = exposure, indur_upper = exposure)

dat_pred3 <- MRData()

dat_pred3$load_df(
  data = df_pred3,
  col_covs=list( 'indur_lower', 'indur_upper')

)



## GET DRAWS
log_rr_draws <- data.table(mr$create_draws(data = dat_pred3,
                                           beta_samples=samples[["beta"]], #samples
                                           gamma_samples=samples[["gamma"]], #samples
                                           random_study = FALSE
))
## PREP DRAWS
setnames(log_rr_draws, old=names(log_rr_draws), new=paste0("draw_", 0:(n_samples-1)))

## CREATE SUMMARIES
log_rr_draws[, lower := apply(.SD, 1, quantile, probs = .025, na.rm =T), .SDcols = (paste0("draw_", 0:(n_samples-1)))]
log_rr_draws[, upper := apply(.SD, 1, quantile, probs = .975, na.rm =T), .SDcols = (paste0("draw_", 0:(n_samples-1)))]
log_rr_draws[, mean  := rowMeans(.SD), .SDcols = (paste0("draw_", 0:(n_samples-1)))]

## PREP TO PLOT
preds <- data.table(cbind(df_pred, log_rr_draws[, .(lower, upper, mean)]))
preds$indur_lower <- preds$indur_lower
preds$indur_upper <- preds$indur_upper

main[, inlier := mr$w_soln]

## FORMAT DRAWS
draws <- log_rr_draws[, .SD, .SDcols = paste0("draw_", 0:(n_samples-1))]
draws <- data.table(cbind(df_pred, draws))


## PLOT RISK CURVE
ggplot(data=preds, aes(x=indur_lower, y=mean)) +
  geom_errorbarh(data=main, aes(xmin=indur_lower, xmax=indur_upper, y=log_mean), alpha=0.45, colour="seagreen", size=1.1) +
  geom_point(data=main, aes(x=(indur_lower+indur_upper)/2, y=log_mean, size=(1/log_se^2)), alpha=0.6, colour="darkgreen", show.legend=F) +
  geom_line(size=1.2) + theme_bw() +
  geom_ribbon(aes(x=indur_lower, ymin=lower, ymax=upper), alpha=0.3) +
  labs(x="Induration Size (mm)", y="Log Risk of Progression to active TB") +
  theme(axis.text=element_text(size=12), axis.title=element_text(size=14)) 


pred_2019 <- as.data.table(read.csv("FILEPATH"))
pred_2019[, lower := apply(.SD, 1, quantile, probs = .025, na.rm =T), .SDcols = (paste0("draw_", 0:(n_samples-1)))]
pred_2019[, upper := apply(.SD, 1, quantile, probs = .975, na.rm =T), .SDcols = (paste0("draw_", 0:(n_samples-1)))]
pred_2019[, mean  := rowMeans(.SD), .SDcols = (paste0("draw_", 0:(n_samples-1)))]

exposure <- unique(c(seq(0, 1, by=0.1), seq(0, 25, by=0.1)))
df2019   <- data.frame(indur_lower=exposure, indur_upper=exposure)
preds_2019 <- data.table(cbind(df2019, pred_2019[, .(lower, upper, mean)]))

preds_2019$source <- "GBD2019"
preds$source <- "GBD2020 - New without covariates"

df<-rbind(preds_2019,preds)
ggplot(data=df, aes(x=indur_lower, y=mean)) +
  geom_errorbarh(data=main, aes(xmin=indur_lower, xmax=indur_upper, y=log_mean), alpha=0.45, colour="seagreen", size=1.1) +
  geom_point(data=main, aes(x=(indur_lower+indur_upper)/2, y=log_mean, size=(1/log_se^2)), alpha=0.6, colour="darkgreen", show.legend=F) +
  geom_line(aes(color = source), size=0.8) + theme_bw() +# scale_shape_manual(values=c(4, 16)) +
  geom_ribbon(aes(x=indur_lower, ymin=lower, ymax=upper, fill = source), alpha=0.2) +
  labs(x="Induration Size (mm)", y="Log Risk of Progression to active TB") +
  theme(axis.text=element_text(size=12), axis.title=element_text(size=14)) #+xlim(0,5)

## SAVE 1000 DRAWS
write.csv(draws,"FILEPATH", row.names = F)
