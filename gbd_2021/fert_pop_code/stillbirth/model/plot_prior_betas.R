####################################################################################
##                                                                                ##
## Purpose: Explore linear prior covariates from the test_prior() function        ##
##                                                                                ##
####################################################################################

library(data.table)
library(ggplot2)
library(RColorBrewer)

##########
## ARGS ##
##########

if (!exists("me")) {
  
  stop("No ME")
  
}

x_var <- "out_rmse" # in_rmse or out_rmse or aic
plot_pairs <- T

output_dir <- "FILEPATH"

plot_output <- "FILEPATH"
pairs_plot_output <- "FILEPATH"

#################
## GET RESULTS ##
#################

betas <- fread("FILEPATH")

betas[, mod_num := 1:.N]

# get covariates used
covs <- grep("*_fixd", names(betas), value = T)
covs <- covs[!grepl("_fixd_se", covs)]

covs <- unlist(lapply(covs, function(x) {
  
  temp <- unlist(tstrsplit(x, "_"))
  temp <- temp[1:(length(temp) - 1)]
  paste0(temp, collapse = "_")
  
}))

message("Covariates tested: ", paste(covs, collapse = ", "))

# reshape betas to show multiple metrics
betas_melt <- melt(betas, id.vars = setdiff(names(betas), c("aic", "in_rmse", "out_rmse")), 
                   measure_vars = c("aic", "in_rmse", "out_rmse"), variable.name = "Metric", value.name = "Fit")

###############################
## GET WEIGHTS AND AGGREGATE ##
###############################

# get weights
scaler <- sum(betas[drop == 0, c(x_var), with = F])

betas[drop == 0, wt := get(x_var) / scaler]
betas[drop == 1, wt := 0]
betas[drop == 0, draws := wt * 1000]

# expand then aggregate weighted for first hist
expanded_full <- list()

for (cov in covs) {
  
  expanded <- copy(betas)
  expanded[!is.na(get(paste0(cov, "_fixd"))), cov := cov]
  expanded <- expanded[!is.na(cov)]
  expanded_full[[length(expanded_full) + 1]] <- expanded

}

expanded_full <- rbindlist(expanded_full)
expanded_full <- expanded_full[cov != "(Intercept)"]

aggd <- expanded_full[, .(total_draws = sum(draws, na.rm = T)), by = .(sex, cov)]
aggd$cov_fact <- factor(aggd$cov, levels = unique(aggd[sex == "both_sexes",][order(total_draws, decreasing = T), cov]))

##################
## PLOT RESULTS ##
##################

pdf(file = plot_output, width = 10)

angle <- 60
angle <- angle + length(unique(covs)) * 2

if (angle > 90) {angle <- 90}

# plot summary weights of each model
p <- ggplot(data = aggd, aes(x = cov_fact, y = total_draws)) +
            geom_bar(stat = "identity") +
            xlab("") +
            ylab("Number of draws") +
            scale_fill_brewer(palette = "Set1") +
            guides(fill = "none") +
            theme_classic() +
            theme(panel.border = element_rect(color = "black", fill = NA), 
                  text = element_text(size = 17),
                  axis.text.x = element_text(color = "black", angle = angle, hjust = 1))

print(p)

for (cov in covs) {
  
  p <- ggplot(data = betas[drop == 0], aes(x = get(x_var), y = get(paste0(cov, "_fixd")), color = sex)) +
              geom_point(size = 2) +
              geom_point(data = betas[drop == 1], aes(x = get(x_var), y = get(paste0(cov, "_fixd"))), shape = 4, size = 2, alpha = .5) +
              geom_errorbar(aes(ymin = get(paste0(cov, "_fixd")) - 1.96 * get(paste0(cov, "_fixd_se")),
                                ymax = get(paste0(cov, "_fixd")) + 1.96 * get(paste0(cov, "_fixd_se")))) +
              geom_abline(aes(intercept = 0, slope = 0)) +
              geom_rug(data = betas[is.na(sign_violation)], aes(x = get(x_var))) +
              scale_color_brewer(palette = "Set1") +
              xlab(x_var) +
              ylab("beta") +
              ggtitle(paste0("Covariates for ", me, " vs ", cov)) +
              facet_wrap(~ sex) +
              theme_bw() +
              theme(text = element_text(size = 17),
                    panel.border = element_rect(color = "black", fill = NA))
  
  print(p)
  
  p <- ggplot(data = betas, aes(x = get(paste0(cov, "_fixd")), fill = sex)) +
              geom_histogram(color = "black") +
              geom_vline(aes(xintercept = 0)) +
              scale_color_brewer(palette = "Set1") +
              ylab("Count") +
              xlab(paste0("Betas for ", cov)) +
              ggtitle(paste0(me, " vs ", cov)) +
              facet_wrap(~ sex) +
              theme_classic() +
              theme(text = element_text(size = 17),
                    panel.border = element_rect(color = "black", fill = NA))
  
  print(p)
  
  p <- ggplot(data = betas_melt, aes(x = Fit, y = get(paste0(cov, "_fixd")), color = sex)) +
              geom_point(size = 2) +
              geom_errorbar(aes(ymin = get(paste0(cov, "_fixd")) - 1.96 * get(paste0(cov, "_fixd_se")),
                                ymax = get(paste0(cov, "_fixd")) + 1.96 * get(paste0(cov, "_fixd_se")))) +
              geom_abline(aes(intercept = 0, slope = 0)) +
              scale_color_brewer(palette = "Set1") +
              facet_wrap(~ Metric, scales = "free") +
              # xlab() +
              ylab("beta") +
              ggtitle(paste0("Covariates for ", me, " vs ", cov)) +
              theme_bw() +
              theme(text = element_text(size = 17),
                    panel.border = element_rect(color = "black", fill = NA))
  
  print(p)
  
}

dev.off()

################
## PLOT PAIRS ##
################

if (plot_pairs == T) {
  
  message("Plotting pairs...")
  cov_pairs <- combn(covs, 2)
  
  pdf(file = pairs_plot_output)
  
  for (i in 1:ncol(cov_pairs)) {
    
    pair <- cov_pairs[, i]
    
    p <- ggplot(data = betas, aes(x = get(paste0(pair[1], "_fixd")), y = get(paste0(pair[2], "_fixd")))) +
                geom_point(aes(color = get(x_var))) +
                xlab(pair[1]) +
                ylab(pair[2]) +
                ggtitle(paste0(me, " beta covariance: ", pair[1], " vs ", pair[2])) +
                facet_wrap(~ sex) +
                scale_color_continuous(name = x_var) +
                theme_bw()
    
    print(p)
    
  }
  
  dev.off()
  message("Done")
  
}

message("Prior betas plots saved here: ", plot_output)
