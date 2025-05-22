## Function for formatting matches, calculating ratios for crosswalks (and errors/log transformations), looking at the ratios in a forest plot (requires metafor package) or histogram (requires ggplot)

calc_ratio <- function(matched_dt) {
  
  dt <- copy(matched_dt)
  dt[ , ratio:= mean2/mean]
  dt[ , rat_se:=sqrt((mean2^2/mean^2)*(((standard_error2^2)/(mean2^2))+((standard_error^2)/(mean^2))))]
  
  return(dt)
  
}

logtrans_ratios <- function(ratio_dt) {
  
  dt <- copy(ratio_dt)
  dt[ , lratio := log(dt$ratio)]
  
  dt$log_ratio_se <- sapply(1:nrow(dt), function(i) {
    ratio_i <- dt[i, "ratio"]
    ratio_se_i <- dt[i, "rat_se"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })
  
  return(dt)
  
}

calc_logitdf <- function(matched_dt) {
  
  dt <- copy(matched_dt)
  dt[ , logit:=log(mean/(1-mean))]
  dt[ , logit2:=log(mean2/(1-mean2))]
  
  dt$se_logit <- sapply(1:nrow(dt), function(i) {
    mean_i <- dt[i, mean]
    standard_error_i <- dt[i, standard_error]
    deltamethod(~log(x1/(1-x1)), mean_i, standard_error_i^2)
  })
  
  dt$se_logit2 <- sapply(1:nrow(dt), function(i) {
    mean_i <- dt[i, mean2]
    standard_error_i <- dt[i, standard_error2]
    deltamethod(~log(x1/(1-x1)), mean_i, standard_error_i^2)
  })
  
  dt[ , diff_logit:=logit2-logit]
  dt[ , se_diff_logit:=sqrt(se_logit2^2 + se_logit^2)]
  
  return(dt)

}

drop_stuff <- function(ratio_dt, drop_missing = 1, drop_zerodiv = 1, drop_inf = 1) {
  
  dt <- copy(ratio_dt)
  
  if (drop_missing==1) {
    dt <- dt[!is.na(ratio) & !is.na(rat_se),]
  }
  if (drop_zerodiv==1) {
    dt <- dt[!is.nan(ratio) & !is.nan(rat_se),]
  }
  if (drop_inf==1) {
    dt <- dt[is.finite(ratio) & is.finite(rat_se),]
  }
  
  return(dt)
  
}

add_compdumminter <- function(ratio_dt) {
  
  dt <- copy(ratio_dt)
  dt[ , (cv_alts):=0]
  
  for (cv in cv_alts) {
    dt <- dt[grep(cv, def), (cv):=get(cv)-1]
    dt <- dt[grep(cv, def2), (cv):=get(cv)+1]
  }
  
  dt[ , comparison:=paste0(dt$def, dt$def2)]
  
  dt[ , intercept:= 1]
  
  return(dt)
  
}


histo_ratios <- function(ratio_dt, bins = 0.2, width = 10, height = 7) {
  
  dt <- copy(ratio_dt)
  ggplot(dt, aes(dt$ratio, fill=dt$comparison)) + 
    geom_histogram(alpha=0.5, position="identity", binwidth = bins) +
    geom_vline(xintercept = 1, colour="black")
  ggsave (paste0(out_path, "/histogram_", date, ".pdf"), width=width, height=height)
  
}

forest_plot <- function(ratio_dt) {
  
  dt <- copy(ratio_dt)
  meta_ratio <- rma(yi = dt$ratio, sei = dt$rat_se)
  
  pdf(paste0(out_path, "/forest_plot_", date, ".pdf"))
  forest(meta_ratio, slab = paste0(dt$nid, ", ", dt$sex, ", ", dt$age_start, ", ", dt$def2, ", ", dt$def), showweights = F, xlab = "Crosswalk vs reference")
  dev.off()
  
}



