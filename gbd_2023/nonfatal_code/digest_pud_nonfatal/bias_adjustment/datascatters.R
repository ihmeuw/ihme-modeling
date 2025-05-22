# requires ggplot2
# data to be scattered needs mean, upper, lower, and definition columns

scatter_bydef <- function(data_raworxw, upper = 0.1, raw = TRUE, width=10, height=7) {
  
  dt <- copy(data_raworxw)
  
  dt <- merge(dt, age_dt, by= "age_start")
  dt <- dt[ , age_group:=as.factor(age_group_id)]
  dt <- dt[age_group_id==28, age_group:="<1"]
  
  if (raw == TRUE) {
    title <- "PRExw"
  } else {
    title <- "POSTxw"
  }
  
  ggplot(dt, aes(age_group, mean, color = definition)) + 
    geom_boxplot(outlier.size=0.1, notch=FALSE) +
    stat_summary(fun.y=mean, geom="point", shape=5, size=1)  + 
    ylim(0, upper)
  ggsave(paste0(out_path, "/scatter_", title, "_", date, ".pdf"), width=width, height=height, limitsize=FALSE)
  
}

ratio_byage <- function(sexratio_dt, maxy = 0) {
  
  dt <- copy(sexratio_dt)
  if (maxy == 0) {
    upper = max(dt$ratio)
  } else {
    upper = maxy
  }
  
  ggplot(dt, aes(age_mid, ratio, color = super_region_name)) + 
    geom_point() +
    geom_segment(aes(x=0, xend=120, y=1, yend=1)) +
    ylim(0, upper) +
  ggsave(paste0(out_path, "/ratio_byage_", maxy, "_", date, ".pdf"), width=10, height=7)
  
}

sex_histo <- function(sex_lratios) {
  
  dt <- copy(sex_lratios)
  ggplot(dt, aes(dt$lratio, fill=dt$super_region_name)) + 
    geom_histogram(binwidth = 0.2, alpha=0.5, position="identity") +
    geom_vline(xintercept = 0, colour="black")
  ggsave (paste0(out_path, "/sexhisto_", date, ".pdf"), width=20, height=7)
  
}

scatter_markout <- function(data, upper = 0.1) {
  
  dt <- copy(data)
  
  dt <- merge(dt, age_dt, by= "age_start")
  dt <- dt[ , age_group:=as.factor(age_group_id)]
  dt <- dt[age_group_id==28, age_group:="<1"]
  
  ggplot(dt, aes(age_group, mean, color = factor(is_outlier))) + 
    geom_point() +
    ylim(0, upper)
  ggsave(paste0(out_path, "/scatter_outliers_", date, ".pdf"), width=10, height=7)
  
}

mean_v_mean <- function(prepostdt, title, width=20, height=7, xlim=1) {
  
  dt <- copy(prepostdt)
  
  ggplot(dt, aes(mean.x, mean.y, color = definition.x)) + 
    geom_point() +
    geom_abline(slope = 1) +
    xlim(0, xlim)
  ggsave(paste0(out_path, "/prevpost_means_", title, "_", date, ".pdf"), width=width, height=height, limitsize=FALSE)
  
  
}