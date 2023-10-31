#############################################################
## Build a framework for OOS validation of mask use, testing
## projections.
#############################################################

library(MASS)
library(ggplot2)
library(data.table)
## install ihme.covid (always use newest version)
tmpinstall <- system("mktemp -d --tmpdir=/tmp", intern = TRUE)
.libPaths(c(tmpinstall, .libPaths()))
devtools::install_github("ihmeuw/ihme.covid", upgrade = "never", quiet = T, ref = "get_latest_output_dir")

date_range <- seq(as.Date("2020-01-01"), as.Date("2021-12-31"), by = "1 day")

# Import data (post barber smooth?)
# Make projections with 2, 4, 6 weeks hold out
# Test variety of weighting, assumptions
# Validate against observed data, barber smoothed data
# Summary of the approaches

# I'm going to start with testing because it seems more obvious to me but I want the function generally flexible to different covariates
# Prep data
dt <- fread("/ihme/covid-19/mask-use-outputs/2020_08_24.01/used_data.csv")
dt[, date := as.Date(date)]

## What exactly do we want to check with mask outputs? 
# Convert to daily
full_dt <- rbindlist(lapply(split(dt, by = "location_id"), function(loc_dt) {
  date_trunc <- date_range[date_range <= max(loc_dt$date)] # Only make projections through the last date of data for location
  observed <- loc_dt[,.(date, N)]
  observed[, observed := 1]
  full_ts <- approx(loc_dt$date, loc_dt$prop_always, xout = date_trunc, rule = 2)$y
  tmp <- data.table(location_id = unique(loc_dt$location_id), location_name = unique(loc_dt$location_name), 
                    date = date_trunc, prop_always = full_ts)
  tmp <- merge(tmp, observed, by="date", all.x = T)
  tmp[, observed := ifelse(is.na(observed), 0, 1)]
}))

### Smooth ###
# We need to make some locations more flexible than others
# One of the concerns is what the right tail is doing
# Make proportion sample size in last week
full_dt[, N := ifelse(is.na(N), 0, N)]
full_dt[, last_week := ifelse(date > max(date)-7, 1, 0), by="location_id"]
full_dt[, n_last_week := sum(N), by=c("location_id","last_week")]
full_dt[, n_total := sum(N, na.rm=T), by=c("location_id")]
full_dt[, p_n_last_week := ifelse(last_week == 1, n_last_week / n_total, NA)]
hist(full_dt$p_n_last_week)

## Function to make changing the smoothing easier
# I guess we can test the mean error at different smoothing amounts
smooth_error <- function(full_dt, neighbors, iterations){
  tmp <- full_dt[, smooth_prop_always := ihme.covid::barber_smooth(prop_always, n_neighbors = neighbors, times = iterations), by = "location_id"]
  tmp <- tmp[observed == 1] # I don't think it is fair to calculate error/residuals based on imputed data
  tmp[, residual := smooth_prop_always - prop_always]
  tmp[, sq_error := residual^2]
  tmp[, rmse_loc := sqrt(mean(sq_error)), by=location_id]
  tmp[, rmse_all := sqrt(mean(sq_error))]
  return(tmp)
}

nml <- smooth_error(full_dt, 5, 10)

# Locations with fewer iterations
more_flexible_locs <- c("Scotland",
                        "Victoria","Belgium","Croatia","England","Greece","Ireland","Northern Ireland",
                        "Serbia","Switzerland","United Kingdom","Wales","Nova Scotia",
                        "New Zealand","Alberta","Manitoba") # Added 8/24
# We need to make some locations more flexible than others
more_flex <- subset(full_dt, location_name %in% more_flexible_locs)
norm_flex <- subset(full_dt, !(location_name %in% more_flexible_locs))

nml <- rbind(smooth_error(more_flex, 5, 2), smooth_error(norm_flex, 5, 10))

plot_locs <- c(525, 128, 161, 43, 123, 528, 4651, 4749, 60403, 89)

## How does changing iterations affect fit?
idt <- data.table()
for(i in 2:10){
  print(paste0("On iteration ", i, " of 10"))
  tmp <- smooth_error(full_dt, 5, i)
  tmp$iterations <- i
  idt <- rbind(idt, tmp)
}

ggplot(idt[location_id %in% plot_locs], aes(x=as.Date(date))) + 
  geom_point(data=nml[location_id %in% plot_locs], aes(y=prop_always), alpha = 0.2) +
  geom_line(aes(y=smooth_prop_always, col=factor(iterations))) +
  geom_line(data=nml[location_id %in% plot_locs], aes(x=as.Date(date), y = smooth_prop_always), col="black") + 
  facet_wrap(~location_name, scales="free_y") + 
  theme_bw() + ylab("Mask use") + xlab("")

## How does changing neighbors affect fit?
ndt <- data.table()
for(i in 2:10){
  print(paste0("On neighbors ", i, " of 10"))
  tmp <- smooth_error(full_dt, i, 10)
  tmp$neighbors <- i
  ndt <- rbind(ndt, tmp)
}

ggplot(ndt[location_id %in% plot_locs], aes(x=as.Date(date))) + 
  geom_point(data=nml[location_id %in% plot_locs], aes(y=prop_always), alpha = 0.2) +
  geom_line(aes(y=smooth_prop_always, col=factor(neighbors))) +
  geom_line(data=nml[location_id %in% plot_locs], aes(x=as.Date(date), y = smooth_prop_always), col="black") + 
  facet_wrap(~location_name, scales="free_y") + 
  theme_bw() + ylab("Mask use") + xlab("")

ts_tail <- ndt[, keep := ifelse(date > max(date)-7, 1, 0), by=location_id]
ts_tail <- ts_tail[keep == 1]
ts_tail[, mean_residual := mean(residual), by=neighbors]

unique(ts_tail[,.(neighbors, mean_residual)])

## Okay, with that all in mind, can I test smoothing by how much *weight* exists in the last 7 days of data?
# Fewer iterations with more weight
p_med <- median(full_dt$p_n_last_week, na.rm = T)
full_dt[, mw := ifelse(mean(p_n_last_week, na.rm=T) > p_med, 1, 0), by=location_id]

mw <- smooth_error(full_dt[mw == 1], neighbors = 5, iterations = 2)
  mw[, type := "Weight last week (2 iterations)"]
bw <- smooth_error(full_dt[mw == 1], neighbors = 5, iterations = 5)
  bw[, type := "Weight last week (5 iterations)"]
lw <- smooth_error(full_dt[mw == 0], neighbors = 5, iterations = 10)
  lw[, type := "More iterations (less weight)"]

nml[, type := "Current approach"]
wdt <- rbind(mw, bw, nml, fill = T)

ggplot(wdt[location_id %in% plot_locs], aes(x=as.Date(date))) + 
  geom_point(data=nml[location_id %in% plot_locs], aes(y=prop_always, size=N), alpha = 0.2) +
  #geom_line(data=nml[location_id %in% plot_locs], aes(x=as.Date(date), y = smooth_prop_always), col="black", lwd=1.1) + 
  geom_line(aes(y=smooth_prop_always, col=type), lwd = 1.1) + guides(size=F) + 
  facet_wrap(~location_name, scales="free_y") + 
  theme_bw() + ylab("Mask use") + xlab("") + ggtitle("Smoothing determined by weight in last week of data")
ggplot(wdt[location_name %in% more_flexible_locs], aes(x=as.Date(date))) + 
  geom_point(data=nml[location_name %in% more_flexible_locs], aes(y=prop_always, size=N), alpha = 0.2) +
  #geom_line(data=nml[location_name %in% more_flexible_locs], aes(x=as.Date(date), y = smooth_prop_always), col="black", lwd=1.1) + 
  geom_line(aes(y=smooth_prop_always, col=type), lwd = 1.1) + guides(size=F) + 
  facet_wrap(~location_name, scales="free_y") + 
  theme_bw() + ylab("Mask use") + xlab("") + ggtitle("Smoothing determined by weight in last week of data")

## Okay, with that all in mind, can I test smoothing by (average? total?) sample size?
# Fewer iterations with more weight
hist(log(full_dt$n_total))
ss_med <- median(full_dt$n_total, na.rm = T)
ss_q <- quantile(full_dt$n_total, c(0.25,0.5,0.75))
full_dt[, mw := ifelse(mean(p_n_last_week, na.rm=T) > p_med, 1, 0), by=location_id]

mw <- smooth_error(full_dt[n_total < ss_q[1]], neighbors = 5, iterations = 10)
mw[, type := "<25% SS, Smooth (10 iterations & 5 neighbors)"]
mw2 <- smooth_error(full_dt[n_total < ss_q[1]], neighbors = 7, iterations = 10)
mw2[, type := "<25% SS, Smooth (10 iterations & 7 neighbors)"]
bw <- smooth_error(full_dt[n_total >= ss_q[1] & n_total < ss_q[3]], neighbors = 5, iterations = 5)
bw[, type := "Interquartile SS (5 iterations & 5 neighbors)"]
bw2 <- smooth_error(full_dt[n_total >= ss_q[1] & n_total < ss_q[3]], neighbors = 3, iterations = 5)
bw2[, type := "Interquartile SS (5 iterations, 3 neighbors)"]
lw <- smooth_error(full_dt[n_total >= ss_q[3]], neighbors = 5, iterations = 2)
lw[, type := ">75% SS, Flexible (2 iterations & 5 neighbors)"]
lw2 <- smooth_error(full_dt[n_total >= ss_q[3]], neighbors = 3, iterations = 2)
lw2[, type := ">75% SS, Flexible (2 iterations & 3 neighbors)"]

nml[, type := "Current approach"]
wdt <- rbind(mw, mw2, bw, bw2, lw, lw2, fill = T)
ldt <- rbind(mw2, bw, lw, fill = T)

ggplot(wdt[location_id %in% plot_locs], aes(x=as.Date(date))) + 
  geom_point(data=nml[location_id %in% plot_locs], aes(y=prop_always, size=N), alpha = 0.2) +
  #geom_line(data=nml[location_id %in% plot_locs], aes(x=as.Date(date), y = smooth_prop_always), col="black", lwd=1.1) + 
  geom_line(aes(y=smooth_prop_always, col=type), lwd = 1.1) + guides(size=F) + 
  facet_wrap(~location_name, scales="free_y") + 
  theme_bw() + ylab("Mask use") + xlab("") + ggtitle("Smoothing determined by cumulative SS")
ggplot(wdt[location_name %in% more_flexible_locs], aes(x=as.Date(date))) + 
  geom_point(data=nml[location_name %in% more_flexible_locs], aes(y=prop_always, size=N), alpha = 0.2) +
  geom_line(data=nml[location_name %in% more_flexible_locs], aes(x=as.Date(date), y = smooth_prop_always), col="black", lwd=1.1) + 
  geom_line(aes(y=smooth_prop_always, col=type), lwd = 1.1) + guides(size=F) + 
  facet_wrap(~location_name, scales="free_y") + 
  theme_bw() + ylab("Mask use") + xlab("") + ggtitle("Smoothing determined by cumulative SS")

ggplot(wdt[location_name %in% more_flexible_locs], aes(x=as.Date(date))) + 
  geom_point(data=nml[location_name %in% more_flexible_locs], aes(y=prop_always, size=N), alpha = 0.2) +
  geom_line(data=nml[location_name %in% more_flexible_locs], aes(x=as.Date(date), y = smooth_prop_always), col="black", lwd=1.1) + 
  geom_line(aes(y=smooth_prop_always, col=type), lwd = 1.1) + 
  facet_wrap(~location_name, scales="free_y") + 
  scale_size_continuous(breaks = c(1, 145, 359, 920), labels = c(1,2,3,4)) + #guides(size=F) + 
  theme_bw() + ylab("Mask use") + xlab("") + ggtitle("Smoothing determined by cumulative SS")

## Loop over locations ##
ldt <- ldt[order(location_id)]
ldt <- merge(ldt, hierarchy[,c("location_id","level")], by="location_id")
ldt <- ldt[level >= 3]
loc_id <- 523
nml[, cut_n := cut(N, breaks = c(1, 150, 400, 920, 100000), labels = c("1 to 149","150 to 399","400 to 999","> 1000"))]

pdf("/ihme/covid-19/mask-use-outputs/2020_08_24.01/sample_size_smoothing.pdf", height=6, width=8)
for(loc_id in unique(ldt$location_id)){
  line_col <- ifelse(unique(ldt[location_id==loc_id]$type) == "<25% SS, Smooth (10 iterations & 7 neighbors)", "#3399FF",
                    ifelse(unique(ldt[location_id==loc_id]$type) == "Interquartile SS (5 iterations & 5 neighbors)", "#9933FF", "#FF3333"))
  p <- ggplot(ldt[location_id == loc_id], aes(x=as.Date(date))) + 
    geom_point(data=nml[location_id == loc_id], aes(y=prop_always, size=N), alpha = 0.2) +
    geom_line(data=nml[location_id == loc_id], aes(x=as.Date(date), y = smooth_prop_always, col="current"), lwd=1.1) + 
    geom_line(aes(y=smooth_prop_always, col="update"), lwd = 1.1) + 
    stat_smooth(data = nml[location_id == loc_id], method = "loess", se = F, aes(x=as.Date(date), y = smooth_prop_always, col = "zloess", weight = N)) + 
    scale_color_manual("", values = c("current" = "black", "update" = line_col, "zloess" = "goldenrod"), labels = c("Current model","Sample size based", "Weighted Loess")) + 
    #scale_size_manual(breaks = c(1, 150, 400, 920), labels = c("1 to 149","150 to 399","400 to 999","> 1000")) + 
    scale_size_continuous("") + 
    theme_bw() + ylab("Mask use") + xlab("") + ggtitle(unique(ldt[location_id==loc_id]$location_name), subtitle = unique(ldt[location_id == loc_id]$type))
  print(p)
}
dev.off()
