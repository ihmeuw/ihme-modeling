# Source: https://github.com/YouGov-Data/covid-19-tracker
# and https://today.yougov.com/topics/international/articles-reports/2020/03/17/personal-measures-taken-avoid-covid-19 
## Question asked in survey: 
# Worn a facemask outside your home (e.g. when on public
# transport, going to a supermarket, going to a main road)

# tmpinstall <- system("mktemp -d --tmpdir=/tmp", intern = TRUE)
# .libPaths(c(tmpinstall, .libPaths()))
# devtools::install_github("ihmeuw/ihme.covid", upgrade = "never", quiet = T)
# library(ihme.covid)
# 
# lsvid <- 746
# OUTPUT_ROOT <- "/ihme/covid-19/mask-use-outputs"
# 
# output_dir <- ihme.covid::get_latest_output_dir(root = OUTPUT_ROOT)
# 
# source(file.path("/ihme/cc_resources/libraries/current/r/get_location_metadata.R"))
# hierarchy <- get_location_metadata(location_set_id = 115, location_set_version_id = lsvid)

dir <- "FILEPATH"
#dir <- "/ihme/covid-19/data_intake/YouGov"
github_path <- file.path(dir, "yougov_github_masks.csv")
fbook_path <- "FILEPATH/mask_ts.csv"
#fbook_path <- "/ihme/covid-19/data_intake/symptom_survey/global/mask_ts.csv"
current_path <- file.path(OUTPUT_ROOT, "best", "mask_use.csv") # With new data
site_path <- file.path(dir, "yougov_masks.csv")
plot_path <- file.path(output_dir, "yougov_mask_plots.pdf")

github_dt <- fread(github_path)
github_dt[, ihme_loc_id := NULL]
melt_gh <- as.data.table(melt(github_dt, id.vars = c("location_id", "location_name", "date", "N")))
melt_gh <- melt_gh[grepl("prop", variable)]
remove_vars <- c("prop_mask_Not_at_all", "prop_mask_Rarely", "prop_mask_Sometimes")
gh_freq_always <- melt_gh[!(variable %in% remove_vars), .(value = sum(value), N = sum(N)), by = .(location_id, location_name, date)]
gh_freq_always[, source := "Github - Frequently + Always"]
gh_always <- melt_gh[!(variable %in% c(remove_vars, "prop_mask_Frequently")), .(value = sum(value), N = sum(N)), by = .(location_id, location_name, date)]
gh_always[, source := "Github - Always"]
gh_dt <- rbind(gh_freq_always, gh_always)
gh_dt[, date := as.Date(date, "%d.%m.%Y")]
gh_dt <- subset(gh_dt, N > 30)

site_dt <- fread(site_path)
site_dt[, date := as.Date(date, "%d.%m.%Y")]

if(max(site_dt$date < "2020-08-01")){
  #supp <- fread("/home/j/temp/ctroeger/COVID19/yougov_masks_supplement.csv")
  supp <- fread("FILEPATH/yougov_masks_supplement.csv")
  supp[, date := as.Date(date, "%d.%m.%Y")]
  site_dt <- rbind(site_dt, supp)
  site_dt <- site_dt[order(location_name, date)]
}

setnames(site_dt, "percent_mask_use", "value")
site_dt[, source := "Website"]

dt <- rbind(gh_dt, site_dt, fill = T)


## Save 
dt[, max_date := max(date)]
dt <- dt[source %in% c("Github - Always", "Website")]
write.csv(dt[source %in% c("Github - Always", "Website")], paste0(output_dir, "/yougov_always.csv"), row.names=F)


# Crosswalk
dt[, count := value * N]
gitagg <- aggregate(cbind(count, N) ~ location_name + source, data=dt[source=="Github - Always"], function(x) sum(x))
gitagg$value_git <- gitagg$count / gitagg$N
webagg <- aggregate(value ~ location_name, data=dt[source=="Website"], function(x) mean(x))

## Weekly aggregates
dt[, week := format(date, "%W")]
week_dt <- copy(dt)
week_df <- aggregate(cbind(count, N) ~ location_name + location_id + week, data=dt[source=="Github - Always"], function(x) sum(x))
week_df$value <- week_df$count / week_df$N
#week_df$date <- as.Date(week_df$week, "%W")


days <- as.Date(paste(2020, 1, 1, sep = "-")) + 0:365
week_df$date <- as.Date(unlist(lapply(as.numeric(week_df$week), function(x) range(days[sprintf("%d %02d", 2020, x) == format(days, "%Y %U")])[2])), origin = "1970-01-01")
week_df$source <- "Week Binned GitHub Always"

## Find average relationship 
crosswalk <- data.table(merge(week_df, dt[source=="Website"], by=c("location_name","week")))
crosswalk[, ratio := value.x / value.y]

country_crosswalk <- aggregate(ratio ~ location_id.x, data=crosswalk, function(x) median(x))
setnames(country_crosswalk, "location_id.x", "location_id")

ggplot(crosswalk, aes(x=week, y=ratio, col=location_name, group=location_name)) + geom_line() +
  facet_wrap(~location_name) + ylab("Weekly ratio") + guides(col=F) + theme_minimal() +
  geom_hline(yintercept = 1, lty=2)

website <- merge(dt[source=="Website"], country_crosswalk, by="location_id")
website$value <- website$ratio * website$value
website$source <- "Adjusted Website"

# 'impute' the GitHub data on the dates
# when we have the Website data so as to avoid compositional bias. 
# Do that by the exact same logic. 
dt <- rbind(dt, week_df, fill = T)
dt <- rbind(dt, website, fill=T)
dt[, date := as.Date(date, "%d.%m.%Y")]


## Save
write.csv(dt[source %in% c("Github - Always", "Website", "Adjusted Website","Week Binned GitHub Always")], paste0(output_dir, "/yougov_always.csv"), row.names=F)

## Compare with current estimates ##
# current_dt <- fread(current_path)
# setnames(current_dt, "mask_use", "value")
# current_dt[, date := as.Date(date)][, observed := NULL]
# current_dt[, source := "Current estimates"]
# current_dt <- current_dt[date <= Sys.Date() & location_id %in% unique(dt$location_id)]
# dt <- rbind(dt, current_dt, fill = T)

plot_yougov <- function(dt, loc) {
  plot_dt <- dt[location_id == loc & !is.na(value)]
  gg <- ggplot(plot_dt, aes(x = as.Date(date), y = value, color = source)) + 
    geom_point(data=plot_dt[source %in% c("Website","Adjusted Website")], pch = 15, alpha = 0.5) +
    geom_point(data=plot_dt[!(source %in% c("Github - Always"))], aes(size = N), alpha = 0.5) +
    ggtitle(unique(plot_dt$location_name)) +
    xlim(min(plot_dt$date), max(dt$date)) +
    ylim(0:1) +
    theme_bw() +
    theme(legend.position = "bottom")
  print(gg)
}

sorted_locs <- ihme.covid::sort_hierarchy(hierarchy)[location_id %in% unique(dt$location_id)]$location_id

pdf(plot_path, width = 10, height = 8)
invisible(pblapply(sorted_locs, plot_yougov, dt = dt))
dev.off()
message(plot_path)
