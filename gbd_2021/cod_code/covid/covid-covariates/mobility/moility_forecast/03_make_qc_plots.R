## purpose: Plotting mobility forecasts    
   
################
## Setting up ##
################

# load libraries
library(dplyr)
library(data.table)
library(tidyr)
library(ggplot2)
library(RColorBrewer)
library(yaml)
library('ihme.covid', lib.loc = 'FILEPATH')
R.utils::sourceDirectory('functions', modifiedOnly = FALSE)

WORK_ROOT <-  "FILEPATH"
version <- basename(ihme.covid::get_latest_output_dir(file.path(WORK_ROOT)))
work_dir <- file.path(WORK_ROOT, version)

end_date <- Sys.Date() + 365 # End date for comparison plot. Currently set to plot one year into the future.

if (interactive()) {
  sd_pred_version <- 'latest'  
  # Add runs versions here that you would like to include in compare plot
  # you can use "best" and "last" and they will be labeled with actual versions in the plot
  # if you do not want to run any comparisons DO NOT COMMENT THIS - instead just delete all lines except "NULL)"
  prev_run_vers <- paste0(WORK_ROOT, c(
    "best",
    # this NULL makes it so you can end the previous line with a "," without any sort of error or bug
    NULL)) # DO NOT DELETE THIS LINE
} else {
  parser <- argparse::ArgumentParser()
  parser$add_argument("--sd_pred_version", type="character", help="Version of mandate forecasts to inform the mobility projections; defaults to the most recent (ie jobmon's) folder")
  parser$add_argument("--prev_run_vers", type="character", help="Covariate version to plot against")

  args <- parser$parse_args(get_args())
  for (key in names(args)) {
    assign(key, args[[key]])
  }
  
  print("Arguments passed were:")
  for (arg in names(args)){
    print(paste0(arg, ": ", get(arg)))
  }

  # Modify args
  if (sd_pred_version == "jobmon") {
    sd_pred_version <- basename(ihme.covid::get_latest_output_dir(file.path(WORK_ROOT, "mandates")))
  }
  prev_run_vers <- strsplit(prev_run_vers, ',')[[1]]
  prev_run_vers <- paste0(WORK_ROOT, prev_run_vers)

}


#################
## Diagnostics ##
#################
ihme.covid::print_debug(work_dir, prev_run_vers)


# Define file paths
SMOOTHED_MOBILITY_FORECAST_PATH <- file.path(work_dir, paste0('mobility_forecast_full.csv'))
SOCIAL_DISTANCING_PATH <- file.path(work_dir, 'social_distancing.csv')
MRBRT_SOCIAL_DISTANCING_RE_PATH <- file.path(work_dir, 'mobility_mandate_coefficients.csv')
metadata_path <- file.path(work_dir, "03_make_qc_plots.metadata.yaml")
REIMP_DATES_PATH <- file.path(WORK_ROOT, "mandates/",sd_pred_version,"/reimplementation_dates.csv")
MEAN_SD_EFFECT_SIZE_PLOT_PATH <- file.path(work_dir, 'mean_sd_effect_size.pdf')
COMPARE_PLOT_PATH <- file.path(work_dir, sprintf("compare_%s.pdf", paste(basename(prev_run_vers), collapse = "_")))

####################################
## Read in the mobility forecasts ##
####################################

# read in the forecast and the sd dataset
df_smooth_w_effs_pred <- fread(SMOOTHED_MOBILITY_FORECAST_PATH)
locs <- unique(df_smooth_w_effs_pred[, .(location_id)])
fwrite(locs, paste0(work_dir, "/smoothed_mobility_jobs.csv")) 

# Write a settings file with other arguments needed for PDF array job 
settings = data.table(work_dir = work_dir, 
                      prev_run_vers = prev_run_vers,
                      mrbrt_social_distancing_re_path = MRBRT_SOCIAL_DISTANCING_RE_PATH,
                      social_distancing_path = SOCIAL_DISTANCING_PATH,
                      reimp_dates_path = REIMP_DATES_PATH, 
                      version = version)
# Need to write this file to a central location, not attached to "work_dir". 
fwrite(settings, paste0("FILEPATH/", Sys.info()[['user']], "/smoothed_mobility_settings.csv")) 
fwrite(settings, paste0(work_dir, "/smoothed_mobility_settings.csv")) # Save a copy for metadata

#######################
## Make the QC plots ##
#######################

##############################
## Plot the mean SD effects ##
##############################
effs_locname <- fread(MRBRT_SOCIAL_DISTANCING_RE_PATH)
effs <- effs_locname[, .(sd1_eff, sd2_eff, sd3_eff, psd1_eff, psd3_eff)]
mean_effs <-  colMeans(effs)
mean_effs['all'] <- sum(mean_effs)
mean_effs['sd1_eff'] <- mean_effs['sd1_eff'] + mean_effs['psd1_eff']
mean_effs['sd3_eff'] <- mean_effs['sd3_eff'] + mean_effs['psd3_eff']

plot_row_names <- c("Stay at home", "School closure", "Non-essential business closure", 
                    "Any Restrict groups", "Any business closure","All measures")
colors <- brewer.pal(10,"Paired")
pdf(MEAN_SD_EFFECT_SIZE_PLOT_PATH, height = 10, width = 12)
bplot <- barplot(mean_effs, main="% reduction in mobility by SD measure", 
                 names.arg=plot_row_names, 
                 ylim=c(-70,0),cex.names=0.8, col = colors[1:5])

text(x = bplot, y = mean_effs, label = round(mean_effs, digits = 1), pos = 3, cex = 0.8, col = "black")

sd_map_dt <-
  data.table(
    name_short = c("sd1", "sd2", "sd3", "psd1", "psd3", "anticipate"),
    name_long = c(
      "Stay at home",
      "School closure",
      "Non-essential business closure",
      "Any Restrict groups",
      "Any business closure",
      "Anticipation"
    )
  )

# Scatter location-specific social distancing from 2 versions
for(compare_version in prev_run_vers){
  
  effs_locname_v2 <- fread(file.path(compare_version, "mobility_mandate_coefficients.csv"))
  
  scatter <- merge(effs_locname_v2, effs_locname, by="location_id")
  
  for(this_eff in sd_map_dt$name_short){
    
    gg <- ggplot(scatter, aes(x=get(paste0(this_eff, "_eff.x")), y=get(paste0(this_eff, "_eff.y"))))+
      geom_point()+
      geom_abline(slope = 1, intercept = 0)+
      theme_bw()+
      labs(title = sd_map_dt[name_short == this_eff, name_long],
           x = compare_version,
           y = version)
    
    print(gg)
  }
}


dev.off()

# #################################
# ## Comparing two runs' results ##
# #################################
#TODO - add labels for mandate lines
df_sd <- fread(SOCIAL_DISTANCING_PATH)

# construct a data table that contains dates for all mandate activity
mandate_list <- c('sd1_lift', 'sd2_lift', 'sd3_lift', 'psd1_lift', 'psd3_lift')
mandate_names <- c('stay_home', 'educational_fac', 'all_noness_business', 'any_gathering_restrict', 'any_business')

df_reimp <- NULL
for (i in 1:5){
  mandate <- mandate_list[i]
  mandate_name <- mandate_names[i]
  
  temp <- copy(df_sd)
  temp[, activity := sequence(rle(as.character(get(mandate)))$lengths), by=location_id]
  # subset to dates where the mandate was either imposed or lifted
  temp <- temp[activity==1 & date!='2020-01-01']
  # distinguish between dates of imposition vs. lifting
  temp[, type := ifelse(get(mandate)==1, 'start_date', 'stop_date')]
  temp$intervention_measure_key <- mandate_name
  setnames(temp, 'date', 'value')
  temp <- temp[, .(location_id, intervention_measure_key, type, value)]
  df_reimp <- rbind(df_reimp, temp)
}

reimp_map_dt <-
  data.table(
    intervention_measure_key = rep(c("stay_home", "educational_fac","all_noness_business","any_gathering_restrict","any_business"),2),
    type = c(rep('start_date',5), rep('stop_date',5)),
    line_label = c('1a.stay home',
                   "2a.school closure",
                   '3a.business closure',
                   '4a.restricted gatherings',
                   '5a.partial business closure',
                   '1b.lifting stay home',
                   '2b.lifting school closure',
                   '3b.lifting business closure',
                   '4b.lifting restricted gatherings',
                   '5b.lifting partial business closure'),
    line_type = rep(1:5,2),
    line_color = c(1,3,5,7,9,2,4,6,8,10)
  )

df_reimp <- merge(df_reimp, reimp_map_dt, by=c('intervention_measure_key', 'type'))
df_reimp <- df_reimp[order(location_id, value)]
df_reimp <- df_reimp[,.(location_id, intervention_measure_key, type, value, line_label, line_type, line_color)]

# the function to plot mobility from multiple runs
compare_runs <- function(run_vers, plot_path, df_sd){
  
  # first create a dataset containing the mobility time series for all the versions that will be compared
  mob_run_all <- NULL
  
  for(run in run_vers){
    mob_run <- fread(file.path(run, "mobility_forecast_smooth_metric_lift.csv"))
    
    setnames(mob_run, "mobility_forecast", "mobility")
    if (basename(run) %in% c("latest", "best")) {
      resolved <- basename(normalizePath(file.path(run)))
      mob_run[, version := sprintf("%s (%s)", resolved, basename(run))]
    } else {
      resolved <- basename(file.path(run))
      mob_run[, version := resolved]
    }
    mob_run_all <- rbindlist(list(mob_run_all, mob_run), use.names = T, fill = T)
  }
  mob_run_all[, date := as.Date(date)]
  
  
  # plot all versions on the same time series
  colors <- brewer.pal(10,"Paired")
  locs <- mob_run_all[, location_id] %>% unique
  
  pdf(plot_path, width = 11)
  for(loc in locs){
    loc_reimp <- df_reimp[location_id==loc]
    current_ver <- version
    
    p <- ggplot(mob_run_all[location_id==loc & date <= end_date,], aes(x=date, y=mobility, group=version)) + 
      geom_line(aes(color= version)) +
      # vertical line indicating last day with observed mobility data
      geom_vline(xintercept = mob_run_all[location_id==loc & !is.na(mean) & version == current_ver, max(date)], linetype=8, color = "grey20", size=0.8, show.legend=T) +
      #xlim(as.Date('2021-12-01'),as.Date('2022-03-01')) +
      theme_bw() +
      xlab(unique(mob_run_all[location_id==loc, location_name])) #+
      #scale_size_manual(name="SD mandates", values=rep(1,10), guide=guide_legend(override.aes = list(colour=colors)))
    
    #add vertical lines to denote mandates
    if(nrow(loc_reimp)>0){
      for (r in 1:nrow(loc_reimp)){
        gg.data <- loc_reimp[r]
        label <- gg.data$line_label
        type  <- gg.data$line_type
        color <- gg.data$line_color
        p <- p + geom_vline(gg.data, mapping = aes(xintercept = as.Date(value)), linetype=type, colour = colors[color])
      }
    }
    
    tryCatch({
      print(p)
    }, error=function(e){cat(loc, ": ",conditionMessage(e), "\n")})
    
  }
  dev.off()
  
}

if (length(prev_run_vers) > 0) {
  # Plot comparison between this run and previous mobility covariate runs
  compare_runs(run_vers = c(work_dir, prev_run_vers), COMPARE_PLOT_PATH, df_sd)
}

yaml::write_yaml(
  list(
    script = "03_make_qc_plots.R",
    output_dir = work_dir,
    input_files = ihme.covid::get.input.files()
  ),
  file = metadata_path
)
