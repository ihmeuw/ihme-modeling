#########################################################
## Best Scenario Mask Use
# Lauren Woyczynski, Chris Troeger
#########################################################
# Code for updating to 85%
# Copy best model folder to new location
# system("cp /ihme/covid-19-2/mask-use-outputs/2020_08_19.02/ /ihme/covid-19-2/mask-use-outputs/2020_08_20.01/ -r")

#read in current best
source(file.path("FILEPATH/get_location_metadata.R"))
library(data.table)
library(ggplot2)

# lsvid <- 746 # Set in parent script
# tmpinstall <- system("mktemp -d --tmpdir=/tmp", intern = TRUE)
# .libPaths(c(tmpinstall, .libPaths()))
# devtools::install_github("ihmeuw/ihme.covid", upgrade = "never", quiet = T, ref = "get_latest_output_dir")

today <- Sys.Date()

hierarchy <- get_location_metadata(location_set_id = 115, location_set_version_id = lsvid, release_id = 9)
hier_supp <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)
hierarchy[, c("super_region_id", "super_region_name", "region_id", "region_name") := NULL]
hier_supp[, merge_id := location_id]
hierarchy[location_id %in% hier_supp$location_id, merge_id := location_id]
hierarchy[is.na(merge_id), merge_id := parent_id]
hier_supp <- unique(hier_supp[, .(merge_id, super_region_id, super_region_name, region_id, region_name)])
hier_full <- merge(hierarchy, hier_supp, by = c("merge_id"), all.x = T)

#df <- fread('/ihme/covid-19/mask-use-outputs/best/mask_use.csv')
df <- fread(paste0(output_dir, '/mask_use.csv'))
df[,date := as.Date(date)]

# Increase mask use up to "universal coverage" in 7 days
  df_increase <- df[date %in% seq(today, today + 7, by = '1 day'), 
                    .(mask_today = head(mask_use,1), date = date, mask_use = mask_use, seq = 7:0, observed = observed), 
                    by = .(location_id, location_name)]
  
  df_increase[, mask_best := mask_today + (universal_level - mask_today)*((7-seq)/7)]
  
  df[, mask_best := mask_use]
  df[date > as.Date(today + 7), mask_best := universal_level]
# If mask use is above the set universal coverage, leave as that level
  df[, mask_best := ifelse(mask_use > universal_level, mask_use, mask_best)]
  df <- rbind(df[date < today | date > as.Date(today + 7), c("location_id","location_name","date","mask_use",
                                                             "observed","mask_best")], 
              df_increase[,c("location_id","location_name","mask_use","date",
                             "observed","mask_best"), with = F])
  df <- df[order(location_id, date)]

  
#write.csv(df, '/ihme/covid-19/mask-use-outputs/best/mask_use_best.csv', row.names = F)
write.csv(df, paste0(output_dir, '/mask_use_best.csv'), row.names = F)


#sorted <- ihme.covid::sort_hierarchy(hierarchy[location_id %in% unique(df$location_id)])
#
#pdf(paste0(output_dir, '/mask_use_best.pdf'), width = 11, height = 8.5)
#for(loc in unique(sorted$location_id)) {
#  gg <- ggplot() + 
#    geom_point(data = df[location_id == loc], aes(x = date, y = mask_best * 100), color = "green") + 
#    geom_point(data = df[location_id == loc], aes(x = date, y = mask_use * 100)) + 
#    xlab("Date") + ylab("Percent always mask use") +
#    facet_wrap(~ location_name) + ylim(c(0, 100)) +
#    theme_bw()
#  print(gg)
#}
#dev.off()


