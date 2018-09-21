######################################################################
## Title: Calculate ARC
## Project: SDG's
## Purpose: Check to see if calculations are done correctly
#######################################################################


if (Sys.info()[1] == "Linux"){
  j <- "/home/j/"
  h <- paste0("/homes/", Sys.info()[6], "/")
}else if (Sys.info()[1] == "Windows"){
  j <- "J:/"
  h <- "H:/"
}else if (Sys.info()[1] == "Darwin"){
  j <- "/Volumes/snfs/"
  h <- paste0("/Volumes/", Sys.info()[6], "/")
}

library(data.table)

get.indic.table <- function() {
  # get the indicator table
  indic_table <- fread(paste0(FLEPATH))
  return(indic_table)
}

read.dt <- function(sdg_version, draws, indic_table, max_draw=999) {
  # read in a data table with values to scale
  if (!draws) {
    path = paste0(FILEPATH)
    dt0 <- fread(path)
    it <- subset(indic_table, select = c("indicator_id", "indicator_target", "scale", "invert"))
    dt0 <- merge(dt0, it, by=c("indicator_id"), all.x = TRUE)
    
    cols <- c(c("indicator_id", "location_id", "year_id", "indicator_target", "scale", "indvert"), c("mean_val", "upper", "lower"))
    dt0 <- subset(dt0, select=cols)
  } else {
    path = paste0(FILEPATH)
    dt0 <- fread(path)
    it <- subset(indic_table, select = c("indicator_id", "indicator_target"))
    dt0 <- merge(dt0, it, by=c("indicator_id"), all.x = TRUE)
    
    cols <- c(c("indicator_id", "location_id", "year_id", "indicator_target", "scale", "invert"), paste0("draw_", 0:max_draw))
    dt0 <- subset(dt0, select=cols)
  }
  
  return(dt0)
}


draws = TRUE
sdg_version = 17
uhc_version = "2017_07_27"
max_draw = 999
indic_table <- get.indic.table()
dt <- read.dt(sdg_version, draws, indic_table, max_draw=max_draw)
uhc <- get.uhc(uhc_version, draws, max_draw=max_draw)
dt <- dt[year_id %in% c(2015, 2016, 2020, 2030)]
draws <- paste0("draw_", 0:999)
dt <- melt(dt, id.vars = c("indicator_id", "location_id", "year_id"), measure.vars = draws, variable.name = "draw")

##Set up ARC calculations
arc_info <- fread(paste0(FILEPATH))
dt <- merge(dt, arc_info, by = "indicator_id")
dt <- dt[!is.na(target_value)]
arc <- dcast(dt, indicator_id + location_id + draw + indicator_short + target_year + target_value + target_type + indicator_unit ~ year_id, value.var = "value") ##get years wide
setnames(arc, "2015", "value_2015")
setnames(arc, "2016", "value_2016")
setnames(arc, "2020", "value_2020")
setnames(arc, "2030", "value_2030")
arc <- arc[!indicator_short == "Death Reg"] ##drop death registry

##Observed ARC
arc[target_year == 2030, arc_obs := log(value_2030/value_2016)/(2030-2016)]
arc[target_year == 2020, arc_obs := log(value_2020/value_2016)/(2020-2016)]

##Target ARC ("Required ARC")
arc[indicator_unit == "0-100%", target_value := target_value/100] ##get onto 0-1 scale
arc[indicator_unit %in% c("per 1,000 livebirths", "per 1,000 people"), target_value := target_value/1000] ##get targets on correct scale
arc[indicator_unit %in% c("per 100,000 livebirths", "per 100,000 people") & !indicator_short == "MMR" & !target_type == "relative", target_value := target_value/100000] ##targets on correct scale (except MMR and relative targets)
arc[target_type == "absolute", target := target_value]
arc[target_type == "relative", target := value_2015*target_value/-100] ##create targets for relative reduction
arc[target_year == 2030, arc_target := log(target/value_2016)/(2030-2016)]
arc[target_year == 2020, arc_target := log(target/value_2016)/(2020-2016)]

##Get Mean, Upper and Lower Across Draws and Final Formatting
arc[, mean_arc_obs := mean(arc_obs), by = c("indicator_id", "location_id")]
arc[, lower_arc_obs := quantile(arc_obs, 0.025), by = c("indicator_id", "location_id")]
arc[, upper_arc_obs := quantile(arc_obs, 0.975), by = c("indicator_id", "location_id")]
arc[, mean_arc_target := mean(arc_target), by = c("indicator_id", "location_id")]
arc[, lower_arc_target := quantile(arc_target, 0.025), by = c("indicator_id", "location_id")]
arc[, upper_arc_target := quantile(arc_target, 0.975), by = c("indicator_id", "location_id")]
final <- unique(arc, by = c("indicator_id", "location_id"))
final <- final[, .(indicator_id, location_id, indicator_short, mean_arc_obs, 
                   lower_arc_obs, upper_arc_obs, mean_arc_target, lower_arc_target, upper_arc_target)]

write.csv(final, paste0(FILEPATH), row.names = F)
