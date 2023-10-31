###########################################################
## Summary of Mask data for technical report! ##
###########################################################
library(data.table)
library(ggplot2)
library(scales)

mask_version <- "best"

source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")
locs <- read.csv('FILEPATH/ihme_loc_metadata_2019.csv')
## Tables (merge together a covid and gbd hierarchy to get needed information for aggregation)
hierarchy <- get_location_metadata(location_set_id = 111, location_set_version_id = 727)
hier_supp <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)
hierarchy[, c("super_region_id", "super_region_name", "region_id", "region_name") := NULL]
hier_supp[, merge_id := location_id]
hierarchy[location_id %in% hier_supp$location_id, merge_id := location_id]
hierarchy[is.na(merge_id), merge_id := parent_id]
hier_supp <- unique(hier_supp[, .(merge_id, super_region_id, super_region_name, region_id, region_name)])
locs <- merge(hierarchy, hier_supp, by = c("merge_id"), all.x = T)

pop <- fread("FILEPATH/age_pop.csv")
pop <- pop[, lapply(.SD, sum, na.rm = T), by = .(location_id), .SDcols = c("population")]

cur_best <- fread(paste0("FILEPATH",mask_version,"/mask_use.csv"))
  cur_best$date <- as.Date(cur_best$date)
cur_best <- merge(cur_best, locs[,c("location_id","parent_id","level","most_detailed","region_name","super_region_name")], by="location_id")
facebook <- read.csv("FILEPATH/mask_ts.csv")
premise <- read.csv(paste0("FILEPATH",mask_version,"/Weekly_Premise_Mask_Use.csv"))
yougov <- fread(paste0("FILEPATH",mask_version,"/yougov_always.csv"))
df <- read.csv(paste0("FILEPATH",mask_version,"/used_data.csv"))
df <- merge(df, locs[,c("location_id","level","region_name","super_region_name", "parent_id")], by="location_id")

head(df)
df$date <- as.Date(df$date)
aggregate(date ~ source, function(x) min(x), data=df)
aggregate(date ~ source, function(x) max(x), data=df)

length(unique(facebook$location_id))
length(unique(premise$loc_id))

length(unique(df$location_name[df$level==3]))
length(unique(df$location_name[df$level==3 & df$source=="Facebook"]))

sum(facebook$N)
sum(premise$question_count)

setdiff(unique(locs$region_name), unique(df$region_name))

sorted <- subset(locs, location_id %in% cur_best$location_id)
sorted <- sorted[order(sorted$path_to_top_parent),]
#sorted <- ihme.covid::sort_hierarchy(hierarchy[location_id %in% unique(cur_best$location_id)])

pdf(paste0("FILEPATH", mask_version, "/mask_use_summary_lines.pdf"), height=8, width=10)
for(loc in unique(sorted[level==3]$location_id)) {
  gg <- ggplot() +
    geom_line(data = cur_best[location_id == loc], aes(x = date, y = mask_use * 100, group=location_id), lwd=1.25) +
    scale_x_date("Date", limits = as.Date(c("2020-01-01","2021-01-01"))) +
    ylab("Percent always mask use") +
    facet_wrap(~ location_name) + ylim(c(0, 100)) +
    theme_bw()
  print(gg)
}
dev.off()

# National facets by region
for(loc in unique(hierarchy[level==2]$location_id)) {
  png(paste0("FILEPATH/",loc,"_lines.png"))
  gg <- ggplot() +
    geom_line(data = cur_best[parent_id == loc], aes(x = date, y = mask_use * 100, group=location_id), lwd=1.25) +
    scale_x_date("Date", limits = as.Date(c("2020-01-01","2021-01-01"))) +
    ylab("Percent always mask use") +
    theme_bw() + 
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) + 
    facet_wrap(~ location_name) + ylim(c(0, 100)) +
    ggtitle(hierarchy[location_id == loc]$location_name)
  print(gg)
  dev.off()
}


## Aggregate US (aggregation in Mask code is an average :facepalm:)
cur_best <- merge(cur_best, pop, by="location_id")
cur_best$number <- cur_best$population * cur_best$mask_use

today <- cur_best[date == "2020-08-10"]
most_detailed <- cur_best[most_detailed==1 & date == "2020-08-10"]
agg_pop <- most_detailed[, lapply(.SD, sum, na.rm = T), by = .(parent_id), .SDcols = c("number","population")]
agg_pop$mask_use <- agg_pop$number / agg_pop$population

region_agg <- most_detailed[, lapply(.SD, sum, na.rm = T), by = .(region_name), .SDcols = c("number","population")]
region_agg$mask_use <- region_agg$number / region_agg$population

## Time trends of mask use ##
most_detailed_ts <- cur_best[most_detailed==1]
global_trend <-  most_detailed_ts[, lapply(.SD, sum, na.rm = T), by = .(date), .SDcols = c("number","population")]
  global_trend$location_name <- "Global"
  global_trend$level <- 0
regional_trend <-  most_detailed_ts[, lapply(.SD, sum, na.rm = T), by = .(region_name, date), .SDcols = c("number","population")]
  setnames(regional_trend, "region_name","location_name")
  regional_trend$level <- 2
super_regional_trend <-  most_detailed_ts[, lapply(.SD, sum, na.rm = T), by = .(super_region_name, date), .SDcols = c("number","population")]
  setnames(super_regional_trend, "super_region_name","location_name")
  super_regional_trend$level <- 1

agg_trend <- rbind(global_trend, regional_trend, super_regional_trend)
agg_trend$mask_use <- agg_trend$number / agg_trend$population

ggplot(agg_trend[location_name=="Global"], aes(x=as.Date(date), y=mask_use)) + geom_line() +
  facet_wrap(~location_name) + theme_bw() + scale_x_date("Date", limits = as.Date(c("2020-02-01","2020-06-20"))) + 
  scale_y_continuous("Mask use", labels=percent)

sr_plot <- agg_trend[level < 2]
sr_plot$location <- reorder(sr_plot$location_name, sr_plot$level)
ggplot(sr_plot, aes(x=as.Date(date), y=mask_use)) + geom_line() +
  facet_wrap(~location) + theme_bw() + scale_x_date("Date", limits = as.Date(c("2020-04-01","2020-07-31"))) + 
  scale_y_continuous("Mask use", labels=percent)

## Wants overlay plot, different colors
cols <- c("black", brewer_pal(palette="Set1")(5),"pink","brown")
ggplot() + 
  geom_line(data = sr_plot[date <= "2020-07-07"], lty = 1, aes(x=as.Date(date), y=mask_use, col=location), lwd=1.25) + 
  geom_line(data=sr_plot[date > "2020-07-07"], lty=2, aes(x=as.Date(date), y=mask_use, col=location), lwd=1.25) + 
  theme_bw() + scale_x_date("", limits = as.Date(c("2020-03-01","2020-08-31")), breaks = "month", date_labels = "%b") + 
  scale_y_continuous("Mask use", labels=percent) +
  scale_color_manual("", values=cols) + theme(legend.position="bottom")


## Find differences in draws ##
diff_date <- as.Date("2021-01-01")
source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/collapse_combine_functions.R"))

# Is it better to specify versions or use the 'map'?
seir_meta <- fread("FILEPATH/seir_scenario_mapping_2020_07_29.csv")

scenarios_compare <- c(1,3)

seir_sum_cumulative <- fread(paste0("FILEPATH",seir_meta[scenario_id==1]$seir_version,"/stats/Cumulative_deaths_summary_",seir_meta[scenario_id==1]$seir_version,".csv"))
  setnames(seir_sum_cumulative, "deaths_mean","cuml_deaths")
hierarchy <- fread(paste0("FILEPATH", seir_meta[scenario_id==1]$seir_version,"/hierarchy.csv"))

df_draws_1 <- fread(paste0("FILEPATH", seir_meta[scenario_id==1]$seir_version,"/prepared_death_estimates.csv"))
df_draws_2 <- fread(paste0("FILEPATH", seir_meta[scenario_id==3]$seir_version,"/prepared_death_estimates.csv"))

# Yuck, I need to rbind US only results
#   df_us1 <- fread(paste0("/ihme/covid-19/hospitalizations/inputs/seir/2020_07_13.11/prepared_death_estimates.csv"))
#   df_us2 <- fread(paste0("/ihme/covid-19/hospitalizations/inputs/seir/2020_07_13.12/prepared_death_estimates.csv"))
#   us_locs <- hierarchy[parent_id %in% c(102, 570)]$location_id
# df_draws_1 <- rbind(df_draws_1[!(location_id %in% us_locs)], df_us1[location_id %in% us_locs])
# df_draws_2 <- rbind(df_draws_2[!(location_id %in% us_locs)], df_us2[location_id %in% us_locs])
# 
full_1 <- df_draws_1[date <= "2021-01-01"]
full_2 <- df_draws_2[date <= "2021-01-01"]

# Subset to future since past will all be the same.
# df_draws_1 <- subset(df_draws_1, observed == 0)
# df_draws_2 <- subset(df_draws_2, observed == 0)
df_draws_1 <- subset(df_draws_1, date==diff_date)
df_draws_2 <- subset(df_draws_2, date==diff_date)

# Modify original function to keep draws
collapse_draws_agg <- function(df_draws, hierarchy){
  # Will summarize draws and aggregates
  non_most_detailed <- hierarchy[most_detailed == 0]
  agg_locations <- rev(non_most_detailed[order(level)]$location_id)
  draw_cols <- grep('draw_', colnames(df_draws), value=T)
  
  # make agggregates
  for(agg_loc in agg_locations){
    
    children_ids <- hierarchy[parent_id==agg_loc & location_id!=agg_loc, location_id]
    child_draws <- df_draws[location_id %in% children_ids]
    if(length(setdiff(children_ids, child_draws$location_id)) > 0){
      warning(paste0("Subnats are missing for national aggregation: subnats ", paste0(setdiff(children_ids, child_draws$location_id), collapse=","), " are missing for national "), agg_loc)
    }
    
    max_obs_day <- min(child_draws[observed==TRUE, max(date), by="location_id"]$V1)  #min last observed date among all subnats
    child_draws <- child_draws[, lapply(.SD,sum), by=date,.SDcols=draw_cols]
    child_draws[, location_id:=agg_loc]
    child_draws[date > max_obs_day, observed:=0]
    child_draws[date <= max_obs_day, observed:=1]
    
    child_draws <- child_draws[order(location_id, date)]
    df_draws <- rbind(df_draws, child_draws, fill = T)
  }
  return(df_draws)
}

df_1 <- collapse_draws_agg(df_draws_1, hierarchy)
df_2 <- collapse_draws_agg(df_draws_2, hierarchy)

# So now I have two full sets of aggregated draws. I think that I need
# to lapply?
df <- rbind(df_1, df_2)
draw_cols <- grep('draw_', colnames(df), value=T)

df <- subset(df, date == diff_date)
locs <- unique(df$location_id)

# I think this is working? It takes forever. 
# For right now, just look at locations in the manuscript
# locs <- c(1, 102, 165, 141, 161)
df_diff <- rbindlist(lapply(locs, function(l){
  print(l)
  mid <- df[location_id == l]
  tmp <- mid[, lapply(.SD, diff), by=.(date, location_id), .SDcols=draw_cols]
  return(tmp)
}))
df_pct <- rbindlist(lapply(locs, function(l){
  print(l)
  mid1 <- data.frame(df_1[location_id == l])
  mid2 <- data.frame(df_2[location_id == l])
  d <- data.frame(location_id=l)
  for(i in 0:999){
    d[,paste0("draw_",i)] <- (mid2[,paste0("draw_",i)] - mid1[,paste0("draw_",i)]) / mid1[,paste0("draw_",i)]
  }
  return(d)
}))

df_diff$deaths_mean <- rowMeans(df_diff[, ..draw_cols]) * (-1)
df_diff$deaths_lower <- apply(df_diff[, ..draw_cols], MARGIN=1, FUN=function(x) quantile(x, 0.025, na.rm=T)) * (-1)
df_diff$deaths_upper <- apply(df_diff[, ..draw_cols], MARGIN=1, FUN=function(x) quantile(x, 0.975, na.rm=T)) * (-1)

df_pct <- data.table(df_pct)
df_pct$pct_mean <- rowMeans(df_pct[, ..draw_cols])
df_pct$pct_lower <- apply(df_pct[, ..draw_cols], MARGIN=1, FUN=function(x) quantile(x, 0.025, na.rm=T))
df_pct$pct_upper <- apply(df_pct[, ..draw_cols], MARGIN=1, FUN=function(x) quantile(x, 0.975, na.rm=T))

df_1$ref_mean <- rowMeans(df_1[, ..draw_cols])
df_1$ref_lower <- apply(df_1[, ..draw_cols], MARGIN=1, FUN=function(x) quantile(x, 0.025, na.rm=T))
df_1$ref_upper <- apply(df_1[, ..draw_cols], MARGIN=1, FUN=function(x) quantile(x, 0.975, na.rm=T))

df_2$mask_mean <- rowMeans(df_2[, ..draw_cols])
df_2$mask_lower <- apply(df_2[, ..draw_cols], MARGIN=1, FUN=function(x) quantile(x, 0.025, na.rm=T))
df_2$mask_upper <- apply(df_2[, ..draw_cols], MARGIN=1, FUN=function(x) quantile(x, 0.975, na.rm=T))

final_df <- copy(df_diff)
final_df[, (draw_cols):=NULL]

final_df <- merge(final_df, hierarchy[,c("location_id","level","sort_order","location_name","location_ascii_name")], by="location_id")
final_df <- merge(final_df, df_pct[,c("location_id","pct_mean","pct_lower","pct_upper")], by="location_id")
final_df <- merge(final_df, df_1[,c("location_id","ref_mean","ref_lower","ref_upper")], by="location_id")
final_df <- merge(final_df, df_2[,c("location_id","mask_mean","mask_lower","mask_upper")], by="location_id")

final_df <- final_df[order(sort_order)]
final_df$value <- paste0(round(final_df$deaths_mean,0), " (",round(final_df$deaths_upper,0)," to ",round(final_df$deaths_lower,0),")")
final_df$pct_value <- paste0(round(final_df$pct_mean*100,1), "% (",round(final_df$pct_lower*100,1)," to ",round(final_df$pct_upper*100,1),"%)")
final_df$reference_value <- paste0(round(final_df$ref_mean,0), " (",round(final_df$ref_lower,0)," to ",round(final_df$ref_upper,0),")")
final_df$mask_value <- paste0(round(final_df$mask_mean,0), " (",round(final_df$mask_lower,0)," to ",round(final_df$mask_upper,0),")")

#final_df <- merge(final_df, seir_sum_cumulative[,c("location_id","date","cuml_deaths")], by=c("location_id","date"))

# Tack on population
# pop <- get_population(age_group_id = 22, year_id = 2019, location_id = unique(final_df$location_id), gbd_round_id=6, decomp_step="step4", sex_id = 3)
final_df <- merge(final_df, pop[,c("location_id","population")], by="location_id", all.x=T)

write.csv(final_df, "FILEPATH/mask_use_deaths_averted_revision.csv", row.names = F)

final_df[date=="2021-01-01"]

## Find the global values ##
# Stupid hybrid model requires me to recalculate these:
agg_mod1 <- summarize_draws_agg(full_1, hierarchy)
agg_mod2 <- summarize_draws_agg(full_2, hierarchy)

srs_locs <- hierarchy[level <= 1]$location_id
global_deaths <- data.table()
# for(s in c(1,3)){
#   seir_version <- seir_meta[scenario_id==s, seir_version]
#   seir_sum_cumulative <- fread(paste0("/ihme/covid-19/hospitalizations/inputs/seir/",seir_version,"/stats/Cumulative_deaths_summary_",seir_version,".csv"))
#   seir_sum_cumulative <- seir_sum_cumulative[location_id %in% srs_locs]
#   seir_sum_cumulative$version <- seir_version
#   global_deaths <- rbind(global_deaths, seir_sum_cumulative)
# }
labels <- c("Reference","Universal mask use")
agg_mod1$version <- "Reference"
agg_mod2$version <- "Universal mask use"
global_deaths <- rbind(agg_mod1, agg_mod2)
global_deaths <- global_deaths[location_id %in% srs_locs]

global_deaths <- merge(global_deaths, hierarchy[,c("location_id","location_name")], by="location_id")
global_deaths$location <- reorder(global_deaths$location_name, global_deaths$location_id)
ggplot(global_deaths, aes(x=as.Date(date), y=deaths_mean)) + geom_line(aes(col=version)) + 
  geom_ribbon(aes(ymin=deaths_lower, ymax=deaths_upper, fill=version), alpha=0.2) + 
  theme_minimal() + scale_y_continuous("Deaths", labels=comma) + 
  scale_x_date("Date") + guides(fill=F) + scale_color_discrete("Model version", labels=c("Reference","Universal mask use")) +
  facet_wrap(~location, scales="free", ncol = 2) + theme(legend.position = "bottom")

## What proportion of the world's population does this cover? ##
model_locs <- unique(final_df[level == 3]$location_id)
gbd_locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)
global_locs <- gbd_locs[level == 3]$location_id

pops <- get_population(location_id = global_locs, age_group_id = 22, year_id = 2019, gbd_round_id = 6, decomp_step = "step4")
model_locs <- subset(pops, location_id %in% model_locs)
global_locs <- subset(pops, location_id %in% global_locs)
fraction_pop <- sum(model_locs$population) / sum(global_locs$population)
fraction_pop



