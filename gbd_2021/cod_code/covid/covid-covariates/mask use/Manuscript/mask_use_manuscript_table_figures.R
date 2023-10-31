###########################################################
## Summary of Mask data for manuscript! ##
###########################################################
library(data.table)
library(ggplot2)
library(scales)
source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/collapse_combine_functions.R"))
source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")

## Set these!
  mask_version <- "best"
  #seir_meta <- fread("/ihme/covid-19/hospitalizations/inputs/seir_scenarios/seir_scenario_mapping_2020_08_27.csv") #'2020_08_27.03'
  seir_version <- "2020_10_28.02"
  seir_meta <- data.table(scenario_id = seir_version, seir_version = c("reference","best_masks"))
  
  diff_date <- as.Date("2021-01-01") # When will results be aggregated for?
  data_date <- as.Date("2020-10-19") # Last day of data
  
  scenarios_compare <- c(1,3)

## Pull in information (pop, location meta)
  ## Tables (merge together a covid and gbd hierarchy to get needed information for aggregation)
  hierarchy <- get_location_metadata(location_set_id = 115, location_set_version_id = 746)
  hier_supp <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)
  hierarchy[, c("super_region_id", "super_region_name", "region_id", "region_name") := NULL]
  hier_supp[, merge_id := location_id]
  hierarchy[location_id %in% hier_supp$location_id, merge_id := location_id]
  hierarchy[is.na(merge_id), merge_id := parent_id]
  hier_supp <- unique(hier_supp[, .(merge_id, super_region_id, super_region_name, region_id, region_name)])
  locs <- merge(hierarchy, hier_supp, by = c("merge_id"), all.x = T)
  
  pop <- fread("FILEPATH/age_pop.csv") # Is there a reason for this version?
  pop <- pop[, lapply(.SD, sum, na.rm = T), by = .(location_id), .SDcols = c("population")]

## Read in mandates
  mandates <- fread("FILEPATH/mask_use_estimates_mandates.csv")
  mandates <- mandates[mandate == 1]
  mandates <- mandates[, min_date := min(date), by="location_id"]
  mandates <- mandates[date == min_date]
  mandates[, date := as.Date(date)]

## Pull in the mask use data / estimates
  cur_best <- fread(paste0("FILEPATH",mask_version,"/mask_use.csv"))
  cur_best$date <- as.Date(cur_best$date)
  cur_best <- merge(cur_best, locs[,c("location_id","parent_id","level","most_detailed","region_name","super_region_name")], by="location_id")
  
  cur_best <- merge(cur_best, mandates[,c("date","location_id","mandate","any_penalty")], by= c("date","location_id"), all.x = T)
  cur_best[, mandate := ifelse(date == "2020-01-01", 0, mandate)]
  cur_best[, mandate := na.locf(mandate), by = "location_id"]
  cur_best[, mandate := ifelse(location_id %in% c(101, 102, 163, 71), 0, mandate)]
  cur_best <- merge(cur_best, df[,c("date","location_id","prop_always","N")], by=c("date","location_id"), all.x=T)
    
  facebook <- read.csv("FILEPATH/mask_ts.csv")
  facebook_us <- read.csv("FILEPATH/mask5days_ts.csv")
  premise <- read.csv(paste0("FILEPATH",mask_version,"/Weekly_Premise_Mask_Use.csv"))
  yougov <- fread(paste0("FILEPATH",mask_version,"/yougov_always.csv"))
  df <- fread(paste0("FILEPATH",mask_version,"/used_data.csv"))
  df <- merge(df, locs[,c("location_id","level","region_name","super_region_name", "parent_id")], by="location_id")
  
  pbest <- df[source == "Adjusted Premise"]
  pbest <- pbest[date == "2020-10-27"]
  pbest[, count := N * prop_always]
  us_premise <- pbest[, lapply(.SD, function(x) sum(x)), .SDcols = c("N","count")]
  us_premise[, prop := count / N]
  
  head(df)
  df$date <- as.Date(df$date)
  aggregate(date ~ source, function(x) min(x), data=df)
  aggregate(date ~ source, function(x) max(x), data=df)

  sum(facebook_us$N)
  table(df$source)
  
  tmp_fb <- df[source %like% "Facebook"]
  tmp_nfb <- df[!(source %like% "Facebook") & source != "KFF" & location_id %in% unique(tmp_fb$location_id)]
  unique(tmp_nfb$location_name)
  
  length(unique(facebook$location_id))
  length(unique(premise$loc_id))
  
  length(unique(df$location_name[df$level==3]))
  length(unique(df$location_name[df$level==3 & df$source=="Facebook"]))
  
  sum(facebook$N)
  fb_mean_n <- aggregate(N ~ date, data=facebook, function(x) sum(x))
  mean(fb_mean_n$N)
  fb_n_country <- aggregate(N ~ location_name, data=facebook, function(x) sum(x))
  range(fb_n_country$N)
  
  sum(yougov$N, na.rm=T)
  sum(premise$question_count)
  
## Find the mask use on last data date
  mask_est <- cur_best[date == data_date]
  mask_est <- mask_est[order(mask_use)]
  head(mask_est[level == 3],10)
  tail(mask_est[level == 3])

  
## Plot Poland for Kelsey P.
  pol <- merge(cur_best, df[,c("location_id","date","source")], by = c("location_id","date"), all = T)
  #pol <- cur_best[location_id == 51]
  pdf("FILEPATH/poland_mask_ts.pdf", height=5, width = 6)
    ggplot() +
      scale_x_date("", limits = as.Date(c("2020-07-01","2020-12-01")), date_labels = "%b") +
      ylab("Percent always mask use") +
      geom_point(data = pol[location_id==51 & source != "KFF"], aes(x = date, y = prop_always * 100), alpha = 0.4, size = 3) + 
      # geom_line(data = pol[location_id == 51], aes(x = date, y = mask_use * 100, group=location_id), lwd=1.25, col = "red") +
      theme_minimal(base_size = 15) + 
      ylim(c(0, 100)) + 
      geom_vline(xintercept = as.Date("2020-10-10"), lty = 2) + 
      ggtitle("Mask use in Poland")
    ggplot() +
      scale_x_date("", limits = as.Date(c("2020-07-01","2020-12-01")), date_labels = "%b") +
      ylab("Percent always mask use") +
      geom_point(data = pol[location_id==51 & source != "KFF"], aes(x = date, y = prop_always * 100), alpha = 0.4, size = 3) + 
      geom_line(data = pol[location_id == 51], aes(x = date, y = mask_use * 100, group=location_id), lwd=1.25, col = "red") +
      theme_minimal(base_size = 15) + 
      ylim(c(0, 100)) + 
      geom_vline(xintercept = as.Date("2020-10-10"), lty = 2) + 
      ggtitle("Mask use in Poland")
    
  dev.off()
  
## Plots of Mask Use over time
  setdiff(unique(locs$region_name), unique(df$region_name))
  
  sorted <- subset(locs, location_id %in% cur_best$location_id)
  sorted <- sorted[order(sorted$path_to_top_parent),]
  #sorted <- ihme.covid::sort_hierarchy(hierarchy[location_id %in% unique(cur_best$location_id)])
  
  pdf(paste0("FILEPATH", mask_version, "/mask_use_summary_lines.pdf"), height=8, width=10)
  for(loc in unique(sorted[level==3]$location_id)) {
    gg <- ggplot() +
      geom_line(data = cur_best[location_id == loc], aes(x = date, y = mask_use * 100, group=location_id, col = factor(mandate)), lwd=1.25) +
      scale_x_date("Date", limits = as.Date(c("2020-01-01","2021-01-01"))) +
      scale_color_manual("", limits = c(0,1), labels = c("No mandate", "Active mandate"), values = c("black","purple")) + 
      ylab("Percent always mask use") +
      facet_wrap(~ location_name) + ylim(c(0, 100)) +
      theme_bw()
    print(gg)
  }
  dev.off()
  
  # National facets by region
  pdf("FILEPATH/mask_region_pdfversion.pdf", height = 8, width = 12)
  for(loc in unique(hierarchy[level==2]$location_id)) {
    #png(paste0("/home/j/temp/ctroeger/COVID19/Mask Use Paper/mask_line_png/",loc,"_lines.png"), height = 200, width = 400)
    gg <- ggplot() +
      scale_x_date("Date", limits = as.Date(c("2020-01-01","2021-01-01"))) +
      ylab("Percent always mask use") +
      geom_point(data = cur_best[parent_id == loc & N > 10], aes(x = date, y = prop_always * 100, col = factor(mandate)), alpha = 0.4, size = 2) + 
      geom_line(data = cur_best[parent_id == loc], aes(x = date, y = mask_use * 100, group=location_id), lwd=1.25) +
      scale_color_manual("", limits = c(0,1), labels = c("No mandate", "Active mandate"), values = c("black","purple"),
                         guide = guide_legend(override.aes = list(size = 3, alpha = 1) )) + 
      theme_bw() + 
      theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
      facet_wrap(~ location_name) + ylim(c(0, 100)) +
      ggtitle(hierarchy[location_id == loc]$location_name)
    print(gg)
    #dev.off()
  }
    ggplot() +
      scale_x_date("Date", limits = as.Date(c("2020-01-01","2021-01-01"))) +
      ylab("Percent always mask use") +
      geom_point(data = cur_best[parent_id ==95 & N > 10], aes(x = date, y = prop_always * 100, col = factor(mandate)), alpha = 0.4, size = 2) + 
      geom_line(data = cur_best[parent_id == 95], aes(x = date, y = mask_use * 100, group=location_id), lwd=1.25) +
      scale_color_manual("", limits = c(0,1), labels = c("No mandate", "Active mandate"), values = c("black","purple"),
                         guide = guide_legend(override.aes = list(size = 3, alpha = 1) )) + 
      theme_bw() + 
      theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
      facet_wrap(~ location_name) + ylim(c(0, 100)) +
      ggtitle("United Kingdom")
    ggplot() +
      scale_x_date("Date", limits = as.Date(c("2020-01-01","2021-01-01"))) +
      ylab("Percent always mask use") +
      geom_point(data = cur_best[parent_id ==102 & N > 10], aes(x = date, y = prop_always * 100, col = factor(mandate)), alpha = 0.4, size = 2) + 
      geom_line(data = cur_best[parent_id == 102], aes(x = date, y = mask_use * 100, group=location_id), lwd=1.25) +
      scale_color_manual("", limits = c(0,1), labels = c("No mandate", "Active mandate"), values = c("black","purple"),
                         guide = guide_legend(override.aes = list(size = 3, alpha = 1) )) + 
      theme_bw() + 
      theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
      facet_wrap(~ location_name) + ylim(c(0, 100)) +
      ggtitle("United States")
    ggplot() +
      scale_x_date("Date", limits = as.Date(c("2020-01-01","2021-01-01"))) +
      ylab("Percent always mask use") +
      geom_point(data = cur_best[parent_id == 71 & N > 10], aes(x = date, y = prop_always * 100, col = factor(mandate)), alpha = 0.4, size = 2) + 
      geom_line(data = cur_best[parent_id == 71], aes(x = date, y = mask_use * 100, group=location_id), lwd=1.25) +
      scale_color_manual("", limits = c(0,1), labels = c("No mandate", "Active mandate"), values = c("black","purple"),
                         guide = guide_legend(override.aes = list(size = 3, alpha = 1) )) + 
      theme_bw() + 
      theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
      facet_wrap(~ location_name) + ylim(c(0, 100)) +
      ggtitle("Australia")
    ggplot() +
      scale_x_date("Date", limits = as.Date(c("2020-01-01","2021-01-01"))) +
      ylab("Percent always mask use") +
      geom_point(data = cur_best[parent_id == 135 & N > 10], aes(x = date, y = prop_always * 100, col = factor(mandate)), alpha = 0.4, size = 2) + 
      geom_line(data = cur_best[parent_id == 135], aes(x = date, y = mask_use * 100, group=location_id), lwd=1.25) +
      scale_color_manual("", limits = c(0,1), labels = c("No mandate", "Active mandate"), values = c("black","purple"),
                         guide = guide_legend(override.aes = list(size = 3, alpha = 1) )) + 
      theme_bw() + 
      theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
      facet_wrap(~ location_name) + ylim(c(0, 100)) +
      ggtitle("Brazil")
    ggplot() +
      scale_x_date("Date", limits = as.Date(c("2020-01-01","2021-01-01"))) +
      ylab("Percent always mask use") +
      geom_point(data = cur_best[parent_id == 130 & N > 10], aes(x = date, y = prop_always * 100, col = factor(mandate)), alpha = 0.4, size = 2) + 
      geom_line(data = cur_best[parent_id == 130], aes(x = date, y = mask_use * 100, group=location_id), lwd=1.25) +
      scale_color_manual("", limits = c(0,1), labels = c("No mandate", "Active mandate"), values = c("black","purple"),
                         guide = guide_legend(override.aes = list(size = 3, alpha = 1) )) + 
      theme_bw() + 
      theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
      facet_wrap(~ location_name) + ylim(c(0, 100)) +
      ggtitle("Mexico")
    ggplot() +
      scale_x_date("Date", limits = as.Date(c("2020-01-01","2021-01-01"))) +
      ylab("Percent always mask use") +
      geom_point(data = cur_best[parent_id == 163 & N > 10], aes(x = date, y = prop_always * 100, col = factor(mandate)), alpha = 0.4, size = 2) + 
      geom_line(data = cur_best[parent_id == 163], aes(x = date, y = mask_use * 100, group=location_id), lwd=1.25) +
      scale_color_manual("", limits = c(0,1), labels = c("No mandate", "Active mandate"), values = c("black","purple"),
                         guide = guide_legend(override.aes = list(size = 3, alpha = 1) )) + 
      theme_bw() + 
      theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
      facet_wrap(~ location_name) + ylim(c(0, 100)) +
      ggtitle("India")
  dev.off()
  
  ## Aggregate for regions (aggregation in Mask code is an average :facepalm:)
    cur_best <- merge(cur_best, pop, by="location_id")
    cur_best$number <- cur_best$population * cur_best$mask_use
    
    today <- cur_best[date == data_date]
    most_detailed <- cur_best[most_detailed==1 & date == data_date]
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

    sr_plot <- agg_trend[level < 2]
    sr_plot$location <- reorder(sr_plot$location_name, sr_plot$level)

    sr_plot[date == "2020-10-19"]
    
    most_detailed <- most_detailed[order(mask_use)]
    most_detailed
    
    ## Wants overlay plot, different colors
    cols <- c("black", brewer_pal(palette="Set1")(5),"pink","brown")
    pdf("FILEPATH/mask_use_lineplot_sregions.pdf", height=7, width = 10)
      ggplot() + 
        geom_line(data = sr_plot[date <= data_date], lty = 1, aes(x=as.Date(date), y=mask_use, col=location), lwd=1.25) + 
        geom_line(data=sr_plot[date > data_date], lty=2, aes(x=as.Date(date), y=mask_use, col=location), lwd=1.25) + 
        theme_bw(base_size = 10) + scale_x_date("", limits = as.Date(c("2020-03-01","2020-09-30")), breaks = "month", date_labels = "%b") + 
        scale_y_continuous("Mask use", labels=percent) +
        scale_color_manual("", values=cols) + theme(legend.position="bottom")
    dev.off()

##----------------------------------------------------------------------------------------  
## Plots of SEIR model results
  seir_sum_cumulative <- fread(paste0("FILEPATH",seir_version,"FILEPATH/cumulative_deaths.csv"))
  setnames(seir_sum_cumulative, "mean","cuml_deaths")
  #hierarchy <- fread(paste0("/ihme/covid-19/hospitalizations/inputs/seir/", seir_meta[scenario_id==1]$seir_version,"/hierarchy.csv"))
  
  df_draws_1 <- fread(paste0("FILEPATH",seir_version,"FILEPATH/cumulative_deaths.csv"))
  df_draws_2 <- fread(paste0("FILEPATH",seir_version,"FILEPATH/cumulative_deaths.csv"))
  
  # Remove Hubei per Emm (8/30)
  #df_draws_1 <- df_draws_1[location_id != 503]
  #df_draws_2 <- df_draws_2[location_id != 503]
  
  # Will be used for region aggregates TS
  full_1 <- df_draws_1[date <= diff_date]
  full_2 <- df_draws_2[date <= diff_date]
  
  ## A lot has to happen here, the gist is to keep only results on Jan 1 2021 and then do some summarizing and aggregating by draw
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
      mid1 <- data.frame(df_1[location_id == l & !is.na(observed)])
      mid2 <- data.frame(df_2[location_id == l & !is.na(observed)])
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

  ## Save the final formatted table   
  final_df <- merge(final_df, pop[,c("location_id","population")], by="location_id", all.x=T)
  
  write.csv(final_df, "FILEPATH/mask_use_deaths_averted_9-23.csv", row.names = F)
  
  ## Last step is to plot the results time series aggregated at super-regional level
    ## Find the global values ##
    # Stupid hybrid model requires me to recalculate these:
    agg_mod1 <- summarize_draws_agg(full_1, hierarchy)
    agg_mod2 <- summarize_draws_agg(full_2, hierarchy)
    
    srs_locs <- hierarchy[level <= 1]$location_id
    global_deaths <- data.table()
    labels <- c("Reference","Universal mask use")
    agg_mod1$version <- "Reference"
    agg_mod2$version <- "Universal mask use"
    global_deaths <- rbind(agg_mod1, agg_mod2)
    global_deaths <- global_deaths[location_id %in% srs_locs]
    
    global_deaths <- merge(global_deaths, hierarchy[,c("location_id","location_name")], by="location_id")
    global_deaths$location <- reorder(global_deaths$location_name, global_deaths$location_id)
    
    pdf("FILEPATH/mask_mandate_time_series_sregions.pdf", height = 8, width = 9)
      ggplot(global_deaths, aes(x=as.Date(date), y=deaths_mean)) + geom_line(aes(col=version)) + 
        geom_ribbon(aes(ymin=deaths_lower, ymax=deaths_upper, fill=version), alpha=0.2) + 
        theme_minimal(base_size = 10) + scale_y_continuous("Deaths", labels=comma) + 
        scale_x_date("") + guides(fill=F) + scale_color_discrete("Model version", labels=c("Reference","Universal mask use")) +
        facet_wrap(~location, scales="free", ncol = 2) + theme(legend.position = "bottom")
    dev.off()
    
  ## What proportion of the world's population does this cover? ##
  model_locs <- unique(final_df[level == 3]$location_id)
  gbd_locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)
  global_locs <- gbd_locs[level == 3]$location_id
  
  pops <- get_population(location_id = global_locs, age_group_id = 22, year_id = 2019, gbd_round_id = 6, decomp_step = "step4")
  model_locs <- subset(pops, location_id %in% model_locs)
  global_locs <- subset(pops, location_id %in% global_locs)
  fraction_pop <- sum(model_locs$population) / sum(global_locs$population)
  fraction_pop
  
## Plot with the alternative scenario
  df_alt <- fread(paste0("FILEPATH/prepared_death_estimates.csv"))
  
  # Will be used for region aggregates TS
  full_alt <- df_alt[date <= diff_date]
  agg_alt <- summarize_draws_agg(full_alt, hierarchy)
  
  srs_locs <- hierarchy[level <= 1]$location_id
  global_deaths <- data.table()
  labels <- c("Reference","Universal mask use")
  agg_mod1$version <- "Reference"
  agg_mod2$version <- "Universal mask use (95%)"
  agg_alt$version <- "Alternative scenario (85%)"
  global_deaths <- rbind(agg_mod1, agg_mod2, agg_alt, fill = T)
  global_deaths <- global_deaths[location_id %in% srs_locs]
  
  global_deaths <- merge(global_deaths, hierarchy[,c("location_id","location_name")], by="location_id")
  global_deaths$location <- reorder(global_deaths$location_name, global_deaths$location_id)
  global_deaths$version <- factor(global_deaths$version, c("Reference","Universal mask use (95%)","Alternative scenario (85%)"))
  
  ggplot(global_deaths, aes(x=as.Date(date), y=deaths_mean)) + geom_line(aes(col=version)) + 
    geom_ribbon(aes(ymin=deaths_lower, ymax=deaths_upper, fill=version), alpha=0.2) + 
    theme_minimal(base_size = 10) + scale_y_continuous("Deaths", labels=comma) + 
    scale_x_date("") + guides(fill=F) + scale_color_discrete("Model version") +
    facet_wrap(~location, scales="free", ncol = 2) + theme(legend.position = "bottom")
  
  agg_alt[date=="2021-01-01"]
  global_deaths[date == "2021-01-01"]
  
  compare_out <- global_deaths[date == "2021-01-01"]
  compare_out[, formatted := paste0(round(deaths_mean,0)," (",round(deaths_lower,0),"-",round(deaths_upper,0),")")]  
  
  write.csv(compare_out, "FILEPATH/mask_use_paper_sregion_alt_scenario.csv", row.names=F)
    