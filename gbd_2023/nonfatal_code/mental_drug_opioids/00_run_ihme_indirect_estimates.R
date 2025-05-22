# Processes the custom indirect estimates of our data
# Uses stgpr model # 25387

### Set up --------------------------------------------------------------------
library(data.table)
library(readxl)
library(writexl, lib = "FILEPATH")
library(general.utilites, lib = "FILEPATH")
library(processing, lib = "FILEPATH")
source("FILEPATH/split_uk_data.R")
`%notin%` <- Negate(`%in%`)
source_shared_functions("bundles")


### Getting multiplier values -------------------------------------------------
library(rhdf5, lib.loc = "FILEPATH")
source('FILEPATH/utility.r')
# m <- model_load(162026, "raked") # IDU final results
m <- model_load(162602, "raked") # opioid results
detach("package:plyr")


### Get bundle version --------------------------------------------------------
# bv <- get_bundle_version(34373, fetch = "all") # PWID bundle
bv <- get_bundle_version(34415, fetch = "all") # opioid
# bv <- get_bundle_version(34619, fetch = "all") # IDU
bv <- bv %>% filter(note_modeler == "custom_indirect")

bv$crosswalk_parent_seq <- bv$seq
# need at least 10 cases
bv_10cases <- bv %>% filter(cases < 10)
bv <- bv %>% filter(cases >= 10)

### Subset relevant population and coverage data ------------------------------
coverage <- m %>% filter(location_id %in% unique(bv$location_id)) %>%
  rename(year_start = year_id) %>%
  dplyr::select(location_id, year_start, gpr_mean, gpr_lower, gpr_upper)

pop <- get_population(location_id = unique(bv$location_id), 
                      decomp_step = 'step3',
                      gbd_round_id = 7,
                      age_group_id = 8:17,
                      year_id = 1980:2019,
                      sex_id = c(1:2))

pop <- merge_gbd_metadata(pop, location = F, age = T, sex = T)
pop <- pop %>% rename(age_start = age_group_years_start, 
                      age_end = age_group_years_end,
                      sex = sex_name, year_start = year_id) %>%
  dplyr::select(location_id, year_start, population, sex, age_start)

### age/sex split us subnats from NSSATS --------------------------------------
# d1 <- bv %>% filter(location_id == 102)
d1 <- read_xlsx("FILEPATH/us_age_split_data.xlsx") %>% filter(location_id == 102)

# Getting the us subnational currently unsplit data
us <- bv[bv$sex == "Both", ] %>% dplyr::select(-sex)

# get sex ratios --------------------------------------------------------------
sex_sum <- d1 %>% group_by(year_start) %>%
  summarise(total = sum(cases))

s <- d1 %>% group_by(year_start, sex) %>%
  summarise(cases = sum(cases)) %>% 
  left_join(sex_sum) %>%
  mutate(ratio = cases / total) %>%
  dplyr::select(year_start, sex, ratio)

s <- bind_rows(s, (s[1:2,] %>% ungroup %>% mutate(year_start = 1997)))
s <- bind_rows(s, (s[s$year_start == 2015, ] %>% ungroup() %>% mutate(year_start = 2017)))
s <- bind_rows(s, (s[s$year_start == 2015, ] %>% ungroup() %>% mutate(year_start = 2019)))

# Convert cases by sex
us <- us %>% left_join(s)
us$cases <- us$cases * us$ratio
us$sex_split_ratio <- us$ratio

us[,c("location_name","year_start", "sex", "cases", "sample_size", "mean", "lower", "upper")] %>%
  filter(location_name == "Vermont")

# get age pattern -------------------------------------------------------------
# To review and ensure age pattern matches if needed
# model_results <- get_model_results(gbd_team = "epi",
#                                    gbd_id = 24644,
#                                    gbd_round_id = 7,
#                                    decomp_step = 'iterative',
#                                    location_id = 102,
#                                    year_id = 1990:2012,
#                                    measure_id = 5,
#                                    sex_id = c(1,2))
# 
# head(model_results)
# s <- model_results %>% group_by(year_id, sex_id) %>%
#   summarise(total = sum(mean))
# 
# s2 <- model_results %>% group_by(year_id, sex_id, age_group_id) %>%
#   summarise(mean = sum(mean)) %>%
#   left_join(s) %>%
#   mutate(ratio = mean / total) %>%
#   add_gbd_metadata(location = F, sex = T, age = T) %>%
#   rename(year_start = year_id, age_start = age_group_years_start,
#          sex = sex_name)

# s2 %>% filter(year_start == 2000) %>%
#   ggplot(aes(x = age_start, y = ratio)) +
#   geom_point() +
#   facet_grid(.~sex_name)

sex_sum <- d1 %>% group_by(year_start) %>%
  summarise(total = sum(cases))

s <- d1 %>% group_by(year_start, age_start, age_end) %>%
  summarise(cases = sum(cases)) %>% 
  left_join(sex_sum) %>%
  mutate(ratio = cases / total) %>%
  dplyr::select(year_start, age_start, age_end, ratio)

s <- bind_rows(s, (s[1:10,] %>% ungroup %>% mutate(year_start = 1997)))
s <- bind_rows(s, (s[s$year_start == 2015, ] %>% ungroup() %>% mutate(year_start = 2017)))
s <- bind_rows(s, (s[s$year_start == 2015, ] %>% ungroup() %>% mutate(year_start = 2019)))

us <- us %>% dplyr::select(-age_start, -age_end, -ratio)

us <- us %>% left_join(s)
us$cases <- us$cases * us$ratio


### Main removals from STGPR --------------------------------------------------
coverage_data <- get_bundle_data(8279, 
                                 gbd_round_id = 7,
                                 decomp_step = 'iterative')

# Remove if 60% of the proportions by YASL of treated is under 10%
include_locs <- coverage_data %>%
  filter(cv_opioid == 1) %>% # Change here to opioid or idu 
  group_by(location_name) %>%
  summarise(mean = mean(val),
            cnt = sum(val < .1),
            total = length(val),
            prop = cnt / total) %>%
  filter(prop < .6)

# Quality check the locations included or excluded
setdiff(unique(coverage_data$location_name), unique(include_locs$location_name))

### Process indirect estimates ------------------------------------------------
### rbind datasets
data <- bv %>% filter(age_issue != 1) # drop if there are issues with age range
data <- bind_rows(data, us)

# join population and coverage info, remove locs
data <- data %>% left_join(coverage)
data <- data %>% left_join(pop)
data <- data %>% filter(location_name %in% include_locs$location_name)

# Basic calcs
data$sample_size <- data$population
data$orig_cases <- data$cases
data$orig_mean <- data$mean
data$orig_lower <- data$lower
data$orig_upper <- data$upper

data$cases <- data$orig_cases / data$gpr_mean
data$mean <- data$cases / data$sample_size

# get stddeviation by location and year
loc_std <- data %>%
  group_by(location_name, year_start, sex) %>%
  summarise(stddev = sd(mean))

data <- data %>% left_join(loc_std)

data$lower <- data$mean - (1.96 * data$stddev)
data$upper <- data$mean + (1.96 * data$stddev)
data$lower[data$lower < 0 ] <- 0
data$upper[data$upper > 1] <- 1

# Split UK into the four regions
data <- split_out_uk_data(data)

### For vetting purposes ------------------------------------------------------
# pdf("opioid_indirect_estimate_plots.pdf")
# for (i in unique(data$location_name[data$year_start == 2005])) {
#   gg <- data %>% filter(location_name == i, year_start == 2005) %>%
#     ggplot(aes(x = age_start, y = mean)) +
#     geom_point() +
#     geom_pointrange(aes(ymin = lower, ymax = upper)) +
#     labs(title = i) +
#     facet_grid(.~sex)
#   print(gg)
# }
# dev.off()

# data %>% filter(location_name == "Nagaland") %>%
#   # filter(year_start == 2007) %>%
#   ggplot(aes(x = age_start, y = mean)) +
#   geom_point() +
#   geom_pointrange(aes(ymin = lower, ymax = upper)) +
#   facet_grid(year_start~sex) 

# summary(data$mean)
# summary(data$lower)
# summary(data$upper)

### Judgement removals --------------------------------------------------------

# # Which locations have extremely high mean
data %>% group_by(location_name) %>%
  filter(is_outlier == 0) %>%
  summarise(min_multiplier = min(gpr_mean, na.rm = T),
            avg_multiplier = mean(gpr_mean, na.rm = T),
            avg_mean = mean(mean, na.rm = T),
            med_mean = median(mean, na.rm = T),
            min_mean = min(mean, na.rm = T),
            max_mean = max(mean, na.rm = T)) %>%
  arrange(desc(avg_mean))

# Which year location pairs are over 1
exclude_year_loc <- data %>%
  group_by(location_name, location_id, year_start) %>%
  summarise(max_mean = max(mean, na.rm = T)) %>%
  arrange(desc(max_mean)) %>%
  filter(max_mean > .2)

table(data$is_outlier)
for (i in 1:nrow(exclude_year_loc)) {
  data$is_outlier[data$location_id == exclude_year_loc$location_id[i] & 
         data$year_start == exclude_year_loc$year_start[i]] <- 1
}

data <- data %>% filter(gpr_mean > .075) # 
data <- data %>% filter(location_id != 142) # iran is weird
# data <- data %>% filter(location_name %notin% c("Wisconsin", "Vermont", "West Virginia",
#                                              "Louisiana", "Kansas", "Connecticut",
#                                              "Maryland", "Tennessee"))

# Run and see which locations have extremely high mean
data <- data %>% filter(location_name != "Mauritius", location_name != "Delaware",
                        location_name != "Rhode Island", location_name != "Malaysia")

data <- data %>% filter(!is.na(upper))
data <- data %>% filter(cases < sample_size)


### Plot all with uncertainity ------------------------------------------------

pdf("indirect_data_only.pdf")
for(i in unique(data$location_name[data$year_start == 2007])) {

  gg <- data %>% filter(location_name == i, year_start == 2007) %>%
    ggplot(aes(x = age_start, y = mean)) +
    geom_point() +
    geom_pointrange(aes(ymin = lower, ymax = upper)) +
    facet_grid(.~sex) +
    labs(title = i) 
  
  print(gg)
  
}
dev.off()


### Compare to current crosswalk prevalence data ------------------------------
# opioid
xwalk.main <- get_crosswalk_version(27176)

xwalk <- xwalk.main %>% filter(measure == "prevalence",
                          year_start == 2000,
                          cv_direct != 1)
pdf("indirect_data_only_xwalk.pdf")
for(i in unique(xwalk$location_name[xwalk$year_start == 2000])) {
  
  gg <- xwalk %>% filter(location_name == i, year_start == 2000) %>%
    ggplot(aes(x = age_start, y = mean)) +
    geom_point() +
    geom_pointrange(aes(ymin = lower, ymax = upper)) +
    facet_grid(.~sex) +
    labs(title = i) +
    lims(y=c(0,.3))
  
  print(gg)
  
}
dev.off()

# IDU
xwalk.main <- get_crosswalk_version(24557)

xwalk <- xwalk.main %>% filter(measure == "prevalence")

locs <- unique(data[,c("location_name", "location_id")])
locs <- add_gbd_metadata(locs, age = F, sex = F, location = T)
locs <- locs %>% arrange(super_region_name, region_name, location_name)
locs2 <- unique(xwalk$location_name)

pdf("compare_idu_indirect_to_xwalk.pdf")
for (i in locs$location_name) {
  print(i)
  if (i %in% locs2) {
    gg <- data %>% filter(location_name == i) %>%
      ggplot(aes(x = age_start, y = mean)) +
      geom_point(aes(color = "new indirect data")) +
      geom_smooth(method = "loess", aes(color = "new indirect data")) +
      geom_point(data = xwalk %>% filter(location_name == i),
                 aes(x = age_start, y = mean, color = "current idu data")) +
      geom_smooth(data = xwalk %>% filter(location_name == i),
                  aes(x = age_start, y = mean, color = "current idu data"),
                  method = "lm") +
      facet_grid(.~sex) +
      labs(title = i, x = "age", y = "Prevalence") +
      theme(legend.position = "bottom") +
      scale_color_manual(name = "Data",
                         breaks = c("new indirect data", "current idu data"),
                         values = c("new indirect data" = "#F8766D", "current idu data" = "#00BFC4"))
    
  } else {
    gg <- data %>% filter(location_name == i) %>%
      ggplot(aes(x = age_start, y = mean, col = "new indirect data")) +
      geom_point() +
      # geom_smooth(method = "loess") +
      facet_grid(.~sex) +
      labs(title = i, x = "age", y = "Prevalence") +
      theme(legend.position = "bottom")
  }
  print(gg)
}
dev.off()

### Compare to current opioid death rates -------------------------------------

death <- get_model_results(gbd_team = "cod",
                           gbd_id = 562,
                           decomp_step = 'step3',
                           gbd_round_id = 7,
                           age_group_id = 8:17,
                           year_id = min(data$year_start):max(data$year_end),
                           sex_id = c(1,2),
                           location_id = unique(data$location_id))

death <- death.main %>% 
  add_gbd_metadata(location = T, sex = T, age = T)
  
death <- death %>% rename(age_start = age_group_years_start,
         year_start = year_id, sex = sex_name) %>%
  dplyr::select(location_id, location_name, 
                year_start, age_group_id, sex_id, sex,
                mean_death_rate,lower_death_rate, upper_death_rate, 
                age_start, region_name, super_region_name)


xwalk_compare <- xwalk %>%
  dplyr::select(location_id, year_start, age_start, sex, mean) %>%
  rename(current_opioid_data = mean)

compare1 <- data %>% left_join(death) %>%
  left_join(xwalk_compare) %>%
  dplyr::select(location_id, location_name, ihme_loc_id, sex, age_start, year_start, mean,
                gpr_mean, mean_death_rate, current_opioid_data)

compare2 <- compare1 %>%
  rename(indirect_estimate = mean) %>%
  gather(key = "key", value = "value", 7:10)

compare2 %>% filter(grepl("USA", ihme_loc_id))




compare2 %>% filter(location_name == i, key != "gpr_mean") %>%
  ggplot(aes(x = age_start, y = value, col = key)) +
  geom_point() +
  facet_grid(key~sex, scales = "free_y") +
  labs(y = "Prevalence or death rate", x = "Age") 


locs3 <- compare2 %>% filter(grepl("USA", ihme_loc_id))

pdf("opioid_indirect_deaths_usa_only_4years.pdf", width = 10)
for(i in unique(locs3$location_name)) {
  plot_title <- paste(i, "; multiplier = ", 
                      round(mean(compare2$value[
                        compare2$location_name == i & compare2$key == "gpr_mean"]), 3))
  
  gg <- compare2 %>% filter(location_name == i, key != "gpr_mean",
                            year_start %in% c(2000, 2005, 2010, 2013)) %>%
    ggplot(aes(x = age_start, y = value, col = key)) +
    geom_point() +
    facet_grid(key~sex, scales = "free_y") +
    labs(y = "Prevalence or death rate", x = "Age",
         title = plot_title) +
    theme(legend.position = "bottom")
  
  print(gg)
  
}
dev.off()


### Compare IDU to opioid rates -----------------------------------------------

# Run first with IDU values
idu <- data
idu$cause <- "idu"

# Re-run with opioid values
opioid <- data
opioid$cause <- "opioid"
opioid$nid <- as.integer(opioid$nid)
opioid$year_issue <- as.integer(opioid$year_issue)

# Join data for plotting
data <- bind_rows(idu, opioid)

locs <- unique(data[,c("location_name", "location_id")])
locs <- add_gbd_metadata(locs, age = F, sex = F, location = T)
locs <- locs %>% arrange(super_region_name, region_name, level, location_name)
locs <- locs %>% filter(region_name == "High-income North America")

i <- "Alabama"

pdf("Compare_idu_opioid_values_USA.pdf")
for (i in locs$location_name) {
  
  plot_data <- data %>% filter(location_name == i)
  ost_coverage <- round(mean(plot_data$gpr_mean[plot_data$cause == "opioid"]), 3)
  idu_coverage <- round(mean(plot_data$gpr_mean[plot_data$cause == "idu"]), 3)
  
  plot_title <- paste0(i, "; opioid multiplier = ", ost_coverage, 
                       "; idu multiplier = ", idu_coverage)
  
  gg <- plot_data %>%
    ggplot(aes(x = age_start, y = mean, col = cause)) +
    geom_point() +
    stat_smooth(method = "loess") +
    labs(title = plot_title, y = "Prevalence", x = "Age")
  
  print(gg)
}
dev.off()


### Bundle sheet setup --------------------------------------------------------
data$standard_error <- NA
data$seq <- NA
# data$upper <- NA
# data$lower <- NA
data$uncertainty_type_value <- 95
data$note_modeler <- "mean = cases/gpr_mean/sample_size, indirect estimates"
data$note_sr <- "Processed data, using ost coverage info, ost client values, and population"
data$year_issue <- as.integer(data$year_issue)
data$nid <- as.integer(data$nid)

data <- data %>%
  dplyr::select(-stddev, -orig_upper, -orig_lower, -orig_mean,
                -population, -gpr_upper, -gpr_lower, -ratio,
                -sex_split_ratio)

### Save and upload pwid data --------------------------------------------------
data$nid <- 455841
write_xlsx(list(extraction = data), "xwalk_ost.xlsx")
# 34373 pwid
# 34415 opioid bv
save_crosswalk_version(34373,
                       description = "test uncertainty",
                       data_filepath = "xwalk_ost.xlsx")




### Append and upload data opioid ---------------------------------------------

xwalk_prev <- get_crosswalk_version(31403) # opioid
xwalk_prev <- xwalk_prev %>% filter(note_modeler != "mean = cases/gpr_mean/sample_size, indirect estimates")
summary(xwalk_prev$seq)
summary(xwalk_prev$crosswalk_parent_seq)
xwalk_prev$nid <- as.integer(xwalk_prev$nid)


xwalk_prev$seq[!is.na(xwalk_prev$crosswalk_parent_seq)] <- NA
xwalk_prev$standard_error[xwalk_prev$standard_error > 1] <- .99
xwalk_prev$standard_error[xwalk_prev$standard_error < 0] <- .01

data_final <- bind_rows(xwalk_prev, data)
names(data_final)

data_final <- data_final %>% 
  dplyr::select(-sex_split_ratio, -ratio, -id, -age_diff, -case_needed,
                -gpr_lower, -gpr_upper, -population, -orig_mean,
                -orig_lower, -orig_upper,
                -cv_diag_other, -cv_marketscan, -cv_marketscan_all_2010,
                -cv_marketscan_all_2012, -cv_marketscan_all_2000,
                -cv_use, -cv_abuse_dependence, -cv_regular_use, -cv_students,
                -cv_distinct_urbanicity, -cv_cannabis_regular_use, -cv_recall_1yr,
                -stddev)


write_xlsx(list(extraction = data_final), "xwalk_ost.xlsx")
# 34373 pwid
# 34415 opioid bv
save_crosswalk_version(34415,
                       description = "0-1 treat, subnat emr, 10% coverage, year-loc above .2 removed, orig opioid data",
                       data_filepath = "xwalk_ost.xlsx")

### Append and upload data idu ---------------------------------------------

xwalk_prev <- get_crosswalk_version(24557) # idu

summary(xwalk_prev$seq)
summary(xwalk_prev$crosswalk_parent_seq)

xwalk_prev$seq[!is.na(xwalk_prev$crosswalk_parent_seq)] <- NA
xwalk_prev$standard_error[xwalk_prev$standard_error > 1] <- .99
xwalk_prev$standard_error[xwalk_prev$standard_error < 0] <- .01

data_final <- bind_rows(xwalk_prev, data)
names(data_final)


data_final <- data_final %>% 
  dplyr::select(-sex_split_ratio, -ratio, -id, -age_diff, 
                -gpr_lower, -gpr_upper, -population, -orig_mean,
                -orig_lower, -orig_upper, -level, -sheet, -region_name,
                -super_region_name, -map_id, -cv_national,
                -cv_registry, -cv_survey, -cv_community, 
                -cv_recall_6m_1yr, -cv_casesoutofpop, -stddev)

write_xlsx(list(extraction = data_final), "xwalk_idu.xlsx")

save_crosswalk_version(34619,
                       description = "0-1 prior treatment, locs 10%",
                       data_filepath = "xwalk_idu.xlsx")

