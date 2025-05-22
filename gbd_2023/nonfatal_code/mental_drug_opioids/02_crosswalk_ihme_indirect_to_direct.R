### Crosswalk, get betas from indirect and direct data
# Uses custom indirect estimates to get betas from direct

library(readxl)
library(tidyverse)
library(data.table)
source("/ihme/cc_resources/libraries/current/r/get_crosswalk_version.R")
library(crosswalk, lib.loc = "/ihme/code/mscm/R/packages/")


### Data setup
df1 <- read_xlsx("/ihme/scratch/projects/yld_gbd/substance_use/opioid/data/process/opioid_data_sex_split.xlsx")
df1$mean[df1$mean == 0] <- .001
prev_xwalk <- df1 %>% filter(grepl("crosswalks", note_sr))
head(prev_xwalk[,c("field_citation_value", "location_name", "year_start", "age_start")])
head(prev_xwalk[i,c("mean")])

df1$cv_direct[df1$nid == 233904] <- 1
df1 <- df1 %>% filter(!grepl("crosswalks", note_sr))

b <- df1 %>% filter(grepl("replaced with micro-data", note_sr))
df1 <- df1 %>% filter(!grepl("replaced with micro-data", note_sr))
df1 <- df1 %>% filter(is.na(crosswalk_parent_seq))
df1 <- df1 %>% filter(cv_direct == 1)
df1 <- df1 %>% filter(!grepl("parent", specificity))

# df1 <- df1 %>% filter(grepl("child", specificity))


check <- c()
c <- data.frame()
for(i in 1:nrow(prev_xwalk)) {
  a <- df1 %>% filter(field_citation_value == prev_xwalk$field_citation_value[i],
                      location_name == prev_xwalk$location_name[i],
                      year_start == prev_xwalk$year_start[i],
                      age_start == prev_xwalk$age_start[i],
                      sex == prev_xwalk$sex[i])
  
  if (nrow(a) > 1) {
    check <- c(check, i)
    c <- bind_rows(c, a)
  }
}




# df1 <- df1 %>% filter(mean > 0)

# df2 <- read_xlsx("/ihme/scratch/projects/yld_gbd/substance_use/opioid/data/process/custom_indirect.xlsx")
df2.main <- get_crosswalk_version(31865) # opioid crosswalk
df2.main$cv_direct[df2.main$note_modeler == "mean = cases/gpr_mean/sample_size, indirect estimates"] <- 0

df2 <- df2.main %>% filter(!grepl("crosswalks", note_sr))
df2 <- df2 %>% filter(measure == "prevalence")
df2 <- df2 %>% filter(!grepl("replaced with micro-data", note_sr))
df2 <- df2 %>% filter(cv_direct == 0)

sum(df2$location_id == 102)

cols_needed <- c("nid", "year_start", "year_end", "location_name", "location_id", "sex", 
                 "age_start", "age_end", "mean", "standard_error", "seq")

cols_final <- c("nid_ref", 'nid_alt', 
                "location_name", "location_id", "sex",
                "id_ref","id_alt", 
                "dorm_ref", "dorm_alt", 
                "prev_ref", "prev_alt",
                "prev_se_ref", "prev_se_alt",
                "age_start_ref", "age_end_ref",
                "age_start_alt", "age_end_alt",
                "year_start_ref", "year_end_ref",
                "year_start_alt", "year_end_alt")

### Getting all data ready ----------------------------------------------------
ref <- df2 %>% 
  dplyr::select(all_of(cols_needed)) %>%
  rename(prev_ref = mean, prev_se_ref = standard_error,
         year_start_ref = year_start, year_end_ref = year_end,
         age_start_ref = age_start, age_end_ref = age_end,
         prev_se_ref = standard_error, nid_ref = nid) %>%
  mutate(dorm_ref = "indirect",
         id_ref = 1:n(),
         maxyear = (year_start_ref + year_end_ref) / 2 + 10,
         minyear = (year_start_ref + year_end_ref) / 2 - 10,
         maxage = (age_start_ref + age_end_ref) / 2 + 10,
         minage = (age_start_ref + age_end_ref) / 2 - 10) %>% 
  as.data.table()

alt <- df1 %>%
  dplyr::select(all_of(cols_needed), field_citation_value) %>%
  rename(prev_alt = mean, prev_se_alt = standard_error,
         year_start_alt = year_start, year_end_alt = year_end,
         age_start_alt = age_start, age_end_alt = age_end,
         prev_se_alt = standard_error, nid_alt = nid) %>%
  mutate(dorm_alt = "direct",
         id_alt = 1:n(),
         midyear = (year_start_alt + year_end_alt) / 2,
         midage = (age_start_alt + age_end_alt) / 2) %>%
  as.data.table()

us_alt <- alt %>% filter(location_id == 102)
locs <- read.csv("id_locations.csv")
locs$location_name <- as.character(locs$location_name)
locs <- locs[locs$parent_id == 102, ]

for (i in 1:nrow(us_alt)) {
  a <- us_alt[i,]
  a <- a[rep(seq_len(nrow(a)), each = 51), ]
  a$location_id <- locs$location_id
  a$location_name <- locs$location_name
  
  alt <- bind_rows(alt, a)  
}

# alt$prev_alt[alt$prev_alt == 0 ] <- 0.00001

# find minimum distance by year, age, sex, location
# use those to match
t1 <- ref
t2 <- alt

t3 <- t1[t2, on=.(location_name = location_name, sex = sex,
                  maxyear > midyear, minyear < midyear,
                  minage < midage, maxage > midage), nomatch=0,
         allow.cartesian = T]

# Check if there are dropped direct estimates
setdiff(unique(t2$id_alt), unique(t3$id_alt))

# Multiple joins based on the criteria. Keeping
# values closest in age
t4 <- t3 %>%
  mutate(age_diff = 
           abs(age_start_alt - age_start_ref) + 
           abs(age_end_alt - age_end_ref)) %>%
  group_by(id_alt) %>%
  slice(which.min(age_diff)) %>%
  dplyr::select(field_citation_value, all_of(cols_final))

dropped_ids <- setdiff(unique(alt$id_alt), unique(t4$id_alt))

# Save this later as part of function
dropped_alts <- alt %>% filter(id_alt %in% dropped_ids)

df_matched <- t4 
df_matched$prev_se_alt[df_matched$prev_se_alt == 0 ] <- 2.697e-06
df_matched$prev_se_alt[is.na(df_matched$prev_se_alt)] <- 2.697e-06

# write.csv(dropped_alts, "Dropped_direct_data.csv", row.names = F)
# write.csv(df_matched, "Opioid_direct_matched_pairs.csv", row.names = F)

df_matched %>% filter(location_name == "Ohio") %>%
  ggplot(aes(x = prev_ref, y = prev_alt)) +
  geom_point() +
  geom_abline(slope = 1, intercept = 0) +
  facet_grid(.~age_start_alt)


# Do rows of matched and unmatched equal all alternatives definitions
nrow(df_matched) + nrow(dropped_alts) == nrow(alt)

dat_diff <- as.data.frame(cbind(
  delta_transform(
    mean = df_matched$prev_alt, 
    sd = df_matched$prev_se_alt,
    transformation = "linear_to_logit"),
  delta_transform(
    mean = df_matched$prev_ref, 
    sd = df_matched$prev_se_ref,
    transformation = "linear_to_logit")
))

names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")

df_matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = dat_diff, 
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref" )


df_matched$location_name <- as.factor(df_matched$location_name)
df_matched$sex <- as.factor(df_matched$sex)
head(df_matched)

### Running the crosswalk -----------------------------------------------------

dat1 <- CWData(
  df = df_matched,
  obs = "logit_diff",       # matched differences in logit space
  obs_se = "logit_diff_se", # SE of matched differences in logit space
  alt_dorms = "dorm_alt",   # var for the alternative def/method
  ref_dorms = "dorm_ref",   # var for the reference def/method
  # covs = list("sex"),       # list of (potential) covariate columns
  study_id = "nid_alt"          # var for random intercepts; i.e. (1|study_id)
)

fit1 <- CWModel(
  cwdata = dat1,           # result of CWData() function call
  obs_type = "diff_logit", # must be "diff_logit" or "diff_log"
  cov_models = list(
    CovModel("intercept")
    # CovModel("location_name"),
    # CovModel("sex")
  ),
  gold_dorm = "indirect"  # level of 'ref_dorms' that's the gold standard
)


fit1$fixed_vars
df_result <- fit1$create_result_df()


### Transform cv_direct values ------------------------------------------------

preds1 <- adjust_orig_vals(
  fit_object = fit1, # object returned by `CWModel()`
  df = df_matched,
  orig_dorms = "dorm_alt",
  orig_vals_mean = "prev_alt",
  orig_vals_se = "prev_se_alt",
  data_id = "id_alt"   # optional argument to add a user-defined ID to the predictions;
  # name of the column with the IDs
)

# now we add the adjusted values back to the original dataset
df_matched[, c("meanvar_adjusted", "sdvar_adjusted", 
               "pred_logit", "pred_se_logit", "data_id")] <- preds1


# Need to re-run line 90 before this one
preds1 <- adjust_orig_vals(
  fit_object = fit1, # object returned by `CWModel()`
  df = alt,
  orig_dorms = "dorm_alt",
  orig_vals_mean = "prev_alt",
  orig_vals_se = "prev_se_alt",
  data_id = "id_alt"   # optional argument to add a user-defined ID to the predictions;
  # name of the column with the IDs
)


alt[, c("meanvar_adjusted", "sdvar_adjusted", 
        "pred_logit", "pred_se_logit", "data_id")] <- preds1

write.csv(alt, "adjusted_direct.csv", row.names = F)
iran <- alt %>% filter(nid_alt == 233904)

locs <- unique(df_matched$location_name)
pdf("compare_xwalk_adjustments_opioid_removedxwalk.pdf")
for (i in locs) {
  
  plot_title <- paste(i)
  gg <- df_matched %>% 
    filter(location_name == i) %>%
    ggplot(aes(x = prev_alt, y = meanvar_adjusted, col = "direct")) +
    geom_point() +
    geom_point(data = ref %>% filter(location_name == i), 
               aes(x = prev_ref, y = prev_ref, col = "indirect")) +
    geom_abline(intercept = 0, slope = 1) +
    facet_grid(.~sex) +
    labs(title = plot_title, x = "Original Mean", y = "Crosswalked Mean") +
    theme(legend.position = "bottom")
  
  print(gg)
}
dev.off()


locs <- unique(df_matched$location_name)
pdf("compare_xwalk_adjustments_opioid_removedxwalk.pdf")
for (i in locs) {
  
  plot_title <- paste(i)
  gg <- df_matched %>% 
    filter(location_name == i) %>%
    ggplot(aes(x = prev_alt, y = meanvar_adjusted, col = "direct")) +
    geom_point() +
    geom_abline(intercept = 0, slope = 1) +
    facet_grid(.~sex) +
    labs(title = plot_title, x = "Original Mean", y = "Crosswalked Mean") +
    theme(legend.position = "bottom")
  
  print(gg)
}
dev.off()

df_matched %>% 
  ggplot(aes(x = prev_alt, y = meanvar_adjusted, col = "direct")) +
  geom_point() +
  geom_abline(intercept = 0, slope = 1) +
  facet_grid(.~sex) +
  labs(title = plot_title, x = "Original Mean", y = "Crosswalked Mean") +
  theme(legend.position = "bottom")



### Compare to previous xwalk adjustments -------------------------------------
xwalk.main <- read_xlsx("/ihme/scratch/projects/yld_gbd/substance_use/opioid/data/process/opioid_data_sex_split_crosswalked_09162020.xlsx")

xwalk <- xwalk.main %>% filter(nid %in% df_matched$nid_alt, cv_direct == 1,
                               location_id != 102) %>%
  dplyr::select(nid, location_name, year_start, 
                location_id, sex, age_start, age_end, mean, cv_direct) %>%
  rename(mean_2019 = mean, nid_alt = nid, year_start_alt = year_start,
         age_start_alt = age_start, age_end_alt = age_end)


compare <- df_matched %>% left_join(xwalk) %>%
  rename(mean_2020 = meanvar_adjusted) %>%
  filter(location_name != "Germany")

test <- compare %>% filter(age_start_alt == 14)
a <- compare %>% 
  # filter(!is.na(mean_2019), age_start_alt != 14) %>%
  # add_gbd_metadata(location = T, age = F, sex = F) %>%
  gather(key = "key", value = "value", c(mean_2020, mean_2019))

compare %>% 
  # filter(!is.na(mean_2019), age_start_alt != 14) %>%
  # add_gbd_metadata(location = T, age = F, sex = F) %>%
  gather(key = "key", value = "value", c(mean_2020, mean_2019)) %>%
  filter(value > prev_alt, location_id != 71, location_name != 'Alabama') %>%
  ggplot(aes(x = prev_alt, y = value, col = key)) +
  geom_point() +
  # geom_text(aes(label=location_name),hjust=0, vjust=0) +
  geom_abline(intercept = 0, slope = 1) +
  labs(x = "Unadjusted prevalence", y = "Adjusted prevalence",
       title = "Comparison of opioid direct adjustments between GBD 2019 and GBD 2020")

compare %>% 
  filter(!is.na(mean_2019), age_start_alt != 14) %>%
  # add_gbd_metadata(location = T, age = F, sex = F) %>%
  gather(key = "key", value = "value", c(mean_2020, mean_2019)) %>%
  ggplot(aes(x = prev_alt, y = value, col = key)) +
  geom_point() +
  geom_abline(intercept = 0, slope = 1) +
  labs(x = "Unadjusted prevalence", y = "Adjusted prevalence",
       title = "Comparison of opioid direct adjustments by age") +
  facet_wrap(.~age_start_alt)

compare %>% 
  filter(!is.na(mean_2019), age_start_alt != 14) %>%
  # add_gbd_metadata(location = T, age = F, sex = F) %>%
  gather(key = "key", value = "value", c(mean_2020, mean_2019)) %>%
  filter(value > prev_alt, location_id != 71) %>%
  ggplot(aes(x = prev_alt, y = value, col = key)) +
  geom_point() +
  geom_abline(intercept = 0, slope = 1) +
  labs(x = "Unadjusted prevalence", y = "Adjusted prevalence",
       title = "Comparison of opioid direct adjustments by sex") +
  facet_wrap(.~sex)

locs <- unique(compare$location_name)
# locs <- locs[2:length(locs)]
pdf("compare_xwalk_adjustments_opioid2.pdf")
for (i in locs) {
  print(i)
  plot_title <- paste(i)
  gg <- compare %>% 
    filter(location_name == i) %>%
    filter(!is.na(mean_2019), age_start_alt != 14) %>%
    # add_gbd_metadata(location = T, age = F, sex = F) %>%
    gather(key = "key", value = "value", c(mean_2020, mean_2019)) %>%
    ggplot(aes(x = prev_alt, y = value, col = key)) +
    geom_point() +
    geom_abline(intercept = 0, slope = 1) +
    facet_grid(.~sex) +
    labs(title = plot_title, x = "Unadjusted mean", y = "Adjusted mean") +
    theme(legend.position = "bottom")
  
  print(gg)
}
dev.off()

### Plots ---------------------------------------------------------------------
# Funnel plots
repl_python()
plots <- import("crosswalk.plots")
plots$funnel_plot(
  cwmodel = fit1,
  cwdata = dat1,
  obs_method = 'direct',
  plot_note = paste0("Funnel plot_opioid"), 
  plots_dir = paste0(""), 
  file_name = paste0("funnel_plot_opioid"),
  write_file = TRUE
)

### Save to crosswalk instead -------------------------------------------------
alt <- read.csv("adjusted_direct.csv")

alt %>%
  ggplot(aes(x = prev_alt, y = meanvar_adjusted)) +
  geom_point() +
  geom_abline(intercept = 0, slope = 1) +
  labs(title = "Opioid direct data adjustments", x = "Unadjusted mean", y = "Adjusted mean")

alt_df <- df1 %>% left_join(alt)
names(alt_df)
alt_df$crosswalk_parent_seq <- alt_df$seq
alt_df$seq <- NA
alt_df$cv_direct <- 1
alt_df$orig_mean <- alt_df$mean
alt_df$orig_standard_error <- alt_df$standard_error
alt_df$mean <- alt_df$meanvar_adjusted
alt_df$standard_error <- alt_df$sdvar_adjusted
alt_df <- alt_df %>%
  dplyr::select(-data_id, -meanvar_adjusted, -sdvar_adjusted,
                -midage, -midyear, -id_alt, -dorm_alt, -prev_se_alt,
                -prev_alt, -age_end_alt, -age_start_alt, -year_end_alt,
                -year_start_alt, -nid_alt, -bundle_name, -cv_diag_other,
                -cv_marketscan, -cv_marketscan_all_2000, -cv_marketscan_all_2010,
                -cv_marketscan_all_2012, -note_modeler1, -cv_use,
                -cv_abuse_dependence, -cv_regular_use, -cv_cannabis_regular_use,
                -cv_recall_1yr, -cv_distinct_urbanicity, -cv_students, -uploaded,
                -cv_recall_lifetime, -cv_directdep, -cv_directuse, -cv_subnational,
                -cv_nesarc)

df2_ref <- df2 %>%
  dplyr::select(-cv_recall_lifetime, -cv_directdep, -cv_directuse, -cv_subnational,
                -cv_nesarc)

alt_df$note_modeler <- "Already sex/age split, xwalk with logit -.831 (.284)"

data_final <- bind_rows(df2, alt_df)

non_prev_data <- df2.main %>% filter(measure != "prevalence") %>%
  dplyr::select(-cv_recall_lifetime, -cv_directdep, -cv_directuse, -cv_subnational,
                -cv_nesarc)

data_final <- bind_rows(data_final, non_prev_data)
data_final$seq[!is.na(data_final$crosswalk_parent_seq)] <- NA
data_final$note_modeler <- gsub('"', '', data_final$note_modeler)


data_final[19460:19470, c("specificity", "group", "group_review")]
data_final[1296:1300, c("note_modeler")]

locs <- read.csv("id_locations.csv")
locs$location_name <- as.character(locs$location_name)
data_final <- data_final %>% filter(location_id %in% locs$location_id)
data_final <- data_final %>% filter(group_review == 1 | is.na(group_review))

data_final[19710:19715, c("location_id", "location_name")]

data$case_definition <- gsub('"', '', data$case_definition)


write_xlsx(list(extraction = data_final), "xwalk_ost_direct.xlsx")
# 34373 pwid
# 34415 opioid bv
save_crosswalk_version(34415,
                       description = "0-1 treat, subnat emr, 10% coverage, year-loc above .2 removed, new crosswalk, remove old xwalk and duplicate",
                       data_filepath = "xwalk_ost_direct.xlsx")



data_final$age_diff <- data_final$age_end - data_final$age_start
sum(data_final$age_diff >= 20)
table(data_final$measure[data_final$age_diff >= 20])
