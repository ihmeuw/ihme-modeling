###age pattern for asthma
rm(list=ls())

#load in library and packages
library(dplyr)
library(knitr)
library(ggplot2)
library(kableExtra)
library(purrr)
library(data.table)
library(openxlsx)
library(tidyr)
library(zoo)
library(stringr)

invisible(sapply(list.files("/filepath/r/", full.names = T), source))

asthma_bv_data <- get_bundle_version(bundle_version_id) 

both_sex <- asthma_at %>%
  filter(sex == "Both" & (measure == "prevalence" | measure == "incidence"))

sex_specific <- asthma_at %>%
  filter((sex == "Male" | sex == "Female") & (measure == "prevalence" | measure == "incidence"))

#within age-sex
age_sex_within <- asthma_at %>%
  filter(nid %in% c(nid_list))


df <- as.data.table(age_sex_within)
asthma_within_agesex_results <- age_sex_split(raw_dt = df)
asthma_within_agesex_results <- asthma_within_agesex_results %>%
  filter(sex != "Both")

#sex split - shared function using MSCA-pydisagg

sex_split(topic_name = "asthma",
          output_dir = "filepath",
          bundle_version_id = NULL,
          data_all_csv = NULL,
          data_to_split_csv = "/filepath/",
          data_raw_sex_specific_csv = "/filepath/",
          nids_to_drop = c("source_list") #list of sources to exclude, studies that were within_age_sex split
          cv_drop = c(),
          mrbrt_model = NULL,
          mrbrt_model_age_cv = TRUE,
          mrbrt_model_age_spline_degree = 3,
          mrbrt_model_age_spline_knots = c(0, 0.028, .0959, .303, .74, .94, 1),
          mrbrt_model_linear_age_cv = FALSE,
          release_id = 16,
          measure = c("prevalence", "incidence"),
          vetting_plots = TRUE)


#get age pattern from super region draws, merge with age and population metadata
pre_agesplit <- as.data.table(pre_agesplit)


#age-sex within remove old data
pre_agesplit <- pre_agesplit %>%

age_sex_within_results <- pre_agesplit %>%

asthma_age_specific <- pre_agesplit %>%
  mutate(age_diff = age_end - age_start) %>%
  mutate(midage = (age_start + age_end) / 2) %>%
  filter((midage <= 18 & age_diff <= 5) | (midage > 18 & age_diff <= 10 | (midage > 76 & age_diff <= 30)))

asthma_age_split <- pre_agesplit %>%
  mutate(age_diff = age_end - age_start) %>%
  mutate(midage = (age_start + age_end) / 2) %>%
  filter((midage <= 18 & age_diff > 5) | (midage > 18 & age_diff > 10 | (midage > 76 & age_diff > 30)))


population <- get_population(age_group_id = age_ids, location_id = super_regions, year_id = "all", sex_id = c(1,2), release_id = 16)
population <- population %>%
  filter(year_id >= 1990)

#age
ages <- get_age_metadata(age_group_set_id = 24, release_id = 16)
age_ids <- c(6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 34, 235, 238)

merge_age <- ages %>%
  select(age_group_id, age_group_years_start, age_group_years_end)

#loc
locs <- get_location_metadata(35, release_id = 16)
super_regions <- locs %>%
  filter(level %in% 1) %>% pull(location_id)
merge_loc <- locs %>%
  select(location_id, super_region_id, super_region_name)

pre_split_supreg <- inner_join(asthma_age_split, merge_loc, by = "location_id")
unmerged_rows <- anti_join(asthma_age_split, merge_loc, by = "location_id")

pattern <- inner_join(asthma_age_pattern, merge_loc, by = "location_id")
population <- inner_join(population, merge_loc, by = "location_id")

pattern <- inner_join(pattern, merge_age, by = "age_group_id")

asthma_age_pattern <- get_draws(
  gbd_id_type = "modelable_entity_id",
  gbd_id = "gbd_id",
  source = "epi",
  location_id = super_regions,
  age_group_id = age_ids,
  sex_id = c(1,2),
  measure_id = 5, #prevalence
  year_id = 'all',
  metric_id = 3, 
  version_id = "version_id", # single model age
  release_id = 16)
#pydisagg - new age split function

library(reticulate)
reticulate::use_python("filepath")
splitter <-import("pydisagg.ihme.splitter")

pre_split_supreg <- pre_split_supreg %>%
  mutate(sex_id = case_when(
    sex == "Male" ~ 1,
    sex == "Female" ~ 2,
    TRUE ~ NA_real_
  ))

pre_split_supreg <- pre_split_supreg %>%
  mutate(midyear = (year_start + year_end) / 2) %>%
  mutate(year_adj = (year_end - year_start) / 2) %>%
  mutate(year_id = midyear)

pre_split_supreg <- pre_split_supreg %>%
  filter(sex != "Both")

pre_split_supreg <- pre_split_supreg %>%
  filter(!grepl(Russia, field_citation_value))

claims <- pre_split_supreg %>%
  filter(clinical_data_type == "claims" | clinical_data_type == "claims - flagged")

sci_lit_age_split <- pre_split_supreg[is.na(pre_split_supreg$clinical_version_id), ]


pre_split_supreg[, id := .I]

na_rows <- pre_split_supreg %>%
  filter(is.na(standard_error))

#asthma age pattern

pattern_test <- pattern %>%
  rowwise() %>%
  mutate(mean_draw = mean(c_across(starts_with("draw_"))))

pattern_test <- pattern_test %>%
  rowwise() %>%
  mutate(se_draw = sd(c_across(starts_with("draw_"))) / sqrt(1000))

pattern_test <- pattern_test %>% ungroup()

pattern <- pattern_test %>%
  select("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id", "age_group_years_start", "age_group_years_end", "mean_draw", "se_draw")

df_long <- pattern %>%
  pivot_longer(cols = c(mean_draw, se_draw), names_to = "metric", values_to = "value") %>%
  complete(location_id, age_group_id, metric, sex_id, year_id = 1990:2024)

df_interpolated2 <- df_long %>%
  group_by(location_id, age_group_id, sex_id, metric) %>%
  arrange(year_id) %>%
  mutate(value = na.approx(value, year_id, na.rm = FALSE)) %>%
  ungroup()

df_wide <- df_interpolated2 %>%
  pivot_wider(names_from = metric, values_from = value)

df_wide2 <- df_wide %>%
  dplyr::select(location_id, age_group_id, sex_id, year_id, mean_draw, se_draw)

df_wide2 <- inner_join(df_wide2, merge_loc, by = "location_id")
df_wide2 <- inner_join(df_wide2, merge_age, by = "age_group_id")

pattern_interp <- df_wide2

pattern_interp <- pattern_interp %>%
  mutate(
    lower = mean_draw - 1.96 * se_draw,  # Lower bound of 95% CI
    upper = mean_draw + 1.96 * se_draw,  # Upper bound of 95% CI
    check_within_bounds = between(mean_draw, lower, upper)  # Check if mean_draw is within bounds
  )

# View the first few rows to check the new columns
head(pattern_interp)

# View the first few rows of the reshaped data
head(df_wide)

library(zoo)

df_interpolated <- df_interpolated2 %>%
  mutate(sex = factor(sex_id, levels = c(1, 2), labels = c("Male", "Female")))

# Plot with separate lines for males and females
plot <- ggplot(df_interpolated, aes(x = year_id, y = value, color = metric, shape = sex)) +
  geom_line(aes(linetype = metric)) +
  geom_point(data = df_interpolated[!is.na(df_interpolated$value), ]) +
  facet_wrap(~ location_id + age_group_id) +
  labs(title = "Original and Interpolated Values by Sex", x = "Year", y = "Value") +
  theme_minimal() +
  theme(
    axis.title = element_text(size = 10),
    axis.text = element_text(size = 8),
    strip.text = element_text(size = 9),
    plot.title = element_text(size = 12, face = "bold"),
    legend.text = element_text(size = 8),
    legend.title = element_text(size = 10)
  )

print(plot)
ggsave("plot3.png", width = 10, height = 8, dpi = 300)

#config
data_config <- splitter$AgeDataConfig(
  index=c("nid", "seq", "year_id", "id","super_region_id", "location_id", "sex_id"),
  age_lwr="age_start",age_upr="age_end",
  val="mean",val_sd="standard_error")

unique_count <- pre_split_supreg %>% summarise(unique_values = n_distinct(id))
print(unique_count)


draw_cols <- grep("^draw_\\d+$", names(pattern), value = TRUE)

pattern_config <- splitter$AgePatternConfig(by=list("sex_id", "super_region_id", "year_id"),
                  age_key="age_group_id",
                  age_lwr="age_group_years_start",
                  age_upr="age_group_years_end",
                  val = "mean_draw",
                  val_sd = "se_draw")
                  #draws=draw_cols)

pop_config <- splitter$AgePopulationConfig(index=c("age_group_id", "super_region_id", "sex_id", "year_id"),val="population")
age_splitter <- splitter$AgeSplitter(data=data_config, pattern=pattern_config, population=pop_config)

result <- age_splitter$split(data=pre_split_supreg,pattern=pattern_interp,population=population,model="rate",output_type="rate")

##############VET Asthma Results

n_distinct(pre_split_supreg$nid)
n_distinct(result$nid)
result<-as.data.table(result)
colnames(result)

# fix result column names
result2<-as.data.table(result)

#result columns
result2$pre_age_split_mean<-result2$mean
result2$mean<-result2$age_split_result
result2$age_split_result<-NULL

#age cols
result2$orig_age_start<-result2$age_start
result2$orig_age_end<-result2$age_end

result2$age_start<-round(result2$pat_age_group_years_start, digit=2)
result2$age_end<-round(result2$pat_age_group_years_end,digit=2)

#uncertainty
result2$pre_age_split_standard_error<-result2$standard_error
result2$standard_error<-result2$age_split_result_se
result2$age_split_result_se<-NULL

final_result<-copy(result2)

#bind back all cols
save_col<-pre_split_supreg %>% select(-nid, -seq, -location_id, -year_start, -year_end, -sex_id, -age_start, -age_end,
                                -upper, -lower, -standard_error, - mean)

final_result<-merge(final_result,save_col, by="id")
unique(final_result$split)

final_result<-final_result[!is.na(split),split:=paste0(split,";Age")]
final_result<-final_result[is.na(split),split:="Age"]
final_result<-final_result[split %like% "Age", note_modeler:=paste0(note_modeler,"| age split using pydisagg, super region age pattern 1990-2024 from model version 862180")]

# calculate upper lower
final_result[,uncertainty_type_value:=NA]

#update crosswalk_parent_seq so that seq = "" and crosswalk_parent_seq == seq
final_result[, "crosswalk_parent_seq" := ("")] # used to track data processing

final_result <- final_result %>%
  mutate(crosswalk_parent_seq = seq, # Set crosswalk_parent_seq to the value of seq
         seq = "")

final_result <- final_result %>%
  mutate(year_start = year_id.x - year_adj,
         year_end = year_id.x + year_adj) %>%
  mutate(across(c(year_start, year_end), ceiling))

  #write results