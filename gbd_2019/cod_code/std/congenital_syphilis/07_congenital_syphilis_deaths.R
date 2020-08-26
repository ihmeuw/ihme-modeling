##########################################################################################################

# Purpose: Calculate the total number of deaths from congenital syphilis (< 10 years) and save results 
#          1) Incorporate neonatal death rate with adjusted number of livebirths to get number of neonatal deaths
#          2) Apply global CoD age-sex patterns to get total deaths from congenital syphilis
#          3) Save results and upload. Final step in pipeline!

# OUTPUTS: Final number of deaths from congenital syphilis; uploaded to database
#          Saved as {out_dir}/FILEPATH/{location_id}_{year_id}_{sex_id}.csv
#          Diagnostic pdfs at {out_dir}/FILEPATH/{location_id}_results.pdf

##########################################################################################################
library(reshape)

# Read in country-specific data -------------------------------------------

pdf(paste0(out_dir, FILEPATH, location_id, "_results.pdf"))

message(paste(Sys.time(), "Reading in data for location_id", location_id))

# Calculate neonatal deaths -----------------------------------------------

message(paste(Sys.time(), "Calculating neonatal deaths"))

emr <- readr::read_csv(paste0(out_dir, FILEPATH,"/excess_mortality_rates_by_stage.csv")) %>% 
  mutate(draw_num = draw_names) %>% 
  select(draw_num, early_nn_emr, untreated_late_nn_emr, inadeq_late_nn_emr)

# for each location - year (at the draw level), calculate the total number of congenital syphilis deaths
neonatal_deaths <- left_join(adjusted_live_births, emr, by = "draw_num") %>% 
  mutate(neonatal_deaths = adj_untreated_early * early_nn_emr + adj_untreated_late * untreated_late_nn_emr + adj_inadequate_late * inadeq_late_nn_emr) %>% 
  select(location_id, year_id, draw_num, neonatal_deaths)

# DIAGNOSTICS
library(ggplot2)
g <- ggplot(neonatal_deaths, aes(neonatal_deaths, fill = as.factor(year_id))) +
  geom_histogram() +
  facet_wrap(~year_id) +
  labs(title = paste0("Neonatal deaths in location ", location_id)) +
  theme(legend.position = "none")
plot(g)

# Apply the CoD age - sex pattern ------------------------------------------

message(paste(Sys.time(), "Applying CoD age-sex pattern"))

cod_proportions <- readr::read_csv(paste0(out_dir, FILEPATH,"/global_cod_ratios.csv"))

# First, need to cross neonatal deaths df with global CoD proportions
cod_proportions_expanded <- reshape::untable(cod_proportions, nrow(neonatal_deaths))
neonatal_deaths_expanded <- reshape::untable(neonatal_deaths, nrow(cod_proportions))

# order df by loc/year/draw so cod proportions can be added straight on
neonatal_deaths_expanded <- neonatal_deaths_expanded[order(neonatal_deaths_expanded$location_id, 
                                                           neonatal_deaths_expanded$year_id, neonatal_deaths_expanded$draw_num), ]

# combine: now we have expanded df with cod proportions for each age/sex group under age 10
# can split the neonatal deaths by age/sex.
# remember the total deaths for each loc/year/draw will add up to greater than the neonatal deaths!
#   this is b/c we're splitting into older congenital deaths as well
crossed <- cbind(neonatal_deaths_expanded, cod_proportions_expanded) %>% 
  mutate(value = neonatal_deaths * ratio,
         draw = "draw_",
         metric_id = 1) %>% 
  select(location_id, year_id, draw_num, age_group_id, sex_id = sex, metric_id, draw, value)

# DIAGNOSTICS
 ggplot(crossed) +
   geom_density(aes(value, fill = as.factor(age_group_id)), alpha = 1/3) +
   facet_wrap(~sex_id) +
   labs(x = "Neonatal deaths",
        title = paste0("Neonatal deaths (number) by sex (1: males, 2: females) in location ", location_id))


# Reshape back to wide ----------------------------------------------------
message(paste(Sys.time(), "Reshape draws back to wide"))

# need sum as aggregating function bc we need something and the length of collapsing cols is 1 so not actually summing anything
draws_wide <- crossed %>% as.data.table() %>% 
  dcast(location_id + year_id + age_group_id + sex_id + metric_id ~ draw_num, value.var = c("value"), fun.aggregate = sum)

# set proper draw names
old_names <- names(draws_wide)
draw_names <- paste0("draw_", 0:999)
new_names <- c("location_id", "year_id", "age_group_id", "sex_id", "metric_id", draw_names)

setnames(draws_wide, old = old_names, new = new_names)

# DIAGNOSTICS
# summarize draws and graph by age group
copy <- copy(as.data.table(draws_wide))
copy[, mean := rowMeans(.SD), .SDcols = draw_names]
copy <- copy[, (draw_names) := NULL]

g <- copy %>% 
  #filter(year_id == 2000) %>% 
  ggplot(aes(age_group_id, mean, fill = as.factor(sex_id))) +
  geom_bar(stat = "identity", position = "dodge") +
  facet_wrap(~year_id) +
  labs(x = "Age group id (2: EN, 3: LN, 4: PN, 5: 1-4, 6: 5-9)",
       y = "Mean deaths")
plot(g)

dev.off()

# Save results as csv by year/sex for upload -------------------------
year_id <- unique(draws_wide$year_id)

save_by_year_sex <- function(df, year, location = location_id) {
  message(paste0("Saving wide draws for location_id ", location, " in ", year))
  subset_male   <- filter(df, year_id == year, sex_id == 1)
  subset_female <- filter(df, year_id == year, sex_id == 2)
  readr::write_csv(subset_male, paste0(out_dir, "results/", location, "_", year, "_1.csv"))
  readr::write_csv(subset_female, paste0(out_dir, "results/", location, "_", year, "_2.csv"))
}

invisible(lapply(year_id, function(year_id) { save_by_year_sex(draws_wide, year = year_id)}))

message(paste(Sys.time(), "DONE"))





