##########################################################################################################
# Purpose: Calculate the total number of deaths from congenital syphilis (< 10 years) and save results 
#          1) Incorporate neonatal death rate with adjusted number of livebirths to get number of neonatal deaths
#          2) Apply global CoD age-sex patterns to get total deaths from congenital syphilis
##########################################################################################################
library(reshape)

# Read in country-specific data -------------------------------------------

pdf("FILEPATH")

message(paste(Sys.time(), "Reading in data for location_id", location_id))

# can read in, but not needed as it's stored in memory
# adjusted_live_births <- read_csv(paste0(out_dir, "adjusted_births/", location_id, "_adjusted.csv"),
#                             col_type = cols(
#                               location_id = col_integer(),
#                               year_id = col_double(),
#                               draw_num = col_character(),
#                               adj_untreated_early = col_double(),
#                               adj_untreated_late = col_double(),
#                               adj_inadequate_late = col_double(),
#                               adequate = col_double()
#                             ))

# Calculate neonatal deaths -----------------------------------------------

message(paste(Sys.time(), "Calculating neonatal deaths"))

emr <- readr::read_csv("FILEPATH") %>% 
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

cod_proportions <- readr::read_csv("FILEPATH")

# First, need to cross neonatal deaths df with global CoD proportions
cod_proportions_expanded <- reshape::untable(cod_proportions, nrow(neonatal_deaths))
neonatal_deaths_expanded <- reshape::untable(neonatal_deaths, nrow(cod_proportions))

# order df by loc/year/draw so cod proportions can be added straight on
neonatal_deaths_expanded <- neonatal_deaths_expanded[order(neonatal_deaths_expanded$location_id, 
                                                           neonatal_deaths_expanded$year_id, neonatal_deaths_expanded$draw_num), ]

# combine: now we have expanded df with cod proportions for each age/sex group under age 10
# can split the neonatal deaths by age/sex.
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
  ggplot(aes(age_group_id, mean, fill = as.factor(sex_id))) +
  geom_bar(stat = "identity", position = "dodge") +
  facet_wrap(~year_id) +
  labs(x = "Age group id (2: EN, 3: LN, 4: PN, 5: 1-4, 6: 5-9)",
       y = "Mean deaths")
plot(g)

dev.off()

# Save results as csv by year/sex for uploadD
year_id <- unique(draws_wide$year_id)

save_by_year_sex <- function(df, year, location = location_id) {
  message(paste0("Saving wide draws for location_id ", location, " in ", year))
  subset_male   <- filter(df, year_id == year, sex_id == 1)
  subset_female <- filter(df, year_id == year, sex_id == 2)
  readr::write_csv(subset_male, "FILEPATH")
  readr::write_csv(subset_female, "FILEPATH")
}

invisible(lapply(year_id, function(year_id) { save_by_year_sex(draws_wide, year = year_id)}))

message(paste(Sys.time(), "DONE"))





