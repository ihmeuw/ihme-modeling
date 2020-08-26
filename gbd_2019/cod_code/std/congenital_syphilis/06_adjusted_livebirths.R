##########################################################################################################
# Purpose: Need to take increased risk of APO for syphilitic women into account: 
#          1) Take the unadjusted live births (by location and early/late), split them into livebirths by visit proportion 
#          2) and then apply treatment definitions to split by treatment status.
#          3) Apply: live births * (1 - early/late death rate) to get the adjusted live births for each treatment group
#               Adequately treated is untouched

# OUTPUTS: number of live births from syphilitic women, adjusted by the increased risk for adverse pregnancy outcomes
#          Saved as {out_dir}/FILEPATH/{location_id}_adjusted.csv
#          Diagnostic pdfs at {out_dir}/FILEPATH/{location_id}_diagnostics.pdf

##########################################################################################################
library(readr)

# Read in country-specific data -------------------------------------------
pdf(paste0(out_dir, FILEPATH, location_id, "_diagnostics.pdf"))

message(paste(Sys.time(), "Beginning live birth adjustment for location_id", location_id))

# specifying column parsers bc in locations where ANC 2 - 3 is often (but not always) 0
# we end up with a bunch of NAs
anc_visits <- read_csv(paste0(out_dir, FILEPATH, location_id, ".csv"),
                       col_types = cols(
                         location_id = col_integer(),
                         year_id = col_integer(),
                         draw_num = col_character(),
                         draw_0_1 = col_double(),
                         draw_2_3 = col_double(),
                         draw_4 = col_double()
                       ))


# Calculate the est. live births for each ANC visit group -----------------
# Currently 0 - 1, 2 - 3, 4+

message(paste(Sys.time(), "Calculating estimated live births for each ANC visit group by stage"))

both <- left_join(unadjusted_births, anc_visits, by = c("location_id", "year_id", "draw_num"))

# calculate births for the (currently) 3 ANC visit groups and drop unneeded rows
births_by_visit <- both %>% 
  mutate(early_0_1 = early_unadj_live_births * draw_0_1, # num births from women with 0 - 1 ANC clinic visits
         late_0_1  = late_unadj_live_births  * draw_0_1,
         early_2_3 = early_unadj_live_births * draw_2_3, # 2 - 3 ANC clinic visits
         late_2_3  = late_unadj_live_births  * draw_2_3,
         early_4   = early_unadj_live_births * draw_4,   # 4+ ANC clinic visits
         late_4    = late_unadj_live_births  * draw_4) %>%  
  select(location_id, year_id, draw_num, early_0_1:late_4)

# DIAGNOSTICS: visit proportions
library(ggplot2)
g <- ggplot(births_by_visit) +
  geom_histogram(aes(early_0_1), fill = "blue",   alpha = 1/2) +
  geom_histogram(aes(early_2_3), fill = "red", alpha = 1/2) +
  geom_histogram(aes(early_4),   fill = "green",  alpha = 1/2) +
  labs(title = paste0("Early syphilis births by ANC visit proportions in location ", location_id, "\n0: blue, 1-3: red, 4+: green")) + 
  facet_wrap(~year_id)
plot(g)

# Create proportion that both test and treat ---------------------------------------------
# create groups of early/late untreated, inadeqautely treated and adequately treated

message(paste(Sys.time(), "Creating test * treat proportions"))

test  <- readr::read_csv(paste0(out_dir, FILEPATH,  location_id, ".csv")) %>% 
  select(location_id, year_id, draw_num, test = draw)

treat <- readr::read_csv(paste0(out_dir, FILEPATH, location_id, ".csv")) %>% 
  select(location_id, year_id, draw_num, treat = draw)

# create proportion that both tests and treats
test_and_treat <- left_join(test, treat, by = c("location_id", "year_id", "draw_num")) %>% 
  mutate(test_and_treat = test * treat) %>% 
  select(location_id, year_id, draw_num, test_and_treat)

# DIAGNOSTICS: TEST AND TREAT
ggplot(test_and_treat, aes(test_and_treat, group = as.factor(year_id), fill = as.factor(year_id))) +
  geom_histogram() +
  facet_wrap(~year_id) +
  labs(title = paste0("Testing and treating proportion in location ", location_id))


# Merge test and treatment proportions onto birth groups 
groups <- left_join(births_by_visit, test_and_treat, by = c("location_id", "year_id", "draw_num"))


# Apply treatment definitions ---------------------------------------------

message(paste(Sys.time(), "Applying treatment definitions"))

#' ASSUMING NO RAPID TESTING aka testing and treatment cannot happen on the same day
#' `EARLY:`` 2+ visits is enough for adequate treatment
#' `LATE:``  2 - 3 visits is inadequate for late syphilis
#' `LATE:`` 4+ visits is adeqyate for late syphilis
#' `ADEQUATE:`` no EMR from adequate treatment
treatment_groups <- groups %>% 
  mutate(untreated_early = early_0_1 + (early_2_3 + early_4) * (1 - test_and_treat),
         adequate_early  = (early_2_3 + early_4) * test_and_treat,
         untreated_late  = late_0_1  + (late_2_3 + late_4)   * (1 - test_and_treat),
         inadequate_late = late_2_3 * test_and_treat,
         adequate_late   = late_4   * test_and_treat,
         adequate        = adequate_early + adequate_late) %>% 
  select(location_id, year_id, draw_num, untreated_early:adequate) %>% 
  select(-adequate_early, -adequate_late) # drop already aggregated sub adequate categories


# save treatment groups
write_csv(treatment_groups, paste0(out_dir, FILEPATH, location_id, "_treatment.csv"))

# Calculate adjusted live births ------------------------------------------

message(paste(Sys.time(), "Calculating adjusted live births"))

fetal_death_rate <- readr::read_csv(paste0(out_dir, FILEPATH, "/excess_mortality_rates_by_stage.csv")) %>%
  mutate(draw_num = draw_names) %>% 
  select(-early_nn_emr, -untreated_late_nn_emr, -inadeq_late_nn_emr, -draw_names) 


# Adjust the estimated live births from uninfected women (general population)
# by multiplying by 1 - excess fetal loss proportion
adjusted_live_births <- left_join(treatment_groups, fetal_death_rate, by = "draw_num") %>% 
  mutate(adj_untreated_early = untreated_early * (1 - early_fetal_emr),
         adj_untreated_late  = untreated_late  * (1 - untreated_late_fetal_emr),
         adj_inadequate_late = inadequate_late * (1 - inadeq_late_fetal_emr),
         fetal_loss = untreated_early * early_fetal_emr + 
                      untreated_late  * untreated_late_fetal_emr + 
                      inadequate_late * inadeq_late_fetal_emr) %>% 
  select(location_id, year_id, draw_num, adj_untreated_early:adj_inadequate_late, fetal_loss, adequate)


# DIAGNOSTICS  
g <- ggplot(adjusted_live_births, aes(adj_untreated_early, group = as.factor(year_id), fill = as.factor(year_id))) +
  geom_histogram() +
  facet_wrap(~year_id) +
  labs(title = paste0("Adjusted live births: untreated early in location ", location_id))
plot(g)
g <- ggplot(adjusted_live_births, aes(adj_untreated_late, group = as.factor(year_id), fill = as.factor(year_id))) +
  geom_histogram() +
  facet_wrap(~year_id) +
  labs(title = paste0("Adjusted live births: untreated late in location ", location_id))
plot(g)
g <- ggplot(adjusted_live_births, aes(adj_inadequate_late, group = as.factor(year_id), fill = as.factor(year_id))) +
  geom_histogram() +
  facet_wrap(~year_id) +
  labs(title = paste0("Adjusted live births: inadequate late in location ", location_id))
plot(g)
g <- ggplot(adjusted_live_births, aes(adequate, group = as.factor(year_id), fill = as.factor(year_id))) +
  geom_histogram() +
  facet_wrap(~year_id) +
  labs(title = paste0("Adjusted live births: adequate treatment in location ", location_id))
plot(g)  

dev.off()

# save fetal loss wide
fetal_loss <- select(adjusted_live_births, location_id, year_id, draw_num, fetal_loss) %>% 
  as.data.table() %>% 
  dcast(location_id + year_id ~ draw_num, value.var = c("fetal_loss"), fun.aggregate = sum)

write_csv(fetal_loss, paste0(out_dir, FILEPATH, location_id, ".csv"))

adjusted_live_births <- select(adjusted_live_births, -fetal_loss)

# save adjusted births result as adjusted_births/{location_id}.csv
message(paste0(Sys.time(), " Saving adjusted birth results in ", out_dir, FILEPATH, location_id, "_adjusted.csv"))

readr::write_csv(adjusted_live_births, paste0(out_dir, FILEPATH, location_id, "_adjusted.csv"))

# Source neonatal deaths script -------------------------------------------

# clear up memory by deleting all but end data set
rm(list = setdiff(ls(), c("out_dir", "root_dir", "location_id", "adjusted_live_births")))

# LAUNCH IT BABY
source(paste0(root_dir, "07_congenital_syphilis_deaths.R"))
  
  
