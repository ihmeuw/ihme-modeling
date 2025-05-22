############################################################################################################
# POST HOC ADJUSTMENT OF SEROPREVALENCE OF HEPATITIS B HBSAG
# Goal:     substract the number of people experienced vaccination from the counterfactual model
# Output:   1000 draws in number space
#
############################################################################################################

rm(list=ls())

pacman::p_load(data.table, openxlsx, ggplot2)
library(gridExtra)
library(grid) 
date <- gsub("-", "_", Sys.Date())


# SOURCE SHARED FUNCTIONS -------------------------------------------------

source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/get_population.R")



# SET OBJECTS -------------------------------------------------------------
# Hepatitis B 3-dose coverage (proportion)
# Fraction of 12-23 month-old children born in a given geography-year vaccinated with at least 3 doses of the hepatitis B vaccine

# GET ARGS ----------------------------------------------------------------
args<-commandArgs(trailingOnly = TRUE)
save_dir <- args[1]
map_path <-args[2]
xw_version <- args[3]

locations <- fread(map_path)
task_id <- ifelse(is.na(as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))), 1, as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID")))
loc_id <- locations[task_num == task_id, location]
# cov_id <- OBJECT
age_cov_id <- OBJECT  #covariate for "Hepatitis B vaccine coverage (proportion), aged through time" 
                      #detailed description of covariate is: "Proportion of people vaccinated with at least three doses of 
                      #                                      Hepatitis B vaccine, aged through time from 12-23 months"		

r_id <- OBJECT
years <- c(seq(1990, 2015, by = 5), 2000:2024)
ages <- c(164, 2, 3, 6:20, 30:32, 34, 235, 238, 388, 389)
measure <- 6 # need to change this to 5 or 6 and run parent twice

draws <- paste0("draw_", c(0:999))
p_draws <- paste0("p_draw_", c(0:999))
f_draws <- paste0("f_draw_", c(0:999))

age_dt <- get_age_metadata()
age_dt2 <- age_dt[age_group_id %in% ages, ]
age_dt2[, midage := (age_group_years_start + age_group_years_end) / 2]
age_dt2 <- age_dt2[, .(age_group_id, midage)]

loc_dt <- get_location_metadata()
loc_dt <- loc_dt[, .(location_id, location_name)]
loc_name <- loc_dt[location_id == loc_id, location_name]

sex_dt <- get_ids("sex")

# GET COVARIATE ESTIMATES 

dt_vac <- get_covariate_estimates(
  covariate_id=age_cov_id,
  release_id = r_id,
  location_id= loc_id)

message("Finished getting covariate estimates")


dt_vac <- dt_vac[, -c("model_version_id", "covariate_name_short", "sex", "sex_id", "lower_value", "upper_value")]
drop_ages <- c(2, 3, 388, 389, 164) #corresponds to EN, PN, 1-5 months, 6-11 months, and birth

dt_vac_fixed <- dt_vac[!(age_group_id %in% drop_ages), ]

vac_fill <- copy(dt_vac[age_group_id == 238, ])


vac_2 <- copy(vac_fill)
vac_2[, `:=` (age_group_id = 2, age_group_name = "Early Neonatal")]

vac_3 <- copy(vac_fill)
vac_3[, `:=` (age_group_id = 3, age_group_name = "Late Neonatal")]

vac_388 <- copy(vac_fill)
vac_388[, `:=` (age_group_id = 388, age_group_name = "1-5 months")]

vac_389 <- copy(vac_fill)
vac_389[, `:=` (age_group_id = 389, age_group_name = "6-11 months")]

vac_164 <- copy(vac_fill)
vac_164[, `:=` (age_group_id = 164, age_group_name = "Birth")]

dt_vac <- rbind(dt_vac_fixed, vac_2, vac_3, vac_388, vac_389, vac_164)

# dt_vac <- dt_vac[year_id > 1989, ]

message("Dropped unneeded columns for vaccinate estimates")


# PULL MODEL ESTIMATES

# Get the estimates from the counterfactual model 
dt_sero <- get_draws(gbd_id_type = "modelable_entity_id", 
                     gbd_id = OBJECT, 
                     source="epi", 
                     location_id=loc_id, 
                     year_id=years, 
                     measure_id = measure,
                     release_id = r_id)

message("Finished getting DisMod draws")


pop <- get_population(age_group_id = ages, 
                      location_id = loc_id, 
                      year_id = years, 
                      sex_id = c(1,2), 
                      release_id = r_id)
pop$run_id <- NULL

# setnames(dt_sero, "model_version_id", "dismod_model_version_id")

# MERGE SEROPREVALENCE AND VACCINATION ESTIMATES TOGETHER 
# MULTIPLY MEAN COVARIATE ESTIMATE BY DRAWS 
# SUBTRACT SEROPREVALENCE DRAW i BY SEROPREV DRAW i * PROPORTION 
# Using 95% vaccine efficacy based on CDC article https://www.cdc.gov/vaccines/pubs/pinkbook/hepb.html

#first calculate #cases (counterfactual)
dt_all <- merge(dt_sero, dt_vac, by = c("year_id", "age_group_id", "location_id"))
dt_all <- merge(dt_all, pop, by = c("age_group_id", "location_id", "year_id", "sex_id"))

dt_all[, (draws) := lapply(.SD, function(x) population * x), .SDcols = draws, 
       by = c("location_id", "year_id", "age_group_id", "sex_id")]

message("Merged draws")

# Calculate mean, upper and lower for DisMod seroprevalence draws
raw_estimates <- copy(dt_all)
raw_estimates[, dis_mean := rowMeans(.SD), .SDcols = draws]
raw_estimates[, dis_lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draws]
raw_estimates[, dis_upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draws]
raw_estimates[, (draws) := NULL]
raw_estimates_final <- copy(raw_estimates[, c("location_id", "location_name", "age_group_id", "sex_id", "year_id", "dis_mean", "dis_lower", "dis_upper")])

# keep_draws <- paste0("draw_", 0:2)
# keep_names <- names(dt_all)[!grepl("draw_", names(raw_draws))]
# keep <- append(keep_names, keep_draws)
# raw_draws <- raw_draws[, keep, with = FALSE]

# Calculate p draws by multiplying vaccine proportion coverage * vaccine efficacy * draw value
dt_all[, (p_draws) := lapply(.SD, function(x) mean_value * 0.95* x), .SDcols = draws,
       by = c("location_id", "year_id", "age_group_id")]

message("Calculated p_draws")

# Subset to only the seroprev draws
keep_cols <- names(dt_all)[!grepl("draw", names(dt_all))]
keep_cols1 <- append(keep_cols, draws)
sero_draws <- copy(dt_all[, (keep_cols1), with = FALSE])
sero_draws$version <- "seroprev"

# Subset to only the proportion draws
keep_cols2 <- append(keep_cols, p_draws)
prop_draws <- copy(dt_all[, (keep_cols2), with = FALSE])
setnames(prop_draws, p_draws, draws)
prop_draws$version <- "prop"


# Estimate the mean, upper, and lower of the proportion draws
p_estimates <- copy(prop_draws)
p_estimates[, p_mean := rowMeans(.SD), .SDcols = draws]
p_estimates[, p_lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draws]
p_estimates[, p_upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draws]
p_estimates[, (draws) := NULL]
p_estimates_final <- copy(p_estimates[, c("location_id", "location_name", "age_group_id", "sex_id", "year_id", "p_mean", "p_lower", "p_upper")])

# NEED TO RESHAPE TO BE ABLE TO SUBTRACT ESTIMATES -------------------------------------------

# Bind the full draw sets for seroprevalence and proportion draws together
all_draws <- rbind(sero_draws, prop_draws)
all_draws <- all_draws[, -c("metric_id", "model_version_id", "modelable_entity_id", "covariate_id", "mean_value")]


# Then melt
all_draws_melt <- melt.data.table(all_draws, measure.vars = draws)

# Then dcast
all_draws_wide <- dcast.data.table(all_draws_melt, location_id + age_group_id + sex_id + year_id + variable ~ version, value.var = "value")

# Then subtract!
all_draws_wide[, final := seroprev - prop]

# Dcast again to (1) so draws are wide for upload (2) so draws are in an easy format to compute upper and lower
all_draws_wide2 <- copy(all_draws_wide[, -c("prop", "seroprev")])
all_draws_wide2$version <- "final"
all_draws_wide2 <- dcast.data.table(all_draws_wide2, location_id + age_group_id + sex_id + year_id ~ variable, value.var = "final")


# Get upper and lower of final draws
f_estimates <- copy(all_draws_wide2)
f_estimates[, f_mean := rowMeans(.SD), .SDcols = draws]
f_estimates[, f_lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draws]
f_estimates[, f_upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draws]
f_estimates[, (draws) := NULL]
f_estimates <- copy(f_estimates[, c("location_id", "age_group_id", "sex_id", "year_id", "f_mean", "f_lower", "f_upper")])
f_estimates_final <- merge(f_estimates, loc_dt, by = "location_id")

# CREATE DRAW FILE FOR SAVE_RESULTS_EPI

dt_draws_final <- copy(all_draws_wide2)
# 
# dt_draws_final <- merge(dt_draws_final, pop, by = c("age_group_id", "location_id", "year_id", "sex_id"))
# dt_draws_final[, (draws) := lapply(.SD, function(x) x / population ), .SDcols = draws, by = c("location_id", "year_id", "age_group_id", "sex_id")]

dt_draws_final$measure_id <- measure
dt_draws_final$metric_id <- 1

message("Writing file")

if (!dir.exists(paste0(save_dir, "draw_files/"))) dir.create(paste0(save_dir, "draw_files/"))
write.csv(dt_draws_final, paste0(save_dir,"draw_files/", measure, "_", loc_id, ".csv"), row.names = FALSE)


# PLOT THE PROPORTION OF COVERAGE----------------------------------------------

# # PREPARE THE DATA ------------------------------------------------------------
# plotting_data <- merge(raw_estimates_final, p_estimates_final, by = c("location_id", "age_group_id", "sex_id", "year_id"))
# plotting_data <- merge(plotting_data, f_estimates_final, by = c("location_id", "age_group_id", "sex_id", "year_id"))
# plotting_data <- merge(plotting_data, age_dt, by = "age_group_id")
# plotting_data <- merge(plotting_data, sex_dt, by = "sex_id")
# 
# for (i in c(1,2)) {
#   sex_name <- sex_dt[sex_id == i, sex]
#   plotting_data2 <- plotting_data[sex_id == i, ]
#   sero_plot <- ggplot() +
#   geom_smooth(data = plotting_data2, aes(x = midage, y = dis_mean, color = "Seroprevalence"), se = FALSE, size = 0.8, alpha = 0.5) +
#   geom_smooth(data = plotting_data2, aes(x = midage, y = p_mean, color = "Seroprev * Prop"), se = FALSE, size = 0.8, alpha = 0.5) +
#   geom_smooth(data = plotting_data2, aes(x = midage, y = f_mean , color = "Final"), se = FALSE, size = 0.8, alpha = 0.5) +
#   scale_color_manual(name = "Estimate Type",
#                      values=c(Seroprevalence="blue4", `Seroprev * Prop` = "red3",  Final = "green4")) +
#   theme(plot.title = element_text(size = 18), axis.title = element_text(size = 14)) +
#   theme_bw() + scale_y_continuous(limits = c(0, max(plotting_data2$dis_upper)))
# }
# 



# crosswalk_data <- get_crosswalk_version(xw_version)
# xw_data <- crosswalk_data[location_id == loc_id & measure == "prevalence", ]
# xw_data[, midyear := (year_start + year_end)/2]
# xw_data[, midage := (age_start + age_end) / 2]
# 
# nrow(xw_data[sex == "Male" & measure == "prevalence"])
# 
# plotting_map <- data.table(
#   midyear = c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019),
#   cond_yr_start = c(1985, 1990, 1995, 2000, 2005, 2010, 2012, 2014),
#   cond_yr_end = c(1995, 2000, 2005, 2010, 2015, 2019, 2019, 2019)
# )
# 
# # CREATE LEGEND ---------------------------------------------------
# g_legend <- function(a.gplot){
#   tmp <- ggplot_gtable(ggplot_build(a.gplot))
#   leg <- which(sapply(tmp$grobs, function(x) x$name) == "guide-box")
#   legend <- tmp$grobs[[leg]]
#   legend
# }
# 
# sero_plot <- ggplot() +
#   geom_smooth(data = plotting_data2, aes(x = midage, y = dis_mean, color = "Seroprevalence"), se = FALSE, size = 0.8, alpha = 0.5) +
#   geom_smooth(data = plotting_data2, aes(x = midage, y = p_mean, color = "Seroprev * Prop"), se = FALSE, size = 0.8, alpha = 0.5) +
#   geom_smooth(data = plotting_data2, aes(x = midage, y = f_mean , color = "Final"), se = FALSE, size = 0.8, alpha = 0.5) +
#   scale_color_manual(name = "Estimate Type",
#                      values=c(Seroprevalence="blue4", `Seroprev * Prop` = "red3",  Final = "green4")) +
#   theme(plot.title = element_text(size = 18), axis.title = element_text(size = 14)) +
#   theme_bw() + scale_y_continuous(limits = c(0, max(plotting_data2$dis_upper)))

# legend <- g_legend(sero_plot)
# 
# # PLOTTING FUNCTION
# if (measure = 5) {
# for (i in c(1,2)) {
#   sex_name <- sex_dt[sex_id == i, sex]
#   xw_data_sex <- xw_data[sex == sex_name, ]
#   plotting_data2 <- plotting_data[sex_id == i, ]
#   for (year in unique(plotting_map$midyear)) {
#     yr_start <- plotting_map[midyear == year, cond_yr_start]
#     yr_end <- plotting_map[midyear == year, cond_yr_end]
#     year <- plotting_map[midyear == year, midyear]
#     xw_data_yr <- xw_data_sex[midyear >= yr_start & midyear <= yr_end]
#     if (measure == 5) {
#     graph_seroprev <- ggplot() +
#       geom_smooth(data = plotting_data2[year_id == year], aes(x = midage, y = dis_mean, color = "Seroprevalence"), se = FALSE, size = 0.8, alpha = 0.5) +
#       geom_smooth(data = plotting_data2[year_id == year], aes(x = midage, y = dis_upper, color = "Seroprevalence_Upper"), se = FALSE, linetype = "dashed", size = 0.4, alpha = 0.5) +
#       geom_smooth(data = plotting_data2[year_id == year], aes(x = midage, y = dis_lower, color = "Seroprevalence_Lower"), se = FALSE, linetype = "dashed", size = 0.4, alpha = 0.5) +
#       geom_smooth(data = plotting_data2[year_id == year], aes(x = midage, y = p_mean, color = "Seroprev * Prop"), se = FALSE, size = 0.8, alpha = 0.5) +
#       geom_smooth(data = plotting_data2[year_id == year], aes(x = midage, y = p_upper, color = "Seroprev * Prop Upper"), se = FALSE, linetype = "dashed", size = 0.4, alpha = 0.5) +
#       geom_smooth(data = plotting_data2[year_id == year], aes(x = midage, y = p_lower, color = "Seroprev * Prop Lower"), se = FALSE, linetype = "dashed", size = 0.4, alpha = 0.5) +
#       geom_smooth(data = plotting_data2[year_id == year], aes(x = midage, y = f_mean , color = "Final"), se = FALSE, size = 0.8, alpha = 0.5) +
#       geom_smooth(data = plotting_data2[year_id == year], aes(x = midage, y = f_upper, color = "Final_Upper"), se = FALSE, linetype = "dashed", size = 0.4, alpha = 0.5) +
#       geom_smooth(data = plotting_data2[year_id == year], aes(x = midage, y = f_lower, color = "Final_Lower"), se = FALSE, linetype = "dashed", size = 0.4, alpha = 0.5) +     xlab('Mid-Age') +
#       # facet_wrap(~year_id) +
#       # ylab('Mean Seroprevalence') +
#       scale_color_manual(name = "Estimate Type",
#                        values=c(Seroprevalence="blue4", Seroprevalence_Upper = "blue3", Seroprevalence_Lower = "blue3",
#                                 `Seroprev * Prop` = "red3", `Seroprev * Prop Upper` = "red2", `Seroprev * Prop Lower` = "red2",
#                                 Final = "green4", Final_Upper = "darkolivegreen4", Final_Lower = "darkolivegreen4",
#                                 Outliered = "red2", Not_Outliered = "grey")) +
#       labs("Estimate Type") +
#       ggtitle(paste(year)) +
#       theme(plot.title = element_text(size = 6)) +
#       theme_bw() + theme(legend.position = "none", axis.title.x = element_blank(), axis.title.y = element_blank()) + scale_y_continuous(limits = c(0, max(plotting_data2$dis_upper)))
#     if (nrow(xw_data_yr[is_outlier == 1]) > 0) {
#       graph_seroprev <- graph_seroprev +
#       geom_point(data = xw_data_yr[is_outlier == 1], aes(x = midage, y = mean, color = "Outliered"), size = 1) +
#       scale_color_manual(name = "Estimate Type",
#                         values=c(Seroprevalence="blue4", Seroprevalence_Upper = "blue3", Seroprevalence_Lower = "blue3",
#                                  `Seroprev * Prop` = "red3", `Seroprev * Prop Upper` = "red2", `Seroprev * Prop Lower` = "red2",
#                                  Final = "green4", Final_Upper = "darkolivegreen4", Final_Lower = "darkolivegreen4",
#                                  Outliered = "red2", Not_Outliered = "grey"))
#       }
#     if (nrow(xw_data_yr[is_outlier == 0]) > 0) {
#       graph_seroprev <- graph_seroprev +
#          geom_point(data = xw_data_yr[is_outlier == 0], aes(x = midage, y = mean, color = "Not_Outliered"), size = 1) +
#          scale_color_manual(name = "Estimate Type",
#                             values=c(Seroprevalence="blue4", Seroprevalence_Upper = "blue3", Seroprevalence_Lower = "blue3",
#                                      `Seroprev * Prop` = "red3", `Seroprev * Prop Upper` = "red2", `Seroprev * Prop Lower` = "red2",
#                                      Final = "green4", Final_Upper = "darkolivegreen4", Final_Lower = "darkolivegreen4",
#                                      Outliered = "red2", Not_Outliered = "grey"))
#      }
#   assign(paste0("plot_", year), graph_seroprev)
#   }
#   plot_title <- textGrob(paste0("HBsAg Seroprevalence for ", loc_name, ", ", sex_name),gp=gpar(fontsize=14))
#   x_axis <- textGrob("Age",gp=gpar(fontsize=14))
#   y_axis <- textGrob("Mean Seroprevalence", gp=gpar(fontsize=14), rot = 90)
#   lay <- rbind(c(1,2,3),
#                c(4,5,6),
#                c(7,8,9))
#   g <- arrangeGrob(plot_1990, plot_1995, plot_2000, plot_2005, plot_2010, plot_2015, plot_2017, plot_2019,
#                            legend, layout_matrix = lay, top = plot_title, bottom = x_axis, left = y_axis)
#   ggsave(file=paste0(save_dir,"pdfs/", measure, "_", loc_id, "_", i, ".pdf"), g, width = 10, height = 12)
#   }
#   }
#   dev.off()
# }