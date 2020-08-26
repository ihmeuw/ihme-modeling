#SETUP
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- FILEPATH
  h_root <- FILEPATH
} else {
  j_root <- FILEPATH
  h_root <- FILEPATH
}
user <- Sys.info()["user"]
source(paste0(FILEPATH, "/function_lib.R"))
library("openxlsx")
library("data.table")

#SOURCE FUNCTIONS-------------------------------------------------------------------------------
functions_dir <- FILEPATH
functs <- c("get_draws")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))

##ARGS & DIRS-----------------------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
print(args)

out_dir <- as.character(args[1])
code_dir<- as.character(args[2])
loc_id <- as.numeric(args[3])
reg_status <- as.character(args[4])
parent_me <- as.numeric(args[5])
eo_meid <- as.numeric(args[6])
mild_meid <- as.numeric(args[7])
asymp_meid <- as.numeric(args[8])

print(paste0("check! the reg status for this location is ", reg_status))
draws <- paste0("draw_", 0:999)
years <- c(1990,1995,2000,2005,2010,2015,2017,2019)

#PROPS SHEET-----
splits_meta <- data.table(read.xlsx("FILEPATH/chla_gono_metadata.xlsx"))

#DURATIONS-------
eo_dur <- 3/52
mild_dur <- 1/52
asymp_dur <- 52/52

#get draws of parent model for both males and females of prevalence
parent_prev <-get_draws(gbd_id_type = "modelable_entity_id", gbd_id = parent_me, source = "epi", measure_id = 5, location_id = loc_id,
                        year_id = years, sex_id = c(1,2), status = "best", gbd_round_id = 6, decomp_step = "step4")
parent_prev[ ,c("model_version_id", "modelable_entity_id") := NULL]

#melt it long
#should be 368,000 rows (8 yrs*2 sexes*23 ages)
prev_long <- melt(data = parent_prev, id.vars = c("age_group_id", "year_id", "location_id", "measure_id", "metric_id", "sex_id"), variable.name = "draw", value.name = "draw_val")

#METHODOLOGY
#'total prev = eo prev + mild prev + asymptomatic prev
#'eo prev = total incidence * (eo mildprop * eo_dur)
#'mild prev = total incidence * (male mildprop * mild_dur)
#'asymp prev = total incidence * (male asymp prop * asymp dur)

#'total prev = [total incidence * (male eo prop * eo_dur)] + [total incidence * (male mild prop * mild_dur)] + [total incidence * (male asymp prop * mild dur)]
#'total prev = total incidence * [(male eo prop*eo dur) + (male mild prop*mild dur) + (male asymp prop*asymp dur)]
#'total incidence = total prev/all sev divisor

#CODE
#get the male prev dt

male_prev <- prev_long[sex_id == 1, ]

#male prop vars
male_eo_prop <- splits_meta[parent_meid == parent_me & sex == "male" & world_region == reg_status , eo_prop]
male_mild_prop <- splits_meta[parent_meid == parent_me & sex == "male" & world_region == reg_status, mild_prop]
male_asymp_prop <- splits_meta[parent_meid == parent_me & sex == "male" & world_region == reg_status, asymp_prop]
male_sev_divisor <- (male_eo_prop*eo_dur) + (male_mild_prop*mild_dur) + (male_asymp_prop*asymp_dur)

#now get dt with prev and inc for each sequela
male_dt <- copy(male_prev)
setnames(x = male_dt, old = "draw_val", new = "total_prev")

#total male inc
male_dt[ ,total_inc := (total_prev/male_sev_divisor)]

#now get male eo inc, mild inc, asymp inc
male_dt[ ,eo_inc := total_inc*male_eo_prop]
male_dt[ ,mild_inc := total_inc*male_mild_prop]
male_dt[ ,asymp_inc := total_inc*male_asymp_prop]

#now get male eo prev, mild prev, asymp prev
male_dt[ ,eo_prev := (total_inc*male_eo_prop*eo_dur)]
male_dt[ ,mild_prev := (total_inc*male_mild_prop*mild_dur)]
male_dt[ ,asymp_prev := (total_inc*male_asymp_prop*asymp_dur)]

#write for loop that will split out each dataset in measure by sequela, than dcast the dataset, then add the measure id column, then save the file
col_dt <-data.table(colname = c("eo_prev", "eo_inc", "mild_prev", "mild_inc", "asymp_prev", "asymp_inc"), measure = c(5,6,5,6,5,6), meid = c(eo_meid, eo_meid, mild_meid, mild_meid, asymp_meid, asymp_meid))
col_dt

for (i in 1:nrow(col_dt)){
  #id vars
  col <- col_dt$colname[i]
  dem_vars <- c("year_id", "age_group_id", "sex_id", "location_id", "metric_id", "draw")
  col_group <- c(dem_vars, col)

  measure <- col_dt$measure[i]

  symp_measure <- copy(male_dt)
  symp_measure <- symp_measure[ ,..col_group]
  symp_measure[ ,measure_id := measure]

  #now dcast symp_measure
  symp_wide <- dcast(data = symp_measure, formula = year_id+age_group_id+sex_id+location_id+metric_id+measure_id~draw, value.var = col)

  #now save symp_measure to file
  save_measure <- unique(symp_wide$measure_id)
  save_loc <- unique(symp_wide$location_id)
  save_sex <- unique(symp_wide$sex_id)
  save_meid <- col_dt$meid[i]
  write.csv(x = symp_wide, file = paste0(out_dir, FILEPATH, save_measure, "_", save_loc, "_", save_sex, ".csv"), row.names = FALSE)

}

#FEMALES------------------------------------------------

#METHODOLOGY
#'total prev = mild prev + asymptomatic prev
#'mild prev = total incidence * (female mildprop * mild_dur)
#'asymp prev = total incidence * (female asymp prop * mild dur)

#'total prev = [total incidence * (female mild prop * mild_dur)] + [total incidence * (female asymp prop * asymp dur)]
#'total prev = total incidence * [(female mild prop*mild dur) + (female asymp prop*asymp dur)]
#'total incidence = total prev/all sev divisor

#CODE
#get the female prev dt

female_prev <- prev_long[sex_id == 2, ]

# female prop vars
fem_mild_prop <- splits_meta[parent_meid == parent_me & sex == "female", mild_prop]
fem_asymp_prop <- splits_meta[parent_meid == parent_me & sex == "female", asymp_prop]
fem_sev_divisor <- (fem_mild_prop*mild_dur) + (fem_asymp_prop*asymp_dur)

#now get dt with prev and inc for each sequela
female_dt <- copy(female_prev)
setnames(x = female_dt, old = "draw_val", new = "total_prev")

#total male inc
female_dt[ ,total_inc := (total_prev/fem_sev_divisor)]

#now get female mild inc, asymp inc
female_dt[ ,mild_inc := total_inc*fem_mild_prop]
female_dt[ ,asymp_inc := total_inc*fem_asymp_prop]

#now get female mild prev, asymp prev
female_dt[ ,mild_prev := (total_inc*fem_mild_prop*mild_dur)]
female_dt[ ,asymp_prev := (total_inc*fem_asymp_prop*asymp_dur)]

#write for loop that will split out each dataset in measure by sequela, than dcast the dataset, then add the measure id column, then save the file
fem_col_dt <-data.table(colname = c("mild_prev", "mild_inc", "asymp_prev", "asymp_inc"), measure = c(5,6,5,6), meid = c(mild_meid, mild_meid, asymp_meid, asymp_meid))
fem_col_dt

for (i in 1:nrow(fem_col_dt)){
  #id vars
  fem_col <- fem_col_dt$colname[i]
  fem_dem_vars <- c("year_id", "age_group_id", "sex_id", "location_id", "metric_id", "draw")
  fem_col_group <- c(fem_dem_vars, fem_col)

  fem_measure <- fem_col_dt$measure[i]

  fem_symp_measure <- copy(female_dt)
  fem_symp_measure[ ,..fem_col_group]
  fem_symp_measure[ ,measure_id := fem_measure]

  #now dcast symp_wide
  fem_symp_wide <- dcast(data = fem_symp_measure, formula = year_id+age_group_id+sex_id+location_id+metric_id+measure_id~draw, value.var = fem_col)

  #now save symp_measure to file
  fem_save_measure<- unique(fem_symp_measure$measure_id)
  fem_save_loc <- unique(fem_symp_measure$location_id)
  fem_save_sex <- unique(fem_symp_measure$sex_id)
  fem_save_meid <- fem_col_dt$meid[i]
  write.csv(x = fem_symp_wide, file = paste0(out_dir, FILEPATH, fem_save_meid, "/", fem_save_measure, "_", fem_save_loc, "_", fem_save_sex, ".csv"), row.names = FALSE)

}





