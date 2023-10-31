#################################################################################################
#' Purpose: Estimate the birth & 28 days prevalence of congenital syphilis.
################################################################################################

#SETUP
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}
user <- Sys.info()["user"]
source("FILEPATH")
library("openxlsx")
library("data.table")
pacman::p_load(data.table, openxlsx, ggplot2, dplyr)

#SOURCE FUNCTIONS----------------------------------------------------------------------------------------------------------------------------------------------------------------------
source_shared_functions(functions = c("get_bundle_data","get_bundle_version", "save_crosswalk_version", "get_crosswalk_version",
                                        "get_draws", "get_population", "get_location_metadata",
                                      "get_age_metadata", "get_ids", "get_demographics", "get_covariate_estimates", "get_outputs"))

#ARGS & DIRS
#switch this to command line args later
args <- commandArgs(trailingOnly = TRUE)
loc_id <- as.numeric(args[1])

acause <- "std_syphilis"
cs_bundle <- 8255
prev_nid <- 450314
csmr_nid <- 450317
years <- 1990:2022
est_years <- c(1990,1995,2000,2005,2010,2015,2019,2020,2021,2022)

adj_fpath <- "FILEPATH"
nnd_fpath <- "FILEPATH"
draw_dir <- "FILEPATH"
nonfatal_dir <- "FILEPATH"

#treatment specific vertical transmission proportions (nnd excluded)
cs_untr_prop <- 0.176 #lower
cs_inadq_prop <- 0.146 #higher
cs_adeq_prop <- 0.037 #lower

#GET TOTAL NUMBER & TR-SPECIFIC PROPORTIONS OF ADJUSTED LIVEBIRTHS------------------------------------------------------------------------------------------------------------------------
#pull in livebirths to syphilitic women after being adjusted for excess risk of stillbirth (step6 of fatal pipeline)
print("pulling in adjusted livebirths")
adj_lbs <- data.table(read.csv(paste0(adj_fpath, loc_id, "FILEPATH")))
adj_lbs <- adj_lbs[year_id >= 1990, ]

#sum adjusted livebirths
adj_lbs[ ,tot_adj := adj_untreated_early + adj_untreated_late + adj_inadequate_late + adequate]

#estimate adj_lbs props by maternal treatment status
adj_lbs[ ,`:=` (untr_early_prop = adj_untreated_early/tot_adj,
                untr_late_prop = adj_untreated_late/tot_adj,
                inadq_late_prop = adj_inadequate_late/tot_adj,
                adq_prop = adequate/tot_adj)]
print(paste0("props sum to: ",sum(adj_lbs$untr_early_prop[1] + adj_lbs$untr_late_prop[1] + adj_lbs$inadq_late_prop[1] + adj_lbs$adq_prop[1])))

#GET NEONATAL DEATHS------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#pull from pipeline, age_ids 2&3
print("pulling in neonatal deaths")
neonate_deaths <- data.table()

for(year in years){

  print(paste0("getting years for ", year, ". Males."))
  #get MALE draws & subset to neonatal age groups
  male_death_draws <- data.table(read.csv(paste0(nnd_fpath, loc_id,"_",year, "_1.csv")))
  male_nn_draws <- male_death_draws[age_group_id %in% c(2,3)]

  #aggregate MALE early and late nn draws
  male_nn_draws <- melt(data = male_nn_draws, id.vars = c("location_id","year_id", "sex_id", "metric_id", "age_group_id"),
                                                              variable.name = "draw_num" ,value.name = c("draw_val"))
  male_nn_draws[ ,all_neo_deaths := sum(draw_val), by = "draw_num"]
  male_nn_draws[ ,c("age_group_id", "draw_val") := NULL]
  male_nn_draws <- unique(male_nn_draws)
  head(male_nn_draws)


  print("Now Females.")
  #get FEMALE draws & subset to neonatal age groups
  female_death_draws <- data.table(read.csv(paste0(nnd_fpath, loc_id,"_",year, "_2.csv")))
  female_nn_draws <- female_death_draws[age_group_id %in% c(2,3)]

  #aggregate FEMALE early and late nn draws
  female_nn_draws <- melt(data = female_nn_draws, id.vars = c("location_id","year_id", "sex_id", "metric_id", "age_group_id"),
                                                            variable.name = "draw_num" ,value.name = c("draw_val"))
  female_nn_draws[ ,all_neo_deaths := sum(draw_val), by = "draw_num"]
  female_nn_draws[ ,c("age_group_id", "draw_val") := NULL]
  female_nn_draws <- unique(female_nn_draws)
  head(female_nn_draws)

  mf_neo_deaths <- rbind(male_nn_draws, female_nn_draws)

  neonate_deaths <- rbind(neonate_deaths, mf_neo_deaths)

}

#should be 66,000X6cols
dim(neonate_deaths)

#APPLY THE SEX PATTERN TO ADJ LBS----------------------------------------------------------------------------------------------------------------------------------------------------------------
#pull male & female neonatal populations (age id 42)
pops <- get_population(age_group_id = 42,location_id = loc_id, year_id = years, sex_id = 1:3, gbd_round_id = 7, status = "best", decomp_step = "step3")

pops_dt <- data.table(pops)
pops_dt[sex_id == 1, sex_name := "male"]
pops_dt[sex_id == 2, sex_name := "female"]
pops_dt[sex_id == 3, sex_name := "both"]
pops_dt[ ,run_id := NULL]

#calculate the proportions for male and female out of the total
mf_proportions <- dcast(data = pops_dt, formula = year_id ~ sex_name,value.var = "population")
mf_proportions[ ,`:=` (male_prop = male/both, female_prop = female/both)]

#merge adjusted livebirths and the sex-specific proportions
ss_lbs <- merge(adj_lbs, mf_proportions, by = "year_id")
ss_lbs[ , c("both", "female", "male") := NULL]

#calc male adj livebirths and female adj livebirths
ss_lbs[ ,`:=` (male_aue = adj_untreated_early*male_prop, female_aue = adj_untreated_early*female_prop,
               male_aul = adj_untreated_late*male_prop, female_aul = adj_untreated_late*female_prop,
               male_ail = adj_inadequate_late*male_prop, female_ail = adj_inadequate_late*female_prop,
               male_adq = adequate*male_prop, female_adq = adequate*female_prop)]
ss_lbs[ ,c("tot_adj", "male_prop", "female_prop",
           "adj_untreated_early", "adj_untreated_late", "adj_inadequate_late", "adequate") := NULL]

#melt long to add on sex ids
ss_lbs_long <- melt(data = ss_lbs, id.vars = c("location_id", "year_id", "draw_num",
                                               "untr_early_prop", "untr_late_prop", "inadq_late_prop", "adq_prop"),
                                                variable.name = "ss_treat", value.name = "num_lbs")
ss_lbs_long[ss_treat %in% c("male_aue", "male_aul", "male_ail", "male_adq") ,sex_id := 1]
ss_lbs_long[ss_treat %in% c("female_aue", "female_aul", "female_ail", "female_adq") ,sex_id := 2]
ss_lbs_long$ss_treat <- unlist(lapply(X = ss_lbs_long$ss_treat, function(x) gsub("[a-z]*male_", "", x)))

#cast wide to break out treatment status
ss_lbs_wide <- dcast(data = ss_lbs_long, formula = ... ~ ss_treat, value.var = "num_lbs")

#ESTIMATE POST-NEONATE SURVIVORS-------------------------------------------------------------------------------------------------------------------------------------------------------------------
births_deaths <- merge(ss_lbs_wide, neonate_deaths, by = c("location_id", "year_id", "sex_id", "draw_num"))

#split nn deaths into treatment statuses
births_deaths[ ,`:=` (nnd_adq = all_neo_deaths * adq_prop, nnd_ail = all_neo_deaths * inadq_late_prop,
                      nnd_aue = all_neo_deaths * untr_early_prop, nnd_aul = all_neo_deaths * untr_late_prop)]

#estimate post neonatal survivors by treatment status
births_deaths[ ,`:=` (surv_adq = adq - nnd_adq, surv_ail = ail - nnd_ail,
                      surv_aue = aue - nnd_aue, surv_aul = aul - nnd_aul)]

#ESTIMATE POST-NEONATE INFECTED----------------------------------------------------------------------------------------------------------------------------------------------------------------------
##apply vertical transmission prop specific to maternal treatment status to get post-neonatal infected survivors
infected_dt <- copy(births_deaths)
infected_dt <- infected_dt[ ,c("location_id", "year_id", "sex_id", "draw_num", "surv_adq", "surv_ail", "surv_aue", "surv_aul") ]
infected_dt[ ,`:=` (inf_adq = surv_adq * cs_adeq_prop, inf_ail = surv_ail * cs_inadq_prop,
                 inf_aue = surv_aue * cs_untr_prop, inf_aul = surv_aul * cs_untr_prop)]

#sum across treatment status to get total infected surivors (cases)
infected_dt[ ,tot_infected := inf_adq + inf_ail + inf_aue + inf_aul]

#drop unneeded columns
infected_dt[ ,c("surv_adq", "surv_ail", "surv_aue", "surv_aul", "inf_adq", "inf_ail", "inf_aue", "inf_aul") := NULL]

#collapse draws

collapse_infected <- infected_dt[ ,lapply(.SD, mean), by = c("location_id", "year_id", "sex_id"), .SDcols = c("tot_infected")]

#ESTIMATE 28D SAMPLE SIZE------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#get population of livebirths
print("getting sample size - 28 day pop")
birth_pop0 <- get_population(age_group_id = 164, location_id = loc_id, year_id = years, sex_id = c(1,2), gbd_round_id = 7, decomp_step = "step3", status = "best")
birth_pop0[ ,c("age_group_id", "run_id") := NULL]

#get all cause mortality rate in early and late neonatal period
allcause_mr <- get_outputs(topic = "cause", measure_id = 1, location_id = loc_id, year_id = years, metric_id = 3,
                         age_group_id = 2:3, sex_id = 1:2, cause_id = 294, gbd_round_id = 7, decomp_step = "iterative", compare_version_id = 7283)

#convert acmr to a number of deaths (acmr * early neo & late neo population, respectively )
neo_pops <- get_population(age_group_id = 2:3, location_id = loc_id, year_id = years, sex_id = c(1,2), gbd_round_id = 7, decomp_step = "step3", status = "best")
neo_pops$run_id <- NULL

ac_deaths <- merge(allcause_mr, neo_pops, by = c("age_group_id", "year_id", "sex_id", "location_id"))

#group acmr by sex and age (should be 4 groups)
ac_deaths[ ,sex_age_group := .GRP, by = c("sex_id", "age_group_id")]

#get combos yearstart and year end
acmr_year_bounds <- data.table(year_start = c(1990,1995,2000,2005,2010,2015), year_end = c(1995,2000,2005,2010,2015,2019))

#begin filling the years not estimated by codcorrect
for (g in unique(ac_deaths$sex_age_group)){
  for (z in 1:nrow(acmr_year_bounds)) {

    print(paste0("filling acmr for group ", g, "and row ", z))

    fill_acmr <- function(year_start = acmr_year_bounds[z,year_start], year_end = acmr_year_bounds[z,year_end], dt = ac_deaths){

      start_val = dt[sex_age_group == g & year_id == year_start, val]
      end_val =  dt[sex_age_group == g & year_id == year_end, val]
      years_to_fill = (year_end - year_start -1)

      tot_diff = end_val - start_val
      year_diff = tot_diff/(years_to_fill+1) #note that the last year is already accounted for

      for (i in 1:years_to_fill){
        dt[sex_age_group == g & year_id == year_start + i, filled_acmr := start_val + year_diff*i]
      }

    }
    fill_acmr()
  }
}
ac_deaths[year_id %in% est_years ,filled_acmr := val]

#multiply times population to get death count
ac_deaths[ ,num_ac_deaths := filled_acmr*population]

#combine deaths into total neonatal deaths , subset to relevant columns
ac_deaths[ ,tot_num_ac_deaths := sum(num_ac_deaths), by = c("year_id", "sex_id", "location_id")]
ac_death_counts <- unique(ac_deaths[ ,c("year_id", "sex_id", "location_id", "tot_num_ac_deaths")])

#merge all cause deaths with livebirth population and subtract all-cause neonatal deaths from livebirths to get 28 day population
get_28_sample <- merge(birth_pop0, ac_death_counts, by = c("year_id", "sex_id", "location_id"))
get_28_sample[ ,pop_28d := (population - tot_num_ac_deaths)]
get_28_sample[ ,c("population", "tot_num_ac_deaths") := NULL]

#merge with the number of infected cases
inf_birth_prev <- merge(collapse_infected, get_28_sample, by = c("location_id", "year_id", "sex_id"))
setnames(inf_birth_prev, c("tot_infected", "pop_28d"), c("cases", "sample_size"))
inf_birth_prev[sex_id == 1, sex := "Male"]
inf_birth_prev[sex_id == 2, sex := "Female"]
inf_birth_prev$sex_id <- NULL


#EPI UPLOADER FORMAT----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# create necessary cols we need
col_order <- function(data.table){
  dtc <- copy(data.table)
  epi_order <- fread(paste0(j_root, "FILEPATH"), header = F)
  epi_order <- tolower(as.character(epi_order[, V1]))
  names_dtc <- tolower(names(dtc))
  setnames(dtc, names(dtc), names_dtc)

  for (name in epi_order){
    if (name %in% names(dtc) == F){
      dtc[, c(name) := ""]
    }
  }
  extra_cols <- setdiff(names(dtc), tolower(epi_order))
  epi_order <- epi_order[epi_order != ""]
  new_epi_order <- c(epi_order, extra_cols)
  setcolorder(dtc, new_epi_order)
  return(dtc)
}
format_inf_bprev <- col_order(as.data.table(inf_birth_prev))

# tack on the right values for required columns
fill_columns <- function(dt, nid_to_use, measure_to_use, note){
  dt[ ,`:=` (bundle_id  = cs_bundle,
             nid = nid_to_use,
             sex_issue  = 0,
             year_issue = 0,
             year_start  = year_id,
             year_end    = year_id,
             age_issue  = 0,
             measure    = measure_to_use,
             unit_type  = "Person*year",
             unit_value_as_published = 1,
             measure_issue           = 0,
             uncertainty_type_value  = 95,
             recall_type             = "Not Set",
             representative_name     = "Representative for subnational location only",
             source_type             = "Unidentifiable",
             urbanicity_type         = "Mixed/both",
             note_modeler = note,
             is_outlier   = 0,
             extractor    = user)]
  dt[ ,year_id := NULL]
}
final_inf_bprev <- fill_columns(dt = format_inf_bprev, nid_to_use = prev_nid, measure_to_use = "prevalence",
                                note = paste0("Infected Birth prevalence of congenital syphilis. From GBD 2020 S3 fatal pipeline estimates.",  "Uploaded on ", Sys.Date(), " by ", user))

#based on age_metadata table (28 days/365 days)
final_inf_bprev[ ,`:=` (age_start = 0.07671233, age_end = 0.07671233)]

#ESTIMATE BIRTH PREVALENCE (INFECTED CASES + NEONATAL DEATHS/ALL LBS)---------------------------------------------------------------------------------------------------------------------------------------

#start from infected cases & neonatal deaths
infected_bprev <- copy(infected_dt)
death_bprev <- copy(neonate_deaths)
death_bprev[ ,metric_id := NULL]

#combine infected cases and neonatal deaths to get presumptive infected livebirths
presumptive_lvbths <- merge(infected_bprev, death_bprev, by = c("location_id", "year_id", "sex_id", "draw_num"))
presumptive_lvbths[ ,cs_lvbths := tot_infected + all_neo_deaths]

#collapse the draws
collapse_presumptive <- presumptive_lvbths[ ,lapply(.SD, mean), by = c("location_id", "year_id", "sex_id"), .SDcols = c("cs_lvbths")]

#now get all livebirths by sex
#i am confident about this bc exact same values as if I pull get_covariate estimates for cov_id 1106
birth_pop <- get_population(age_group_id = 164, location_id = loc_id, year_id = years, sex_id = c(1,2), gbd_round_id = 7, decomp_step = "step3", status = "best")
birth_pop[ ,c("age_group_id","run_id") := NULL]

#merge the two and estimate prev
cases_at_birth <- merge(collapse_presumptive, birth_pop, by = c("location_id", "year_id", "sex_id"))
cases_at_birth[ ,mean := cs_lvbths/population]

#now format them for the epiuploader
setnames(cases_at_birth, old = c("cs_lvbths", "population"), new = c("cases", "sample_size"))
format_cab <- col_order(cases_at_birth)
final_cab <- fill_columns(dt = format_cab, nid_to_use = prev_nid, measure_to_use = "prevalence", note = "Presumptive Cases of Congenital Syphilis at Birth.
                                                                                                        (CS 28day olds + neonatal deaths)/Population at Birth
                                                                                                                            (aka get_pop age_id164).")
final_cab[ ,`:=` (age_start = 0, age_end = 0)]

final_cab[sex_id == 1, sex := "Male"]
final_cab[sex_id == 2, sex := "Female"]
final_cab$sex_id <- NULL

#ESTIMATE EXCESS MORTALITY RATE----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
print("estimating excess mortality rate")
#take sex specific nn deaths in early and late nn stage (sum)
collapse_nnd <- copy(neonate_deaths)
collapse_nnd <- collapse_nnd[ ,lapply(.SD, mean), by = c("location_id", "year_id", "sex_id"), .SDcols = c("all_neo_deaths")]

# collapse_ss_lbs <- ss_lbs_wide[ ,c("location_id", "year_id", "draw_num", "sex_id", "adq", "ail", "aue", "aul")]
# collapse_ss_lbs[ ,tot_adj := adq + ail + aue + aul]
# collapse_ss_lbs[ ,c("adq", "ail", "aue", "aul") := NULL]
# collapse_ss_lbs <- collapse_ss_lbs[ ,lapply(.SD, mean), by = c("location_id", "year_id", "sex_id"), .SDcols = c("tot_adj")]

emr_dt <- merge(collapse_nnd, collapse_presumptive, by = c("location_id", "year_id", "sex_id"))
setnames(emr_dt, c("all_neo_deaths", "cs_lvbths"), c("cases", "sample_size"))
emr_dt[sex_id == 1, sex := "Male"]
emr_dt[sex_id == 2, sex := "Female"]
emr_dt$sex_id <- NULL

#EMR EPI UPLOADER FORMAT
format_emr <- col_order(as.data.table(emr_dt))
final_emr <- fill_columns(dt = format_emr, nid_to_use = csmr_nid, measure_to_use = "mtexcess", note = "EMR of congenital syphilis pulled from GBD 2020 S3 fatal pipeline.Cases are nndeaths, sample size is presumptive cases of CS (nndeaths + 28d cases).NID needs updating.")
final_emr[ ,`:=` (age_start = 0, age_end = 27/365)]
#final_emr$cause_name <- NULL
dim(final_emr)


#GET CSMR VALUES FROM LATEST COD CORRECT RUN----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
print("pulling csmr")
ages_0_10_gbd20 <- c(2,3, 388, 389, 238, 34, 6) #early neo, late neo, 1-5 mnths, 6-11 mnths, 12-23 mnths, 2 to 4, 5 to 9

#updating this for GBD2020
csmr_values <- get_outputs(topic = "cause", measure_id = 1, location_id = loc_id, year_id = est_years, metric_id = 3,
                           age_group_id = ages_0_10_gbd20, sex_id = 1:2, cause_id = 394, gbd_round_id = 7, decomp_step = "iterative", compare_version_id = 7283)

#format csmr values for epi upload
csmr_dt <- csmr_values[ ,c("age_group_id", "location_id", "year_id", "cause_name", "location_name", "sex", "val", "lower", "upper")]
setnames(csmr_dt, "val", "mean")

#get gbd 2020 age metadata
gbd20_ages <- get_age_metadata(19)

csmr_dt[age_group_id == 2, `:=` (age_start = gbd20_ages[age_group_id == 2, age_group_years_start],
                                 age_end = gbd20_ages[age_group_id == 2, age_group_years_end])]

csmr_dt[age_group_id == 3, `:=` (age_start = gbd20_ages[age_group_id == 3, age_group_years_start],
                                   age_end = gbd20_ages[age_group_id == 3, age_group_years_end])]
csmr_dt[age_group_id == 388, `:=` (age_start = gbd20_ages[age_group_id == 388, age_group_years_start],
                                     age_end = gbd20_ages[age_group_id == 388, age_group_years_end])]

csmr_dt[age_group_id == 389, `:=` (age_start = gbd20_ages[age_group_id == 389, age_group_years_start],
                                     age_end = gbd20_ages[age_group_id == 389, age_group_years_end])]

csmr_dt[age_group_id == 238, `:=` (age_start = gbd20_ages[age_group_id == 238, age_group_years_start],
                                     age_end = gbd20_ages[age_group_id == 238, age_group_years_end])]

csmr_dt[age_group_id == 34, `:=` (age_start = gbd20_ages[age_group_id == 34, age_group_years_start],
                                    age_end = gbd20_ages[age_group_id == 34, age_group_years_end])]

csmr_dt[age_group_id == 6, `:=` (age_start = gbd20_ages[age_group_id == 6, age_group_years_start],
                                   age_end = gbd20_ages[age_group_id == 6, age_group_years_end])]
csmr_dt[ ,age_group_id := NULL]

#CSMR EPI UPLOADER FORMAT
format_csmr <- col_order(as.data.table(csmr_dt))
final_csmr <- fill_columns(dt = format_csmr, nid_to_use = csmr_nid, measure_to_use = "mtspecific", note = "CSMR of congenital syphilis pulled from GBD 2020 S2 CodCorrect (7283).")
final_csmr$cause_name <- NULL

#RBIND PREV & CSMR TOGETHER
all_loc_data <- rbind(final_cab, final_inf_bprev, final_emr, final_csmr, fill = TRUE)

loc_name <- unique(final_csmr$location_name)
all_loc_data[ ,location_name := loc_name]
all_loc_data$V1 <- NULL

#CREATING VISUALIZATIONS
#prev at birth (presumptive)
summary(final_cab$cases)
summary(final_cab$sample_size)
summary(final_cab$mean)

plot_data <- copy(all_loc_data)

ggplot(data = plot_data[year_start == 2020 & measure != "mtspecific"], aes(x = as.factor(age_start), y = as.numeric(cases), color = measure )) + geom_bar(stat = "identity", position = "dodge") + facet_wrap(~sex)

ggplot(data = plot_data[year_start == 2020 & measure != "mtspecific"], aes(x = as.factor(age_start), y = as.numeric(sample_size), color = measure )) + geom_bar(stat = "identity", position = "dodge") + facet_wrap(~sex)

#SAVE TO FILE
print("saving file")
write.csv(all_loc_data, FILEPATH, row.names = FALSE)


 


