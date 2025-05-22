################################################################################################
## Author:NAME
## Date: DATE
## Purpose: Clean data for MR-BRT XWs.

##################################################################################################

#SETUP-----------------------------------------------------------------------------------------------
#rm(list=ls())
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- '/home/j/'
  h_root <- '~/'
} else {
  j_root <- 'J:/'
  h_root <- 'H:/'
}
user <- Sys.info()["user"]

#SOURCE FUNCTIONS------------------------------------------------------------------------------------------
source(paste0(h_root,"FILEPATH/function_lib.R"))
source("FILEPATH/data_tests.R")
pacman::p_load(data.table, openxlsx, ggplot2, dplyr)

source_shared_functions(functions = c("get_bundle_data","get_bundle_version", "save_crosswalk_version", "get_crosswalk_version",
                                      "save_bulk_outlier", "get_draws", "get_population", "get_location_metadata",
                                      "get_age_metadata", "get_ids"))



#ARGS & DIRS-----------------------------------------------------------------------------------------------

#general args
cause <- "uterine_fibroids_net"
match_dir <- paste0("FILEPATH")
match_dir

#custom functions args
read_from_database <- T               ## Does your data come from the database?
read_flat_file <- NA                  ## If you have the data pulled out of the bundle, file path here
year_overlap <- 10                    ## Default is 5-year bins, which corresponds to mean +/- 5 years
age_overlap <-10                      ## Default is 5-year bins, which corresponds to mean +/- 5 years
national_ok <- FALSE
loc_match <- "exact"                     ## Do you want to use national (subnat-subnat-nat) comparisons or just strict location matches (exact)?

#case def args (typically your cv_ columns)
reference_def <- "cv_case_def_acog"
alt_def1 <- "cv_self_report"
alt_def2 <- "cv_dx_symptomatic_only"
alt_def3 <- "cov_clinical"

alternate_defs <- c(alt_def1, alt_def2, alt_def3)

#shared function args
bundle_id <- 199                        ## Change the bundle ID
gbd_round_id <- 7
decomp_step <- "iterative"
bundle_version <- 29825

#GET LOCATION METADATA
#loc_set_id9 == Dismod & Epi Data
# "The set of locations used to prep Epi input data and by Dismod inputs"
loc_ids <- get_location_metadata(location_set_id = 9, gbd_round_id = gbd_round_id, decomp_step = "step3")

#BEGIN FUNCTION----------------------------------------------------------------------------------------------

#make sure to confirm each argument
if (read_from_database) {
  b_id <- bundle_id
  gbd_round <- gbd_round_id
  decomp <- decomp_step
} else {
  data_path <- read_flat_file
  long <- F                         ## Are your case definitions long or wide? Long = there is a column called "Definition"
                                      ##    with either reference or alternative definitions in them. Wide = there are dummy
                                      ##    variables with reference or alternate definitions
}

## Insert XWs you want to go through with.
#The first one in the list is the numerator, second is denominator.
xws <- list(

  c(alt_def1, reference_def),
  c(alt_def1, alt_def2),
  c(alt_def1, alt_def3),
  c(alt_def2, reference_def),
  c(alt_def2, alt_def3),
  c(alt_def3, reference_def)

)
xws

#PREP VARS TO MATCH ON-------------------------------------------------------------------------------------------
age_prep <- function(df, alt) {

  #Only look at rows with a fewer than 10-year age range
  df[age_start > 90 & age_end > 120, age_end := 100]

  if (alt == "trep"){
    age_narrow <- 10
  } else {
    age_narrow <- 10
  }
  df[, narrow := ifelse((age_end - age_start) <= age_narrow, 1, 0)] #originally 10 years

  #Create dummy variables "age_0_5" etc.populate with ages that cross those ranges.
  #15-24 would fit in age_15_20 and age_20_35
  for (n in seq(from=0, to=100, by=age_overlap)) {

    m <- n + (age_overlap - 1)
    interval <- paste0("age_", n, "_", m)
    ages <- seq(from=n, to=m)

    df[narrow == 1 & ((age_start %in% ages) | (age_end %in% ages)), paste0(interval) := 1]

  }

  ## Melt down. This allows duplicated data (15-24 would become 2 rows, with age groups age_15_20 and age_20_25)
  df <- melt(df, measure.vars = names(df)[grepl("age_\\d", names(df))])
  df <- df[value==1,]
  df[, c("value", "narrow") := NULL]
  setnames(df, old="variable", new="age_group")

  return(df)

}

year_prep <- function(df) {

  ## Only look at rows with a fewer than 10-year age gap.
  df[, narrow := ifelse(year_end - year_start <= 10, 1, 0)]

  for (n in seq(from=1980, to=2010, by=year_overlap)) {

    m <- n + (year_overlap - 1)
    interval <- paste0("year_", n, "_", m)

    years <- seq(from=n, to=m)

    df[narrow == 1 & ((year_start %in% years) | (year_end %in% years)), paste0(interval) := 1]

  }

  df <- melt(df, measure.vars = names(df)[grepl("year_\\d", names(df))])
  df <- df[value==1,]
  setnames(df, old="variable", new="year_group")
  df[, c("value", "narrow") := NULL]

  return(df)

}

#READ IN DATA---------------------------------------------------------------------------------------------------
if (read_from_database) {

  bv_data <- get_bundle_version(bundle_version_id = bundle_version, fetch = "all", export = FALSE)
  bv_data[nid == 120728, `:=` (cv_self_report = NA, cv_dx_symptomatic_only = 1)]
  print(nrow(bv_data[nid == 120728]))
  print(nrow(bv_data[cv_dx_symptomatic_only == 1]))

} else {

  bundle <- data.table(read.xlsx(data_path))
  if (long) {
    bundle$count <- 1
    bundle <- dcast(bundle, formula = as.formula(paste0(paste(names(bundle)[!(names(bundle) %like% "Definition")], collapse=" + "), " ~ Definition")), value.var = "count")
    bundle[is.na(bundle)] <- 0
    bundle[, count := NULL]
  }
}

#ADJUST COLUMNS AS NEEDED FOR EAC CAUSE GET PREV ROWS (NOT OUTLIERS, NOT GR'ed OUT)

if (cause == "ats" | cause == "uterine_fibroids"){
  prev_data <- bv_data[measure == "prevalence" & clinical_data_type %in% c("claims", "inpatient")]
  prev_data[clinical_data_type == "claims", `:=` (cdt_claims = 1, cdt_inpatient = 0)]
  prev_data[clinical_data_type == "inpatient", `:=` (cdt_claims = 0, cdt_inpatient = 1)]

  print(unique(prev_data[ ,c("measure", "cdt_claims", "cdt_inpatient")]))
  print(paste0("There are ", nrow(prev_data[cdt_claims == 1]), " rows of claims data. ", nrow(prev_data[cdt_inpatient == 1]), " rows of inpatient data."))
  print(paste0("Leaving out ", nrow(prev_data[clinical_data_type == "claims" & mean == 0, ]), " rows of claims data & ",  nrow(prev_data[clinical_data_type == "inpatient" & mean == 0, ]), " rows inpatient data w/ mean = 0."))

  prev_data <- prev_data[mean != 0]

} else if (cause == "uterine_fibroids_net"){
  prev_data <- copy(bv_data)
  print(paste0("Mean = 0 removed: ",nrow(prev_data[mean == 0]), " rows."))
  #prev_data <- prev_data[mean != 0]
  prev_data[clinical_data_type %in% c("claims"), cov_clinical := 1] #bc this is the reference clinical data type

  print(unique(prev_data[ ,c("measure", "cv_case_def_acog", "cv_self_report", "cv_dx_symptomatic_only", "cov_clinical")]))
  print(paste0("There are ", nrow(prev_data[cv_case_def_acog == 1]), " rows of ref acog data. ", nrow(prev_data[cv_self_report == 1]), " rows of self-reported data."))
  print(paste0("There are ", nrow(prev_data[cv_dx_symptomatic_only == 1, ]), " rows of symptomatic data & ",  nrow(prev_data[cov_clinical == 1, ]), " rows of clinical data."))



} else {
  prev_data <- bv_data[measure == "prevalence"]
  prev_data <- prev_data[measure == "prevalence" & (is_outlier %in% c(0,NA) & group_review %in% c(1,NA)) ]
  prev_data <- prev_data[mean != 0]
  prev_data[cv_blood_donor == 1 | cv_pregnant == 1 ,general_pop := 0]
  prev_data[is.na(general_pop), general_pop := 1]

  #use only for the dx match finding
  #other_data <- bv_data[measure == "prevalence" & dx_crosswalk_use == 1]
  #write.csv(x = other_data, file = paste0(match_dir, "dx_crosswalk_use.csv"))

  # all_prev_data <- rbind(prev_data, other_data)
  # dim(all_prev_data)
  # all_prev_data <- unique(all_prev_data)
  # dim(all_prev_data)
}

dim(bv_data)
dim(prev_data)

# CONDUCT BETWEEN STUDY COMPARISON COUNTS---------------------------------------------------------------------------

#age and year prep the prevalence data
#use the "dx crosswalk use" column to match find for ES
bundle_prep <- age_prep(copy(prev_data), alt = "inpatient") 
bundle_prep <- year_prep(bundle_prep)
dim(bundle_prep)

## Create a variable "full" which identifies unique age-sex-loc-year-measure matches, by subnat or national
if (loc_match == "regional") {
  merge_locs <- loc_ids[ ,c("location_id", "region_id", "region_name")]

  #bundle_prep$ihme_loc_id <- NULL
  bundle_prep <- merge(bundle_prep, merge_locs, by=c("location_id"), all.x=T)
  bundle_prep[, full := paste(region_id, age_group, year_group, sex, measure, sep = "-")]
  bundle_prep[ ,unique_full := .GRP, by = full]
  #setnames(bundle_prep, "ihme_loc_id", "location_id")
} else {
  bundle_prep[, full := paste(location_id, age_group, year_group, sex, measure, sep = "-")]
  bundle_prep[ ,unique_full := .GRP, by = full]
}

## Identify age-sex-location-years that contain a particular definition.
for (def in c(reference_def,alternate_defs)) {
  col <- paste0("has_", def)
  bundle_prep[get(def) == 1, paste0(col) := unique_full]
  # dt_cols <- c(paste0(def), "full")
  # def_vector <- unlist(lapply(bundle_prep[ ,..dt_cols], function(x) sum(x, by = "full")))
  # bundle_prep[, paste0(col) := as.numeric(def_vector)] #, by=full
}

comb <- combn(c(reference_def, alternate_defs), 2) # Combinations of definitions

## For every combination, report the number of data points that can be compared to each other.
for (com in 1:ncol(comb)) {

  a <- comb[1, com]
  b <- comb[2, com]
  m <- paste0("has_", a)
  n <- paste0("has_", b)
  stat <- ifelse(national_ok, "(location-age-sex-year)", "(region-age-sex-year)")

  #subset_cols <- c("location_id", "nid", "mean", "cases", "sample_size", "standard_error", "full", "region_id", "region_name",paste0(a), paste0(b), paste0(m), paste0(n), "unique_full", "clinical_data_type")
  subset_cols <- c("location_id", "nid", "mean", "cases", "sample_size", "standard_error", "full", paste0(a), paste0(b), paste0(m), paste0(n), "unique_full", "clinical_data_type")

  full_groups_a <- (bundle_prep[measure == "prevalence" & !is.na(get(m)), ..subset_cols])
  unique_full_a <- unique(full_groups_a$unique_full)
  full_groups_b<- (bundle_prep[measure == "prevalence" & (get(n) %in% unique_full_a), ..subset_cols])

  full_dt <- rbind(full_groups_a, full_groups_b)
  #write.xlsx(x = full_dt, file = paste0(match_dir, "bv_29825_self_report_clinical.xlsx"))
  message(paste0("There are ", nrow(full_groups_a), " ",a, " to ",nrow(full_groups_b), " ", b, " data points to be compared ", stat))

}


## Identify age-sex-location-years that contain more than one definition
n <- paste0("has_", c(reference_def, alternate_defs))
bundle_prep[rowSums(bundle_prep[, ..n]) > 1, has_match := 1]

## Restrict to data points that have a match
full_matches <- bundle_prep[has_match==1,]


#View(full_matches)

#PREPPING BETWEEN STUDY COMPS FOR XWS-------------------------------------------------------------------------------
full_matches <- full_matches[, c('has_match', 'seq') := NULL]

paired_data <- data.table()

### This does the matching
for (each in unique(full_matches$full)) {
  for (n in 1:length(xws)) {

    pair_1 <- xws[[n]][1]
    pair_2 <- xws[[n]][2]
    m <- paste0("has_", pair_1)
    n <- paste0("has_", pair_2)

    df <- full_matches[full == each & get(m) == 1 & get(n) == 1,]

    if (nrow(df)>0){

      m <- df[get(pair_1)>0,]
      n <- df[get(pair_2)>0,]

      for (a in 1:nrow(m)) {
        for (b in 1:nrow(n)) {

          est_alt <- m[a, mean]
          est_ref <- n[b, mean]

          match <- data.table(full=each, ratio = est_alt/est_ref, alt_mean = est_alt, ref_mean = est_ref, alt_se = (m[a, upper]-m[a, lower])/3.92, ref_se = (n[b, upper]-n[b, lower])/3.92, alt_nid = m[a, nid], ref_nid = n[b, nid],
                              age_group = unique(df$age_group), year_group = unique(df$year_group), sex = unique(df$sex), measure = unique(df$measure), location_id = unique(df$location_id))
          match[, ratio_se := sqrt(est_alt^2/est_ref^2 * (alt_se^2/est_alt^2 + ref_se^2/est_ref^2))]
          match[, comp := paste0(pair_1, "-", pair_2)]

          # Set up dummy variables for MR-BRT (if comparison is A:B with B as the reference def, we need a column A marked 1)
          match[, paste0(pair_1) := 1]
          if (pair_2 != reference_def) match[, paste0(pair_2) := -1]

          paired_data <- rbind(paired_data, match, fill=T)

        }
      }
    }
  }
  paired_data[is.na(paired_data)] <- 0
}

dim(paired_data)

#View(paired_data[cv_pregnant == 1])
#View(paired_data[cv_blood_donor == 1])
#paired_data[, c("full", "comp") := NULL]

save_matched_fpath <- paste0(match_dir, "inp_claims_", bundle_version, "_age", age_overlap, "_year", year_overlap, "_loc", loc_match, ".csv")
print(save_matched_fpath)
write.csv(paired_data, file=save_matched_fpath, row.names = FALSE)

#PLOT UF DATA
plot_data <- copy(bundle_prep)
plot_data[cv_case_def_acog == 1 ,data_type := "acog"]
plot_data[cv_self_report == 1, data_type := "self_report"]
plot_data[cv_dx_symptomatic_only == 1, data_type := "symptomatic"]
plot_data[cov_clinical == 1, data_type := "clinical"]

#prep years
# prev_data[ ,mid_year := (year_start + year_end)/2]
# prev_data[mid_year >=1980 & mid_year <= 1989, year_group := "1980-1989"]
# prev_data[mid_year >=1990 & mid_year <= 1999, year_group := "1990-1999"]
# prev_data[mid_year >=2000 & mid_year <= 2009, year_group := "2000-2009"]
# prev_data[mid_year >=2010 & mid_year <= 2022, year_group := "2010+"]

#make axeses print diagonally, set ymax, choose individual color for each graph
regions <- unique(plot_data$region_name)
pdf(file = paste0(match_dir, "data_plots_by_region.pdf"), width = 15, )
for (region in regions) {
    print(paste0(region))

    reg_plot <- ggplot(plot_data[region_name == region & !is.na(data_type) & !is.na(year_group)], aes(x=age_group, y=mean, color = data_type, shape = measure)) +geom_point()+
      scale_colour_manual(values = c("clinical" = "forestgreen", "self_report" = "blue", "acog" = "red")) + scale_shape_manual(values = c("prevalence" = "circle", "incidence" = "triangle"))+
      labs(title = paste0("Uterine Fibroids Data Type: ", region)) + theme(axis.text.x = element_text(angle = 30, vjust = 0.5, hjust=1)) + facet_wrap(~year_group)
    print(reg_plot)
  }
dev.off()

hina_matches <- bundle_prep[region_id == 100 & (cov_clinical == 1 | cv_case_def_acog == 1), c("full", "mean", "standard_error", "cases", "sample_size", "age_group", "year_group", "sex", "measure", "location_id", "region_id", "region_name")]
write.csv(x = hina_matches, file = paste0(match_dir, "network_loc531_acog_clinical_matches.csv"), row.names = FALSE)
bundle_prep[cov_clinical == 1 & region_id == 65 & measure == "prevalence" & age_group %in% c("age_30_39", "age_40_49") & year_group %in% c("year_2000_2009"),]

nrow(full_groups_b[unique_full == 91])
nrow(full_groups_b[unique_full == 93])

#age 30-39 are age_ids 210
#age 40-49 are age_ids 217
gbd20_ages <- get_age_metadata(19)
gbd20_ages

group_93 <- full_groups_b[unique_full == 93, ]

pops_dt <- get_population(age_group_id = 13:14, location_id = group_93$location_id, year_id = 2005, sex_id = 2, gbd_round_id =  7, decomp_step =  "step3")
names(pops_dt)
pops_dt[ ,aa_pop := sum(population), by = c("location_id")]
pops_dt[ ,c("age_group_id", "population", "run_id", "year_id") := NULL]

smushed_dt <- copy(pops_dt)
smushed_dt <- unique(smushed_dt)
smushed_dt[ ,total_pop := sum(aa_pop)]
smushed_dt[ ,loc_prop := aa_pop/total_pop]
sum(smushed_dt$loc_prop)
smushed_dt[ ,c("aa_pop", "total_pop") := NULL]
names(smushed_dt)

#now merge the location population proportions onto group 91
group_93_props <- merge(group_93, smushed_dt, by = c("location_id"), all.x = TRUE)
group_93_props[ ,wghtd_mean := mean*loc_prop]
group_93_props[ ,se_sq := standard_error^2]
sum(group_93_props$wghtd_mean)
sqrt(sum(group_91_props$se_sq))












