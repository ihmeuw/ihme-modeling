## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: copy_infections_deaths_data_child.R
## Description: Copy and paste COVID infections and deaths datasets into our folders
## Contributors: NAME
## --------------------------------------------------------------------- ----


## Environment Prep ---------------------------------------------------- ----

# Dynamically set user specific repo base where nf_covid is located
.repo_base <-
  strsplit(
    whereami::whereami(path_expand = TRUE),
    "nf_covid"
  )[[1]][1]

source(file.path(.repo_base, 'FILEPATH/utils.R'))
source("FILEPATH/get_age_metadata.R")

## --------------------------------------------------------------------- ----


## Functions ----------------------------------------------------------- ----

.fill_nas_inf <- function(dataset, columns_to_fill) {
  
  # Loop over each column
  for (col in columns_to_fill){
    
    # Fill NAs with 0
    dataset[is.na(get(col)), eval(col) := 0]
    
    # Fill Inf with 0
    dataset[is.infinite(get(col)), eval(col) := 0]
    
  }
  
  # Return
  return(dataset)
  
}

main <- function(version_tag, loc_id) {
  
  #  version_tag_save <- paste0(version_tag, '.2')
  version_tag_save <- version_tag
  
  # Set output directory
  out_dir <- file.path(
    'FILEPATH', version_tag_save)
  

  cat('Reading in data...\n')
  
  ## Read in data -------------------------------------------------------- ----
  

  # Infections --------------------------------------------------------------

  inf <- fread(
    glue::glue('FILEPATH/infections.csv')
  )
  
  covid_counts <- copy(inf)
  
  draw_cols <- grep('draw', names(covid_counts), value=T)
  
  covid_counts <- covid_counts[location_id == loc_id, 
                                   c('location_id', 'age_group_id', 'sex_id', 'year_id', draw_cols), 
                                   with = FALSE]
  
  # Reshape draws long
  covid_counts <- melt(covid_counts, 
                       measure.vars = draw_cols, 
                       value.name='infections',
                       variable.name = 'draw')
  
  # Repeat draws 0-99 up to 999
  # extract the draw number from the column name
  covid_counts[, draw_num := as.numeric(gsub("draw_", "", draw))]
  
  # This should only apply if the infections file has 99 draws
  if (max(covid_counts$draw_num) == 99) {
    
    covid_counts <- lapply(0:9, function(x) {
      covid_counts_copy <- copy(covid_counts)
      covid_counts_copy[, draw_num := (x * 100) + draw_num]
      
      covid_counts_copy[, draw := paste0("draw_", draw_num)]
      covid_counts_copy[, draw_num := NULL]
      
      return(covid_counts_copy)
    }) %>% 
      rbindlist()
    
  }
  

  # Hospitalization infection ratio --------------------------------------------

  # Use hospitalization infection ratio to get hosps from infections
  
  # This data comes from a screenshot from covid team since file no longer exists
  # See email from NAME on 2024-07-11
  
  # NOTE: NAME - duplicate 0-4 age group HIR for all age_group_id's under 5
  # Manually add on age_group_id based on the age_group_start and age_group_end
  HIR <- tribble(
    ~age_group_id, ~age_group_start, ~age_group_end, ~logit_hir, ~hir,
    2, 0, 0.01917808, -5.28328749, 0.005050084,
    3, 0.01917808, 0.07671233, -5.28328749, 0.005050084,
    388, 0.07671233, 0.50000000, -5.28328749, 0.005050084,
    389, 0.50000000, 1.00000000, -5.28328749, 0.005050084,
    238, 1.00000000, 2.00000000, -5.28328749, 0.005050084,
    34, 2.00000000, 5.00000000, -5.28328749, 0.005050084,
    6, 5, 9, -5.83296348, 0.002920827,
    7, 10, 14, -5.41469769, 0.004430962,
    8, 15, 19, -4.92351469, 0.007221,
    9, 20, 24, -4.62999715, 0.00966055,
    10, 25, 29, -4.16998205, 0.01521739,
    11, 30, 34, -3.60479581, 0.026473113,
    12, 35, 39, -3.21470828, 0.038615963,
    13, 40, 44, -3.07918759, 0.043973957,
    14, 45, 49, -3.07438927, 0.044176121,
    15, 50, 54, -3.05229502, 0.045118494,
    16, 55, 59, -2.86248916, 0.054039315,
    17, 60, 64, -2.50893914, 0.075233884,
    18, 65, 69, -2.14241434, 0.105042205,
    19, 70, 74, -1.77599653, 0.144798186,
    20, 75, 79, -1.40970492, 0.196280602,
    30, 80, 84, -1.04355011, 0.260465578,
    31, 85, 89, -0.67752637, 0.336813614,
    32, 90, 94, -0.31160572, 0.422722849,
    235, 95, 99, 0.05426175, 0.513562111
  ) %>% 
    setDT()
  
  # Join HIR
  covid_counts <- merge(covid_counts, HIR[, .(age_group_id, hir)], 
                          by = "age_group_id",
                          all.x = TRUE)
  


  # Proportion asymptomatic incidence-------------------------------------------
  cat('Calculating asymptomatic from asymptomatic proportions...\n')
  
  # Read in proportion asymp and merge
  prop_asymp <- fread(paste0(roots$j, 'FILEPATH', 
                             'final_asymptomatic_proportion_draws.csv'))
  prop_asymp$V1 <- NULL
  
  prop_asymp <- melt(prop_asymp[, c(roots$draws), with=F], 
                     measure.vars = roots$draws,
                     value.name = 'prop_asymp',
                     variable.name = 'draw')
  
  covid_counts <- merge(covid_counts, prop_asymp, by = 'draw', all.x = TRUE)
  
  # Ensure % asymptomatic + IHR < 0.95
  covid_counts[(prop_asymp + hir) > 0.95, flag := 1]
  covid_counts[flag == 1, prop_asymp := (prop_asymp * 0.95) / (prop_asymp + hir)]
  covid_counts[flag == 1, hir := (hir * 0.95) / (prop_asymp + hir)]
  
  # Calculate asymptomatic incidence
  covid_counts[, asymp_inc := infections * prop_asymp]

  # Hospitalizations --------------------------------------------------------
  cat('Calculating hospitalizations from infections x HIR...\n')
  
  # Calculate hospitalization admissions
  covid_counts[, hsp_admit := infections * hir]

  # Adjust hospital admissions downward for omicron wave based on
  # ADDRESS
  covid_counts[year_id >= 2022, hsp_admit := 0.404952 * hsp_admit]
  

  # ICU Admissions ----------------------------------------------------------
  cat('Calculating ICU admissions ICU/Hosp ratio...\n')
  
  # Use proportion ICU among hospitalizations to get ICU admissions
  icu_hosp_ratio <- fread("FILEPATH/final_prop_icu_among_hosp.csv")
  icu_hosp_ratio$V1 <- NULL
  
  covid_counts <- merge(covid_counts, 
                          icu_hosp_ratio, 
                          all.x = TRUE, 
                          by = "draw")
  
  # Calculate icu_admit
  covid_counts[, icu_admit := hsp_admit * proportion]
  
  ## Deaths --------------------------------------------------------------------
  cat('Calculating deaths from hosp and icu CFR by age...\n')
  
  # Read in hospital and icu case fatality ratios
  hosp_icu_cfr_by_age <- fread("FILEPATH/hosp_icu_case_fatality_by_age.csv")
  
  covid_counts <- merge(covid_counts, 
                          hosp_icu_cfr_by_age, 
                          all.x = TRUE, 
                          by = "age_group_id")
  
  # Calculate deaths
  covid_counts[, hsp_deaths := hsp_admit * hospital_case_fatality_ratio]
  covid_counts[, icu_deaths := icu_admit * icu_case_fatality_ratio]
  covid_counts[, deaths := hsp_deaths + icu_deaths]

  
  ## Fill NAs with Zero -------------------------------------------------- ----
  cat('Filling NAs with Zero...\n')
  
  covid_counts <- .fill_nas_inf(
    covid_counts, c('infections', 'hsp_admit', 'hsp_deaths', 'icu_deaths', 
                      'icu_admit', 'icu_deaths', 'deaths', 'prop_asymp', 'asymp_inc'))
  

  # Write out final file ----------------------------------------------------
  cat('Writing out final file...\n')
  
  keep_cols <- c("age_group_id", "draw", "location_id", "sex_id", "year_id", 
                "infections", "asymp_inc", "hsp_admit", "icu_admit", "hsp_deaths", 
                "icu_deaths", "prop_asymp"
  )
  
  covid_counts <- covid_counts[, ..keep_cols]
  
  file_name <- paste0(loc_id, "_covid_cases_at_risk.csv")
  file_path <- file.path(out_dir, file_name)
  fwrite(covid_counts, file_path)
  
  cat("File written out to:", file_path)
  
}


## Run all ------------------------------------------------------------- ----
if (!interactive()){
  begin_time <- Sys.time()
  
  parser <- argparse::ArgumentParser()
  parser$add_argument(
    "--version_tag",
    type = "character"
  )
  parser$add_argument(
    "--loc_id",
    type = "integer"
  )
  
  args <- parser$parse_args()
  list2env(args, envir = environment())

  # Run main function
  main(version_tag, loc_id)
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  
} else {
  begin_time <- Sys.time()
  
  
  version_tag <- '2024_07_24.01'
  loc_id <- 33
  
  # Run main function
  main(version_tag, loc_id)
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  
}
