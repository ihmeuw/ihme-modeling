################################################################################
## Purpose: Process Rhogam data for modeling
## Input:   RhoD sales data received from Marketing Research Bureau on 10/27/2023
## Output:  A prepared dataframe for uploading to Bundle 10513
################################################################################

# Setup ------------------------------------------------------------------------
source("map_rhod_loc.R")
'%>%' <- magrittr::'%>%'
library(data.table)

# Prep locations ---------------------------------------------------------------
locs <- ihme::get_location_metadata(location_set_id = 35,
                                    release_id = 16)[, .(location_id, location_name)]

# Process RhoD sales -----------------------------------------------------------

# read in sales sheet
rhod_sales_file <- readxl::read_excel(
  'FILEPATH',
  sheet = "RhoD sales 1994-2022"
)

# formatting
rhod_sales <- rhod_sales_file %>%
  dplyr::rename(location_name = Country,
                vials = "Vials Sold (000)",
                year_id = Year) %>%
  dplyr::mutate(vials = vials * 1000) %>%
  dplyr::mutate(location_name = stringr::str_to_title(location_name))

# change to GBD location_names
rhod_sales <- as.data.table(rhod_sales)
update_location_names(rhod_sales) #from map_rhod_loc source

# unique locations:
# Baltic States is not a loc in GBD. Setting as Estonia for now and adjusting
# proportionally for Estonia, Latvia and Lithuania in later section below.
# We are going to add Kosovo's values to Serbia's since GBD maps Kosovo to
# Serbia national.
rhod_sales[location_name == 'Baltic States', location_name := 'Estonia']
rhod_sales[location_name == 'Kosovo', location_name := 'Serbia']

# remove the WinRho products sold by these companies because they are mainly used for ITP (not HDN)
rhod_sales <- rhod_sales %>%
  dplyr::filter(!(
    Company %in% c(
      "CANGENE",
      "BAXTER",
      "EMERGENT BIOSOLUTIONS",
      "APTEVO",
      "SAOL",
      "KAMADA"
    )
  ))

# add location_id column
rhod_sales <-
  merge(rhod_sales,
        locs,
        by = "location_name",
        all.x = TRUE,
        all.y = FALSE)

# Drop Georgia, the US state, because the loc merge above duplicates values to capture both the country and US state
rhod_sales <- rhod_sales %>%
  dplyr::filter(is.na(location_id) | location_id != 533) # need to specify to keep NAs for test

testthat::test_that("Data frame contains no NA values", {
  testthat::expect_true(!any(is.na(rhod_sales)))
})

# calculate the sum of vials by country and year
rhod_sales_summarised <- rhod_sales %>%
  dplyr::group_by(location_name, location_id, year_id) %>%
  dplyr::summarise(sum_vials = sum(vials))

# Process RhoD estimates -------------------------------------------------------

rhod_estimates_file <-
  readxl::read_excel(
    'FILEPATH',
    sheet = 'Country-year',
    skip = 1
  )

# melting the data so we have year and estimate as a column
rhod_estimates <- rhod_estimates_file %>%
  tidyr::pivot_longer(cols = -`Row Labels`,
                      names_to = "Year",
                      values_to = "Estimate")

# formatting
rhod_estimates <- rhod_estimates %>%
  dplyr::rename(location_name = `Row Labels`, year_id = Year) %>% #renaming as location_name column
  dplyr::mutate(location_name = stringr::str_to_title(location_name))

# remove regional headings, which have no data
rhod_estimates <- rhod_estimates %>%
  dplyr::filter(!(
    location_name %in% c(
      "AFRICA",
      "ASIA",
      "CENTRAL AMERICA",
      "EUROPE",
      "MIDDLE EAST",
      "NORTH AMERICA",
      "SOUTH AMERICA"
    )
  )) %>%
  dplyr::filter(!is.na(Estimate))

# change to GBD location_names
rhod_estimates <- as.data.table(rhod_estimates)
update_location_names(rhod_estimates)

# unique locations:
# designating Baltic States as Estonia here and then further adjusting in Baltic States section below
rhod_estimates[location_name == 'Baltic States', location_name := 'Estonia']
# In rhod_sales section we had combined the doses for Serbia and Kosovo due to
# GBD's location system. But here we are going to remove Kosovo's rows from the
# estimation file and use Serbia's estimation designation (all of Kosovo's values
# are identified as not estimated, but Serbia's values for 2014, 2017, 2020 are estimated.
# Keep NA values, if there are any, for test, since filter function drops NA values.
rhod_estimates <- rhod_estimates %>%
  dplyr::filter(is.na(location_name) | location_name != "Kosovo")

# add location_id column
rhod_estimates <-
  merge(rhod_estimates,
        locs,
        by = "location_name",
        all.x = TRUE,
        all.y = FALSE)

# Drop Georgia, the US state, because the loc merge above duplicates values to capture both the country and US state
rhod_estimates <- rhod_estimates %>%
  dplyr::filter(is.na(location_id) | location_id != 533)

testthat::test_that("Data frame contains no NA values", {
  testthat::expect_true(!any(is.na(rhod_estimates)))
})

# Merge sales and estimates files ----------------------------------------------

rhod_sales_master <- merge(
  rhod_sales_summarised,
  rhod_estimates,
  by = c("location_name", "location_id", "year_id"),
  all.x = TRUE,
  all.y = FALSE
) # we don't need all of rhod_estimates because it includes the exclusion companies

testthat::test_that("Data frame contains no NA values", {
  testthat::expect_true(!any(is.na(rhod_sales_master)))
})

# Baltic states ----------------------------------------------------------------

setDT(rhod_sales_master)

# Sales values are adjusted proportionally by number of live births
baltic_births <- ihme::get_covariate_estimates(
  nch::id_for("covariate", "Live Births (thousands)"),
  year_id = rhod_sales_master[location_name == "Estonia", year_id],
  location_id = nch::id_for("location", c("Estonia", "Latvia", "Lithuania")),
  release_id = nch::id_for("release", "GBD 2023")
)[, .(location_id, location_name, year_id, mean_value)]

# rename
setnames(baltic_births, 'mean_value', 'births')

baltic_births <- baltic_births %>%
  dplyr::group_by(year_id) %>%
  dplyr::mutate(total_baltic_births = sum(births)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(proportion = births / total_baltic_births)

setDT(baltic_births)

baltic_births <- merge(baltic_births,
                       rhod_sales_master[location_name == "Estonia", c("year_id", "sum_vials", "Estimate")],
                       by = "year_id",
                       all = TRUE)

baltic_births <- baltic_births %>%
  dplyr::mutate(vials = sum_vials * proportion)

baltic_births <-
  baltic_births[, c("year_id", "vials", "location_name", "location_id", "Estimate")] |>
  dplyr::rename(sum_vials = vials)

rhod_sales_master <- rhod_sales_master %>%
  dplyr::filter(location_name != "Estonia")

rhod_sales_master <- rbind(rhod_sales_master,
                           baltic_births)

# Test -------------------------------------------------------------------------
testthat::test_that("Data frame contains no NA values", {
  testthat::expect_true(!any(is.na(rhod_sales_master)))
})

testthat::test_that("Data frame contains no duplicate rows", {
  testthat::expect_true(all(!duplicated(rhod_sales_master)))
})

# Save output of RhoD adjusted pregnancies -------------------------------------
params_global <- readr::read_rds("params_global.rds")
data.table::fwrite(
  rhod_sales_master,
  file.path("FILEPATH")
)
