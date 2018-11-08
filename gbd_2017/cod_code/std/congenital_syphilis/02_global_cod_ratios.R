#####################################INTRO#############################################
#' @author: 
#' 6/26/18
#' @purpose: Calculate the global ratios of VR syphilis deaths for each age group relative 
#'           to neonatal VR deaths in order to expand calculated neonatal deaths into total deaths under age 10.
#'           In other words, this is used to extrapolate how many TOTAL deaths there are from congenital syphilis
#'           based on the neonatal deaths we calculate in this pipeline and also to split the neonatal deaths we
#'           do calculate to age-group and sex-specific estimates. We have to do it this way because our meta-analysis
#'           is based on neonatal death rates, not overall congenital syphilis death rates, which is harder to study
#'          @1 Get CoD VR data for syphilis deaths under age 10
#'          @2 Collapse deaths globally by age group and sex to get total deaths for each under age 10
#'          @3 Divide VR deaths for each age group - sex pair by the total VR neonatal deaths to get the ratio
#'             of `CATEGORY DEATHS to NEONATAL DEATHS``
#'          @5 Save
#'
#' @outputs: cod death proportions for scaling deaths up to age 10 in csv:
#'          FILEPATH/global_cod_ratios.csv
#'
#####################################INTRO#############################################


# Functions ---------------------------------------------------------------

#' Helper function to scale congenital syphilis VR deaths to ratios based
#' on the total number of neonatal deaths from VR data.
#'
#' @param deaths                number of deaths (globally) recorded in VR data for the given category (age group - sex)
#' @param total_neonatal_deaths number to use as the denominator in the ratio, the total neonatal deaths from vr
#' 
#' @return ratio of # of vr deaths for category to total neonatal vr deaths
#' 
#' @return a death ratio for the given age group based on the total neonatal deaths
scale_deaths_help <- function(deaths, total_neonatal_deaths) {
  return(deaths / total_neonatal_deaths)
}

#' Wrapper for scale_deaths_helper function to allow use in data.table/dplyr.
#' Needed because arguments need to be vectorizable to work for these tools
#'
#' @param deaths number of deaths (globally) recorded in VR data
#' 
#' @return vector of scaled deaths
scale_deaths <- function(deaths, total_neonatal_deaths) {
  # mapply returns a usable vector; lapply would return a list which is less useful
  mapply(scale_deaths_help, deaths = deaths, total_neonatal_deaths = total_neonatal_deaths)
}

# Get syphilis COD data under age 10 --------------------------------------

# Grabbing global cause of death data for syphilis
cause_id       <- 394 # syphilis
age_groups_ids <- 2:6 # early neonates to age 10

message(paste(Sys.time(), "Getting cod data for global syphilis deaths ratios by age group"))
cod_data <- get_cod_data(cause_id = cause_id, age_group_id = age_groups_ids)

# drop all non VR data
vr <- cod_data[data_type == "Vital Registration", ]

# sum all deaths, collapsing by age group and sex
vr <-  vr[, lapply(.SD, sum, na.rm = TRUE), .SD = c("deaths"), by = c("age_group_id", "age_name", "sex")]

# Calculate death proportions by age group --------------------------------

# get total neonatal deaths by sex
total_neonatal_deaths <- vr[age_group_id %in% c(2, 3), lapply(.SD, sum, na.rm = TRUE), .SD = "deaths", by = "sex"]

# scale deaths based on total recorded neonatal deaths
vr[, ratio := scale_deaths(deaths, total_neonatal_deaths = sum(total_neonatal_deaths$deaths))]



# Create diagnostics and save ---------------------------------------------

# DIAGNOSTICS
library(ggplot2)

pdf(paste0(out_dir, "EX/cod_ratios_diagnostics_", Sys.Date(), ".pdf"))

ggplot(vr, aes(age_name, ratio, fill = as.factor(sex))) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(y = "Ratio",
       title = "Global congenital syphilis death ratios for age groups under 10,\nrelative to total neonatal deaths")

dev.off()

# drop unneeded columns and save ratios
vr <- vr[, c("age_group_id", "sex", "ratio")]

message(paste(Sys.time(), "Saving global cod ratios as a csv in models/"))
readr::write_csv(vr, paste0(out_dir, "EX/global_cod_ratios.csv"))








