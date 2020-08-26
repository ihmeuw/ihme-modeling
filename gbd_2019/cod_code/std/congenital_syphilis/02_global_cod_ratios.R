#####################################INTRO#############################################

#' @purpose: Calculate the global ratios of VR syphilis deaths for each age group relative 
#'           to neonatal VR deaths in order to expand calculated neonatal deaths into total deaths under age 10.
#'           In other words, this is used to extrapolate how many TOTAL deaths there are from congenital syphilis
#'           based on the neonatal deaths we calculate in this pipeline and also to split the neonatal deaths we
#'           do calculate to age-group and sex-specific estimates. 
#'          @1 Get CoD VR data for syphilis deaths under age 10
#'          @2 Collapse deaths globally by age group and sex to get total deaths for each under age 10
#'          @3 Divide VR deaths for each age group - sex pair by the total VR neonatal deaths to get the ratio
#'             of `CATEGORY DEATHS to NEONATAL DEATHS``
#'          @5 Save
#'
#' @outputs: cod death proportions for scaling deaths up to age 10 in csv:
#'          FILEPATH
#'
#####################################INTRO#############################################

# Functions ---------------------------------------------------------------

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
	mapply(scale_deaths_help, deaths = deaths, total_neonatal_deaths = total_neonatal_deaths)
}

#Get syphilis Cause of Death data under age 10 --------------------------------------

#Grabbing global cause of death data for syphilis
cause_id       <- 394 # syphilis
age_groups_ids <- 2:6 # early neonates to age 10

message(paste(Sys.time(), "Getting cod data for global syphilis deaths ratios by age group"))
cod_data <- get_cod_data(cause_id = cause_id, age_group_id = age_groups_ids, decomp_step = decomp)

#Use high-quality data only to calculate ratios 
vr_star <- read.csv(FILEPATH) 
star_locs <- unique(vr_star$location_id)

#drop all non VR data & all low quality data
vr <- cod_data[data_type == "Vital Registration", ]
vr <- unique(vr)
vr_datarich <- vr[location_id %in% star_locs]

#sum all deaths, collapsing by age group and sex 
vr_datarich <-  vr_datarich[, lapply(.SD, sum, na.rm = TRUE), .SD = c("deaths"), by = c("age_group_id", "age_name", "sex")]

#Calculate death proportions by age group --------------------------------

#get total neonatal deaths by sex & superregion
total_neonatal_deaths <- vr_datarich[age_group_id %in% c(2, 3), lapply(.SD, sum, na.rm = TRUE), .SD = "deaths"] 
setnames(total_neonatal_deaths, "deaths", "totneo_deaths")
total_neonatal_deaths[ ,location_name := "global"]

#scale deaths based on total recorded neonatal death
vr_datarich[ ,location_name := "global"]
vrdr_merge <- merge(vr_datarich, total_neonatal_deaths, by = "location_name", all.x = TRUE)
vrdr_merge[ ,ratio := (deaths/totneo_deaths)]


#Create diagnostics and save ---------------------------------------------

# DIAGNOSTICS
library(ggplot2)

pdf_title <- paste0("Data-Rich (Global), \ncongenital syphilis death ratios for age groups under 10,\nrelative to total neonatal deaths")

pdf(FILEPATH)

ggplot(vrdr_merge, aes(age_name, ratio, fill = as.factor(sex))) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(y = "Ratio", title = pdf_title) + 
   theme(axis.text.x = element_text(angle = 45, hjust = 1))

dev.off()

# drop unneeded columns and save ratios
vrdr_merge <- vrdr_merge[, c("age_group_id","age_name", "sex", "location_name", "deaths", "totneo_deaths", "ratio")]

message(paste(Sys.time(), "Saving global cod ratios as a csv in models/"))
readr::write_csv(vrdr_merge, FILEPATH)  









