#'####################################`INTRO`##########################################
#' Purpose: creating custom CSMR input for Pelvic Inflammatory Disease
#'####################################`INTRO`##########################################


date <- gsub("-", "_", Sys.Date())

# Read in Arguments ------------------------------------------------------
cat(paste0("Reading In Arguments..."))
args <- commandArgs(trailingOnly = TRUE)
print(args)

loc_id <- args[1]

# get shared functions 
functions_dir <- "fILEPATH"
functs <- c("get_outputs", "get_ids", "get_draws", "get_population")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))

# source custom functions ------------------------------------------------------
age_table_code <- paste0("FILEPATH") 
age_table <- source(age_table_code)

#get proper ids 
age_groups <- c(seq(15,54,by=5))
age_ids <- ages[age_start %in% age_groups, age_group_id]
years <- seq(1990,2019,5)
eti_ids <- c(395, 396, 399) #chlamydia, gonorrhea, other

#get cod-corrected deaths
print(paste0("getting draws for location_id: ", loc_id))

deaths <- get_draws(gbd_id_type = "cause_id", gbd_id = eti_ids, source = "codcorrect", status = "best", location_id = loc_id, year_id = years, age_group_id = age_ids, sex_id = 2, measure_id = 1)
vars <- c(paste0("draw_", seq(0,999, by=1)))
all_deaths <- unique(deaths[, (vars) := lapply(.SD, function(x) sum(x)), .SDcols=vars , by = c("age_group_id", "location_id", "year_id")], by = c("age_group_id", "location_id", "year_id"))  

pops <- get_population(age_group_id = age_ids, location_id = loc_id, year_id = years, sex_id = 2)

death_pops <- merge(all_deaths, pops, by = c("age_group_id", "location_id", "year_id", "sex_id"))
death_pops[ ,c("cause_id", "measure_id", "metric_id", "sex_id", "run_id") := NULL]

csmr <- death_pops[ ,.(dmean = rowMeans(.SD)), by = c("age_group_id", "location_id", "year_id", "population")]
csmr[ ,csmr := dmean/population]

write.csv(csmr)
