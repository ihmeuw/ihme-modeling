################################################################################
## Purpose: Prep all inputs for Spectrum
## Date modified:
## Run instructions: 
## Notes:
## 1) Declare which inputs are estimated as part of GBD; inputs not
## 		estimated for GBD will use the most recent year of UNAIDS data
##		for each location
## 2) Iterate through inputs and copy 1:1 locations that are run through  
## 		Spectrum into a current_run folder for that input
##		a) Extend input out to current year by copying the last year of data
## 		b) Identify inputs with non-ihme_loc_id location identifiers and write
##			 them to the current_run folder with an ihme_loc_id
################################################################################

### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,"FILEPATH")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, "FILEPATH")

## Packages
library(data.table); library(parallel)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
	current.run <- args[1]
	ncores <- args[2]
} else {
	current.run <- ""  # Update with new Spectrum name 
	ncores <- 40  # Cores for multi-processing
}

cyear <- 2016  # GBD year

date <- substr(gsub("-", "", Sys.Date()), 3, 8)

# UNAIDS data folder names with most recent data first
input.source.list <- list(unaids_2016 = "UNAIDS_2016", unaids_2015 = "UNAIDS_2015", unaids_2013 = "140520")

# Folder name for custom GBD run
inputs.list <- list(
	migration = "",
	population = "",
	ASFR = "",
	TFR = "",
	SRB = "",
	survivalRates = "",
	averageCD4duration = "",
	noARTmortality = "",
	onARTmortality = "",
	adultARTeligibility = "",
	adultARTcoverage = "",
	childARTeligibility = "",
	childARTcoverage = "",
	TFRreduction = "",
	PMTCT = "",
	PMTCTdropoutRates = "",
	percentBF = "",
	incidence = "",
	prevalence = "",
	incSexRatio = ""
)

# Inputs to update
	migration <- F
	population <- F
	ASFR <- F
	TFR <- F
	SRB <- F
	survivalRates <- F
	averageCD4duration <- T
	noARTmortality <- T
	onARTmortality <- F
	adultARTeligibility <- F
	adultARTcoverage <- F
	childARTeligibility <- F
	childARTcoverage <- F
	TFRreduction <- F
	PMTCT <- F
	PMTCTdropoutRates <- F
	percentBF <- F
	incidence <- F
	prevalence <- F
	incSexRatio <- F

# Fill in arguments to break loops
test <- F

# Subset inputs
inputs.idx <- c()
for(input in names(inputs.list)) { 
	if(get(input)) {
		idx <- which(names(inputs.list) == input)
		inputs.idx <- c(inputs.idx, idx)
	}
}
subset.inputs <- inputs.list[inputs.idx]

### Paths
estimates.dir <- paste0(root, "FILEPATH")
run.dir <- paste0(estimates.dir, current.run, "/")
run.tracking.path <- paste0(run.dir, "FILEPATH")
overall.tracking.path <- paste0(estimates.dir, "FILEPATH")
pop.dir <- paste0(root, "FILEPATH")

## Create run-specific directory
dir.create(run.dir, showWarnings = F)

### Functions
del_files <- function(dir, prefix = "", postfix = "") {
	system(paste0("perl -e 'unlink <", dir, "/", prefix,  "*", postfix, ">' "))
}

### Tables
loc.table <- get_locations()
# For mapping to old location identifiers
input.loc.map <- fread(paste0(root, "FILEPATH"))[, .(ihme_loc_id, gbd15_spectrum_loc)]
input.struc <- fread(paste0(root, "FILEPATH"))

## Create tracking table
spectrum.locs <- loc.table[spectrum == 1, ihme_loc_id]
run.locs <- spectrum.locs
tracking.dt <- data.table(expand.grid(location = spectrum.locs, input = names(inputs.list), stringsAsFactors = F), folder_name = "", source = "", level = "")
# tracking.dt <- fread(overall.tracking.path)
# tracking.dt[input %in% names(subset.inputs) & location %in% run.locs, c("source", "folder", "level") := ""]

group1.list <- loc.table[spectrum == 1 & group %in% c("1A", "1B"), ihme_loc_id]

## Prep ihme_loc_id to non-ihme_loc_id map
loc.map <- input.loc.map[ihme_loc_id != gbd15_spectrum_loc & ihme_loc_id %in% spectrum.locs]

## Parent-child table for subnationals
subnat.locs <- spectrum.locs[grepl("_", spectrum.locs)]
parent.child.table <- data.table()
for(loc in subnat.locs) {
	parent.location <- loc.table[ihme_loc_id == loc, parent_id]
	while(loc.table[location_id==parent.location, level] != 3) {
		parent.location <- loc.table[location_id == parent.location, parent_id]
		if(loc.table[location_id==parent.location, level] > 3) break
	}
	parent.ihme_loc <- loc.table[location_id == parent.location, ihme_loc_id]
	parent.child.table <- rbind(parent.child.table, data.table(parent = parent.ihme_loc, child = loc))
}

# Subnat split table
subnat.split.table <- list(
	adultARTcoverage = list(sex = 1:2, age = c(8:20, 30:32, 235)),
	childARTcoverage = list(sex = 3, age = 2:7),
	PMTCT = list(sex = 2, age = 9:11)
)

# Subnat pop table
locs <- unique(c(parent.child.table$parent, parent.child.table$child))
pop.locs <- loc.table[ihme_loc_id %in% locs, location_id]
if(length(pop.locs) > 0) {
	subnat.pop.table <- get_population(age_group_id = -1, location_id = pop.locs, year_id = -1, sex_id = -1, location_set_id = 79)
}

############
### Code ###
############

## Iterate through inputs that are being updated
for(cinput in names(subset.inputs)) {
	if(test) cinput <- "adultARTeligibility"
	print(cinput)
	# Determine input structure
	struc <- input.struc[input == cinput]
	dir <- struc$directory
	extra.folder <- struc$extra_folder
	suffix <- struc$suffix
	id.vars <- strsplit(struc$id.vars, ", ")[[1]]
	value.vars <- strsplit(struc$value.vars, ", ")[[1]]
	value.metric <- strsplit(struc$value.metric, ", ")[[1]]
	if(any(grepl("\\*", value.vars))) {
		value.vars <- paste0(gsub("\\*", "", value.vars), struc$draw_start:struc$draw_end)  # include all draws
		value.metric <- rep(value.metric, length(struc$draw_start:struc$draw_end))
	}
	

	# Create/clear out current run directory as needed
	output.save.folder <- "current_run"
	output.dir <- paste0(dir, output.save.folder, extra.folder)
	dir.create(output.dir, showWarnings = F, recursive = T)
	del_files(output.dir)

	# Add GBD estimate to source list
	if(inputs.list[[cinput]] != "") {
		source.list <- c(GBD16 = inputs.list[[cinput]], input.source.list)  # append GBD estimate to front of list to keep priority
	} else {
		source.list <- input.source.list
	}

	#######
	### One to one locations
	#######

	print("1:1")
	# Iterate through sources to look for input data
	for(csource in names(source.list)) {
		if(test) csource <- "unaids_2013"
		print(csource)
		# if(cinput %in% c("incidence", "prevalence") & csource %in% c("unaids_2016", "unaids_2015")){
		# 	next
		# }

		if(cinput %in% c("TFR", "ASFR", "SRB") & csource != "GBD16") break

		input.dir <- paste0(dir, source.list[[csource]], extra.folder)

		if(csource == "GBD16") {
			# Source all spectrum locations in GBD estimates
			file.list <- list.files(input.dir)			
			source.locs <- gsub(suffix, "", file.list)
			loc.list <- source.locs[source.locs %in% spectrum.locs]
			if(any(source.locs %in% loc.map$gbd15_spectrum_loc)) {
				add.locs <- loc.map[gbd15_spectrum_loc %in% source.locs, ihme_loc_id]
				loc.list <- c(loc.list, add.locs)
			}
			if(cinput %in% c("incidence", "prevalence")) {
				loc.list <- c(loc.list, grep("IND_", loc.table[epp == 1, ihme_loc_id], value = T))
			}
		} else {
			# Source for all locations where data is available for that source and not already sourced
			source.locs <- loc.table[get(csource) == 1 & ihme_loc_id %in% spectrum.locs, ihme_loc_id]
			if(csource == "unaids_2013" & cinput %in% c("incidence", "prevalence")) {
				chn.list <- loc.table[grepl("CHN", ihme_loc_id) & spectrum == 1, ihme_loc_id]
				source.locs <- c(setdiff(source.locs, "CHN"), chn.list)
			}
			sourced.locs <- tracking.dt[!(source == "") & input == cinput, location]
			loc.list <- setdiff(source.locs, sourced.locs)
		}

		# Iterate through locations, read in the data, extend to current year, and write
		if(length(loc.list) > 0 ) {
			mclapply(loc.list, function(loc) {
			# for(loc in loc.list) {
				# if(test) loc <- "MYS"
				print(loc)
				input.path <- paste0(input.dir, loc, suffix)
				if(file.exists(input.path)) { 
					if (cinput=="percentBF") {dt <- data.table(read.csv(input.path, blank.lines.skip=T))} else {
					dt <- tryCatch({fread(input.path)}, error = function(error) {data.table(read.csv(input.path, blank.lines.skip=T))})}
				} else {
					if(loc %in% unique(loc.map$ihme_loc_id)) {
						if(cinput %in%  c("adultARTeligibility", "childARTeligibility", "TFRreduction", "PMTCTdropoutRates", "percentBF") & grepl("ZAF", loc)) {
							# Exception for South Africa because counts are split by HIV prevalence
							alt.loc <- "ZAF"
							if (cinput=="percentBF") {
								input.loc.map <- fread(paste0(root, "FILEPATH"))[,.(ihme_loc_id, gbd15_spectrum_loc)]
								iso3 <- input.loc.map[ihme_loc_id==loc, gbd15_spectrum_loc]	
								alt.loc <- iso3
							}
						} else {
							# Old location identifier
							alt.loc <- loc.map[ihme_loc_id == loc, gbd15_spectrum_loc]
						}
						input.path <- paste0(input.dir, alt.loc, suffix)
						dt <- tryCatch({fread(input.path)}, error = function(error) {data.table(read.csv(input.path, blank.lines.skip=T))})
					} else {
						print(paste0(loc), "missing")
					}
				}

				if(cinput == "TFRreduction" & "year" %in% names(dt)) {
					ages <- c("15-19", "20-24", "25-29", "30-34", "35-39", "40-44", "45-49")
					values <- unique(round(dt[year < 2000, tfr_ratio], 2))
					dt <- data.table(age = ages, tfr_ratio = values)
				}

				# Extend data to 2016 if needed
				if("year" %in% names(dt) & !(cyear %in% dt$year)) {
					extension.dt <- dt[year == (cyear - 1)]
					extension.dt[, year := cyear]
					out.dt <- rbind(dt, extension.dt)
				} else {
					out.dt <- dt
				}

				if (cinput == "adultARTcoverage") {

					gbd.pop.start <- fread(paste0(pop.dir, loc, ""))[age >=15]
					adult.pop.start <- gbd.pop.start[, lapply(.SD, sum), by=c("year", "sex")]
					setnames(adult.pop.start, "value", "pop_start")
					adult.pop.start[, age:=NULL]
					merge.dt <- merge(out.dt, adult.pop.start[, .(year, sex, pop_start)], by=c("year", "sex")) 
					merge.dt[, ART_cov_pct_total:= 100*ART_cov_num/pop_start]
					merge.dt[,pop_start:=NULL]
					out.dt <- merge.dt
				}

				write.csv(out.dt, paste0(output.dir, loc, suffix), row.names = F)
			# }
			}, mc.cores = ncores)
		}

		# Update tracker
		tracking.dt[location %in% loc.list & input == cinput, c("source", "folder", "level") := .(csource, source.list[[csource]], "Same")]

		# Identify remaining locations and break if all locations are accounted for
		remaining.locs <- tracking.dt[input == cinput & source == "", location]
		if(length(remaining.locs) == 0) break

		if (cinput=="migration") {
	    year <- seq(1970, 2020) 
	    age <- seq(0, 80)
	    sex <- c(1,2)
	    value <- 0
	    out.dt <- expand.grid(year=year, age=age, sex=sex, value=value)
	    mclapply(remaining.locs, function(loc) {
	    	write.csv(out.dt, paste0(output.dir, loc, suffix), row.names = F)
	    }, mc.cores = ncores)

	    # Update tracker
			tracking.dt[location %in% remaining.locs & input == cinput, c("source", "folder", "level") := .("DUSERt", "", "Zero")]
  	}
	}
	
	# Identify remaining locations and break if all locations are accounted for
	remaining.locs <- tracking.dt[input == cinput & source == "", location]
	if(length(remaining.locs) == 0) next

	#######
	### Subnational Splitting
	#######

	## Subnational locations using national data for non-GBD estimate locations
	print("Subnational")
	for(csource in names(source.list)) {
		if(test) csource <- "unaids_2015"
		# print(csource)
		input.dir <- paste0(dir, source.list[[csource]], extra.folder)
		if(csource == "GBD16") {
			# Source all spectrum locations in GBD estimates
			file.list <- list.files(input.dir)			
			source.locs <- gsub(suffix, "", file.list)
		} else {
			source.locs <- loc.table[get(csource) == 1, ihme_loc_id]
		}

		# if(cinput %in% c("incidence", "prevalence") & csource %in% c("unaids_2016", "unaids_2015")){
		# 	next
		# }

		# Identify subnationals in remaining locations and compile national level parents
		subset.parent.table <- parent.child.table[parent %in% source.locs & child %in% remaining.locs]

		# Iterate through parents, read in the data, extend to current year, and write for all subnationals of that parent
		lapply(unique(subset.parent.table$parent), function(cparent) {
			# cparent <- "BRA"
			print(cparent)
			child.list <- subset.parent.table[parent==cparent, child]
			input.path <- paste0(input.dir, cparent, suffix)
			if(file.exists(input.path)) {
				dt <- tryCatch({fread(input.path)}, error = function(error) {data.table(read.csv(input.path, blank.lines.skip=T))})
				

				if(cinput == "TFRreduction" & "year" %in% names(dt)) {
					ages <- c("15-19", "20-24", "25-29", "30-34", "35-39", "40-44", "45-49")
					values <- unique(round(dt[year < 2000, tfr_ratio], 2))
					dt <- data.table(age = ages, tfr_ratio = values)
				}

				# Extend data to 2016 if needed
				if("year" %in% names(dt) & !(cyear %in% dt$year)) {
					extension.dt <- dt[year == (cyear - 1)]
					extension.dt[, year := cyear]
					out.dt <- rbind(dt, extension.dt)
				} else {
					out.dt <- dt
				}

				# Split and write for all subnationals
				mclapply(child.list, function(child) {
					# child <- child.list[1]
					# print(child)
					# Pop split for count inputs
					if("Count" %in% value.metric) {
						parent.id <- loc.table[ihme_loc_id == cparent, location_id]
						child.id <- loc.table[ihme_loc_id == child, location_id]
						sexes <- subnat.split.table[[cinput]]$sex
						ages <- subnat.split.table[[cinput]]$age
						parent.pop <- subnat.pop.table[age_group_id %in% ages & sex_id %in% sexes & location_id == parent.id]
						setnames(parent.pop, "population", "parent")
						child.pop <- subnat.pop.table[age_group_id %in% ages & sex_id %in% sexes & location_id == child.id]
						setnames(child.pop, "population", "child")
						merged.pop <- merge(parent.pop, child.pop, by = c("age_group_id", "year_id", "sex_id"))
						collapsed.pop <- merged.pop[, lapply(.SD, sum), by = .(year_id, sex_id), .SDcols = c("parent", "child")]
						collapsed.pop[, prop := child / parent]
						setnames(collapsed.pop, c("year_id", "sex_id"), c("year", "sex"))
						if(length(sexes) > 1){
							merge.vars <- c("year", "sex")
						} else {
							merge.vars <- "year"
						} 
						merged.dt <- merge(out.dt, collapsed.pop[, .(year, sex, prop)], by = merge.vars)
						vars <- grep("num", names(merged.dt), value = T)
						for (var in vars) {
							merged.dt[, (var) := get(var) * prop]
						}
						if("value" %in% names(dt)) {
							merged.dt[, value := ART_cov_num + ART_cov_pct]
						}
						merged.dt[, prop := NULL]
						if(length(sexes) == 1) {
							merged.dt[, sex := NULL]
						}
						split.dt <- merged.dt
					} else {
						split.dt <- out.dt
					}

				if (cinput == "adultARTcoverage") {
					gbd.pop.start <- fread(paste0(pop.dir, child, ""))[age >=15]
					adult.pop.start <- gbd.pop.start[, lapply(.SD, sum), by=c("year", "sex")]
					setnames(adult.pop.start, "value", "pop_start")
					adult.pop.start[, age:=NULL]
					merge.dt <- merge(split.dt, adult.pop.start[, .(year, sex, pop_start)], by=c("year", "sex")) 
					merge.dt[, ART_cov_pct_total:= 100*ART_cov_num/pop_start]
					merge.dt[,pop_start:=NULL]
					split.dt <- merge.dt
				}


					write.csv(split.dt, paste0(output.dir, child, suffix), row.names = F)
				}, mc.cores = ncores)
			} else {
				# Read in the subnational file directly and write it if there isn't a national file
				mclapply(child.list, function(child) {
					input.path <- paste0(input.dir, child, suffix)
					dt <- fread(input.path)
					if(cinput == "TFRreduction" & "year" %in% names(dt)) {
						mean.dt <- dt[year < 2000, .(mean = mean(tfr_ratio)), by = "age"]
						ages <- c("15-19", "20-24", "25-29", "30-34", "35-39", "40-44", "45-49")
						values <- mean.dt$mean
						dt <- data.table(age = ages, tfr_ratio = values)
					}

					# Extend data to 2016 if needed
					if("year" %in% names(dt) & !(cyear %in% dt$year)) {
						extension.dt <- dt[year == (cyear - 1)]
						extension.dt[, year := cyear]
						out.dt <- rbind(dt, extension.dt)
					} else {
						out.dt <- dt
					}

				if (cinput == "adultARTcoverage") {
					gbd.pop.start <- fread(paste0(pop.dir, child, ""))[age >=15]
					adult.pop.start <- gbd.pop.start[, lapply(.SD, sum), by=c("year", "sex")]
					setnames(adult.pop.start, "value", "pop_start")
					adult.pop.start[, age:=NULL]
					merge.dt <- merge(out.dt, adult.pop.start[, .(year, sex, pop_start)], by=c("year", "sex")) 
					merge.dt[, ART_cov_pct_total:= 100*ART_cov_num/pop_start]
					merge.dt[,pop_start:=NULL]
					out.dt <- merge.dt
				}

					write.csv(out.dt, paste0(output.dir, child, suffix), row.names = F)
				}, mc.cores = ncores)
			}
			# Update tracker
			tracking.dt[location %in% child.list & input == cinput, c("source", "folder", "level") := .(csource, source.list[[csource]], "National")]
		})

		# Identify remaining locations and break if all locations are accounted for
		remaining.locs <- tracking.dt[input == cinput & source == "", location]
		if(length(remaining.locs) == 0) break
	}

	# Identify remaining locations and break if all locations are accounted for
	remaining.locs <- tracking.dt[input == cinput & source == "", location]
	if(length(remaining.locs) == 0) next

	#######
	### Regional Averages
	#######

	## Calculate regional average for remaining locations
	print("Regional Average")
	regions <- unique(loc.table[ihme_loc_id %in% remaining.locs, region_name])
	for(region in regions) {
		if(test) region <- regions[2]; print(region)
		# Read in all locations in region that have already been sourced
		region.dt.locs <- setdiff(loc.table[region_name == region & spectrum == 1, ihme_loc_id], c(remaining.locs, group1.list))
		region.dt <- rbindlist(
			mclapply(region.dt.locs, function(loc) {
				# if(test) loc <- region.dt.locs[2]
				input.path <- paste0(output.dir, loc, suffix)
				dt <- fread(input.path)
				dt <- dt[, c(id.vars, value.vars), with = F]
				if("year" %in% names(dt)) {
					# Merge on year specific population
					dt[, year := as.integer(year)]
					in.pop <- fread(paste0(pop.dir, loc, ""))
					loc.pop <- in.pop[, .(population = sum(value)), by =.(year)]
					merged.dt <- merge(dt, loc.pop, by = "year")
					merged.dt[, loc := loc]
				} else {
					merged.dt <- dt
				}
				return(merged.dt)
			}, mc.cores = ncores)
		, use.names = T, fill = T)

		if(length(id.vars) == 0) {
			# Add dummy for data with no id.vars
			region.dt[, dummy := .I]
			id.vars <- "dummy"
		}

		if("Rate" %in% value.metric) {
			# Convert rates to counts
			rate.vars <- value.vars[which(value.metric == "Rate")]
			for(rate.var in rate.vars) {
				region.dt[, (rate.var) := as.numeric(get(rate.var)) * population]
			}
		}

		if("Percent" %in% value.metric) {
			# Convert percentages to counts
			percent.vars <- value.vars[which(value.metric == "Percent")]
			for(percent.var in percent.vars) {
				region.dt[, (percent.var) := (get(percent.var) / 100) * population]
			}
		}

		if("Percent" %in% value.metric & "Count" %in% value.metric) {
			# Create single value column for Rate/Count combo
			hold.value.vars <- value.vars
			value.vars <- unique(gsub("_pct", "", gsub("_num", "", value.vars)))
			for(value.var in value.vars) {
				region.dt[, (value.var) := get(paste0(value.var, "_pct")) + get(paste0(value.var, "_num"))]
			}
		}

		# Calculate mode of ART eligibility thresholds
		if ("Category" %in% value.metric) {
      ## mode function 
     	getmode <- function(v) {
 				uniqv <- unique(v)
				return(uniqv[which.max(tabulate(match(v, uniqv)))])
			}
			sum.dt <- region.dt[, lapply(.SD, getmode), by = id.vars, .SDcols = c(value.vars)]
		} else {
			# Calculate average (sum of counts over total population in the sourced region locations)
			if("population" %in% names(region.dt)){
				sd.vars <- c(value.vars, "population")
			} else {
				sd.vars <- value.vars
			}
			if("Ratio" %in% value.metric) {
				sum.dt <- region.dt[, lapply(.SD, mean), by = id.vars, .SDcols = sd.vars]
			} else {
				sum.dt <- region.dt[, lapply(.SD, sum), by = id.vars, .SDcols = sd.vars]
				for(value.var in value.vars) {
					sum.dt[, (value.var) := get(value.var) / population]
				}
				sum.dt[, population := NULL]
			}
		}
		if("Percent" %in% value.metric & "Count" %in% value.metric) {
			# Rename percent column and create 0 count column
			setnames(sum.dt, value.vars, paste0(value.vars, "_pct"))
			sum.dt[, (paste0(value.vars, "_num")) := 0]
			value.vars <- hold.value.vars
		}

		if("Percent" %in% value.metric) {
			# Convert rates to percentages for variables that were originally percentages
			percent.vars <- value.vars[which(value.metric == "Percent")]
			for(percent.var in percent.vars) {
				sum.dt[, (percent.var) := .(get(percent.var) * 100)]
			}
		}

		# Write regional average for all no data locations in the region
		region.locs <- loc.table[ihme_loc_id %in% remaining.locs & region_name == region, ihme_loc_id]
		for(loc in region.locs) {
			if("Count" %in% value.metric & !("Percent" %in% value.metric)) {
				# Merge on year specific population
				in.pop <- fread(paste0(pop.dir, loc, "FILEPATH"))
				loc.pop <- in.pop[, .(population = sum(value)), by =.(year)]
				sum.dt <- merge(sum.dt, loc.pop, by = c("year"))
				# Convert rates to count for variables that were originally counts
				for(value.var in value.vars) {
					sum.dt[, (value.var) := get(value.var) * population]
				}
				sum.dt[, population := NULL]
			}
			if("dummy" %in% names(sum.dt)) {
				sum.dt[, dummy := NULL]
			}

				if (cinput == "adultARTcoverage") {
					gbd.pop.start <- fread(paste0(pop.dir, loc, ""))[age >=15]
					adult.pop.start <- gbd.pop.start[, lapply(.SD, sum), by=c("year", "sex")]
					setnames(adult.pop.start, "value", "pop_start")
					adult.pop.start[, age:=NULL]
					merge.dt <- merge(sum.dt, adult.pop.start[, .(year, sex, pop_start)], by=c("year", "sex")) 
					merge.dt[, ART_cov_pct_total:= 100*ART_cov_num/pop_start]
					merge.dt[,pop_start:=NULL]
					sum.dt <- merge.dt
				}	
			write.csv(sum.dt, paste0(output.dir, loc, suffix), row.names = F)
		}
		# Update tracker
		tracking.dt[location %in% region.locs & input == cinput, c("source", "folder", "level") := .(region, "", "Regional")]
	}

	## Add Universal eligibility in 2016
	if(cinput == "adultARTeligibility") {
		file.list <- list.files(output.dir)
		mclapply(file.list, function(file) {
			dt <- fread(paste0(output.dir, file))
			dt[year >= 2016, cd4_threshold := 999]
			write.csv(dt, paste0(output.dir, file), row.names = F)
		}, mc.cores = ncores)
	}
}


## Write overall tracking table
write.csv(tracking.dt, overall.tracking.path, row.names = F)

## Subset and write run tracking path
write.csv(tracking.dt[location %in% run.locs], run.tracking.path, row.names = F)

### End