rm(list = ls())
user <- "USERNAME"
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
hpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
library(data.table)
c.proj <- "-P proj_hiv"
code.root <- paste0(hpath,"hiv_forecasting_inputs/")
################################################
### Set Run Settings
################################################
c.fbd_version <- "20170727"
r.drivers  <- 1
r.haz_roc  <- 1
r.art_fit  <- 1
r.art      <- 1
r.inc      <- 1
r.scale    <- 1
r.agg      <- 1
r.compare  <- 1


###load run arguments
c.args <- fread(paste0(code.root,"run_versions.csv"))
c.args <- c.args[fbd_version==c.fbd_version]
c.gbd_version <- c.args[["gbd_version"]]
extension.year <- c.args[["extension_year"]]
c.draws <- c.args[["draws"]]
scenarios <- c()
for(scen in c("reference", "better", "worse")) {
	if(c.args[[scen]] == 1) {
		scenarios <- c(scenarios, scen)
	}
}
dah.scalar <- c.args[["dah_scalar"]]
if(dah.scalar != 0) {
	dah_scenario <- T
	c.ref <- c.args[["reference_dir"]]
} else {
	dah_scenario <- F
}

### Functions
source(paste0(hpath, "FILEPATH/shared_functions/get_locations.R"))

### Tables
loc.table <- get_locations()


# Set locations
agg.locs <- c("ZWE", "CIV", "MOZ", "HTI", "IND_44538", "SWE", "MDA", "ZAF", "KEN", "JPN", "GBR", "USA", "MEX", "BRA", "SAU", "IND", "GBR_4749", "CHN_44533", "CHN", "IDN")
island.locs <- c("GRL", "GUM", "MNP", "PRI", "VIR", "ASM", "BMU")
locs.list <- setdiff(loc.table[spectrum == 1, ihme_loc_id], island.locs)
spec.locs <- loc.table[spectrum == 1, ihme_loc_id]
nat.locs <- setdiff(loc.table[level == 3, ihme_loc_id], island.locs)
subnat.list <- loc.table[spectrum == 1 & level > 3, ihme_loc_id]

parent.locs <- loc.table[spectrum ==1 & most_detailed == 0 & new_location_2016 != 1, ihme_loc_id]
parent.locs <- parent.locs[!(parent.locs %in% c("MDA_44823", "MDA_44824", "MOZ_44827"))]
parent.locs <- c(parent.locs, "IND_44538")

####################
### DAH Scenario
###################


if(dah_scenario) {
	scenarios <- "reference"

	r.drivers  <- 0
	r.haz_roc  <- 0
	r.art_fit  <- 0

	# Reset locations
	inputs <- fread("FILEPATH/forecasted_inputs.csv")
	locs.list <- intersect(unique(inputs[variable == "HIV DAH" & (year_id == extension.year & pred_mean > 0), ihme_loc_id]),
				locs.list)

	##### Copy over reference hazard and drivers
	dir.create("FILEPATH/", c.fbd_version)
	
	##### Copy over treatment
	dir.create("FILEPATH/", c.fbd_version, "_reference")
	system(paste0("cp ", "FILEPATH/", c.ref, "_reference/*csv ", jpath, 
		"FILEPATH/", c.fbd_version, "_reference"))

	dir.create(paste0(jpath, "FILEPATH/", c.fbd_version, "_reference"))
	system(paste0("cp ", jpath, "FILEPATH/", c.ref, "_reference/*csv ", jpath, 
		"FILEPATH/", c.fbd_version, "_reference"))

	#### Scale DAH  in the future
	system(paste0("cp ", jpath, "FILEPATH/", c.ref, "/*csv ", jpath, "FILEPATH/", c.fbd_version))
	dt <- fread(paste0(jpath, "FILEPATH/", c.fbd_version, "/forecasted_inputs.csv"))[scenario == "reference"]
	dt[year_id > extension.year & variable == "HIV DAH", new_dah := (1-dah.scalar) ^ (year_id - extension.year) * pred_mean]
	dt[year_id > extension.year & variable == "HIV DAH", pred_mean := new_dah]
	dt[,new_dah := NULL]
	write.csv(dt, paste0(jpath, "FILEPATH/", c.fbd_version, "/forecasted_inputs.csv"), row.names = F)
}

##################################################
### Forecast Counterfactual Hazard Rate of Change
##################################################
if (r.haz_roc==1) { system(paste0("qsub -N hivf_01_hazard ",c.proj," -pe multi_slot 20 ",
code.root,"r_shell.sh ", code.root,"01_counterfactual_hazard_roc.R ",c.fbd_version))}

################################################
### Forecast Drivers
################################################
if (r.drivers==1) { system(paste0("qsub -N hivf_01_drivers ",c.proj," -pe multi_slot 20 ",
code.root,"r_shell.sh ", code.root,"01_roc_forecast_drivers.R ",c.fbd_version))}

################################################
### Fit ART model
################################################
if(r.art_fit) {
	system(paste0("qsub -hold_jid hivf_01_drivers -N hivf_02a ",c.proj," -pe multi_slot 20 ",
	code.root,"r_shell.sh ", code.root,"02a_ART_granular_model.R ",c.fbd_version))	
}

################################################
### Forecast ART
################################################
if (r.art==1) { 
for(c.loc in locs.list) {
for(c.scenario in scenarios) {
system(paste0("qsub -hold_jid hivf_02a -N hivf_02_",c.loc,"_",c.scenario," ",c.proj," -pe multi_slot 5 ",
code.root,"r_shell.sh ", code.root,"02_ART_granular_forecast.R ",c.fbd_version," ",c.loc," ",c.scenario))
}}}

################################################
### Forecast Incidence Hazard
################################################
if (r.inc==1) { 
for(c.loc in locs.list) {
for(c.scenario in scenarios) {
system(paste0("qsub -N hivf_03_",c.loc,"_",c.scenario," -hold_jid hivf_02_",c.loc,"_",c.scenario," ",c.proj," -pe multi_slot 5 ",
code.root,"r_shell.sh ", code.root,"03_forecast_incidence.R ",c.fbd_version," ",c.loc," ",c.scenario))
}}}

# Spectrum
cluster.project <- "proj_hiv"
n.jobs <- c.draws / 4
n.runs <- 4
stage.1.inc.adj <- 1


for(c.loc in locs.list) {
	for(c.scenario in scenarios) {
		config.dir <- paste0(jpath, "FILEPATH/", c.fbd_version,"_", c.scenario)
		dir.create(config.dir, showWarnings = F)
		input.folders.path <- paste0(config.dir, "/input_folders.csv")
		if(!file.exists(input.folders.path)) {
			dt <- fread(paste0(jpath, "FILEAPTH/forecast_input_folders.csv"))
			write.csv(dt, input.folders.path, row.names = F)
		}

		# Launch first stage Spectrum
		for(i in 1:n.jobs) {
			first.spectrum <- paste0("qsub -pe multi_slot 2 -P ", cluster.project, " -l mem_free=2G ", 
															 "-e FILEPATH ",
															 "-o FILEPATH ",
															 "-hold_jid FILEPATH ",
															 "-N FILEPATH")
			print(first.spectrum)
			system(first.spectrum)
		}

		# Compile results
		compile <- paste0("qsub -pe multi_slot 1 -P ", cluster.project, " -l mem_free=2G ",
											"-hold_jid FILEPATH ", 
											"-N FILEPATH ")
		print(compile)
		system(compile)
	}
}

if(dah_scenario) {
	cp.locs <- setdiff(spec.locs, locs.list)
	for(loc in cp.locs) {
		system("cp FILEPATH")
	}
}

### Aggregate Spectrum subnationals
for(parent in agg.locs) {
	for(c.scenario in scenarios) {	
		aggregate <- paste0("qsub -P proj_hiv -pe multi_slot 10 ",
		                   "-e FILEPATH ",
		                   "-o FILEPATH ",
		                   "-N FILEAPTH ",
		                   "-hold_jid FILEPATH/r_shell.sh ",
		                   code.root,"/aggregate.R ",
		                   "FILEPATH")
		print(aggregate)
		system(aggregate)
	}
}

## Split into GBD subnationals
for(parent in parent.locs) {
	for(c.scenario in scenarios) {	
		split <- paste0("qsub -P proj_hiv -pe multi_slot 2 ",
		                   "-e FILEPATH ",
		                   "-o FILEPATH ",
		                   "-N FILEPATH ",
		                   "-hold_jid FILEPATH/r_shell.sh ",
		                   code.root,"/apply_location_splits.R ",
		                   "FILEPATH")
		print(split)
		system(split)
	}
}


################################################
### Scale (Intercept Shift) Spectrum Output
################################################
jobs.list <- c("junk")
if (r.scale==1) { 
for(c.loc in nat.locs) {
for(c.scenario in scenarios) {
system(paste0("qsub -N hivf_04_",c.loc,"_",c.scenario," ",c.proj," -pe multi_slot 5 ",
code.root,"r_shell.sh ", code.root,"04_scale_spectrum_output_all_age.R ",c.fbd_version," ",c.loc," ",c.scenario))
jobs.list <- c(jobs.list, paste0("hivf_04_",c.loc,"_",c.scenario))
}}}

################################################
### Make Aggregations
################################################
if (r.agg==1) { system(paste0("qsub -N hivf_05 ",c.proj," -pe multi_slot 20 ",
code.root,"r_shell.sh ", code.root,"05_aggregate_spectrum.R ",c.fbd_version))}

################################################
### Compare Scenarios, Plots
################################################
if (r.compare==1) { system(paste0("qsub -hold_jid hivf_05 -N hivf_06 ",c.proj," -pe multi_slot 5 ",
code.root,"r_shell.sh ", code.root,"06_compare_model_runs.R ",c.fbd_version))}