###################
### Setting up ####
###################

## OS locals
os <- .Platform$OS.type
if (os == "windows") {
  jpath <- "FILEPATH"
} else {
  jpath <- "FILEPATH"
}
package_lib <- paste0(FILEPATH)
library(data.table, lib.loc = package_lib)
library(haven, lib.loc = package_lib)
library(rhdf5, lib.loc = package_lib)
source('utility.r')

###################################################################
# Begin function
####################################################################

register_data <- function(me_name, path, user_id, notes, is_best=0, bypass=FALSE) {

	## Set paths
	path <- gsub("J:/", jpath, path)

	## Import data
	if (grepl(".dta", tolower(path))) {
		df <- data.table(read_dta(path))
	}
	if (grepl(".csv", tolower(path))) {
		df <- fread(path)
	}


	######################
	# Checks
	######################

	## Check that me_name is in me_db
	me_db <- fread(ubcov_path("me_db")) 

		if (!(me_name %in% unique(me_db$me_name))) {
			stop('The me_name provided doesnt match/exist in the me_db, please add if need be.')
		}

	## Check against template
	template <- fread(ubcov_path("data_template"))

		## Route "smaller_site_unit" to "cv_subgeo"
		if ("smaller_site_unit" %in% names(df)) {
			setnames(df, "smaller_site_unit", "cv_subgeo")
		}
		if ("cv_subnat" %in% names(df)) {
			setnames(df, "cv_subnat", "cv_subgeo")
		}

		## Check that all the required variables are there
		missing.cols <- names(template)[!names(template) %in% names(df)]
		if (length(missing.cols) > 0 ) {
			stop(paste0("You are missing the following required columns in your data: ", toString(missing.cols)))
		}

		## Remove any additional columns, throw a warning
		extra.cols <- names(df)[!names(df) %in% c(names(template), grep("cv_", names(df), value=T))]
		## Throw warning
		if (length(extra.cols) > 0 ) {
			df <- df[, paste0(extra.cols) := NULL]
			print(paste0('The following columns have been removed: ', toString(extra.cols)))
		}

	## Data Checks
		## Drop row if data is missing unless have custom covariates
		if (!any(grepl("cv_", names(df)))) df <- df[!is.na(data),]
		## me_name in the dataset is filed and consistent with me_name
		if (length(df[is.na(me_name), me_name]) > 0) {
			stop("me_name must never be missing in the data fame")
		}
		if (unique(df$me_name) != me_name) {
			stop("me_name provided doesn't match that in the data frame")
		}
		## Must have some measure of variance or means to impute variance
		if (length(df[!is.na(variance), variance]) == 0 & length(df[!is.na(sample_size), sample_size]) == 0) {
			stop("Data must have some way to impute variance: variance and sample_size cant be entirely missing")
		}
		## Throw a break if continuous and are missing variance
		me.name <- me_name
		measure <- me_db[me_name==me.name]$measure_type
		if (measure == "continuous" & nrow(df[!is.na(data) & is.na(variance)]) > 0) {
			stop("Continuous measures must have variance in all data rows. Imputation must occur prior to ST-GPR")
		}	

	######################
	# Register
	#####################

		## Load database
		data.db <- fread(ubcov_path("data_db"))

		## Set date
		date <- format(Sys.time(), "%Y%m%d_%H:%M:%S")

		## Data id
		data.id <- nrow(data.db) + 1

		## me_name
		me.name <- me_name

		## Set data_id, me_name, uploader, notes, date_added
		new.data <- data.table(data_id=data.id)
		new.data <- new.data[,me_name := me.name]
		new.data <- new.data[,uploader_id := user_id]
		new.data <- new.data[,notes := notes]
		new.data <- new.data[,date_added := date]

		## Append to data.db
		data.db <- rbind(data.db, new.data, fill=T)
		
		## Mark best if option, or first of me_name
		if (is_best==1 | length(data.db[me_name==me.name, me_name]) == 1) {
			## Ensure data types
			data.db <- data.db[, is_best := as.integer(is_best)]
			data.db <- data.db[, best_start := as.character(best_start)]
			data.db <- data.db[, best_end := as.character(best_end)]
			## Set best_end of previous best
			data.db <- data.db[me_name == me.name & is_best == 1, best_end := date]
			## Remove previous best
			data.db <- data.db[me_name == me.name & (is_best == 1 | is.na(is_best)), is_best := 0]
			## Set as best
			data.db <- data.db[data_id == data.id, is_best := 1]
			data.db <- data.db[data_id == data.id, best_start := date]
			data.db <- data.db[data_id == data.id, best_end := "Current best"]

		}


	######################
	# Upload
	######################

	## Prompt confirmation
	if (!bypass) {
		## Print Frame
		meta <- data.db[data_id == data.id,]
		for (col in names(meta)) {
			print(paste0(col, ": ", meta[,col, with=F]))
		}
		confirm <- NA
		while (!(confirm %in% c("y", "n"))) {
			confirm <- readline(prompt="Confirm Data Upload (y/n): ")
		}
		if (confirm == "n" ) {
			stop("Data upload stopped")
		}
	}

	## Save
	H5close()
	data.folder <- ubcov_path("cluster_model_data")
	if (!file.exists(data.folder)){
		system(paste0("mkdir -m 777 -p", data.folder))
	}
	output.path <- paste0(data.folder, "/", data.id, ".h5" )
	h5createFile(file=output.path)
	h5write(df, file=output.path, 'data', createnewfile=TRUE)	
	## Ensure file has open permissions
	system(paste0("chmod 777 ", output.path)) 


	## Save data_db
	write.csv(data.db, ubcov_path("data_db"), na="", row.names=F)

	print(paste0("Your data for ", me_name, " has successfully uploaded. The data_id is ", data.id))

	return(data.id)

}


