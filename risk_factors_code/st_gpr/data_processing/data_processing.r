####################################################################################################################################################
# 															   Table of Contents
####################################################################################################################################################


## LAUNCH SCRIPTS
	## register.config
	## submit.master
	## submit.kos

## DATA PREP UTILITY
	## outlier
	## offset.data
	## wilson_interval_method
	## impute.variance

## MODELING PREP
	## init.param
	## init.settings
	## prep.files
	## prep.square
	## prep.parameters
	## prep.data

## MODEL PREP CASCADE
	## prep.model
		## 1.) prep.folders
		## 2.) prep.parameters
		## 3.) get.kos
		## 4.) prep.data

####################################################################################################################################################

#--LAUNCH SCRIPTS-------------------------------------------------------------------------------


register.config <- function(path, my.model.id, data.id) {
	#--SETUP-------------------------------------------
	## Load personal config
	df <- fread(path)[my_model_id==my.model.id]
	if (nrow(df)>1) stop("Multiple rows assigned to the inputted model_id")
	if (nrow(df)==0) stop("No rows assigned to the inputted model_id")
	## Load config temp and enforce structure
		template <- ubcov_path("model_template") %>% fread
		## Check that all columns exist
		missing.cols <- setdiff(setdiff(names(template), names(df)), "model_id")
		if (length(missing.cols)>0) stop(paste0("Missing entries in config template: ", toString(missing.cols)))
		## Drop extraneous columns
		extra.cols <- setdiff(names(df), names(template))
		if (length(extra.cols)>0) {
			print(paste0("Dropping extra columns from config: ", toString(extra.cols)))
			df <- df[, (extra.cols) := NULL]
		}
	## Set date
	date <- format(Sys.time(), "%Y%m%d_%H:%M:%S")
	df$date_added <- date
	#--CHECKS---------------------------------------------
	## Check that me_name in model matches that of data_id
	me_name <- df$me_name
	data.me_name <- fread(ubcov_path("data_db"))[data_id==data.id]$me_name
	if (me_name != data.me_name) stop(paste0("ME Name conflict between data (", data.me_name, ") and model (", me_name, ")"))
	#--MODEL DB-------------------------------------------
	## Add model to model_db
	model_db <- ubcov_path("model_db") %>% fread
		model_id <- max(model_db$model_id) + 1
		df$model_id <- model_id
	model_db <- rbind(model_db, df, use.names=T, fill=TRUE)
	write.csv(model_db, ubcov_path("model_db"), na="", row.names=F)
	#--RUN DB-------------------------------------------
	## Add run_id to run_db
	run_db <- ubcov_path("run_db") %>% fread
		run_id <- max(run_db$run_id) + 1
		hash <- system("git rev-parse --short HEAD", intern = TRUE)
		new.row <- data.table(run_id=run_id, me_name=me_name, data_id=data.id, model_id=model_id, central_commit=hash, date_added=date)
	run_db <- rbind(run_db, new.row, use.names=T, fill=TRUE)
	write.csv(run_db, ubcov_path("run_db"), na="", row.names=F)
	#--FIN----------------------------------------------
	print(paste0("Registered new run_id (", run_id, ") for me_name (", me_name, ") under model_id (", model_id, ") and data_id (", data.id, ")"))
	return(list(model_id=model_id, run_id=run_id))
}

#---------------------------------------------------------------------------------------------

submit.master <- function(run_id, holdouts, draws, cluster_project, nparallel, slots, model_root, logs=NULL, master_slots=40, ko_pattern="source") {

	## Check Parameters
		## Draws must be 0 if kos > 0
		if (draws > 0 & holdouts > 0) stop("Must run with 0 draws if performing knockouts")

	## Launch
		script <- paste0(model_root, "/launch.r")
		args <- paste("master_pipeline", run_id, holdouts, draws, cluster_project, nparallel, slots, model_root, logs, ko_pattern, sep=" ")
		message(paste0('Saving logs for parent and all child st/gpr jobs in ', logs,'. This will be ', ((nparallel*2)*(holdouts+1)), ' log files, please delete later.')) 
		qsub(job_name=paste("MASTER", run_id, sep="_"), script=script, slots=master_slots, memory=master_slots*2, arguments=args, cluster_project=cluster_project, logs=logs)
		message(paste0('MASTER st-GPR pipeline job submitted for run_id = ', run_id))
}

#------------------------------------------------------------------------------------------

submit.kos <- function(ko, logs=NULL, kos, ko_pattern="source") {
  script <- paste0(model_root, "/launch.r")
  args <- paste("ko_pipeline", ko, run_root, kos, logs, ko_pattern, sep=" ")
  qsub(job_name=paste("cv", run_id, "ko", ko, sep="_"), script=script, slots=16, memory=32, arguments=args, cluster_project=cluster_project, logs=logs)
  message(paste0('st-GPR pipeline job submitted for run_id = ', run_id, ' ko = ', ko))
}



##--DATA PREP UTILITY---------------------------------------------------------------

outlier <- function(df, me_name, merge_vars) {
##	Given a dataframe, a file that contains the outliers,
##	a list of variables to merge on creates a column 
##	called outliers that contains the value outliered
	file <- list.files(ubcov_path("outlier_root"), me_name, full.names=T)
	if (length(file) == 1) {
		outliers <- fread(paste0(file, "/outlier_db.csv"))
		outliers[, outlier_flag := 1]
		## Batch outlier by NID
		batch_outliers <- outliers[batch_outlier==1, .(nid, outlier_flag)]
		setnames(batch_outliers, "outlier_flag", "batch_flag")
		if (nrow(batch_outliers) > 0) {
			df <- merge(df, batch_outliers, by='nid', all.x=T, allow.cartesian=TRUE)
			## Set outliers
			df <- df[batch_flag == 1, outlier := data]
			df <- df[batch_flag == 1, data := NA]
			df[, batch_flag := NULL]
		}
		## Specific merges
		specific_outliers <- outliers[batch_outlier==0, c(merge_vars, "outlier_flag"), with=F]
		setnames(specific_outliers, "outlier_flag", "specific_flag")
		if (nrow(specific_outliers) > 0) {
			df <- merge(df, specific_outliers, by=merge_vars, all.x=T)
			## Set outliers
			df <- df[specific_flag==1, outlier := data]
			df <- df[specific_flag==1, data := NA]
			df[, specific_flag:= NULL]
		}
		print("Outliers outliered")
	} else {
		print(paste0("No outliers for ", me_name))
	}
	return(df)
}

offset.data <- function(df, data_transform, offset) {
	## Offset 0's if logit or log
	if (length(df[data == 0 | data == 1, data]) > 0) {
		if (data_transform %in% c("logit", "log")) {
			df[data == 0, data := data + offset]
			## Offset 1's if logit
			if (data_transform == "logit") {
				df[data == 1, data := data - offset]
			}
		} 
	} else {
			df[, offset_data := 0]
	}
	return(df)
}


wilson_interval_method <- function(data, sample_size, variance) {
	df <- data.table(cbind(data=data, sample_size=sample_size, variance=variance))
	## Impute sample size if only some cases are missing
	imputed_sample_size <-  quantile(df$sample_size, 0.05, na.rm=T)
    df <- df[!is.na(data) & is.na(sample_size), sample_size := imputed_sample_size]
    ## Fill in variance using p*(1-p)/n if variance is missing
    df <- df[!is.na(data) & is.na(variance), variance := (data*(1-data))/sample_size]
    ## Replace variance using Wilson Interval Score Method: p*(1-p)/n + 1.96^2/(4*(n^2)) if p*n or (1-p)*n is < 20
    df <- df[, cases_top := (1-data)*sample_size]
    df <- df[, cases_bottom := data*sample_size]
    df <- df[!is.na(data) & (cases_top<20 | cases_bottom<20), variance := ((data*(1-data))/sample_size) + ((1.96^2)/(4*(sample_size^2)))]
    return(df$variance)
}



impute.variance <- function(df, measure_type) {

	## Continuous
	if (measure_type=="continuous") if (nrow(df[!is.na(data) & is.na(variance)])) stop("Continuous data must have asssocaited variance estimates")
	## Proportion 
	if (measure_type=="proportion") {
		## Must have sample size
		if (nrow(df[!is.na(sample_size)])>0) {
			df[!is.na(data) & is.na(variance), imputed_variance := 1]
			df[, variance := wilson_interval_method(data, sample_size, variance)]
			df[, imputed_variance := ifelse(imputed_variance==1, 1, 0)]
		}
	}

	return(df)

}


##--PREP MODELING SETUP----------------------------------------------------------------

init.param <- function(run_id, get.param=FALSE) {

	## Set parameters into global environment
	param <- get_parameters(run_id=run_id) %>% data.frame
	for(p in names(param)) assign(p, param[, p], envir=globalenv())

	## Split up covariates, custom age, custom_sex
	for (var in c("covariates", "custom_age_group_id", "custom_sex_id", "aggregate_ids")) {
		if (var %in% c("covariates")) {
			assign(paste0(var), unlist(strsplit(gsub(" ", "", get(var)), split=",")), envir=globalenv())
		}
		else {
			assign(paste0(var), as.numeric(unlist(strsplit(gsub(" ", "", get(var)), split=","))), envir=globalenv())
		}
	}

	## Set paths into global environment
	assign("run_root", paste0(ubcov_path("cluster_model_output"), "/", run_id), envir=globalenv())
	assign("model_root", ubcov_path("model_root"), envir=globalenv())

	if (get.param) return(param)
}

init.settings <- function(run_id, kos, draws, cluster_project, parallel, nparallel, slots, logs) {
	## Set run settings (match.call wasn't working)
	args <- list(run_id=run_id, kos=kos, draws=draws, cluster_project=cluster_project, parallel=parallel, nparallel=nparallel, slots=slots, logs=logs)
	args.str <- c("run_id", "kos", "draws", "cluster_project", "parallel", "nparallel", "slots", "logs")
	names(args) <- args.str
    lapply(args.str, function(x) assign(x, args[[x]], envir=globalenv()))

    ## Set model parameters
    init.param(run_id)

    ## Set start time
    assign("model.start", proc.time(), envir=globalenv())

    ## Set cores
    assign("cores", 10, envir=globalenv())

    print("Settings loaded")   
}

prep.square <- function(run_id) {

	## Make square and grab covariates
	square <- make_square(location_set_version_id,
						 year_start, year_end,
						 by_sex, by_age,
						 custom_sex_id, custom_age_group_id,
						 covariates=covariates)
	## Get levels
	locs <- get_location_hierarchy(location_set_version_id)
	levels <- locs[,grep("level|location_id", names(locs)), with=F]
	square <- merge(square, levels, by="location_id")
	## PYL : Quick fix, issue with forecasting hierarchy
	square <- square[location_id == 39, level_3 := 39]
	## MR : Quick fix for holdouts running with custom covariates
	cv_df <- model_load(run_id, "data")
	cv_cols<-colnames(cv_df)
	cv_cols<-cv_cols[cv_cols%like%"cv_"]
	cv_cols<-cv_cols[!(cv_cols%in%c("cv_smaller_site_unit", "cv_subgeo"))]
	cv_df<-cv_df[,c(cv_cols, "location_id", "year_id", "age_group_id", "sex_id"), with=F]
	square<-merge(square, cv_df, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE)

	return(square)
}

prep.parameters <- function(run_id, holdout=1) {

	## Save parameters into output.h5
	param <- get_parameters(run_id=run_id, cv=1)
	model_save(param, run_id, "parameters", holdout)

	## Save covariate versions into output.h5
	cov_version <- get_covariate_version(covariates)
	model_save(cov_version, run_id, "covariate_version", holdout)

	## Save location hierarchy temp.h5
	locs <- get_location_hierarchy(location_set_version_id)
	locs <- locs[, c('location_id', 'parent_id', 'region_id', 'super_region_id', 'level', grep("level_", names(locs), value=T)), with=F]
	model_save(locs, run_id, "location_hierarchy", holdout)

	## Save square file into temp.h5
	square <- prep.square(run_id)
	model_save(square, run_id, "square", holdout)

}

get.kos <- function(run_id, model_root, run_root, kos, holdout_num=1) {

	## Run script to get a frame of knockouts with
	## codem_ko, then return that for use
	
	## Run script to get knockouts
	script <- paste0(model_root, "/ko.py")
	args <- paste(run_root, model_root, kos, holdout_num, sep=" ")
	system(paste("FILEPATH", script, args, sep=" "))

	print(paste0("Saved kos to ", model_path(run_id, 'kos', holdout=holdout_num)))

}

##--PREP DATASET ---------------------------------------------------------------


prep.data <- function(run_id, me_name, data_transform, data_offset, measure_type, output=FALSE, holdout=1, with_kos=0, holdout_type="source") {

	## Load data
	df <- model_load(run_id, "data")
	
	## If only one sex data, copy as 0s to other sex so that linear model (set up to be run independently by sex) won't break.
	if (length(unique(df$sex_id)) == 1 & unique(df$sex_id) != 3) {
		df.dupe <- df %>% copy
		df.dupe <- df.dupe[, sex_id := ifelse(unique(df$sex_id)==1, 2, 1)]
		df.dupe <- df.dupe[, data := 0]
		df <- rbind(df, df.dupe)
	}

	## Subset based on specificed age, sex, year
	df <- df[year_id >= year_start & year_id <= year_end]

	## Check if sample_size is missing, if so add
	if (!("sample_size" %in% names(df))) df <- df[, sample_size := NA]
	if (!("variance" %in% names(df))) df <- df[, variance := NA]

	## Outlier
	df <- outlier(df, me_name, c("me_name", "nid", "location_id", "year_id", "age_group_id", "sex_id"))

	## Save originals
	df <- df[, `:=` (original_data = data, original_variance = variance)]

	## Offset
	df <- offset.data(df, data_transform, data_offset)

	## Impute variance
	df <-  impute.variance(df, measure_type)

	## Increase variance if cv_subgeo == 1
	if (!("cv_subgeo" %in% names(df))) {
		if ("cv_subnat" %in% names(df)) { 
			setnames(df, "cv_subnat", "cv_subgeo") 
		} else if ("smaller_site_unit" %in% names(df)) {
			setnames(df, "smaller_site_unit", "cv_subgeo")
		} else {
			df <- df[, cv_subgeo :=0]
		}
	}
	df <- df[is.na(cv_subgeo), cv_subgeo := 0]
	df <- df[cv_subgeo == 1, variance := variance * 10]

	## Data, Variance transformations
	df <- df[, variance := delta_transform(data, variance, data_transform)]
	df <- df[, data := transform_data(data, data_transform)]
	
	## Sort by location year age sex
	df <- df[order(location_id, year_id, age_group_id, sex_id)]
	
	## Create a unique id for datapoints
	df <- df[, data_id := 1:nrow(df)]

	# Save
  	model_save(df, run_id, "preko", holdout)

  	## Knockout
	get.kos(run_id, model_root, run_root, kos, holdout_num=holdout)
	## Knockout
	ko.df <- model_load(run_id, "kos", holdout=holdout)
	df <- merge(df, ko.df, by=c("data_id"))	

	model_save(df, run_id, "prepped", holdout)
	
	if(output) return(df)

}

###########################################################
## Full setup
###########################################################


prep.model <- function(holdout_num=1, holdouts=0, ko_type="source") {
  
  	## Prep files
  	H5close()
	for (db in c("output", "param", "temp")) {
		path <- paste0(run_root, "/", db, "_", holdout_num, ".h5")
		h5createFile(file=path)
		system(paste("chmod 777", path))
	}

	## Parameter prep
	prep.parameters(run_id, holdout=holdout_num)

	## Data Prep
	prep.data(run_id, me_name, data_transform, data_offset, measure_type, holdout=holdout_num, holdout_type=ko_type)
	
}

