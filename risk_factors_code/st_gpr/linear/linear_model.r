####################################################################################################################################################
# 															   Table of Contents
####################################################################################################################################################

## PRIOR BLOCKS
	## prep.df
	## fit.lm
	## pred.lm
	## run.lm
	## clean.prior

## PRIOR MAIN
	## run.prior
		## 1.) prep.df
		## 2.) run.lm
		## 3.) clean.prior

## ST BLOCKS
	## calculate.mad
	## nsv
	## calculate.nsv

## ST MAIN
	## run.spacetime

## GPR MAIN
	## run.gpr

####################################################################################################################################################

##--PRIOR BLOCKS------------------------------------------------------------------------------------------------------------------------------------


prep.df <- function(run_id, holdout_num) {
	##--SETUP-----------------------------------------------
	key <- c("location_id", "year_id", "age_group_id", "sex_id")
	## Load objects
	df <- model_load(run_id, "prepped", holdout=holdout_num)
	df <- df[is.na(sex_id), sex_id := 3]
	square <- model_load(run_id, "square", holdout=holdout_num)
	## Merge on square
	df <- merge(square, df, by=key, all.x=TRUE)
	## Study level covariates
	if (!is.blank(study_covs)) {
		## Pull original study level covs from data upload
		study_covs <- str_trim(unlist(strsplit(study_covs, split=",")))
		df.cov <- model_load(run_id, 'data')[, c(key, study_covs), with=FALSE] %>% unique
		df.cov <- df.cov[is.na(sex_id), sex_id := 3]
		## Drop study level covs from current dataset
		df <- df[, -c(study_covs), with=F]
		## Merge the original study level covs
		df <- merge(df, df.cov, by=key, all.x=TRUE)
	}
	return(df)
}

fit.lm <- function(df, model) {
	## Detect function
	func <- ifelse(grepl("[|]", model), "lmer", "lm")
	## Run fit data
    if (func == "lmer") mod <- lmer(as.formula(model), data=df, na.action=na.omit)
    if (func == "lm") mod <- lm(as.formula(model), data=df, na.action=na.omit)
    ## Store summary
	Vcov <- vcov(mod, useScale = FALSE) 
    if (func=="lmer") betas <- fixef(mod) 
    if (func=="lm") betas <- coefficients(mod) 
    se <- sqrt(diag(Vcov))
    zval <- betas / se 
    pval <- 2 * pnorm(abs(zval), lower.tail = FALSE) 
    sum <- cbind(model=model, betas, se, zval, pval)
    return(list(mod=mod, sum=sum))
}

pred.lm <- function(df, model, predict_re=0) {
	## RE form
	if (predict_re==1) re.form=NULL else re.form=NA
	## Predict
	if (class(model) == "lmerMod") {
		prior <- predict(model, newdata=df, allow.new.levels=T, re.form=re.form)
	} else {
		prior <- predict(model, newdata=df)
	}
	return(prior)
}

run.lm <- function(df, prior_model, predict_re, study_covs=NULL, no.subnat=TRUE, holdout_num) {
	## If sex_id in prior model, run by sex. Otherwise, run once
	sex.run <- if (grepl("sex_id", prior_model)) sex.run <- 3 else sex.run <- unique(df$sex_id)
	## Run
	sum.out <- NULL
	for (sex in sex.run) {
		## Subset frame to subnational and sex if necessary
		sub.subnat <- ifelse(no.subnat, paste0("level == 3 | location_id %in% c(4749, 4636, 434, 433)"), "1==1")
		sub.sex <- ifelse(sex %in% c(1,2),  paste0("sex_id==", sex), "1==1")
		## Model
		mod.out <- fit.lm(df[eval(parse(text=sub.subnat)) & eval(parse(text=sub.sex))], prior_model)
		mod <- mod.out$mod
		sum <- mod.out$sum
		names <- rownames(sum)
		sum <- data.table(sum)
		sum <- cbind(covariate=names, sum)
		sum <- sum[, sex_id := sex]
		sum.out <- rbind(sum.out, sum)
		## Predict (subset on sex if necessary)
		df <- df[eval(parse(text=sub.sex)), prior := pred.lm(df[eval(parse(text=sub.sex))], mod, predict_re)]
	}
	## Save model summary
	if (holdout_num == 1) write.csv(sum.out, paste0(run_root, "/prior_summary.csv"), na="", row.names=FALSE)
	return(df)
}


clean.prior <- function(df) {
	cols <- c("location_id", "year_id", "sex_id", "age_group_id", "prior")
	return(df[, cols, with=F] %>% unique)
}


##--PRIOR MAIN------------------------------------------------------------------------------------------------------------------------------------


run.prior <- function(return=FALSE, holdout_num=1) {
	## Prep frame
	df <- prep.df(run_id, holdout_num=holdout_num)
	## Run regression by sex
	df <- run.lm(df, prior_model, predict_re, holdout_num=holdout_num)
	## Clean
	df <- clean.prior(df)
	## Save prior or return to memory
	if (!return) model_save(df, run_id, "prior", holdout_num)
	if (return) return(df)
}

##--ST BLOCKS------------------------------------------------------------------------------------------------------------------------------------


calculate.mad <- function(location_id, prior, prediction, level) {
	## Setup
	resid <- abs(prior - prediction)
	df <- data.table(location_id, resid)
	## Merge location hierarchy
	hierarchy <- get_location_hierarchy(location_set_version_id)
	hierarchy <- hierarchy[, grep("location_id|level", names(hierarchy)), with=F]
	df <- merge(df, hierarchy, by="location_id", all.x=T)
	## Calculate MAD by level specified
	df <- df[, mad := median(abs(resid - median(resid, na.rm=T)), na.rm=T), by=eval(paste0("level_", level))]
	## Return MAD at the specified level
	return(df$mad)
}

nsv <- function(residual, variance) {
	N = length(residual[!is.na(residual)])
	inv_var = 1 / variance
	sum_wi = sum(inv_var, na.rm=T)
	sum_wi_xi = sum( residual * inv_var, na.rm=T) 
	weighted_mean = sum_wi_xi / sum_wi
	norm_weights = N * inv_var / sum_wi
	nsv = 1/(N-1) * sum(norm_weights * (residual - weighted_mean)**2, na.rm=T)
	return(nsv)
}

calculate.nsv <- function(location_id, data, prediction, variance, threshold) {
	## Setup
	resid <- data - prediction
	df <- data.table(location_id, resid, variance)
	## Merge location hierarchy
	hierarchy <- get_location_hierarchy(location_set_version_id)
	hierarchy <- hierarchy[, grep("location_id|level", names(hierarchy)), with=F]
	df <- merge(df, hierarchy, by="location_id", all.x=T)
	## Count the number of data points at each level
	levels <- as.numeric(gsub("level_", "", grep("level_", names(hierarchy), value=T)))
	for (lvl in levels) { 
		df[, paste0("count_", lvl) := sum(!is.na(resid)), by=eval(paste0("level_", lvl))]
		## Its treating NA's as a category make these missing if the location level is < mad level
		df[level < lvl, paste0("count_", lvl) := NA]
	}
	## Calculate nsv at each level
	for (lvl in levels) { 
		df[, paste0("nsv_", lvl) := nsv(resid, variance), by=eval(paste0("level_", lvl))]
		## Its treating NA's as a category make these missing if the location level is < mad level
		df[level < lvl, paste0("nsv_", lvl) := NA]
	}
	## Replace with nsv at lowest level where number of data exceeds the threshold
	for (lvl in levels) {
		df[get(paste0("count_", lvl)) > threshold, nsv := get(paste0("nsv_", lvl))]
	}
	return(df$nsv)
}


##--ST MAIN------------------------------------------------------------------------------------------------------------------------------------


run.spacetime <- function(holdout_num, logs=NULL, slots=10) {

	## Check if prior exists
	if (!('prior' %in% h5ls(paste0(run_root, "/temp_", holdout_num, ".h5"))$name)) stop(paste0("Missing prior in ~/temp_", holdout_num, ".h5"))
  
	## Run spacetime
	script <- paste0(model_root, "/spacetime.py")
	if (parallel) {
		slots <- slots
		memory <- slots*2
		locs <- get_location_hierarchy(location_set_version_id)[level >= 3]$location_id
		loc_ranges <- split_args(locs, nparallel)
		file_list <- NULL
		for (i in 1:nparallel) {
			loc_start <- loc_ranges[i, 1]
			loc_end <- loc_ranges[i, 2]
			file_list <- c(file_list, paste0(run_root, "/st_temp_", holdout_num, "/", loc_start, "_", loc_end, ".csv"))
			args <- paste(run_root, model_root, holdout_num, parallel, loc_start, loc_end, sep=" ")
			qsub(job_name=paste("st", run_id, holdout_num, loc_start, loc_end, sep="_"), script=script, slots=slots, memory=memory, arguments=args, cluster_project=cluster_project, logs=logs)
		}
		resubmit_me<-job_hold(paste("st", run_id, holdout_num, sep="_"), file_list = file_list, resub = 1)
		if (resubmit_me == 1) {
			print("At least one job failed, resubmitting once")
			unlink(paste0(run_root, "/st_temp_", holdout_num, "/"), recursive=T)
			file_list <- NULL
			for (i in 1:nparallel) {
				loc_start <- loc_ranges[i, 1]
				loc_end <- loc_ranges[i, 2]
				file_list <- c(file_list, paste0(run_root, "/st_temp_", holdout_num, "/", loc_start, "_", loc_end, ".csv"))
				args <- paste(run_root, model_root, holdout_num, parallel, loc_start, loc_end, sep=" ")
				qsub(job_name=paste("st", run_id, holdout_num, loc_start, loc_end, sep="_"), script=script, slots=slots, memory=memory, arguments=args, cluster_project=cluster_project, logs=logs)
			}
			job_hold(paste("st", run_id, holdout_num, sep="_"), file_list = file_list, resub = 0)
		}
		## Append output
		st <- append_load(output=paste0(run_root, "/st_full_", holdout_num, ".csv"), files=file_list, delete=TRUE)
		## Clean
		vars <- c("location_id", "year_id", "age_group_id", "sex_id")
		st <- st[order(location_id, year_id, age_group_id, sex_id)]
		st <- st[, (vars) := lapply(.SD, as.numeric), .SDcols=vars]
		## Save
		model_save(st, run_id, 'st', holdout=holdout_num)
		unlink(paste0(run_root, "/st_temp_", holdout_num, "/"), recursive=T)
	} else {
		slots <- 30
		memory <- 60
		args <- paste(run_root, model_root, holdout_num, parallel, sep=" ")
		qsub(job_name=paste0("st_", run_id), script=script, slots=slots, memory=memory, arguments=args, cluster_project=cluster_project, logs=logs)
		job_hold(paste0("st_", run_id), file_list=model_path(run_id, 'st'))
		st <- model_load(run_id, 'st')
	}

	## Merge on prior and data
	prior <- model_load(run_id, 'prior', holdout=holdout_num)
	data <- model_load(run_id, 'prepped', holdout=holdout_num)
	df <- merge(st, prior, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=T)
	df <- merge(df, data, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=T)

	## Calculate MAD (should be calculated in modeling space)
	df <- df[, st_amp := calculate.mad(location_id, prior, st, gpr_amp_unit), by="sex_id"] 

	### Transform into real space
	cols <- c('data', 'st')
	df <- df[, (cols) := lapply(.SD, function(x) transform_data(x, data_transform, reverse=T)), .SDcols=cols]
	df <- df[, variance := delta_transform(data, variance, data_transform, reverse=T)]

	## Calculate NSV (should be calculated in real space)
	df <- df[, nsv := calculate.nsv(location_id, data, st, variance, threshold=5), by="sex_id"]
	if (add_nsv == 1) df <- df[, variance := variance + nsv]

	### Transform back into modeling space
	df <- df[, variance := delta_transform(data, variance, data_transform)]
	cols <- c('data', 'st')
	df <- df[, (cols) := lapply(.SD, function(x) transform_data(x, data_transform)), .SDcols=cols]
	
	## Save data
	data_save <- df[!is.na(data_id), c(names(data), "nsv"), with=F]
	model_save(data_save, run_id, 'adj_data', holdout=holdout_num)
	
	## Save st_amp
	st_amp <- unique(df[, .(location_id, year_id, age_group_id, sex_id, st_amp)])
	model_save(st_amp, run_id, 'st_amp', holdout=holdout_num)

}


###################################################################
# GPR Process
###################################################################

run.gpr <- function(holdout_num, logs=NULL, slots=10) {

	## Check file
	if (!(paste0('st') %in% h5ls(paste0(run_root, "/temp_", holdout_num, ".h5"))$name)) stop(paste0("Missing st in ~/temp_", holdout_num, ".h5"))

	## Run GPR
	script <- paste0(model_root, "/gpr.py")
	if (parallel) {
		slots <- slots
		memory <- slots*2
		locs <- get_location_hierarchy(location_set_version_id)[level >= 3]$location_id
		loc_ranges <- split_args(locs, nparallel)
		file_list <- NULL
		for (i in 1:nparallel) {
			loc_start <- loc_ranges[i, 1]
			loc_end <- loc_ranges[i, 2]
			file_list <- c(file_list, paste0(run_root, "/gpr_temp_", holdout_num, "/", loc_start, "_", loc_end, ".csv"))
			args <- paste(run_root, model_root, holdout_num, draws, parallel, loc_start, loc_end, sep=" ")
			qsub(job_name=paste("gpr", run_id, holdout_num, loc_start, loc_end, sep="_"), script=script, slots=slots, memory=memory, arguments=args, cluster_project=cluster_project, logs=logs)
		}
		resubmit_me<-job_hold(paste("gpr", run_id, holdout_num, sep="_"), file_list = file_list, resub = 1)
		if (resubmit_me == 1) {
			print("At least one job failed, resubmitting once")
			unlink(paste0(run_root, "/gpr_temp_", holdout_num, "/"), recursive=T)
			file_list <- NULL
			for (i in 1:nparallel) {
				loc_start <- loc_ranges[i, 1]
				loc_end <- loc_ranges[i, 2]
				file_list <- c(file_list, paste0(run_root, "/gpr_temp_", holdout_num, "/", loc_start, "_", loc_end, ".csv"))
				args <- paste(run_root, model_root, holdout_num, draws, parallel, loc_start, loc_end, sep=" ")
				qsub(job_name=paste("gpr", run_id, holdout_num, loc_start, loc_end, sep="_"), script=script, slots=slots, memory=memory, arguments=args, cluster_project=cluster_project, logs=logs)
			}
			job_hold(paste("gpr", run_id, holdout_num, sep="_"), file_list = file_list, resub = 0)
		}
		if (draws == 0) {
			delete <- ifelse(holdout_num==1, FALSE, TRUE)
			gpr <- append_load(output=paste0(run_root, "/gpr_full_", holdout_num, ".csv"), files=file_list, delete=delete)
			## Bring in and clean
			vars <- c("location_id", "year_id", "age_group_id", "sex_id")
			gpr <- gpr[order(location_id, year_id, age_group_id, sex_id)]
			gpr <- gpr[, (vars) := lapply(.SD, as.numeric), .SDcols=vars]
			model_save(gpr, run_id, 'gpr', holdout=holdout_num)
			unlink(paste0(run_root, "/gpr_temp_", holdout_num, "/"), recursive=T)
		}
		if (draws != 0) {
		  delete <- ifelse(holdout_num==1, FALSE, TRUE)
		  gpr <- append_load(output=paste0(run_root, "/gpr_full_", holdout_num, ".csv"), files=file_list, delete=delete)
		  ## Bring in and clean
		  vars <- c("location_id", "year_id", "age_group_id", "sex_id")
		  gpr <- gpr[order(location_id, year_id, age_group_id, sex_id)]
		  gpr <- gpr[, (vars) := lapply(.SD, as.numeric), .SDcols=vars]
		  draw_cols <- grep("draw_", names(gpr), value = T)
		  gpr <- gpr[, (draw_cols) := lapply(.SD, function(x) transform_data(x, data_transform, reverse=T)), .SDcols=draw_cols]
		  gpr <- gpr[, gpr_mean := rowMeans(.SD), .SD=draw_cols]
		  gpr <- gpr[, gpr_lower := apply(.SD, 1, quantile, c(.025)), .SDcols=draw_cols]
		  gpr <- gpr[, gpr_upper := apply(.SD, 1, quantile, c(.975)), .SDcols=draw_cols]
		  gpr <- gpr[, c("location_id", "year_id", "age_group_id", "sex_id", "gpr_mean", "gpr_lower", "gpr_upper"), with=FALSE]
		  model_save(gpr, run_id, 'gpr', holdout=holdout_num)
		}
	} else {
		slots <- 30
		memory <- 60
		args <- paste(run_root, model_root, holdout_num, draws, parallel, sep=" ")
		qsub(job_name=paste0("gpr_", run_id), script=script, slots=slots, memory=memory, arguments=args, cluster_project=cluster_project, logs=logs)
		job_hold(paste0("gpr_", run_id), file_list=model_path(run_id, 'gpr'))
	}

}

