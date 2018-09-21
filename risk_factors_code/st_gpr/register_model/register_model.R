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
