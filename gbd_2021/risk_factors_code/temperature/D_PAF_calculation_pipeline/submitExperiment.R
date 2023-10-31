library("data.table")

launch.jobs <- function(arg.list, name.stub, name.args, max.attempts = 1, pause = 1, return = F, outfile.dir = NULL, outfile.suffix = NULL, relaunch = T, cluster = "NAME") {

  # Convert the list of arguments into a data table with all combinations of all arguments
  arg.table <- data.table(merge = 1, missing = 1, running = 0, failed = 0, attempts = 0, job.name = "")
  for (arg in names(arg.list)) {
    temp <- data.table(unlist(arg.list[arg]), 1)
    names(temp) <- c(paste0("arg.", arg), "merge")
    
    arg.table <- merge(arg.table, temp, by = "merge", allow.cartesian = T, all = T)
  }
  
  arg.vars <- grep("^arg.", names(arg.table), value = T)
  arg.table[, job.name := do.call(paste, c(name.stub, .SD, sep = "_")), .SDcols = paste0("arg.", name.args)]
  arg.table[, args := do.call(paste, .SD), .SDcols = c(arg.vars, "job.name")]
  
  arg.table[, merge := NULL]  
  
  if (is.null(outfile.dir)==T) {
    outfile.dir <- outdir
  }
  
  stop  <- 0 # DO NOT CHANGE
  if (relaunch==T) {
    count <- 2
  } else {
    count <- 1
  }
  
  
  while (stop==0) {
    

    # Determine which jobs need to be run
    arg.table[, to.run := ifelse(missing==1 & running==0 & attempts<max.attempts, 1, 0)]

    
    # The next code block submits the jobs.  
    for (i in which(arg.table$to.run==1)) {
        
      jname <- arg.table[i, job.name]
      print(paste(jname))
      
      args  <- arg.table[i, args]
      
      # only submit the job if the final output for that location has not already been created (i.e. for jobs that have not successfully completed)
      if (count==1) {
        skip <- F
      } else {
        skip <- file.exists(paste0(outfile.dir, "/", jname, outfile.suffix))
      }
        
      if (skip==F) {
        # Create submission call
        if (cluster=="NAME") {
          sys.sub <- paste0("qsub -l archive -l m_mem_free=", mem, " -l fthread=", slots, " -P ", project, " -q all.q", sge.output.dir, " -N ", jname)
          system(paste(sys.sub, r.shell, save.script, args))
          
        } else if (cluster=="NAME") {
          sys.sub <- paste0("sbatch -C archive --mem=", mem, " -c ", slots, " -t ", time, " -A ", project, " -p all.q", sge.output.dir, " -J ", jname)
          system(paste(sys.sub, r.shell, "-s", save.script, args))
        } else {
          warning("Not a supported option for cluster argument. Must be either NAME or NAME.")
        }
        
        
       # qsub -N job_name -l fmem=128M,fthread=1,h_rt=00:00:20 -P ihme_general -q all.q \~/my_program.sh
       # sbatch -J job_name --mem=128M -c 1 -t 00:00:20 -A ihme_general -p all.q \~/my_program.sh
        # Run
          
        
        # Increment the number of attempts for this job
        arg.table[i, attempts := attempts + 1]
      }
    }
 
    count <- count + 1
    
    # Pull a list of running jobs, remove those from the list of incomplete jobs to create list of jobs to run
    # (these will be jobs that have either not been submitted or have failed)

    if (cluster=="NAME") {
      q <- system(paste0("qstat -xml | tr '\\n' ' ' | sed 's#<job_list[^>]*>#\\n#g' | sed 's#<[^>]*>##g' | grep \"", name.stub, "\" | column -t"), intern = T)
      is.running <- grep(paste0(name.stub, "_"), sapply(strsplit(q, split = " "), function(x) {x[5]}), value = T)
    } else if (cluster=="NAME") {
      is.running <- grep(name.stub, system(paste0("squeue --me -o '%j'"), intern = T), value = T)
    }
    
    arg.table[, running := ifelse(job.name %in% is.running, 1, 0)]   
    
    # Determine if the final output for each location has been created to determine which jobs have finished; remove completed jobs from the list of results that are missing
    #complete <- grep(paste0("^", name.stub), list.files(path = outdir, pattern = "complete.txt$"), value = T)
    #complete <- gsub("/", "", gsub(outdir, "", system(paste0("ls -d ", outdir, "/*"), intern = T)))
    #complete <grep(paste0("^", name.stub), 
    # if (length(complete)>0) {
    #   complete <- data.table(job.name = gsub("_complete.txt$", "", complete), complete = 1)
    #   arg.table <- merge(arg.table, complete, by = "job.name", all.x = T)
    #   arg.table[complete==1, missing := 0][, complete := NULL]
    # }
    
    for (i in which(arg.table$missing==1)) {
      jname <- arg.table[i, job.name]
      if (file.exists(paste0(outfile.dir, "/", jname, outfile.suffix))==T) {
        arg.table[i, missing := 0]
      }
    }
    

    # Determine which (if any) jobs have failed
    arg.table[, failed := ifelse(missing==1 & running==0, 1, 0)]
    
    # Determine jobs that we are done submitting (those that have either completed or been attempted the max number of times)
    arg.table[, relaunch := ifelse(attempts==max.attempts | missing==0, 0, 1)]
    
    
    # If we found missing jobs, then pause before the next iteration of the while loop
    n.missing  <- sum(arg.table$missing)
    n.failed   <- sum(arg.table$failed)
    n.running  <- sum(arg.table$running)
    n.relaunch <- sum(arg.table$relaunch)
    
    
    
    if (nrow(arg.table[relaunch==1 | running==1,])==0) {
      print(paste0("Job submission complete.  ", n.failed, " jobs have failed."))
      stop <- 1
    } else {
      print(paste0(n.missing, " jobs are incomplete; ", n.failed, " jobs have failed."))
      for (t in 1:pause) {
        cat(".")
        Sys.sleep(60)
      }
    }
  }
  
  if (return==T) return(arg.table)  
  cat("All locations complete.")
}
  
  