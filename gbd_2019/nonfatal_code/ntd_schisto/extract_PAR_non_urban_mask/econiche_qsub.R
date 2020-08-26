############
## Define qsub function
############

## jobname: Name for job, must start with a letter
## code:    Filepath to code file
## hold:    Comma-separated list of jobnames to hold the job on
## pass:    List of arguments to pass on to receiving script
## slots:   Number of slots to use in job
## submit:  Should we actually submit this job?
## log:     Should this job create a log for output and errors? 
##              Don't log if there will be many log files
## proj:    What is the project flag to be used?


qsub <- function(jobname, code, hold=NULL, pass=NULL, submit=F, log=T, intel=F, proj = "ADDRESS") {
  user <- Sys.getenv("USERNAME") 
  # choose appropriate shell script 
  if(grepl(".r", code, fixed=T) | grepl(".R", code, fixed=T)) shell <- "FILEPATH" else if(grepl(".py", code, fixed=T)) shell <- "FILEPATH" else shell <- "FILEPATH" 
  # set up jobs to hold for 
  if (!is.null(hold)) { 
      hold.string <- paste(" -hold_jid \"", hold, "\"", sep="")
      }
  # set up arguments to pass in 
  if (!is.null(pass)) { 
      pass.string <- ""
      for (ii in pass) pass.string <- paste(pass.string, " \"", ii, "\"", sep="")
      }

  # construct the command 
  sub_fair <- paste("qsub -l m_mem_free=7G -l fthread=1 -l archive=True -l h_rt=0:15:00 -q all.q",
                if(log==F) " -e FILEPATH/null -o FILEPATH/null ",   # 
                if(log==T) paste0(" -e FILEPATH/errors -o FILEPATH/output "),
                if(proj != "") paste0(" -P ",proj," "),
                if (!is.null(hold)) hold.string, 
                " -N ", jobname, " ",
                shell, " ",
                "-m 1 -o 1 -e r ",
                code, " ",
                if (!is.null(pass)) pass.string, 
                sep="")

  # submit the command to the system
  if (submit) {
      system(sub_fair) 
  } else {
      cat(paste("\n", sub_fair, "\n\n "))
      flush.console()
      }
  }
