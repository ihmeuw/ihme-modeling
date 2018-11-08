# simple qsub wrapper with option for array jobs
qsub <- function(job_name, script, slots, arguments = NULL, array = NULL,
                 cluster_project = NULL, logs = NULL, cwd = TRUE) {
    command <- "qsub"
    # Required
    name <- paste0("-N ", job_name)
    shell <- "FILEPATH/r_shell_singularity.sh"
    memory <- paste0("-l mem_free=", slots*2, "g")
    slots <- paste0("-pe multi_slot ", slots)
    command <- paste(command, name, slots, memory, sep = " ")
    # Optional
    if (!is.null(cluster_project)) {
        project <- paste0("-P ", cluster_project)
        command <- paste(command, project, sep = " ")
    }
    if (!is.null(logs)) {
        logs <- paste0("-o ", logs, "/output -e ", logs, "/errors ")
        command <- paste(command, logs, sep = " ")
    }
    if (cwd) {
        command <- paste(command, "-cwd", sep = " ")
    }
    if (!is.null(array)) {
        command <- paste0(command, " -t 1:", array, " ")
    }
    # Script
    command <- paste(command, shell, sep = " ")
    if (!is.null(arguments)) {
        args <- paste(gsub(", ", " ", toString(arguments)))
        command <- paste(command, script, args, sep = " ")
    }
    # Submit Job
    system(command)
}

# not an actual job hold, just a sys sleep until all jobs are done
job_hold <- function(job_name) {
    Sys.sleep(5)
    start.time <- proc.time()
    flag <- 0
    while (flag == 0) {
        stats <- data.table(job = system("qstat -r | grep \"Full jobname\" -B1",intern=T))
        job_stats <- data.table(job_name = stats[job %like% "Full jobname:"],
                                stats = stats[!(job %like% "Full jobname:") & job != "--"])
        suppressWarnings(job_stats[, c("jid", "user", "state", "node", "slots",
                                       "task") := tstrsplit(stats.job, "\\s+")[c(2,5,6,9,10,11)]])
        job_stats[, name := tstrsplit(job_name.job, "\\s+")[4]]
        job_stats <- job_stats[name %like% job_name][ , c("stats.job", "job_name.job") := NULL]
        if (nrow(job_stats) == 0) {
            flag <- 1
        } else {
            message(format(Sys.time(), "%D %H:%M:%S"), " - ", nrow(job_stats), " total job(s), ",
                    nrow(job_stats[state=="r", ]), " running, ",
                    nrow(job_stats[state=="qw", ]), " queued")
            if ("task" %in% names(job_stats)) {
                job_stats[, task := as.numeric(task)]
                message("Tasks running - ", job_stats[state=="r", ]$task %>% sort %>% paste(., collapse=", "))
                message("Tasks queued - ", gsub(":1","",unique(job_stats[state=="qw", ]$slots)))
            } else {
                message("Jobs running - ", job_stats[state=="r", ]$jid %>% sort %>% paste(., collapse=", "))
                message("Jobs queued - ", job_stats[state=="qw", ]$jid %>% sort %>% paste(., collapse=", "))
            }
            Sys.sleep(60)
        }
    }
    job.runtime <- proc.time() - start.time
    job.runtime <- job.runtime[3]
    Sys.sleep(10)
    message("Job ", job_name, " has completed. Time elapsed: ", job.runtime)
}

# wrapper around qsub and job hold function that will retry array jobs with a
# file existence check.
retry_qsub <- function(job_name, script, params, out_dir, slots, arguments = NULL,
                       cluster_project = NULL, logs = NULL, cwd = TRUE, tries = 3) {
    write.csv(params, paste0(out_dir, "/params.csv"), row.names = F)
    # submit until all files exist up to ntries
    while (tries != 0 & nrow(params) != 0) {
        qsub(job_name = job_name, script = script,
             slots = slots, logs = logs, array = nrow(params),
             arguments = paste(paste0(out_dir, "/params.csv"), arguments, sep = " "),
             cluster_project = cluster_project)
        job_hold(job_name)
        fini <- NULL
        message("Checking to make sure all output files exist...")
        for (i in 1:nrow(params)) {
            ## Ensure that all files are there & didn't get killed by oom etc
            if (file.exists(paste0(out_dir, "/", params[i, risk_id], "/", params[i, location_id], ".csv"))) {
                fini <- c(fini, i)
            }
        }
        # make sure that not every single job failed
        if (is.null(fini)) {
            unlink(paste0(out_dir, "/params.csv"))
            stop("All jobs failed. Check out your logs!")
        }
        tries <- tries - 1
        params <- params[-fini, ]
        message(nrow(params), " files missing, ", tries, " retries remaining.")
        write.csv(params, paste0(out_dir, "/params.csv"), row.names = F)
    }
    # fail if still not all jobs done
    if (nrow(params) != 0) {
        stop("Failing jobs were relaunched ", tries-1, "x more and still did not ",
             "all complete. Check out your logs!")
    } else {
        message("Success! All jobs complete.")
    }
    unlink(paste0(out_dir, "/params.csv"))
}
