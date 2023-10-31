# simple qsub wrapper with option for array jobs
qsub <- function(job_name, script, m_mem_free, fthread=1, queue="all.q",
                 arguments = NULL, array = NULL,
                 cluster_project = NULL, logs = NULL, cwd = TRUE,
                 runtime = "24:00:00") {
    command <- "qsub"
    # Required
    name <- paste0("-N ", job_name)
    shell <- "/FILEPATH/execRscript.sh -s"

    mem_core <- paste0("-l fmem=", m_mem_free,",fthread=", fthread,
                       ",h_rt=", runtime)
    que <- paste0("-q ", queue)
    command <- paste(command, name, mem_core, que, sep = " ")
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
        command <- paste0(command, " -t 1:", array, " -tc 500 ")
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
            logdebug("%d total job(s), %d running, %d queued", nrow(job_stats),
                     nrow(job_stats[state=="r", ]), nrow(job_stats[state=="qw", ]))
            if ("task" %in% names(job_stats)) {
                job_stats[, task := as.numeric(task)]
                logdebug("Tasks running: %s", job_stats[state=="r", ]$task %>% sort %>% paste(., collapse=", "))
                logdebug("Tasks queued: %s", gsub(":1","",unique(job_stats[state=="qw", ]$slots)))
            } else {
                logdebug("Jobs running: %s", job_stats[state=="r", ]$jid %>% sort %>% paste(., collapse=", "))
                logdebug("Jobs queued: %s", job_stats[state=="qw", ]$jid %>% sort %>% paste(., collapse=", "))
            }
            Sys.sleep(60)
        }
    }
    job.runtime <- proc.time() - start.time
    job.runtime <- round(job.runtime[3] / 60, 0)
    Sys.sleep(10)
    loginfo("Job %s has completed (elapsed time: %s minutes).", job_name, job.runtime)
}

# wrapper around qsub and job hold function that will retry array jobs with a
# file existence check. files must currently be save by loc or loc sex. not flexible.
retry_qsub <- function(job_name, script, params, out_dir,
                       m_mem_free, fthread=1, queue="all.q", arguments = NULL,
                       cluster_project = NULL, logs = NULL, cwd = TRUE, tries = 3,
                       loc_only = FALSE, runtime = "24:00:00") {
    write.csv(params, paste0(out_dir, "/params.csv"), row.names = F)
    # submit until all files exist up to ntries
    while (tries != 0 & nrow(params) != 0) {
        qsub(job_name = job_name, script = script,
             m_mem_free = m_mem_free, fthread = fthread, queue = queue,
             logs = logs, array = nrow(params),
             arguments = paste(paste0(out_dir, "/params.csv"), arguments, sep = " "),
             cluster_project = cluster_project, runtime = runtime)
        job_hold(job_name)
        fini <- NULL
        loginfo("Checking to make sure all output files exist...")
        if (loc_only) {
            params[, file := paste0(out_dir, "/", location_id, ".csv")]
        } else {
            params[, file := paste0(out_dir, "/", location_id, "_", sex_id, ".csv")]
        }
        fini <- list.files(out_dir, full.names=T)
        # make sure that not every single job failed
        if (nrow(params) == nrow(params[!file %in% fini, ])) {
            unlink(paste0(out_dir, "/params.csv"))
            err_msg <- "All jobs failed. Check out your logs and submit a help ticket!"
            logerror(err_msg);stop(err_msg)
        }
        tries <- tries - 1
        params <- params[!file %in% fini,]
        params[, file := NULL]
        loginfo("%d files missing, %d retries remaining.", nrow(params), tries)
        write.csv(params, paste0(out_dir, "/params.csv"), row.names = F)
    }
    # fail if still not all jobs done
    if (nrow(params) != 0) {
        err_msg <- paste0("Failing jobs were relaunched and still did not all complete. ",
                          "Check out your logs and try again if you believe it was ",
                          "cluster error, otherwise submit a help desk ticket.")
        logerror(err_msg);stop(err_msg)
    }
    unlink(paste0(out_dir, "/params.csv"))
}
