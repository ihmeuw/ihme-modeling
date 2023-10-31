library(data.table)

settings <- read.csv('FILEPATH/bundle_versions.csv')
settings <- settings[!is.na(settings$save),]

bundles <- settings$bundle_id
versions <- settings$bundle_version
names <- settings$acause_name

username <- Sys.getenv('USER')
m_mem_free <- '-l m_mem_free=5G'
fthread <- '-l fthread=4'
runtime_flag <- '-l h_rt=02:00:00'
queue_flag <- '-q long.q'
jdrive_flag <- '-l archive'
shell_script <- '-cwd FILEPATH'

script <- paste0('FILEPATH/adjust_data.R')
errors_flag <- paste0('-e FILEPATH/', username, '/errors')
outputs_flag <- paste0('-o FILEPATH/', username, '/output')

for (i in 1:length(bundles)) {
  bundle <- bundles[i]
  name <- names[i]
  version <- versions[i]
  job_name <- paste0('-N', ' job_', bundle)
  job <- paste(
    'qsub',
    m_mem_free,
    fthread,
    runtime_flag,
    jdrive_flag,
    queue_flag,
    job_name,
    '-P proj_injuries',
    outputs_flag,
    errors_flag,
    shell_script,
    script,
    bundle,
    name,
    version
  )
  system(job)
}
