## Database Queries ####
USER = Sys.info()[7]
source(paste0('~/00a_prep_setup.R'))

odbc <- ini::read.ini("FILEPATH")
conn_def <- "DATABASE"
myconn <- RMySQL::dbConnect(RMySQL::MySQL(),
                            host = odbc[[conn_def]]$SERVER,
                            username = odbc[[conn_def]]$USER,
                            password = odbc[[conn_def]]$PASSWORD)

### Pull all active bundles ####
active_bundles = dbGetQuery(myconn, 
                            paste0("QUERY"))

active_bundles = as.data.table(active_bundles)

### Pull injury bundles ####
injury_bundles = dbGetQuery(myconn, 
                            paste0("QUERY"))

# manual add of inj bundles until we have a better soln
injury_bundles_addl = c(762, 763, 764)
injury_bundles_ids = c(injury_bundles$bundle_id, injury_bundles_addl)

### Create CF bundles list #### 
# remove injury bundles from active bundles
all_cf_bundles = active_bundles[ !bundle_id %in% injury_bundles_ids, ]

### Pull bundle names #### 
bun_names = dbGetQuery(myconn, sprintf("QUERY"))

### Pull model vers used by bundle in GBD 2021 ####
cf_bundle_versions = dbGetQuery(myconn,
                                paste0("QUERY"))
cf_bundle_versions = as.data.table(cf_bundle_versions)


dbDisconnect(myconn)
