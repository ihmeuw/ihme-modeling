## Author:NAME 
## Date: 2/22/2016
## Purpose: Compiles the Summary envelope and lifetable files to all-locations

library(data.table)
if (Sys.info()[1] == 'Windows') {
  username <- "USER"
  root <- ""
} else {
  username <- Sys.getenv("USER")
  root <- ""
  version <- commandArgs()[3] 
}

source(paste0(root,"PATH"))
source(paste0(root,"PATH"))

locs <- get_locations(level="all")
locs2 <- get_locations(level="all", gbd_type = "sdi") 
locs <- rbind(locs,locs2[!locs2$location_id %in% unique(locs$location_id),])

loc_id <- locs[, colnames(locs) %in% c("location_id")]

# ## Compiling evelope summary files

missing_files <- c()
compiled_env <- list()
  for (loc in loc_id){
    cat(paste0(loc,";")); flush.console()
    file <- paste0("PATH", loc, ".csv")
    if(file.exists(file)){ 
      compiled_env[[paste0(file)]] <- fread(file)
      compiled_env[[paste0(file)]]$location_id <- loc
    } else {
      missing_files <- c(missing_files, file)
    }
  }

if(length(missing_files)>0) stop(paste("Files are missing.",missing_files))

compiled_env <- as.data.frame(rbindlist(compiled_env))
  
row.names(compiled_env) <- 1:nrow(compiled_env)

write.csv(compiled_env, paste0("PATH" ,version, ".csv"), row.names=F)
write.csv(compiled_env, "PATH", row.names=F)
write.csv(compiled_env, paste0(root, "PATH", version, ".csv"), row.names=F)
write.csv(compiled_env, paste0(root, "PATH"), row.names=F)

# Compiling lifetable summary files

missing_files <- c()
compiled_lt <- list()
for (loc in loc_id){
  cat(paste0(loc,";")); flush.console()
  file <- paste0("PATH", loc, ".csv")
  if(file.exists(file)){ 
    compiled_lt[[paste0(file)]] <- fread(file)
    compiled_lt[[paste0(file)]]$location_id <- loc
  } else {
    missing_files <- c(missing_files, file)
  }
}

if(length(missing_files)>0) stop(paste("Files are missing.",missing_files))

compiled_lt <- as.data.frame(rbindlist(compiled_lt))
row.names(compiled_lt) <- 1:nrow(compiled_lt)

 
write.csv(compiled_lt, paste0("PATH" ,version, ".csv"), row.names=F)
write.csv(compiled_lt, "PATH", row.names=F)
write.csv(compiled_lt, paste0(root, "PATH", version, ".csv"), row.names=F)
write.csv(compiled_lt, paste0(root, "PATH"), row.names=F)


## Compiling 5q0

missing_files <- c()
u5m <- list()
for (loc in loc_id){
  file <- paste0(paste0("PATH",loc,"_summary.csv"))
  if(file.exists(file)){ 
    u5m[[paste0(file)]] <- read.csv(file, stringsAsFactors = F)
    u5m[[paste0(file)]]$location_id <- loc
  } else {
    missing_files <- c(missing_files, file)
  }
}

if(length(missing_files)>0) stop("Files are missing.")

u5m <- do.call("rbind", u5m)
row.names(u5m) <- 1:nrow(u5m)

write.csv(u5m, paste0("PATH"), row.names=F)
write.csv(u5m, paste0("PATH", version, ".csv"), row.names=F)
write.csv(u5m, paste0(root, "PATH"), row.names=F)
write.csv(u5m, paste0(root, "PATH", version, ".csv"), row.names=F)


## Compiling 45q15


missing_files <- c()
adult <- list()
for (loc in loc_id){
  file <- paste0("PATH", loc, ".csv")
  if(file.exists(file)){ 
    adult[[paste0(file)]] <- read.csv(file, stringsAsFactors = F)
    adult[[paste0(file)]]$location_id <- loc
  } else {
    missing_files <- c(missing_files, file)
  }
}

if(length(missing_files)>0) stop("Files are missing.")

adult <- do.call("rbind", adult)
row.names(adult) <- 1:nrow(adult)

write.csv(adult, paste0("PATH"), row.names=F)
write.csv(adult, paste0("PATH", version, ".csv"), row.names=F)
write.csv(adult, paste0(root, "PATH"), row.names=F)
write.csv(adult, paste0(root, "PATH", version, ".csv"), row.names=F)


## Compiling life expectancy
missing_files <- c()
le <- list()
for (loc in loc_id){
  file <- paste0("PATH", loc, ".csv")
  if(file.exists(file)){ 
    le[[paste0(file)]] <- read.csv(file, stringsAsFactors = F)
    le[[paste0(file)]]$location_id <- loc
  } else {
    missing_files <- c(missing_files, file)
  }
}

if(length(missing_files)>0) stop("Files are missing.")

le <- do.call("rbind", le)
row.names(le) <- 1:nrow(le)

write.csv(le, paste0("PATH"), row.names=F)
write.csv(le, paste0("PATH", version, ".csv"), row.names=F)
write.csv(le, paste0(root, "PATH"), row.names=F)
write.csv(le, paste0(root, "PATH", version, ".csv"), row.names=F)




