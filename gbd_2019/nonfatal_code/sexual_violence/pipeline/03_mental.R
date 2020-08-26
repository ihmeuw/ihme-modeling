## SETUP -----------------------------------------------------------------

# clear workspace environment

rm(list = ls())

if(Sys.info()[1] == 'Windows') {
  username <- "USERNAME"
  root <- "FILEPATH"
  code_dir <- "FILEPATH"
} else {
  username <- "USERNAME"
  root <- "FILEPATH"
  code_dir <- "FILEPATH"
}

pacman::p_load(magrittr, data.table, readstata13, lubridate, sandwich, car)

# settings for this run
shared_dir <- "FILEPATH"

data_dir <- paste0(shared_dir, "FILEPATH")
files <- list.files(data_dir)

out_dir <- paste0(shared_dir, "FILEPATH")
dir.create(out_dir, recursive = T)

## ------------------ READ IN DATA SOURCES ---------------------------------- ##

# read in the data
system.time(data <- lapply(paste0(data_dir, "/", list.files(data_dir)), fread, verbose = TRUE))

indices <- lapply(data, nrow) %>% unlist
indices <- which(indices != 0)

master <- data[indices]

# define function to convert enrolid to character
switch <- function(data){
  result <- data[, enrolid := as.character(enrolid)]
  result <- result[, V1 := NULL]
  return(result)
}

system.time(master <- lapply(master, switch) %>% rbindlist(use.names = T))

system.time(model <- glm(ACT ~ SV + DEP + ANX + age + sex, data = master, family = poisson))

save(model, file = paste0(out_dir, "FILEPATH"))
load(file = "FILEPATH")

robust <- vcovHC(model, type = "HC0")

ages <- seq(0, 95, by = 5)
sexes <- 0:1

params <- expand.grid("age" = ages, "sex" = sexes) %>% data.table

transforms <- paste0(params[[1]], "*age + ", params[[2]], "*sex2")

# function to transform the coefficient estimates and standard errors
# to represent the risk of ACT due to sexual violence
transform.coefs <- function(transform){
  
  cat("Working on ", transform, "\n")
  
  g <- paste0("exp(SV) ",  "* exp((Intercept) + ", transform, ") - exp((Intercept) + ", transform, ")")
  
  cat("g(.) is ", g, "\n")
  coefs <- deltaMethod(object = model, g = g, vcov = robust)
  
  est <- coefs$Estimate
  se <- coefs$SE
  
  result <- data.table("Estimate" = est, "SE" = se)
  
  return(result)
}

# loop over all parameters
coefs <- lapply(transforms, transform.coefs)

# rbindlist and then add to the parameter grid
transcoef <- rbindlist(coefs, use.names = T)

# for first submission
transcoef <- cbind(Estimate = c(0.0967, 0.0933, 0.0899, 0.0867, 0.0836, 0.0806, 0.0777, 0.0749, 0.0722, 0.0697, 0.0672, 0.0648, 0.0624, 0.0602, 0.0581, 0.0560, 0.0540, 0.0520, 0.0502, 0.0484,
                                0.1205, 0.1162, 0.1120, 0.1080, 0.1042, 0.1004, 0.0968, 0.0934, 0.0900, 0.0868, 0.0837, 0.0807, 0.0778, 0.0750, 0.0723, 0.0697, 0.0672, 0.0648, 0.0625, 0.0603),
                   SE = c(0.0023, 0.0021, 0.0021, 0.0020, 0.0020, 0.0019, 0.0018, 0.0018, 0.0017, 0.0016, 0.0016, 0.0015, 0.0015, 0.0014, 0.0014, 0.0013, 0.0013, 0.0012, 0.0012, 0.0011,
                          0.0028, 0.0026, 0.0026, 0.0025, 0.0024, 0.0024, 0.0023, 0.0022, 0.0021, 0.0020, 0.0020, 0.0019, 0.0018, 0.0018, 0.0017, 0.0016, 0.0016, 0.0015, 0.0015, 0.0014))

coefficients <- cbind(params, transcoef)

drawdata <- copy(coefficients)

draws <- paste0("draw_", 0:999)

drawdata[, id := seq_len(.N)]

drawdata[, (draws) := as.list(rnorm(n = 1000, mean = Estimate, sd = SE)), by = id]

filename <- paste0(out_dir, "FILEPATH")
write.csv(drawdata, filename)
