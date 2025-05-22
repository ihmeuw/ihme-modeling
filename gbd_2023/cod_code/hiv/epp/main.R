

### Setup
rm(list=ls())
windows <- Sys.info()[1][["sysname"]]=="Windows"
root <- ifelse(windows,"J:/","/home/j/")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, "H:", paste0("/ihme/homes/", user)), "/hiv_gbd2019/02_EPP2019/")
## Packages
library(data.table); library(mvtnorm); library(survey); library(dplyr); library(ggplot2)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
print(args)
if(length(args) > 0) {
  run.name <- args[1]
  loc <- args[2]
  proj.end <- as.numeric(args[3])
  i <- as.integer(Sys.getenv("SGE_TASK_ID"))
} else {
  run.name <- "19"
  loc <- "NGA_25329"
  proj.end <- 2019.5
  i <- 1
}


### Arguments
run.name1 <- "190610_piranha"
run.args <- fread(paste0(ifelse(windows, "H:", paste0("/ihme/homes/", user)), "/Decomp 2 Vetting/run_args.csv"))
run.args <- run.args[run.name=="190610_piranha"]

if(!run.name1 %in% run.args$run.name){
  stop(paste0("Add run toggles to spreadsheet ",code.dir,"/gbd/run_args.csv"))
}

run.args <- run.args[run.name==run.name1]
start.year <- 1970
stop.year <- 2019
trans.params <- TRUE
stop.collapse <- FALSE
anc.prior <- TRUE
gbd.pop <- TRUE
no.anc <- FALSE
art.sub <- TRUE
eq.prior <- as.logical(run.args$eq.prior)
anc.backcast <- as.logical(run.args$backcast)
num.knots <- 7
geo_adj <- as.logical(run.args$geo_adj)
uncertainty <- as.logical(run.args$uncertainty)
high_risk <- as.logical(run.args$high_risk)


### Paths
input.dir <- paste0()
out.dir <- paste0('/ihme/hiv/epp_output/gbd19/', run.name, "/", loc)
dir.create(out.dir,showWarnings = FALSE, recursive=TRUE)
out.path <- paste0(out.dir, "/results", i, ".RData")
pdf.path <- paste0(out.dir, "/test_results", i, ".pdf")
offset.out.path <- paste0(out.dir,"/offsets.RData")
anc.out.path <- paste0(out.dir,"/probit_anc.RData")

### Functions
## GBD
source(paste0(code.dir,"gbd/prep_data.R"))
source(paste0(code.dir,"gbd/prep_output.R"))
source(paste0(code.dir,"gbd/data_sub.R"))
source(paste0(code.dir,"gbd/plot_fit.R"))
source(paste0(code.dir,"gbd/ind_data_prep.R"))


## EPP
source(paste0(code.dir,"R/epp.R"))
source(paste0(code.dir,"R/fit-model.R"))
source(paste0(code.dir,"R/generics.R"))
source(paste0(code.dir,"R/IMIS.R"))
source(paste0(code.dir,"R/likelihood.R"))
source(paste0(code.dir,"R/read-epp-files.R"))


## Model in C
if(trans.params) {
  # Load C version
  dyn.load(paste0(code.dir, "src/fnEPP", .Platform$dynlib.ext))  # LoaPd C version with time series for transition parameters
} else {
  dyn.load(paste0(code.dir, "src/epp", .Platform$dynlib.ext))
}


library(mortdb, lib = "/ihme/mortality/shared/r")

### Tables
loc.table <- data.table(get_locations(hiv_metadata = T))

### Code
## Prep data and collapse location subpopulations
debug(sub.anc)
dt <- prep_epp_data(loc, proj.end = 2019.5, stop_collapse = stop.collapse, gbd_pop = gbd.pop, art_sub = art.sub, num_knots = num.knots)


#Substitute IHME data
#Prevalence surveys
if((!stop.collapse & length(dt) == 1) | grepl("IND_", loc)) {
  print("Substituting prevalence surveys")
  dt <- sub.prev(loc, dt)
}

# These locations do not have information from LBD team estimates
no_geo_adj <-  c(loc.table[epp ==1 & grepl("IND",ihme_loc_id),ihme_loc_id],"PNG","HTI","DOM", "CPV","GNQ",loc.table[epp ==1 & grepl("ZAF",ihme_loc_id),ihme_loc_id])

# ANC data
if(geo_adj & !loc %in% no_geo_adj){
  geo_adj <<- TRUE
} else {
  geo_adj <<- FALSE
}


high.risk.list <- loc.table[epp == 1 & collapse_subpop == 0 & !grepl("ZAF", ihme_loc_id) & !grepl("KEN", ihme_loc_id), ihme_loc_id]
ken.anc.path <- paste0(root, "WORK/04_epi/01_database/02_data/hiv/data/prepped/kenya_anc_map.csv")
ken.anc <- fread(ken.anc.path)
no.anc.ken <- setdiff(loc.table[epp == 1 & grepl("KEN", ihme_loc_id), ihme_loc_id], ken.anc$ihme_loc_id)

if(loc %in% c(high.risk.list, "PNG", no.anc.ken,"BEN","COG","TGO")) {
  anc.backcast <- FALSE
}


if(loc %in% loc.table[grepl("ZAF", ihme_loc_id),ihme_loc_id]) {
  anc.backcast <- TRUE
  
}


if(anc.backcast | geo_adj) {
  dt <- sub.anc(loc, dt, uncertainty=uncertainty,i)
}


if(geo_adj){
  offsets <- attr(dt[[1]],"likdat")$anclik.dat$offset
  anc <-  attr(dt[[1]],"likdat")$anclik.dat$W.lst
  save(anc, file = anc.out.path)
  save(offsets, file= offset.out.path)
}

# Transition parameters
if(trans.params) {
  dt <- extend.trans.params(dt, start.year, stop.year)
  dt <- sub.off.art(dt, loc, i)
  dt <- sub.on.art(dt, loc, i)
  dt <- sub.cd4.prog(dt, loc, i)
}


if(!high_risk){
  gen.pop.dict <- c("General Population", "General population", "GP", 
                    "GENERAL POPULATION", "GEN. POPL.", "General population(Low Risk)", 
                    "Remaining Pop", "population feminine restante","Pop féminine restante","Rift Valley", 
                    "Western","Eastern","Central","Coast","Nyanza","Nairobi",
                    "Female remaining pop","Remanente Femenina",loc, "FCT Total")
  
  if(!any(names(dt) %in% gen.pop.dict)){
    stop("subpop not found")
  }
  
 dt <-  dt[names(dt) %in% c(gen.pop.dict)]
  
}

## Fit model
fit <- list()

for(subpop in names(dt)) {
  print(subpop)

  if(all(!attr(dt[[subpop]], "eppd")$anc.used)) {
    no.anc <<- TRUE
  } else {
    no.anc <<- FALSE
  }

  attr(dt[[subpop]], "eppd")$anc.used[1] <- FALSE
  attr(dt[[subpop]], "eppfp")$artelig.idx.ts <- as.integer(attr(dt[[subpop]], "eppfp")$artelig.idx.ts)
  if(anc.prior) {
    set.anc.prior(loc, subpop)
  }
  if(length(names(dt)) == 1) {
    fit[[subpop]] <- fitmod(dt[[subpop]], equil.rprior=eq.prior, B0=2e5, B=1e3, B.re=1e3, D=4, opt_iter=3)

  } else {
    fit[[subpop]] <- fitmod(dt[[subpop]], equil.rprior=eq.prior, B0=2e5, B=1e3, B.re=1e3)

  }
}


## Prepare output
result <- lapply(fit, simfit.gbd)
dir.create(out.dir, showWarnings = F, recursive = T)
save(result, file = out.path)

## Aggregate subpopulations to national and write prevalence and incidence draws
years <- unique(floor(result[[1]]$fp$proj.steps))
nat.data <- nat.draws(result)
var_names <- sapply(1:ncol(nat.data$prev), function(a) {paste0('draw',a)})
out_data <- lapply(nat.data, data.frame)
for (n in c('prev', 'incid', 'art', 'art_num', 'pop')) {
  names(out_data[[n]]) <- var_names
  out_data[[n]]$year <- years
  col_idx <- grep("year", names(out_data[[n]]))
  out_data[[n]] <- out_data[[n]][, c(col_idx, (1:ncol(out_data[[n]]))[-col_idx])]
  write.csv(out_data[[n]], paste0(out.dir, "/results_", n , i, ".csv"), row.names=F)
}
## Plot results
plot.fit(result, pdf.path, nat.data)


### END