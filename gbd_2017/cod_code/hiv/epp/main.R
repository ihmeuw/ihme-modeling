
### Setup
rm(list=ls())
windows <- Sys.info()[1][["sysname"]]=="Windows"
root <- ifelse(windows,"","")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, "", paste0("", user)), "")
## Packages
library(data.table); library(mvtnorm); library(survey)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
print(args)
if(length(args) > 0) {
	run.name <- args[1]
	loc <- args[2]
	proj.end <- as.integer(args[3])
	i <- as.integer(Sys.getenv(""))
} else {
	run.name <- ""
	loc <- ""
	proj.end <- 2017
	i <- 1
}

### Arguments
start.year <- 1970
stop.year <- 2017
trans.params <- TRUE
stop.collapse <- FALSE
anc.prior <- TRUE
gbd.pop <- TRUE
no.anc <- FALSE
art.sub <- TRUE
eq.prior <- TRUE
anc.backcast <- FALSE
num.knots <- 7

### Paths
input.dir <- paste0()
out.dir <- paste0('', loc)
out.path <- paste0(out.dir, "")
pdf.path <- paste0(out.dir, "")

### Functions
## GBD
source(paste0(code.dir,""))
source(paste0(code.dir,""))
source(paste0(code.dir,""))
source(paste0(code.dir,""))
source(paste0(code.dir,""))


## EPP
source(paste0(code.dir,""))
source(paste0(code.dir,""))
source(paste0(code.dir,""))
source(paste0(code.dir,""))
source(paste0(code.dir,""))
source(paste0(code.dir,""))

## Model in C
if(trans.params) {
 # Load C version
	dyn.load(paste0(code.dir, "", .Platform$dynlib.ext))  # Load C version with time series for transition parameters
} else {
	dyn.load(paste0(code.dir, "", .Platform$dynlib.ext))	
}

source(paste0(root, ""))

### Tables
loc.table <- data.table(get_locations(hiv_metadata = T))

### Code
## Prep data and collapse location subpopulations
dt <- prep_epp_data(loc, proj.end = 2017.5, stop_collapse = stop.collapse, gbd_pop = gbd.pop, art_sub = art.sub, num_knots = num.knots )

## Substitute IHME data
# Prevalence surveys
if((!stop.collapse & length(dt) == 1) | grepl("IND_", loc)) {
	print("Substituting prevalence surveys")
	dt <- sub.prev(loc, dt)	
}


# ANC data
high.risk.list <- loc.table[epp == 1 & collapse_subpop == 0 & !grepl("ZAF", ihme_loc_id) & !grepl("KEN", ihme_loc_id), ihme_loc_id]
ken.anc.path <- paste0(root, "")
ken.anc <- fread(ken.anc.path)
no.anc.ken <- setdiff(loc.table[epp == 1 & grepl("KEN", ihme_loc_id), ihme_loc_id], ken.anc$ihme_loc_id)
if(loc %in% c(high.risk.list, "PNG", no.anc.ken)) {
	anc.backcast <- F
}
if(anc.backcast) {
	dt <- sub.anc(loc, dt)
}

# Transition parameters
if(trans.params) {
	dt <- extend.trans.params(dt, start.year, stop.year)
	dt <- sub.off.art(dt, loc, i)
	dt <- sub.on.art(dt, loc, i)
	dt <- sub.cd4.prog(dt, loc, i)
}

## Fit model
fit <- list()
for(subpop in names(dt)) {
	print(subpop)
	# if(subpop == "HSH") {
	# 	attr(dt[[subpop]], "likdat")$hhslik.dat[1:2, "used"] <- FALSE
	# }
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
  write.csv(out_data[[n]], paste0(out.dir, ""), row.names=F)
}

## Plot results
plot.fit(result, pdf.path, nat.data)

### END