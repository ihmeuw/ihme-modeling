################################################################################
## Purpose: Run EPP in parallel by sourcing code from github
## Run instructions: Launch from run_all
################################################################################

### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,FILEPATH,FILEPATH)
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, FILEPATH, FILEPATH), user, "/HIV/")

## Packages
library(data.table); library(ggplot2); #library(devtools)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
	loc <- args[1]
	run.name <- args[2]
	prepped.dir <- args[3]
	model.type <- args[4]
  art_new_dis <- args[5]
  random_walk <- args[6]
  region_anc <- args[7]
	i <- Sys.getenv("SGE_TASK_ID")
} else {
	loc <- "NER"
	run.name <- "170419_NER_rw" #paste0(substr(gsub("-","",Sys.Date()),3,8), "_test")
	prepped.dir <- "170321_golf_elig" #paste0(substr(gsub("-","",Sys.Date()),3,8), "_test")
	model.type <- "rspline"
  art_new_dis <- F
  random_walk <- F
  region_anc <- F
	i <- 1
}

print(art_new_dis)
print(random_walk)
#print(region_anc)

## Paths
input.path <- paste0(FILEPATH, prepped.dir, "/", loc, "/prepped_epp_data", i, ".RData")
out.dir <- paste0(FILEPATH, run.name, "/", loc, "/")
dir.create(out.dir, showWarning = F, recursive = T)
pdf.path <- paste0(out.dir, "test_results", i, ".pdf")

## Functions
## Install epp and anclik packages directly from github
# install.packages("devtools", repos="http://cran.us.r-project.org")
# install.packages("libcurl", repos="http://cran.us.r-project.org")
# library(devtools)
# devtools::install_github("jeffeaton/anclik/anclik")
# devtools::install_github("jeffeaton/epp")
library(anclik)
library(epp)

source(paste0(code.dir, "EPP2016/read_epp_files.R"))
#  Extra Functions for calculating GBD outputs and aggregating to national
new.inf <- function(mod, fp) {
  attr(mod, "rvec")[fp$proj.steps %% 1 == 0.5] * (rowSums(mod[,-1,1]) + fp$relinfectART * rowSums(mod[,-1,-1])) / rowSums(mod) * mod[,1,1]
}

suscept.pop <- function(mod) {
  mod[,1,1]
}

plwh <- function(mod) {
  rowSums(mod[,-1,])
}

art <- function(mod) {
  rowSums(mod[,-1,-1])
}

total.pop <- function(mod) {
  rowSums(mod)
}

nat.draws <- function(result) {
  nat.inf <- Reduce('+', lapply(result, function(x){x$new.inf}))
  nat.suscept <- Reduce('+', lapply(result, function(x){x$suscept.pop}))

  nat.incid <- nat.inf/nat.suscept

  nat.plwh <- Reduce('+', lapply(result, function(x){x$plwh}))
  nat.total.pop <- Reduce('+', lapply(result, function(x){x$total.pop}))

  nat.art <- Reduce('+', lapply(result, function(x){x$art}))

  nat.art.cov <- nat.art / nat.plwh
  nat.prev <- nat.plwh/nat.total.pop
  output <- list(prev=nat.prev, incid=nat.incid, art = nat.art.cov, art_num=nat.art, pop=nat.total.pop)
  return(output)
}

######## random walk function ############
sim_rvec_rwproj <- function(rvec, firstidx, lastidx, first_projidx, firstidx.mean, dt){
  logrvec <- log(rvec)
  mean <- median(diff(logrvec[firstidx.mean:lastidx]))  ## define the mean of the random walk
  sd <- mean(sqrt(diff(logrvec[firstidx:lastidx])^2)) #sqrt(mean(diff(logrvec[firstidx:lastidx])^2))  
  projidx <- if (first_projidx<length(rvec)){projidx <- (first_projidx +1):length(rvec)} else {projidx <- 0}  # (lastidx+1):length(rvec)   ## start the projection from 2005

  ## simulate differences in projection log(rvec)
  ## variance increases with time: sigma^2*(t-t1) [Hogan 2012]
  
  ldiff <- rnorm(length(projidx), mean, sd)  #*sqrt((1+dt*(projidx-lastidx-1)))  ## assume the SD won't increase with time + rvec is decreasing with years

  rvec[projidx] <- exp(logrvec[first_projidx] + cumsum(ldiff))
  return(rvec)
}

# Simulate fit and output incidence and prevalence
simfit.gbd <- function(fit){
  fit$param <- lapply(seq_len(nrow(fit$resample)), function(ii) fnCreateParam(fit$resample[ii,], fit$fp))
  # Random-walk projection method
  if(random_walk){
    if(exists("eppmod", where=fit$fp) && fit$fp$eppmod == "rtrend")
      stop("Random-walk projection is only used with r-spline model")

    fit$rvec.spline <- sapply(fit$param, "[[", "rvec")
    firstidx <- (fit$likdat$firstdata.idx-1)/fit$fp$dt+1 # which(fit$fp$proj.steps == fit$fp$tsEpidemicStart) ## assume SD only depends on rvec of years with ANC/survey data
    lastidx <- (fit$likdat$lastdata.idx-1)/fit$fp$dt+1

    firstidx.mean <- which(fit$fp$proj.steps==min(epp.art[(epp.art$m.val+epp.art$f.val)>0,]$year)) # which(fit$fp$proj.steps == fit$fp$tsEpidemicStart) 

    ### Get the year of prev data that starts to decrease in the recent years
    hhs.dt <- fit$likdat$hhslik.dat
    if(nrow(fit$likdat$hhslik.dat)!=0){
      hhs.dt <- hhs.dt[order(hhs.dt$year),]
      hhs.dt$dif <- c(0, diff( hhs.dt$prev))
      proj.year <- hhs.dt[hhs.dt$dif<=0,]$year[nrow(hhs.dt[hhs.dt$dif<=0,])-1]
      proj.year.idx <- ifelse(length(proj.year)>0, which(fit$fp$proj.steps==proj.year), 0)
    } else {proj.year.idx <- 0}
    ## replace rvec with random-walk simulated rvec
    fit$param <- lapply(fit$param, function(par){
      ### compare the prev year with the lowest rvec year  
      rvec.min.idx <- which(par$rvec==min(par$rvec))
      first_projidx <- ifelse(proj.year.idx>rvec.min.idx, proj.year.idx, rvec.min.idx)

      par$rvec <- sim_rvec_rwproj(par$rvec, firstidx, lastidx, first_projidx, firstidx.mean, fit$fp$dt); par}) 
  }
  
  fp.list <- lapply(fit$param, function(par) update(fit$fp, list=par))
  mod.list <- lapply(fp.list, simmod)
  
  fit$rvec <- sapply(mod.list, attr, "rvec")
  fit$prev <- sapply(mod.list, prev)
  fit$incid <- mapply(incid, mod = mod.list, fp = fp.list)
  fit$popsize <- sapply(mod.list, rowSums)
  # fit$pregprev <- mapply(epp::fnPregPrev, mod.list, fp.list)

  ## GBD additions
  fit$new.inf <- mapply(new.inf, mod = mod.list, fp = fp.list)
  fit$suscept.pop <- sapply(mod.list, suscept.pop)
  fit$plwh <- sapply(mod.list, plwh)
  fit$total.pop <- sapply(mod.list, total.pop)
  fit$art <- sapply(mod.list, art)
  return(fit)
}

### Add subpop information into fitmod
fitmod <- function(obj, ..., B0 = 1e5, B = 1e4, B.re = 3000, number_k = 500, D=0, opt_iter=0,
                   sample.prior=epp:::sample.prior,
                   prior=epp:::prior,
                   likelihood=epp:::likelihood){

  ## ... : updates to fixed parameters (fp) object to specify fitting options
  likdat <- attr(obj, 'likdat')  # put in global environment for IMIS functions.
  fp <- attr(obj, 'eppfp')
  fp$subpop_id <- attr(obj, "region")
  fp <- update(fp, ...)

  ## If IMIS fails, start again
  fit <- try(stop(""), TRUE)
  while(inherits(fit, "try-error")){
    start.time <- proc.time()
    fit <- try(IMIS(B0, B, B.re, number_k, D, opt_iter, fp=fp, likdat=likdat,
                    sample.prior=sample.prior, prior=prior, likelihood=likelihood))
    fit.time <- proc.time() - start.time
  }
  fit$fp <- fp
  fit$likdat <- likdat
  fit$time <- fit.time

  class(fit) <- "eppfit"

  return(fit)
}

### Code
load(input.path)

### Get ART cov input type from ep4 and adjust the count based on GBD pop
  temp.loc.table <- fread(paste0(FILEPATH))
  ## Determine the path to the most recent year of UNAIDS data
  unaids.dt.years <- c(2016, 2015, 2013)
  check_i <- 1
  unaids.dt.path <- NA
  while(is.na(unaids.dt.path)) {
    dt.year <- unaids.dt.years[check_i]
    print(dt.year)
    unaids.dt.path <- temp.loc.table[ihme_loc_id == loc][[paste0("unaids_", dt.year)]]
    check_i <- check_i + 1
  }
  # Prep pjnz path
  split <- strsplit(unaids.dt.path, split = "/", fixed = T)[[1]]
  pjnz <- paste0(paste(split[1:(length(split) - 1)], collapse = "/"), ".PJNZ")

  ## ep4
  ep4file <- grep(".ep4", unzip(pjnz, list=TRUE)$Name, value=TRUE)
  con <- unz(pjnz, ep4file)
  ep4 <- scan(con, "character", sep="\n")
  close(con)

  artstart.idx <- grep("ARTSTART", ep4)+1
  artend.idx <- grep("ARTEND", ep4)-1

  epp.art <- setNames(read.csv(text=ep4[artstart.idx:artend.idx], header=FALSE, as.is=TRUE),
                        c("year", "m.isperc", "m.val", "f.isperc", "f.val", "cd4thresh", "m.perc50plus", "f.perc50plus", "perc50plus", "1stto2ndline"))
  isperc.dt <- data.table(epp.art[,c("year", "m.isperc")])

  ## Read in country file pop for subpop ##
  eppd <- read_epp_data(pjnz)
  epp.subp <- read_epp_subpops(pjnz)
  subpops <- data.table(do.call ("cbind",lapply(names(epp.dt), function(x){
                                                                temp <- epp.subp$subpops[[x]][,2] 
  })))
  subpops$total <- apply(subpops, 1, sum)
  subpops$year <- epp.subp$subpops[[names(epp.dt)[1]]][,1]
  colnames(subpops) <- c(names(epp.dt), "total", "year")
  subpops_melted <- melt(subpops, id.var=c("year", "total"))
  subpops_melted [,ratio := value/total]

  ## Read epidemic type
  TYPfile <- grep(".TYP", unzip(pjnz, list=TRUE)$Name, value=TRUE)
  con <- unz(pjnz, TYPfile)
  TYP <- scan(con, "character", sep="\n")
  close(con)

####### Region specific ANC bias table ######
ancbias.factors <- data.table(expand.grid(subpop_anc_bias = c("Urban", "Rural"), region_anc_bias = c("Eastern Africa", "Southern Africa", "Western/Central Africa", "GENERALIZED", "CONCENTRATED")))
ancbias.factors[region_anc_bias=="CONCENTRATED" & subpop_anc_bias=="Urban"]$subpop_anc_bias <- "Women"
ancbias.factors[region_anc_bias=="CONCENTRATED" & subpop_anc_bias=="Rural"]$subpop_anc_bias <- "Men"

ancbias.mean <- c(-0.01, 0.03, 0.14, 0.04, 0.17, 0.24, 0.11, 0.17, 0.27, 0.22)
ancbias.sd <- c(0.14, 0.24, 0.1, 0.07, 0.13, 0.22, 0.14, 0.22, 0.07, 0.07)
ancbias.table <- data.table(ancbias.factors, mean = ancbias.mean, sd = ancbias.sd)

######## Identify the region #########
source(paste0(code.dir, "shared_functions/get_locations.R"))
loc.table <- get_locations()
##
region <- loc.table[ihme_loc_id==loc]$region_name
region <- sub(" Sub-Saharan ", " ", region)
region <- ifelse(region %in% c("Western Africa", "Central Africa"), "Western/Central Africa", region)

## IMIS
fit <- list()
for(subpop in names(epp.dt)) {
  # subpop <- names(epp.dt)[4]
	print(subpop)
  if(nrow(attr(epp.dt[[subpop]], "likdat")[["hhslik.dat"]]) == 0) {
    region_anc <- T
  } else {
    region_anc <- F
  }

  ## Rename unusual urbal/rural subpop name 
  # if (subpop %in% XXX) { subpop_new <- "Urban" } else if (subpop %in% YYY) { subpop_new <- "Rural" } else { subpop_new <- subpop}
  if (region_anc) {
      subpop_new <- subpop
      ### get men/women pop
      if (subpop_new %in% c("Pop masculine restante","Remanente Masculina", "population masculine restante","Male remaining pop")){
        subpop_new <- "Men"
      }
      if (subpop_new %in% c("Pop féminine restante", "Remanente Femenina", "population feminine restante","Female remaining pop")){
        subpop_new <- "Women"
      }
      ## Region specific ANC bias
      region_new <- ifelse(region %in% c("Eastern Africa", "Southern Africa", "Western/Central Africa"), region, TYP)

      gen.pop.dict <- c("TOTAL" , "Población Total", "Remaining Pop", "GP", "General Population", "Total",
                        "General population", "GENERAL POPULATION", "GEN. POPL.", "General population(Low Risk)")

      if (subpop_new %in% unique(ancbias.table[region_anc_bias == region_new]$subpop_anc_bias)) {
        ancbias.pr.mean <- ancbias.table[subpop_anc_bias==subpop_new & region_anc_bias==region_new]$mean 
        ancbias.pr.sd <- ancbias.table[subpop_anc_bias==subpop_new & region_anc_bias==region_new]$sd 
      } else {
          if (!subpop_new %in% c(gen.pop.dict, "Men", "Women" )) {
            ancbias.pr.mean <- 0
            ancbias.pr.sd <- 0.0001  ## sd of mean 
          } else {
            ancbias.pr.mean <- mean(ancbias.table[region_anc_bias==region_new]$mean) 
            ancbias.pr.sd <- mean(sqrt(sum(ancbias.table[region_anc_bias==region_new]$sd^2)))  ## sd of mean 
          }
        }

  } else {
    ancbias.pr.mean <- 0.15
    ancbias.pr.sd <- 1
  }
    ### assign new value to package ##
  assignInNamespace("ancbias.pr.mean", ancbias.pr.mean, ns = "epp")
  assignInNamespace("ancbias.pr.sd", ancbias.pr.sd, ns = "epp")

  ## 
	fit[[subpop]] <- fitmod(epp.dt[[subpop]], equil.rprior=TRUE, B0=2e5, B=1e3, B.re=5e2, D=4, opt_iter=3)

  
}

## Simulate fit
result <- lapply(fit, simfit.gbd)
save(result, file = paste0(out.dir, "results", i, ".RData"))

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
  write.csv(out_data[[n]], paste0(out.dir, "results_", n , i, ".csv"), row.names=F)
}

## Plot results
# Functions
cred.region <- function(x, y, ...)
  polygon(c(x, rev(x)), c(y[1,], rev(y[2,])), border=NA, ...)

transp <- function(col, alpha=0.5)
  return(apply(col2rgb(col), 2, function(c) rgb(c[1]/255, c[2]/255, c[3]/255, alpha)))

plot.prev <- function(fit, ylim=c(0, 0.22), col="blue"){
  plot(1970:2015, rowMeans(fit$prev), type="n", ylim=ylim, ylab="", yaxt="n", xaxt="n")
  axis(1, labels=FALSE)
  axis(2, labels=FALSE)
  cred.region(1970:2015, apply(fit$prev, 1, quantile, c(0.025, 0.975)), col=transp(col, 0.3))
  lines(1970:2015, rowMeans(fit$prev), col=col)
  ##
  points(fit$likdat$hhslik.dat$year, fit$likdat$hhslik.dat$prev, pch=20)
  segments(fit$likdat$hhslik.dat$year,
           y0=pnorm(fit$likdat$hhslik.dat$W.hhs - qnorm(0.975)*fit$likdat$hhslik.dat$sd.W.hhs),
           y1=pnorm(fit$likdat$hhslik.dat$W.hhs + qnorm(0.975)*fit$likdat$hhslik.dat$sd.W.hhs))
}

plot.incid <- function(fit, ylim=c(0, 0.05), col="blue"){
  plot(1970:2015, rowMeans(fit$incid), type="n", ylim=ylim, ylab="", yaxt="n", xaxt="n")
  axis(1, labels=FALSE)
  axis(2, labels=FALSE)
  cred.region(1970:2015, apply(fit$incid, 1, quantile, c(0.025, 0.975)), col=transp(col, 0.3))
  lines(1970:2015, rowMeans(fit$incid), col=col)
}

plot.rvec <- function(fit, ylim=c(0, 3), col="blue"){
  rvec <- lapply(fit$mod, attr, "rvec")
  rvec <- mapply(function(rv, par){replace(rv, fit$fp$proj.steps < par$tsEpidemicStart, NA)},
                 rvec, fit$param)
  plot(fit$fp$proj.steps, rowMeans(rvec, na.rm=TRUE), type="n", ylim=ylim, ylab="", yaxt="n")
  axis(2, labels=FALSE)
  cred.region(fit$fp$proj.steps, apply(rvec, 1, quantile, c(0.025, 0.975), na.rm=TRUE), col=transp(col, 0.3))
  lines(fit$fp$proj.steps, rowMeans(rvec, na.rm=TRUE), col=col)
}


# Plot
anc_plot_data.final <- data.table()

pdf(pdf.path, width=13, height=8)
for (subpop in names(result)) {  # Subnationals
  country_name <- attr(result[[subpop]], 'country')

  site_names <- rownames(result[[subpop]]$likdat$anclik.dat$anc.prev)
  name <- site_names[1]

  bias_i <- ifelse(model.type=='rspline', ncol(result[[subpop]]$resample)-2, ncol(result[[subpop]]$resample)) 
  mean_bias <- mean(result[[subpop]]$resample[,bias_i])
  ancadjrr <- result[[subpop]]$fp$ancadjrr

  adj.length <- length(ancadjrr)
  anc.length <- ncol(result[[subpop]]$likdat$anclik.dat$anc.prev)
  ancadjrr <- ancadjrr[(adj.length - anc.length + 1):adj.length]
  
  result[[subpop]]$likdat$anclik.dat$anc.prev <- pnorm(qnorm(result[[subpop]]$likdat$anclik.dat$anc.prev) - mean_bias)
  result[[subpop]]$likdat$anclik.dat$anc.prev <- sweep(result[[subpop]]$likdat$anclik.dat$anc.prev, 2, ancadjrr, `*`)
  
  anc_plot_data <- melt(result[[subpop]]$likdat$anclik.dat$anc.prev)
  anc_plot_data$data_type <- 'Bias Adjusted ANC Data'
  anc_plot_data <- anc_plot_data[!is.na(anc_plot_data$value),]
  rownames(result[[subpop]]$prev) <- years
  prev_data <- melt(result[[subpop]]$prev)
  prev_data$data_type <- 'EPP Estimate'

  has_hhs <- ifelse(nrow(result[[subpop]]$likdat$hhslik.dat) > 0, TRUE, FALSE)
  if (has_hhs) {
    hhs_plot_data <- result[[subpop]]$likdat$hhslik.dat[,c('year', 'prev', 'W.hhs', 'sd.W.hhs')]
    hhs_plot_data$upper <- pnorm(hhs_plot_data$W.hhs + qnorm(0.975)*hhs_plot_data$sd.W.hhs)
    hhs_plot_data$lower <- pnorm(hhs_plot_data$W.hhs - qnorm(0.975)*hhs_plot_data$sd.W.hhs)
    hhs_plot_data$data_type <- "Survey Data"
  }
  c_palette <- c('#006AB8','#4D4D4D','#60BD68')
  gg <- ggplot() + geom_line(data=prev_data, aes(x=Var1, y=value, colour=data_type, group=Var2), alpha=0.1) +
    geom_line(data=anc_plot_data, aes(x=Var2, y=value, colour=data_type, group=Var1)) + 
    geom_point(data=anc_plot_data, aes(x=Var2, y=value, colour=data_type, group=Var1)) +
    #geom_point(data=hhs_plot_data, aes(x=year, y=prev, colour=data_type), size=3) + 
    #geom_errorbar(data=hhs_plot_data, aes(x=year, ymax=upper, ymin=lower, colour=data_type), size=1) + 
    scale_colour_manual(values=c_palette) + ggtitle(paste0(country_name,' ', subpop, ": ", model.type))
  if (has_hhs) {
    gg <- gg + geom_point(data=hhs_plot_data, aes(x=year, y=prev, colour=data_type), size=3) +
      geom_errorbar(data=hhs_plot_data, aes(x=year, ymax=upper, ymin=lower, colour=data_type), size=1)
  }
  print(gg)

  anc_plot_data$source <- subpop
  anc_plot_data.final <- rbind(anc_plot_data.final, anc_plot_data)

}

  write.csv(anc_plot_data.final, paste0(out.dir, "results_anc_adj" , i, ".csv"), row.names=F)


# National
rownames(nat.data$prev) <- years
prev_data <- melt(nat.data$prev)
prev_data$data_type <- 'EPP Estimate'

gg <- ggplot() + geom_line(data=prev_data, aes(x=Var1, y=value, colour=data_type, group=Var2), alpha=0.1) +
  scale_colour_manual(values=c_palette) + ggtitle(paste0(country_name,' national', ": ", model.type))
print(gg)

dev.off()
