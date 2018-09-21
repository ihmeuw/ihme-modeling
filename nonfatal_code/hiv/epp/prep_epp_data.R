################################################################################
## Purpose: Prep EPP data from UNAIDS country files
################################################################################

### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,FILEPATH,FILEPATH)
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, FILEPATH, FILEPATH), user, "/HIV/EPP2016/")

## Packages
library(data.table)

## Arguments
args <- commandArgs(trailingOnly=T)
if(length(args) > 0) {
  ihme_loc <- args[1]
  run_name <- args[2]
  rank_draws <- args[3]
  proj.end <- as.numeric(args[4])
} else {
  ihme_loc <- "ZAF_485"
  run_name <- paste0(substr(gsub("-","",Sys.Date()),3,8), "_test")
  rank_draws <- T
  proj.end <- 2016
}

### Paths
inputs.dir <- paste0(FILEPATH)
aim.dir <- paste0(inputs.dir, "AIM_assumptions/")
output.dir <- paste0('FILEPATH/',run_name,'/',ihme_loc)
dir.create(output.dir, recursive = T)

### Functions
## Install epp package directly from github
if("epp" %in% rownames(installed.packages()) == FALSE) {
  if("devtools" %in% rownames(installed.packages()) == FALSE) {
    install.packages("devtools", repos="http://cran.us.r-project.org")
  }
  library(devtools)
  devtools::install_github("jeffeaton/epp")
}
library(epp)

## Functions for finding hidden subpopulations
source(paste0(code.dir, "read_epp_files.R"))
source(paste0(code.dir, "fnPrepareANCLikelihoodData.R"))
assignInNamespace("fnPrepareANCLikelihoodData", fnPrepareANCLikelihoodData, ns = "anclik")

source(paste0(root, "FILEPATH/get_population.R"))
source(paste0(root, "FILEPATH/get_age_map.r"))
source(paste0("FILEPATH/get_locations.R"))

### Tables
loc.table <- get_locations()
input.loc.map <- fread(paste0(inputs.dir, "location_map.csv"))
age.table <- data.table(get_age_map())
supplement_survey <- fread(paste0(inputs.dir, "supplement_survey_data.csv"))  # Additional prevalence surveys
zaf.map <- fread(paste0(FILEPATH))

###### Updated function ########
prepare_epp_fit <- function(pjnz, proj.end=2015.5){

  ## epp
  eppd <- read_epp_data(pjnz)
  # ## get rid of the clinics with anc data in early years
  # for (subpop in names(eppd)){
  # eppd[[subpop]]$anc.used <- eppd[[subpop]]$anc.used[-1]  
  # eppd[[subpop]]$anc.prev <- eppd[[subpop]]$anc.prev[-1,]
  # eppd[[subpop]]$anc.n <- eppd[[subpop]]$anc.n[-1,]
 
  # }
  # ###
  epp.subp <- read_epp_subpops(pjnz)
  ### add in GBD adult pop to scale the subpops: only for ZAF and KEN
 if ( "ZAF" %in% gsub('\\_.*', "", ihme_loc) | "KEN" %in% gsub('\\_.*', "", ihme_loc)) {

    location_id <- loc.table[ihme_loc_id==ihme_loc, location_id]
    ### ZAF read in national level country file
    if ("ZAF" %in% gsub('\\_.*', "", ihme_loc)) {
    location_id <- loc.table[ihme_loc_id=="ZAF", location_id]
    } 

    gbd16.pop <- get_population(year_id = -1, location_id = location_id, sex_id = 3, age_group_id = 24, gbd_round_id = 4)
    epp.adult.pop <- data.table(year_id=epp.subp$total$year, epp_pop=epp.subp$total$pop15to49)
    merged.pop <- merge(epp.adult.pop, gbd16.pop[,.(year_id, population)], by="year_id")
    merged.pop[,scale:=population/epp_pop]
    scale <- merged.pop[,scale]
    scale <- c(scale, rep(scale[length(scale)], (nrow(epp.subp$total)- length(scale))))

    for (pop in names(epp.subp)) {
    temp.dt <- epp.subp[[pop]]
          if(pop=="subpops") {
            for (subpop in names(temp.dt)){
               temp.dt1 <- temp.dt[[subpop]]          
             for (col_id in c("pop15to49", "pop15", "pop50", "netmigr")){
              epp.subp[[pop]][[subpop]][,col_id] <- temp.dt1[,col_id]*scale
             }
            } 
          } else {
              for (col_id in c("pop15to49", "pop15", "pop50", "netmigr")){
                epp.subp[[pop]][,col_id] <- temp.dt[,col_id]*scale
              }
            }
    }
  }
  ######
  epp.input <- read_epp_input(pjnz)

  epp.subp.input <- fnCreateEPPSubpops(epp.input, epp.subp, eppd)

  ## output
  val <- setNames(vector("list", length(eppd)), names(eppd))

  set.list.attr <- function(obj, attrib, value.lst)
    mapply(function(set, value){ attributes(set)[[attrib]] <- value; set}, obj, value.lst)

  val <- set.list.attr(val, "eppd", eppd)
  val <- set.list.attr(val, "likdat", lapply(eppd, fnCreateLikDat, anchor.year=epp.input$start.year))
  val <- set.list.attr(val, "eppfp", lapply(epp.subp.input, fnCreateEPPFixPar, proj.end = proj.end))
  val <- set.list.attr(val, "country", attr(eppd, "country"))
  val <- set.list.attr(val, "region", names(eppd))

  return(val)
}


### Code
## Prep alternate location identifiers
iso3 <- input.loc.map[ihme_loc_id == ihme_loc, gbd15_spectrum_loc]
dt_iso3 <- iso3  # for subsetting in data.table
location <- input.loc.map[ihme_loc_id == ihme_loc, location_id]

## Determine the path to the most recent year of UNAIDS data
unaids.dt.years <- c(2016, 2015, 2013)
i <- 1
unaids.dt.path <- NA
while(is.na(unaids.dt.path)) {
  dt.year <- unaids.dt.years[i]
  print(dt.year)
  unaids.dt.path <- input.loc.map[ihme_loc_id == ihme_loc][[paste0("unaids_", dt.year)]]
  i <- i + 1
}
# Prep pjnz path
split <- strsplit(unaids.dt.path, split = "/", fixed = T)[[1]]
pjnz <- paste0(paste(split[1:(length(split) - 1)], collapse = "/"), ".PJNZ")

# Determine projection start and end year in UNAIDS country data
ep1 <- scan(paste(unaids.dt.path, ".ep1", sep=""), "character", sep="\n")
ep1 <- ep1[3:length(ep1)]
firstprojyr.idx <-  which(sapply(ep1, substr, 1, 11) == "FIRSTPROJYR")
lastprojyr.idx <-  which(sapply(ep1, substr, 1, 10) == "LASTPROJYR")
start.year <- as.integer(read.csv(text=ep1[firstprojyr.idx], header=FALSE)[2])
stop.year <- as.integer(read.csv(text=ep1[lastprojyr.idx], header=FALSE)[2])

## Read in and prep UNAIDS data 
epp.dt <- prepare_epp_fit(pjnz, proj.end = (proj.end + 0.5))
# Split South Africa Province
if (grepl('ZAF', ihme_loc)) {
  spectrum_code <- zaf.map[ihme_loc_id == ihme_loc, spectrum_code]
  zaf.stub <- gsub("ZAF_", "", spectrum_code)
  tmp_obj <- list()
  tmp_obj[[zaf.stub]] <- epp.dt[[zaf.stub]]
  epp.dt <- tmp_obj
}

## Add Prevalence surveys
if (iso3 %in% supplement_survey[,unique(iso3)] & !(iso3 %in% c("MWI", "ZMB"))) {
  print('found survey')
  survey_subpop <- supplement_survey[iso3==dt_iso3, unique(subpop)]
  print(names(epp.dt))
  print(survey_subpop)
  tmp_survey <- supplement_survey[iso3==dt_iso3,.(year, prev, se, n)]
  tmp_survey[,used:=TRUE]
  tmp_survey[prev==0,used:=FALSE]
  # tmp_survey[year == 2000, used:=FALSE]
  tmp_survey[,W.hhs:=qnorm(prev)]
  tmp_survey[,v.hhs:=2*pi*exp(W.hhs^2)*se^2]
  tmp_survey[,sd.W.hhs := sqrt(v.hhs)]
  tmp_survey[,idx := year - (start.year-1)]

  attr(epp.dt[[survey_subpop]], 'likdat')$hhslik.dat <- rbind(attr(epp.dt[[survey_subpop]], 'likdat')$hhslik.dat, as.data.frame(tmp_survey[used==TRUE,]))
}


IRR2 <- fread(paste0(aim.dir, "sex_age_pattern/age_IRRs/Feb17/GEN_IRR.csv"))
IRR2 <- IRR2[age < 55,]

sex_IRR <- fread(paste0(aim.dir, "sex_age_pattern/FtoM_inc_ratio_epidemic_specific.csv"))
sex_IRR <- sex_IRR[epidemic_class=="GEN",]
sex_IRR[,year:=year+start.year-1]

missing_years <- c()
if (sex_IRR[,max(year)] < stop.year)
  missing_years <- (sex_IRR[,max(year)]+1):stop.year
replace_IRR <- sex_IRR[order(year)][rep(nrow(sex_IRR), times=length(missing_years))]
if (length(missing_years) > 0)
  replace_IRR[,year:=missing_years]
sex_IRR <- rbind(sex_IRR, replace_IRR)

sex_IRR[,sex:=2]

male_IRR <- copy(sex_IRR)
male_IRR[,FtoM_inc_ratio:=1.0]
male_IRR[,sex:=1]

sex_IRR <- rbind(sex_IRR, male_IRR)



## Read in population for age-sex structure for aggregation
if(location %in% get_population(location_id = -1, location_set_id = 79)$location_id) {# Check if population in database
  in.pop <- get_population(age_group_id = 8:15, location_id = location, year_id = -1, sex_id = 1:2, location_set_id = 79)
  pop <- merge(in.pop, age.table[, .(age_group_id, age_group_name_short)], by = "age_group_id", all.x = T)
  setnames(pop, 
    c("year_id", "sex_id", "age_group_name_short", "population"),
    c("year", "sex", "age", "value")
  )
  pop[, (setdiff(names(pop), c("year", "sex", "age", "value"))) := NULL]
} else {
  ## Get single age population from .DP file
  dp <- suppressWarnings(fread(paste0(unaids.dt.path, ".DP")))
  names(dp)[1] <- "Tag"
  pop.start.idx <- which(dp$Tag %in% c("<BigPop MV>", "<BigPop3>")) + 3
  if(dp[pop.start.idx, Notes] != "Males, Total, Age 0") {
    pop.start.idx <- pop.start.idx - 1
  }
  pop.end.idx <- pop.start.idx + 161
  pop.col.idx <- which(is.na(dp[pop.start.idx]))[1] - 1
  start.year <- as.integer(dp[pop.start.idx - 1, 4, with = F])
  in.pop <- dp[pop.start.idx:pop.end.idx, 4:pop.col.idx, with = F]
  names(in.pop) <- as.character(start.year:(ncol(in.pop) + start.year - 1))
  # Try checking if "Males" in row
  suppressWarnings(in.pop[, sex := 1])
  in.pop[nrow(in.pop)/2:nrow(in.pop), sex := 2]
  in.pop[, age := rep(seq(0:((nrow(in.pop) / 2) - 1)) - 1, 2)]
  melted.pop <- suppressWarnings(melt.data.table(in.pop, id.vars = c("sex", "age"), variable.name = "year"))
  melted.pop[, year := as.integer(as.character(year))]
  melted.pop[, value := as.numeric(value)]
  subset.pop <- melted.pop[age %in% 15:54]
  subset.pop[, age := age - (age %% 5)]
  pop <- subset.pop[!is.na(value), .(value = sum(value)), by = .(year, sex, age)]
}

pop <- pop[year >= start.year,]

pop$age <- strtoi(pop$age)
pop[(age-5) %%  10 != 0, age:=as.integer(age-5)]
pop[,value:=as.numeric(value)]

pop1 <-data.table(aggregate(value ~ sex + age + year,pop,FUN=sum))[order(sex,age)]
missing_years <- c()
if (pop1[,max(year)] < stop.year)
  missing_years <- (pop1[,max(year)]+1):stop.year
replace_pop <- pop1[rep(which(pop1[,year] == pop1[,max(year)]), times=length(missing_years))]
replace_years <- rep(1:(stop.year-pop1[,max(year)]), each=length(which(pop1[,year] == pop1[,max(year)])))
replace_pop[,year:=year+replace_years]
pop1 <- rbind(pop1, replace_pop)


## Read On-ART mortality, Off-ART mortality, and CD4 duration 
#****** Need to read in parent for places with subnationals
# On-ART mortality
mortart <- fread(paste0(aim.dir,"transition_parameters/HIVmort_onART_regions/DisMod/", ihme_loc,"_HIVonART.csv"))
mortart <- melt(mortart, 
                id = c("durationart", "cd4_category", "age", "sex","cd4_lower",
                       "cd4_upper"))
setnames(mortart, c("variable","value"), c("drawnum","draw"))
mortart <- mortart[,drawnum := substr(drawnum, 5,8)]
if(rank_draws) {
  mortart[,rank:=rank(-draw,ties.method="first"),by=c("durationart", "cd4_category", "age", "sex")]
  mortart[,drawnum:=rank]
}
mortart <- mortart[order(durationart,cd4_category,age,sex,drawnum)]
mortart <- mortart[,c("durationart", "cd4_category", "age", "sex","cd4_lower",
                      "cd4_upper", "drawnum", "draw"), with=F]
mortart_read <- data.table(dcast(mortart,durationart+cd4_category+age+sex~drawnum, value.var='draw'))
for (i in 1:1000) {
  j <- i + 4
  setnames(mortart_read, j, paste0("draw",i))
}
mortart_read <- melt(mortart_read, id = c("durationart", "cd4_category", "age", "sex"))
setnames(mortart_read, c("variable","value","cd4_category"),c("draw","mort","cd4"))
mortart_read <- mortart_read[age!="55-100",]
mortart_read <- mortart_read[,draw := substr(draw,5,8)]

# Off-ART Mortality
mortnoart <- fread(paste0(aim.dir, "transition_parameters/HIVmort_noART/current_draws/",ihme_loc,"_mortality_par_draws.csv"))
if(rank_draws) {
  mortnoart[,draw:=rank(-mort,ties.method="first"),by=c("age","cd4")]
}
mortnoart <- mortnoart[order(age,cd4,draw)]
mortnoart_read <- mortnoart[,c("age","cd4","draw","mort"), with=F]

# CD4 duration
progdata <- fread(paste0(aim.dir, "transition_parameters/DurationCD4cats/current_draws/",ihme_loc,"_progression_par_draws.csv"))
if(rank_draws) {
  progdata[, draw := rank(-prog,ties.method="first"),by=c("age","cd4")]
}
progdata <- progdata[order(age,cd4,draw)]
progdata_read <- progdata[,c("age","cd4","draw","prog"), with=F]
progdata_read <- progdata_read[,lambda:=1/prog]



for (k in 1:1000) {
  print(k)

  IRR <- runif(16, IRR2$lower, IRR2$upper)
  IRR2[,IRR:=IRR]

  IRR2[,IRR:=IRR2[,IRR]/IRR2[age==25 & sex==1,IRR]]

  combined_IRR <- merge(sex_IRR, IRR2, by='sex', allow.cartesian=TRUE)
  combined_IRR[,comb_IRR := FtoM_inc_ratio * IRR]

  pop2 <- merge(pop1, combined_IRR, by=c('sex', 'age', 'year'))

  pop2[,wt:=comb_IRR*value]
  sex_agg <- pop2[,.(wt=sum(wt)),by=.(year, age)]

  total <- pop2[,.(total = sum(wt)),by=.(year)]
  pop2 <- merge(pop2, total, by=c('year'))
  pop2[,ratio:=wt/total]


  sex_agg <- merge(sex_agg, total, by=c('year'))
  sex_agg[,ratio:=wt/total]

  mortart <- mortart_read[draw==k,]
  mortart[,age:= as.integer(sapply(strsplit(mortart[,age],'-'), function(x) {x[1]}))]
  mortart[,sex:=as.integer(sex)]

  cd4_cats <- unique(mortart[,cd4])
  durat_cats <- unique(mortart[,durationart])
  cd4_vars <- expand.grid(durationart=durat_cats, cd4=cd4_cats)

  expanded_pop <- pop2[rep(1:nrow(pop2), times=length(cd4_cats)*length(durat_cats))]
  expanded_pop <- expanded_pop[order(year, sex, age)]
  expanded_pop <- cbind(expanded_pop, cd4_vars[rep(1:(nrow(cd4_vars)), times=nrow(pop2)),])
  combined_mort <- merge(expanded_pop, mortart, by=c('durationart', 'cd4', 'sex', 'age'))
  mortart <- combined_mort[,.(mort=sum(ratio*mort)), by=.(durationart, cd4, year)]

  mortart <- mortart[cd4=="ARTGT500CD4", cat := 1]
  mortart <- mortart[cd4=="ART350to500CD4", cat := 2]
  mortart <- mortart[cd4=="ART250to349CD4", cat := 3]
  mortart <- mortart[cd4=="ART200to249CD4", cat := 4]
  mortart <- mortart[cd4=="ART100to199CD4", cat := 5]
  mortart <- mortart[cd4=="ART50to99CD4", cat := 6] 
  mortart <- mortart[cd4=="ARTLT50CD4", cat := 7]
  mortart[,risk:=-1*log(1-mort)]
  mortart <- mortart[,c("risk","cat","durationart","year"), with=F]
  mortart <- mortart[, setattr(as.list(risk), 'names', cat), by=c("year","durationart")]
  mortart <- mortart[order(year, durationart)]
  # mortart <- mortart[age!="55-100",]

  mortart1 <- mortart[durationart=="LT6Mo",]
  mortart2 <- mortart[durationart=="6to12Mo",]
  mortart3 <- mortart[durationart=="GT12Mo",]

  alpha1gbd <- as.matrix(data.frame(mortart1[,c("1","2","3","4","5","6", "7"), with=F]))
  alpha2gbd <- as.matrix(data.frame(mortart2[,c("1","2","3","4","5","6", "7"), with=F]))
  alpha3gbd <- as.matrix(data.frame(mortart3[,c("1","2","3","4","5","6", "7"), with=F]))

  progdata <- progdata_read[draw==k,]
  progdata[,risk:=-1*log(1-prog)/0.1]
  progdata[,prob:=1-exp(-1*risk)]
  progdata[,age:= as.integer(sapply(strsplit(progdata[,age],'-'), function(x) {x[1]}))]

  cd4_cats <- unique(progdata[,cd4])
  cd4_vars <- data.table(cd4=cd4_cats)

  expanded_pop <- sex_agg[rep(1:nrow(sex_agg), times=length(cd4_cats))]
  expanded_pop <- expanded_pop[order(year, age)]
  expanded_pop <- cbind(expanded_pop, cd4_vars[rep(1:(nrow(cd4_vars)), times=nrow(sex_agg)),])
  combined_prog <- merge(expanded_pop, progdata, by=c('cd4', 'age'))
  progdata <- combined_prog[,.(prob=sum(ratio*prob)), by=.(cd4, year)]

  progdata <- progdata[cd4=="GT500CD4", cat := 1]
  progdata <- progdata[cd4=="350to500CD4", cat := 2]
  progdata <- progdata[cd4=="250to349CD4", cat := 3]
  progdata <- progdata[cd4=="200to249CD4", cat := 4]
  progdata <- progdata[cd4=="100to199CD4", cat := 5]
  progdata <- progdata[cd4=="50to99CD4", cat := 6] 
  progdata[,risk:=-1*log(1-prob)]
  progdata <- progdata[,.(year,risk,cat)]
  progdata <- progdata[, setattr(as.list(risk), 'names', cat), by=.(year)]
  progdata <- progdata[order(year)]
  progdata <- progdata[,c("1","2","3","4","5","6"), with=F]
  progdata <- data.frame(progdata)


  mortnoart <- mortnoart_read[draw==k,]
  mortnoart[,age:= as.integer(sapply(strsplit(mortnoart[,age],'-'), function(x) {x[1]}))]
  mortnoart[,risk:=-1*log(1-mort)/0.1]
  mortnoart[,prob:=1-exp(-1*risk)]

  cd4_cats <- unique(mortnoart[,cd4])
  cd4_vars <- data.table(cd4=cd4_cats)

  expanded_pop <- sex_agg[rep(1:nrow(sex_agg), times=length(cd4_cats))]
  expanded_pop <- expanded_pop[order(year, age)]
  expanded_pop <- cbind(expanded_pop, cd4_vars[rep(1:(nrow(cd4_vars)), times=nrow(sex_agg)),])
  combined_mu <- merge(expanded_pop, mortnoart, by=c('cd4', 'age'))
  mortnoart <- combined_mu[,.(prob=sum(ratio*prob)), by=.(cd4, year)]

  mortnoart <- mortnoart[cd4=="GT500CD4", cat := 1]
  mortnoart <- mortnoart[cd4=="350to500CD4", cat := 2]
  mortnoart <- mortnoart[cd4=="250to349CD4", cat := 3]
  mortnoart <- mortnoart[cd4=="200to249CD4", cat := 4]
  mortnoart <- mortnoart[cd4=="100to199CD4", cat := 5]
  mortnoart <- mortnoart[cd4=="50to99CD4", cat := 6] 
  mortnoart <- mortnoart[cd4=="LT50CD4", cat := 7] 
  mortnoart[,risk:=-1*log(1-prob)]
  mortnoart <- mortnoart[,.(year,risk,cat)]
  mortnoart <- mortnoart[, setattr(as.list(risk), 'names', cat), by=.(year)]
  mortnoart <- mortnoart[order(year)]
  mortnoart <- mortnoart[,c("1","2","3","4","5","6", "7"), with=F]
  mortnoart <- data.frame(mortnoart)
  mugbd <- as.matrix(mortnoart)



  mu <- as.vector(t(mugbd))
  alpha1 <- as.vector(t(alpha1gbd))
  alpha2 <- as.vector(t(alpha2gbd))
  alpha3 <- as.vector(t(alpha3gbd))
  cd4artmort <- cbind(mu, alpha1, alpha2, alpha3)
  for (n in names(epp.dt)) {
    attr(epp.dt[[n]], 'eppfp')$mortyears <- nrow(attr(epp.dt$Urban, 'eppfp')$cd4prog)
    attr(epp.dt[[n]], 'eppfp')$cd4years <- nrow(attr(epp.dt$Urban, 'eppfp')$cd4prog)
  }

  save(epp.dt, file = paste0(output.dir,"/prepped_epp_data",k,".RData"))
}

### End