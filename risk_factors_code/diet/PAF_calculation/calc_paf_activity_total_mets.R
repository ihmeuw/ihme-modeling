rm(list=ls())
if (Sys.info()["sysname"] == "Darwin") j_drive <- "/Volumes/snfs"
if (Sys.info()["sysname"] == "Linux") j_drive <- "/home/j"
user <- Sys.info()[["user"]]

source(paste0(j_drive,"/temp/",user,"/GBD_2016/calc_paf/parallel/ihmeDistList.R"))
dlist <- c(classA,classB,classM)
distlist <- dlist

library(foreach)
library(iterators)
library(doParallel)
library(dplyr)
library(data.table)
library(dfoptim)
library(fitdistrplus)
library(RColorBrewer)
library(ggplot2)
library(actuar)
library(grid)
library(RMySQL)
library(mvtnorm)
library(splitstackshape)
library(compiler)
library(rio)
library(hashmap)
library(pbmcapply)
library(pbapply)

setCompilerOptions(suppressAll = TRUE)
enableJIT(3)

set.seed(3855)

if (Sys.info()["sysname"] == "Darwin") {
  arg <- c(101)
} else {
  arg <- commandArgs()[-(1:3)]
}

if (exists("DEBUG")=="TRUE") {
  L <- 4674
  CORES <- 20
} else {
  L <- as.numeric(arg[1])
  CORES <- as.numeric(arg[2])
}
if(is.na(L)) L <- 146
if(is.na(CORES)) CORES <- 20

print(paste0("LOCATION ID: ",L," CORES: ",CORES))

try(dir.create(paste0("/share/epi/risk/paf/DBD/","activity")))
try(dir.create(paste0("/share/epi/risk/paf/DBD/","activity","/calc_out")))
try(dir.create(paste0("/share/epi/risk/paf/DBD/","activity","/calc_out/logs")))

source(paste0(j_drive,"/temp/central_comp/libraries/current/r/get_demographics.R"))
source(paste0(j_drive,"/temp/central_comp/libraries/current/r/get_demographics_template.R"))
comp_dem <- get_demographics(gbd_team="epi", gbd_round_id=4)
ar <- get_demographics(gbd_team="epi_ar", gbd_round_id=4)
`%notin%` <- function(x,y) !(x %in% y) 
ay <- ar$year_ids[ar$year_ids %notin% comp_dem$year_ids]

## read in weights
weight_list <- import(paste0(j_drive,"/WORK/05_risk/ensemble/weights/activity.csv"))
w <- weight_list %>% dplyr::select(-location_id,-year_id,-sex_id,-age_group_id) %>% dplyr::slice(1)
w <- data.frame(w)
weight_list <- w

## read in regression object
reg <- readRDS(paste0(j_drive,"/temp/",user,"/GBD_2016/calc_paf/parallel/mean_sd_reg_all_md_","activity",".rdata"))
# Draws from the FEs
draws_fe <- rmvnorm(n=1001, reg$coefficients, vcov(reg))

## draws with correlation across METs
rd <- import(paste0(j_drive,"/temp/",user,"/GBD_2016/activity/activity_RRs_BMJ_draws.csv"))
rd <- rd %>% mutate(N=1)
rd <- data.table(rd)
setkey(rd,cause_id)


TM <- runif(1001,min=3000,max=4500)

source(paste0(j_drive,"/temp/",user,"/GBD_2016/calc_paf/get_draws.R"))
act_draws <- get_draws(gbd_id_field = c('modelable_entity_id'), gbd_round_id = c('4'), gbd_id = c('10729'), source = c('risk'), location_ids = c(paste0(L)), status=c('best'), year_ids=c(comp_dem$year_ids))
act_ay <- get_draws(gbd_id_field = c('modelable_entity_id'), gbd_round_id = c('4'), gbd_id = c('10729'), source = c('risk'), location_ids = c(paste0(L)), status=c('best'), year_ids=c(ay))
act_draws <- bind_rows(act_draws,act_ay)
act_draws <- act_draws %>% dplyr::filter(model_version_id==max(model_version_id, na.rm = TRUE))
act_draws <- act_draws %>% dplyr::filter(age_group_id>=10)
act_draws <- act_draws %>% dplyr::select(location_id,year_id,age_group_id,sex_id,starts_with("draw_"))
act_draws <- act_draws[!duplicated(act_draws), ]

act_draws$ID <- seq.int(nrow(act_draws))
exp_long <- merged.stack(act_draws,var.stubs=c("draw_"),sep="var.stubs",keep.all=T)
setnames(exp_long,".time_1","draw")
exp_long <- sapply(exp_long, as.numeric)
exp_long <- data.table(exp_long)
exp_long <- exp_long[is.finite(draw_)]
exp_long <- exp_long[draw_>0]
setkey(exp_long, location_id, year_id, age_group_id, sex_id, ID, draw)

causes <- unique(rd$cause_id)
## define splines for each RR
for(c in causes) {
  for(i in 0:999) {
  # i = 200
  rd_l_df <- rd %>% dplyr::filter(cause_id==paste0(c)) 
  rr = rd_l_df[,paste0('rr_',i)]
  ## rr = sort(rr,decreasing = TRUE)
  rr = ifelse(rr>1,1,rr)
  EXP = log(rd_l_df$exposure)
  EXP[1] <- 0
    ## flatten
    rr_f = vector(mode="numeric", length=length(rr))
    rr_f[1] <- 1
        for(j in 2:length(rr_f)) {
          # j = 17
          rb = rr[[j-1]]
          rp = rr[[j]]
          rp = ifelse(rp<min(rr[2:(j-1)]),rp,min(rr[2:(j-1)]))
          rr_f[[j]] <- rp
        }
    
  ## flatten rr 
  rr_flat<-approxfun(EXP, rr_f, method="linear", rule=2)

  assign(paste0("rr_fun_", c, i), rr_flat)
  }
}

calcPAFbyrow <- function(jjj,density, DRAW, TMREL) {
  rr_fun <- get(paste0("rr_fun_", jjj, DRAW))
  denom <- integrate(function(x) density(x) * (1/rr_fun(x))^((TMREL-x + abs(TMREL-x))/2), log(20), log(50000), stop.on.error=FALSE)$value
  paf <- integrate(function(x) density(x) * (1/rr_fun(x)^((TMREL-x + abs(TMREL-x))/2) - 1)/denom, log(20), log(50000), stop.on.error=FALSE)$value
  paf = ifelse(paf<0,0,paf)
  paf = ifelse(paf>1,1,paf)
  return(list(paf=paf,cause_id=jjj))
}
calcPAFbyrow <- cmpfun(calcPAFbyrow)

source(paste0(j_drive,"/temp/",user,"/GBD_2016/activity/get_edensity_activity.R"))
source(paste0(j_drive,"/temp/",user,"/GBD_2016/calc_paf/parallel/ihmeDistList.R"))
dlist <- c(classA,classB,classM)

xx = seq(20,50000,length=1000)
xx = log(xx)

setDT(exp_long)
exp_long <- exp_long[complete.cases(exp_long),]
exp_long_cp <- exp_long

calcbyrow <- function(jj) {
  
  LL = as.numeric(exp_long[jj,"location_id"])
  YY = as.numeric(exp_long[jj,"year_id"])
  SS = as.numeric(exp_long[jj,"sex_id"])
  AA = as.numeric(exp_long[jj,"age_group_id"])
  Mval = as.numeric(exp_long[jj,"draw_"])
  ix = as.numeric(exp_long[jj,"draw"])
  ii = ix+1
  ID = as.numeric(exp_long[jj,"ID"])
  tmrel = log(TM[ii])
  
    Spred = exp((draws_fe[ii,"(Intercept)"][[1]] + draws_fe[ii,"log(mean)"][[1]]*log(log(Mval))))
    Mval = log(Mval)
    ## build density for that exposure
    den <- NULL
    den <- get_edensity_activity(weights = weight_list,mean=Mval,sd=Spred)
    den_l<-approxfun(x=xx, y=den$fx, yleft=0, yright=0) #density function
    PAFOUT <- rbindlist(lapply(causes, calcPAFbyrow, density=den_l, DRAW=ix, TMREL=tmrel),fill=TRUE)
    pafs <- data.table(location_id=LL,year_id=YY,sex_id=SS,age_group_id=AA,draw=ix,PAFOUT,log_METS=Mval,sd=Spred)
    return(pafs)
}

calcbyrow <- cmpfun(calcbyrow)

cores = floor(CORES*.6)
ST <- format(Sys.time(), "%H:%M:%S")
print(paste0("Computing PAFs: ",ST))
options(show.error.messages = FALSE)
R_P <- mclapply(X=(1:nrow(exp_long)), FUN=try(calcbyrow), mc.cores=cores)
options(show.error.messages = TRUE)
SE <- format(Sys.time(), "%H:%M:%S")
print(paste0("PAF calc complete: ",SE))

idx <- sapply(R_P, is.data.table)
R_P <- R_P[idx]
file <- rbindlist(R_P, fill=TRUE)
file <- file[complete.cases(file), ]

exp_long <- sapply(exp_long, as.numeric)
exp_long <- data.table(exp_long)
file <- sapply(file, as.numeric)
file <- data.table(file)

file_ex <- file %>% dplyr::select(location_id,year_id,sex_id,age_group_id,draw)
file_ex <- file_ex[!duplicated(file_ex), ]
file_ex <- sapply(file_ex, as.numeric)
file_ex <- data.table(file_ex)
failed <- exp_long[!file_ex, on=c("year_id", "sex_id", "age_group_id", "sex_id", "draw")]
NT <- nrow(failed)
if (NT>0) {
  print("Computing failures")
  exp_long <- failed
  options(show.error.messages = FALSE)
  R_P <- lapply(1:nrow(exp_long),try(calcbyrow))
  options(show.error.messages = TRUE)
  idx <- sapply(R_P, is.data.table)
  R_P <- R_P[idx]
  file_fail_out <- rbindlist(R_P, fill=TRUE)
  file_fail_out <- file_fail_out[complete.cases(file_fail_out), ]
  file <- rbind(file,file_fail_out)
}

exp_long <- sapply(exp_long_cp, as.numeric)
exp_long <- data.table(exp_long)
file <- sapply(file, as.numeric)
file <- data.table(file)
file_ex <- file %>% dplyr::select(location_id,year_id,sex_id,age_group_id,draw)
file_ex <- file_ex[!duplicated(file_ex), ]
file_ex <- sapply(file_ex, as.numeric)
file_ex <- data.table(file_ex)
failed <- exp_long[!file_ex, on=c("year_id", "sex_id", "age_group_id", "sex_id", "draw")]
NT <- nrow(failed)
if (NT>0) {
  try(dir.create(paste0("/share/epi/risk/paf/DBD/","activity","/calc_out/failed")))
  fwrite(failed,paste0("/share/epi/risk/paf/DBD/","activity","/calc_out/failed/",L,"_failed.csv"))
}

R = "activity"
write.csv(file,paste0("/share/epi/risk/paf/DBD/",R,"/calc_out/",L,".csv"))
file <- data.table(file)
file_pafs <- file %>% dplyr::select(location_id,year_id,sex_id,age_group_id,cause_id,paf,draw)
cols <- setdiff(names(file_pafs),c("paf","draw"))
file <- dcast(file_pafs,paste(paste(cols, collapse = " + "), "~ draw"),value.var=c("paf"),sep="")
names(file)[grep("[0-9]+", names(file))] <- paste("paf", names(file)[grep("[0-9]+", names(file))], sep="_")

for(s in unique(file$sex_id)) {
  for(y in unique(file$year_id)) {
    file_out <- file %>% dplyr::filter(sex_id==s & year_id==y)
    write.csv(file_out,paste0("/share/epi/risk/paf/DBD/",R,"/","paf_","yll","_",L,"_",y,"_",s,".csv"))
    write.csv(file_out,paste0("/share/epi/risk/paf/DBD/",R,"/","paf_","yld","_",L,"_",y,"_",s,".csv"))
  }   
}

print(paste0("PAF CALC TIME: ", ST, " END TIME: ", SE))

try(closeAllConnections())





