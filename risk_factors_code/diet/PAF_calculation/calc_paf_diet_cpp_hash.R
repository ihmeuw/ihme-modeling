rm(list=ls())
if (Sys.info()["sysname"] == "Darwin") j_drive <- "/Volumes/snfs"
if (Sys.info()["sysname"] == "Linux") j_drive <- "/home/j"
user <- Sys.info()[["user"]]

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

setCompilerOptions(suppressAll = TRUE)
enableJIT(3)

source(paste0(j_drive,"/temp//",user,"//GBD_2016/calc_paf/get_draws.R"))
source(paste0(j_drive,"/temp/central_comp/libraries/current/r/get_demographics.R"))
source(paste0(j_drive,"/temp/central_comp/libraries/current/r/get_demographics_template.R"))
comp_dem <- get_demographics(gbd_team="epi", gbd_round_id=4)
comp_dem$year_ids <- c(comp_dem$year_ids, 2006)
ar <- get_demographics(gbd_team="epi_ar", gbd_round_id=4)
`%notin%` <- function(x,y) !(x %in% y) 
ay <- ar$year_ids[ar$year_ids %notin% comp_dem$year_ids]

Rcpp::sourceCpp(paste0(j_drive,"/temp//",user,"//GBD_2016/calc_paf/paf_c_plus.cpp"))

if (Sys.info()["sysname"] == "Darwin") {
  arg <- c(101)
} else {
  arg <- commandArgs()[-(1:3)]
}

if (exists("DEBUG")=="TRUE") {
  REI_ID <- 116
  L <- 43942
  CORES <- 20
} else {
  REI_ID <- as.numeric(arg[1])
  L <- as.numeric(arg[2])
  CORES <- as.numeric(arg[3])
}
if(is.na(REI_ID)) REI_ID <- 116
if(is.na(L)) L <- 101
if(is.na(CORES)) CORES <- 20

sessionInfo()

print(paste0("REI ID: ",REI_ID," LOCATION ID: ",L," CORES: ",CORES))

risk_t <- fread(paste0(j_drive,"/temp//",user,"//GBD_2016/calc_paf/shared_rei_table.csv"))
risk_map <- risk_t %>% dplyr::filter(rei_id==paste0(REI_ID))
R <- paste0(risk_map$risk[1])
if(R=="diet_fish") R<-"diet_omega_3"

try(dir.create(paste0("/share/epi/risk/paf/DBD/",R)))
try(dir.create(paste0("/share/epi/risk/paf/DBD/",R,"/calc_out")))
try(dir.create(paste0("/share/epi/risk/paf/DBD/",R,"/calc_out/logs")))

exp_draws <- get_draws(gbd_id_field = c('rei_id'), gbd_round_id = c('4'), draw_type = c('exposure'), gbd_id = c(paste0(REI_ID)), source = c('risk'), location_ids = c(paste0(L)), status=c('best'), year_ids=c(comp_dem$year_ids))
exp_ay <- get_draws(gbd_id_field = c('rei_id'), gbd_round_id = c('4'), draw_type = c('exposure'), gbd_id = c(paste0(REI_ID)), source = c('risk'), location_ids = c(paste0(L)), year_ids=c(ay), status=c('best'))
exp_draws <- bind_rows(exp_draws,exp_ay)
exp_draws <- exp_draws %>% dplyr::filter(model_version_id==max(exp_draws$model_version_id, na.rm = TRUE))
exp_draws <- exp_draws %>% dplyr::select(location_id,year_id,age_group_id,sex_id,starts_with("draw_"))
exp_draws <- exp_draws[!duplicated(exp_draws), ]

## read in regression object
reg <- readRDS(paste0(j_drive,"/temp//",user,"//GBD_2016/calc_paf/parallel/mean_sd_reg_",R,".rdata"))
# Draws from the FEs
draws_fe <- rmvnorm(n=1001, reg$coefficients, vcov(reg))

## SD correction
sd_correction <- fread(paste0(j_drive,"/WORK/05_risk/risks/diet_general/data/intake_sd_correction_wknd.csv"))
sd_correction <- sd_correction %>% mutate(risk=ifelse(risk=="diet_calcium","diet_calcium_low",risk))
sd_correction <- sd_correction[sd_correction$risk==paste0(risk_map$risk[1]),"ratio_one_day"]

## read in meta-data
md <- fread(paste0(j_drive,"/temp//",user,"//GBD_2016/calc_paf/risk_variables.csv"))
md <- md[md$risk==paste0(risk_map$risk[1]),]
RR_SCALAR = (as.numeric(md[,"rr_scalar"][[1]]))
INV = (as.numeric(md[,"inv_exp"][[1]]))
set.seed(11180*REI_ID)
TM <- runif(1000,min=md[,"tmred_para1"][[1]],max=md[,"tmred_para2"][[1]])

## read in weights - for diet they are universal
w <- fread(paste0(j_drive,"/WORK/05_risk/ensemble/weights/",risk_map$risk[1],".csv"))
w <- w %>% dplyr::slice(1) %>% dplyr::select(-location_id,-year_id,-sex_id,-age_group_id)

## pull relative risks
rr_draws <- get_draws(gbd_id_field = c('rei_id'), gbd_round_id = c('4'), draw_type = c('rr'), gbd_id = c(paste0(REI_ID)), source = c('risk'), location_ids = c(paste0(L)))
rr_draws <- rr_draws %>% dplyr::filter(model_version_id==max(rr_draws$model_version_id, na.rm = TRUE))
rr_draws <- rr_draws %>% dplyr::select(cause_id,sex_id,age_group_id,mortality,morbidity,starts_with("rr_"))
rr_draws <- rr_draws[!duplicated(rr_draws), ]

exp_draws <- exp_draws[exp_draws$age_group_id %in% rr_draws$age_group_id ,]
exp_mean <- exp_draws %>% dplyr::select(starts_with("draw_")) %>% dplyr::mutate(exp_mean = rowMeans(.,na.rm=T)) %>% dplyr::select(exp_mean)
exp_draws <- bind_cols(exp_draws,exp_mean)

exp_draws$ID <- seq.int(nrow(exp_draws))
exp_long <- merged.stack(exp_draws,var.stubs=c("draw_"),sep="var.stubs",keep.all=T)
setnames(exp_long,".time_1","draw")
exp_long <- sapply(exp_long, as.numeric)
exp_long <- data.table(exp_long)
exp_long <- exp_long[draw_>0]
setkey(exp_long, location_id, year_id, age_group_id, sex_id, ID, draw)

## create hashmap of RRs
rr_long <- merged.stack(rr_draws,var.stubs=c("rr_"),sep="var.stubs",keep.all=T)
setnames(rr_long,".time_1","draw")
RRARRAY <- as.array(rr_long$rr_)
rr_long$cause_id = paste(rr_long$cause_id,rr_long$mortality,rr_long$morbidity,sep='_') ## create cause id from mortality and morbidity groups
NAMES <- as.array(paste(rr_long$sex_id,rr_long$age_group_id,rr_long$draw,rr_long$cause_id,sep='|'))
RRHASH <- hashmap(NAMES,RRARRAY)
CAUSES = unique(rr_long$cause_id)

cores = floor(CORES*.6)
source(paste0(j_drive,"/temp//",user,"//GBD_2016/calc_paf/parallel/get_edensity.R"))
source(paste0(j_drive,"/temp//",user,"//GBD_2016/calc_paf/parallel/ihmeDistList.R"))
dlist <- c(classA,classB,classM)

exp_long <- exp_long[complete.cases(exp_long),]
exp_long_cp <- exp_long

calcPAFbyrow <- function(jjj,xx,yy,tm,rs,inv,cp,sex,age,iixx) {
  rrval <- RRHASH$find(paste(sex,age,iixx,jjj,sep='|'))
  paf <- as.double((calc_paf_c(x=xx,fx=yy,tmrel=tm,rr=rrval,rr_scalar=rs,inv_exp=inv,cap=cp)$paf))
  paf = ifelse(paf<0,0,paf)
  return(list(paf=paf,cause_id=jjj))
}
calcPAFbyrow <- cmpfun(calcPAFbyrow)

calcbyrow <- function(jj) {
  tryCatch({
    LL = as.numeric(exp_long[jj,"location_id"])
    YY = as.numeric(exp_long[jj,"year_id"])
    SS = as.numeric(exp_long[jj,"sex_id"])
    AA = as.numeric(exp_long[jj,"age_group_id"])
    Mval = as.numeric(exp_long[jj,"draw_"])
    Mmean = as.numeric(exp_long[jj,"exp_mean"])
    ix = as.numeric(exp_long[jj,"draw"])
    ii = ix+1
    ID = as.numeric(exp_long[jj,"ID"])
    TMREL = TM[[ii]]
    Spred = exp((draws_fe[ii,"(Intercept)"][[1]] + draws_fe[ii,"log(mean)"][[1]]*log(Mmean))) * sd_correction

    if (R=="diet_omega_3") Mval<-Mval*1000
    if (R=="diet_omega_3") Spred<-Spred*1000
    
    D <- NULL
    D <- (get_edensity(weights=w,mean=Mval,sd=Spred))
    base <- (D$x)
    denn <- (D$fx)
    
    if(INV==1) {
      (cap <- (D$XMIN))
    } else if(INV==0) {
      (cap <- (D$XMAX))
    }

    PAFOUT <- rbindlist(lapply(unique(CAUSES), calcPAFbyrow, xx=base, yy=denn, tm=TMREL, rs=RR_SCALAR, inv= INV, cp=cap, sex=SS, age=AA, iixx=ix),fill=TRUE)
    pafs = data.table(location_id=LL,year_id=YY,sex_id=SS,age_group_id=AA,draw=ix,PAFOUT,mean=Mval,sd=Spred)
    return(pafs)
    
  }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
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
file <- cSplit(file, c("cause_id"), c("_"))
names(file)[names(file) == "cause_id_1"] = "cause_id"
names(file)[names(file) == "cause_id_2"] = "mortality"
names(file)[names(file) == "cause_id_3"] = "morbidity"

exp_long <- sapply(exp_long, as.numeric)
exp_long <- data.table(exp_long)
file <- sapply(file, as.numeric)
file <- data.table(file)
file <- file[paf>=0 & paf<1]
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
  file_fail_out <- cSplit(file_fail_out, c("cause_id"), c("_"))
  names(file_fail_out)[names(file_fail_out) == "cause_id_1"] = "cause_id"
  names(file_fail_out)[names(file_fail_out) == "cause_id_2"] = "mortality"
  names(file_fail_out)[names(file_fail_out) == "cause_id_3"] = "morbidity"
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
  try(dir.create(paste0("/share/epi/risk/paf/DBD/",R,"/calc_out/failed")))
  write.csv(failed,paste0("/share/epi/risk/paf/DBD/",R,"/calc_out/failed/",L,"_failed.csv"))
}

write.csv(file,paste0("/share/epi/risk/paf/DBD/",R,"/calc_out/",L,".csv"))
file <- data.table(file)
file_pafs <- file %>% dplyr::select(location_id,year_id,sex_id,age_group_id,cause_id,paf,draw,mortality,morbidity)
cols <- setdiff(names(file_pafs),c("paf","draw"))
file <- dcast(file_pafs,paste(paste(cols, collapse = " + "), "~ draw"),value.var=c("paf"),sep="")
names(file)[grep("[0-9]+", names(file))] <- paste("paf", names(file)[grep("[0-9]+", names(file))], sep="_")

setkey(file, year_id, sex_id)
cy = comp_dem$year_ids

for(s in unique(file$sex_id)) {
  for(y in cy) {
    ds <- file[year_id==y & sex_id==s,]
    file_out <- ds[sex_id==s & mortality==1]
    write.csv(file_out,paste0("/share/epi/risk/paf/DBD/",R,"/","paf_","yll","_",L,"_",y,"_",s,".csv"))
    file_out <- ds[sex_id==s & morbidity==1]
    write.csv(file_out,paste0("/share/epi/risk/paf/DBD/",R,"/","paf_","yld","_",L,"_",y,"_",s,".csv"))
  }
}

print(paste0("PAF CALC TIME: ", ST, " END TIME: ", SE))

try(closeAllConnections())
