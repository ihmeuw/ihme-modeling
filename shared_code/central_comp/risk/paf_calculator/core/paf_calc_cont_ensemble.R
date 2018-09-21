## Computes attributable fraction provided a set of weights across a number of distributions

rm(list = ls())
set.seed(12345)
trailargs <- commandArgs(trailingOnly=TRUE)
rr_scalar <- as.numeric(trailargs[1])
W <- (trailargs[2])
F <- (trailargs[3])
inv_exp <- as.numeric(trailargs[4])
code_dir <- (trailargs[5])

## load PAF function
Rcpp::sourceCpp(paste0(code_dir,"/core/paf_calc_cont_ensemble.cpp"))

## CALC PAF ----------------------------------------------------------------------------------------

library(data.table);library(compiler);library(parallel)
library(fitdistrplus);library(splitstackshape)

# read draws and weights
file <- fread(paste0(F,".csv"))
wlist <- fread(paste0(W,".csv"))

# long by draw
file <- merged.stack(file,var.stubs=c("exp_mean_","exp_sd_","rr_","tmred_mean_"),sep="var.stubs",keep.all=T)
setnames(file,".time_1","draw")

## fit functions
## dlist is the universe of distribution families as defined in ihmeDistList.R
## classA is the majority of families, classB is scaled beta, classM is the mirror family of distributions
source(paste0(code_dir,"/core/get_edensity.R"))
rei_id <- unique(file$rei_id)

gen_paf <- function(i) {
  ## Build density given w=data.frame of weights, M = Mean, S=SD
  age <- file[i,]$age_group_id
  w <- wlist[age_group_id==age,][,age_group_id:=NULL][1,]
  dens <- get_edensity(w,file[i,]$exp_mean_,file[i,]$exp_sd_,scale=T)
  # calc paf
  calc_paf_c(dens$x,
                 dens$fx,
                 file[i,]$tmred_mean_,
                 file[i,]$rr_,
                 rr_scalar,
                 inv_exp,
                 file[i,]$cap)$paf
}
paf <- mclapply(1:nrow(file),gen_paf,mc.cores = 10)
file <- cbind(file,paf_=unlist(paf))

# reshape back and save
cols <- setdiff(names(file),c("exp_mean_","exp_sd_","rr_","tmred_mean_","paf_","draw"))
file <- dcast(file,paste(paste(cols, collapse = " + "), "~ draw"),
              value.var=c("exp_mean_","exp_sd_","rr_","tmred_mean_","paf_"),sep="")
write.csv(file, paste0(F,"_OUT.csv"), row.names=T,na="")

## END_OF_R