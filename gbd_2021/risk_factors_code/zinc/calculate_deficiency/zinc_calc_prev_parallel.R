##################################################################################
# Calculate prevalence of zinc deficiency from ensemble distribution weights and cutoff
#
## Written by: 
## 
#############################################################################

rm(list = ls())
os <- Sys.info()[1]
user <- Sys.info()[7]

j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"
work_dir <- paste0(j, 'FILEPATH')
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""


library(data.table)
library(dplyr)
library(rio)
library(lme4)
library(mvtnorm)
library(actuar)
library(zipfR)
library(Rcpp)

#functions 
source(paste0(code_dir, 'FILEPATH/primer.R'))
source("FILEPATH/edensity.R")
source("FILEPATH/pdf_families.R")
sourceCpp("FILEPATH/scale_density_simpson.cpp")         
source("FILEPATH/get_draws.R")

#bring in arguments from wrapper call
args<-commandArgs(trailingOnly = TRUE)
map_path <-args[1]
save_dir <-args[2]
run_id <- args[3]
params <- fread(map_path)
task_id <- Sys.getenv("SGE_TASK_ID")
location_id <- params[task_num == task_id, location] 
save_dir <- paste0(save_dir, run_id)
save_dir2 <- paste0(save_dir, "_age5/")


if(!dir.exists(save_dir)){
  dir.create(save_dir)}
if(!dir.exists(save_dir2)){
  dir.create(save_dir2)}


message( paste0("This is the log file for location id: ", location_id, "and zinc run_id: ", run_id))


#import weights, cutoff points, microdata and stgpr output!
blist <- fread("FILEPATH/zinc_weights.csv")
f <- fread("FILEPATH/compiled_zinc_microdata_2.csv")
co <- fread("FILEPATH/decomp1_cutpoints.csv")
df <- fread(paste0("FILEPATH", run_id, "/draws_temp_0/", location_id,".csv"))

#make some edits to imported objects
co$age_group_id = 22
co[year_id==2016, year_id := 2015]
co_new <- co[year_id==2015]
co_new$year_id <- 2020
co <- rbind(co,co_new)
estimation_years <- c(1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020,2021 ,2022)
df <- df[year_id  %in% estimation_years]

setnames(df, "draw_0", "draw_1000")

## variance regression: convert to g
set.seed(52506)
f$zinc <- f$zinc/1000
sd <- f %>% dplyr::group_by(year_start,ihme_loc_id) %>% dplyr::summarise(sd=sd(zinc))
mean <- f %>% dplyr::group_by(year_start,ihme_loc_id) %>% dplyr::summarise(mean=mean(zinc))
freg <- left_join(mean,sd)

freg <- freg %>% dplyr::mutate(outlier=ifelse(((sd / mean > 2) | (sd / mean < .1)),1,0))
freg <- freg %>% dplyr::filter(outlier==0)
o1 <- lm(data = freg, log(sd)~log(mean))

if( !file.exists("FILEPATH/variance_reg_plot.pdf")){
  pdf("FILEPATH/variance_reg_plot.pdf", height=8.5, width=11)
  
  gplot <- ggplot(data=freg, aes(x=log(mean), y=log(sd)))+
    geom_point(aes(color=factor(ihme_loc_id)))+theme_bw()+
    geom_smooth(method=lm)
  print(gplot)

  dev.off()
}

# Draws from the FEs
N <- 1001
draws_fe <- rmvnorm(n=N, o1$coefficients, vcov(o1))

dlist <- c(classA,classB,classM)
options(warn=-1)
for (i in 1:1000) {
  message(i)
  df[,paste0('prev_',i)]=NA
  for(j in 1:nrow(df))
    tryCatch({
  
      Mval <- df[j,get(paste0("draw_",i))]
      Spred = exp((draws_fe[i,"(Intercept)"] + draws_fe[i,"log(mean)"]*log(Mval)))[[1]]   #multiply draw of beta from mean-sd regression by draw of mean to get sd
     
      
      D <- NULL
      D <- get_edensity(weights=blist,mean=Mval,sd=Spred,scale=TRUE)  
      print(plot(D$x, D$fx, main = paste0("location ", location_id," ", i, "/",j)))
      den = approxfun(D$x, D$fx, yleft=0, yright=0)
      TOTAL_INTEG = integrate(den,D$XMIN,D$XMAX)$value
      
      ## get loc, age, sex, year
      L = df[j,'location_id']$location_id
      A = df[j,'age_group_id']$age_group_id 
      S = df[j,'sex_id']$sex_id
      Y = round_any(df[j,'year_id']$year_id, 5)

      cut <- co %>% dplyr::filter(location_id==get("L") & age_group_id==get("A") & sex_id==get("S") & year_id==get("Y")) %>% dplyr::select(gbd_pzr)
      cut <- cut[[1]]
      print(abline(v=cut, col='red'))
      
      PROP = integrate(den,0,cut)$value/TOTAL_INTEG
      PROP = ifelse(is.na(PROP),0,PROP)
      
      df[j,paste0('draw_',i)]=PROP
      print(text(x=0.003, y=200, labels=paste0( round(PROP, 5)), col="red"))
      
      print(paste0("DRAW:",i," ROW = ",j,"/",nrow(df)))
      
    }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
}
#dev.off()
options(warn=0)

df_out <- df %>% dplyr::select(location_id,age_group_id,sex_id,year_id,starts_with("draw_"))
setnames(df_out, "draw_1000", "draw_0")
df_out <- df_out %>% mutate(measure_id=18)
df_out$age_group_id <- 5
message("Hooray! All draws are complete!")
write.csv(df_out,paste0(save_dir2,"/",location_id,".csv"), row.names = FALSE)

### fix for new age group ids
copy_df <- copy(df_out)
copy_df[, age_group_id:=238]
df_out[, age_group_id:=34]
df_out <- rbind(copy_df, df_out)
message("Hooray! New age groups are complete!")
write.csv(df_out,paste0(save_dir,"/",location_id,".csv"), row.names = FALSE)

