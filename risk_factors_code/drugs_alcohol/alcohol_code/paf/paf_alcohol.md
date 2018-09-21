
# Calculate Alcohol PAF


**Summary**  

This code calculates the PAF for alcohol use, for each population under interest.

For GBD 2016, this takes in exposures of alcohol consumption and calculates the population attributable fraction for each cause, using a dose-response curve and an individual-level distribution. See the calculation section below for a definition of the mathematical functions being used. 

Each cell of code should be run sequentially. All options are determined in the initial setup. If changes are needed for underlying functions, they can be changed in the appendix. If these cells are run, they will overwrite the existing scripts being called in the main sections of code.

**Brief overview of the code**

1. Setup: environment with libraries, directories, and inital settings
2. Submission: Submit jobs to calculate PAF.
3. Calculation: For each population under interest, calculate paf.

4. Appendix: Holds text of supplemental scripts called in calculation. Re-running these cells overwrites existing scripts in code directory. Contains:

    * 4a. Functions
    * 4b. Shells  

```python
#Get R working in jupyter. Necessary if one wishes to use magic functions. 

%load_ext rpy2.ipython
```

# 1. Setup


```python
%%R

# Clear environment and source libraries.
rm(list=ls())

library(data.table)  #For easy slicing/dicing of dataframes
library(plyr)        #For fast and clear joins

#Source central functions in GBD
setwd("")
source("get_demographics_template.R")
source("get_location_metadata.R")
source("get_draws.R")

#Set up directories and code settings for paf calculation.

version <- 16
debug   <- FALSE

directory <- ""

code_directory     <- paste0(directory, "code/")
exposure_directory <- paste0(directory, "exp/exposure_2016/13/")
paf_directory      <- paste0(directory, "paf/paf_2016/", version, "/")
rr_directory       <- paste0(directory, "rr/rr_2016/5")
tmrel_directory    <- paste0(directory, "tmrel/tmrel_2016/2")

dir.create(paste0(paf_directory, "epi_upload/"), showWarnings = FALSE, recursive = TRUE)

#Grab template for outputs.

template <- get_demographics_template()

#Set up settings for cluster submission

project         <- " -P proj_custom_models "
shell           <- paste0(code_directory, "r_shell.sh")
errors          <- ""
cluster_cores   <- 2

#Set up arguments to pass to calculation

sexes       <- c(1,2)
ages        <- c(8:20, 30:32, 235)
years       <- c(1990, 1995, 2000, 2005, 2006, 2010, 2016)
locations   <- c(48, 52, 53)
#get_location_metadata(location_set = 35)[, location_id]
causes      <- c(297,322,411,420,423,429,441,444,447,450,493,495,496,498,500,524,535,545,587,688,696,718,724)

draws       <- 999     # Accepts values from 0-999
```

# 2. Submission


```python
%%R

#Build folders if they don't exist

#Parallelize jobs by location & cause

for (location in locations){
    for (cause in causes){
        for (sex in sexes){
            
            #Build qsub
            name <- paste0("alcohol_paf_",location,"_",sex, "_", cause)

            script <- paste0(code_directory,"paf_alcohol_calculation.R")

            arguments <- paste(debug, 
                               version, 
                               code_directory,
                               exposure_directory,
                               paf_directory,
                               rr_directory,
                               tmrel_directory,
                               location,
                               paste(years, collapse=","), 
                               sex,
                               paste(ages, collapse=","),
                               paste(cause),
                               draws)

            qsub <- paste0("qsub -N ", name, 
                           project,
                           " -pe multi_slot ", cluster_cores,
                           " -l mem_free=", 2*cluster_cores,
                           " -e ", errors, " -o ", errors)

            #Run qsub
            if (debug == FALSE){
                system(paste(qsub, shell, script, arguments))
            } else{
                system(paste(qsub, shell, script, arguments))
            }
        }
    }
}
```

# 3. Calculation

rm(list=ls())
arg <- commandArgs()[-(1:3)]

#Source packages
library(plyr)
library(data.table)
source("")

#Read in arguments

debug  <- arg[1]

if (debug == "FALSE"){
    version             <- as.numeric(arg[2])
    code_directory      <- arg[3]
    exposure_directory  <- arg[4]
    paf_directory       <- arg[5]
    rr_directory        <- arg[6]
    tmrel_directory     <- arg[7]

    location            <- as.numeric(arg[8])
    years               <- as.numeric(unlist(strsplit(arg[9][[1]], ",")))
    sex                 <- as.numeric(unlist(strsplit(arg[10][[1]], ",")))
    ages                <- as.numeric(unlist(strsplit(arg[11][[1]], ",")))
    cause               <- as.numeric(arg[12])
    draws               <- arg[13]
    
} else{
    
    location            <- 66
    years               <- c(1990, 1995, 2000, 2005, 2010, 2016)
    sex                 <- c(1)
    ages                <- c(7:21, 31:33, 235)
    cause               <- 587
    draws               <- 999
}

cat(format(Sys.time(), "%a %b %d %X %Y"))

#Read in exposures & change draw name to integer
exposure <- fread(paste0(exposure_directory, "alc_exp_", location, ".csv"))
exposure[, draw := as.numeric(gsub("draw_", "", exposure$draw))]

#Temporary - calculate drink_gday sd based on rate of mean drink_gday to sd of drink_day
exposure[, sd_rate := mean(.SD$drink_gday)/sd(.SD$drink_gday), by=c("location_id", "sex_id", "year_id", "age_group_id")]
exposure[, drink_gday_se := drink_gday/sd_rate]

#Only hold onto the categories we're interested in
exposure <- exposure[(age_group_id %in% ages) & (year_id %in% years) & (sex_id == sex) & (draw <= draws),]  

#For each cause, calculate paf and save

#For liver cancer & cirrhosis, set PAF = 1 and exit
if (cause %in% c(420, 524)){
    
    exposure[, paf := 1]
    exposure[, tmrel := 1]
    exposure[, attribute := 0]
    exposure[, cause_id := cause]
    
    write.csv(exposure, paste0(paf_directory, location,  "_", cause, "_", sex, ".csv"), row.names=F)

    cat(format(Sys.time(), "%a %b %d %X %Y"))
    cat("Finished!")
    quit()
}

#Use sex_specific RR if IHD, Stroke, or Diabetes. Otherwise, use both-sex RR
if (cause %in% c(493, 495, 496, 587)){
    relative_risk <- fread(paste0(rr_directory, "/rr_", cause, "_", sex, ".csv"))
} else{
    relative_risk <- fread(paste0(rr_directory, "/rr_", cause, ".csv"))
}

#Sex-specific TMREL
#tmrel <- fread(paste0(tmrel_directory, "/tmrel_", sex, ".csv"))

#For each location, sex, year, age, & draw, calculate attributable risk. Pass to function only selected columns of
#the subset dataframe.

exposure[, attribute := attributable_risk(.BY$draw, .SD, relative_risk), 
       by=c("location_id", "sex_id", "year_id", "age_group_id", "draw"), 
        .SDcols = c("draw", "current_drinkers","drink_gday", "drink_gday_se")]

#exposure[, tmrel := construct_tmrel(.BY$draw, tmrel, relative_risk),
#        by=c("location_id", "sex_id", "year_id", "age_group_id", "draw")]

exposure[, tmrel := 1]

#Using the calculated attributable risk, calculate PAF
exposure[, paf := (abstainers+former+attribute-tmrel)/(abstainers+former+attribute)]
exposure[, cause_id := cause]

#For MVA, adjust for fatal harm caused to others
if (cause == 688){
    exposure <- mva_adjust(exposure)
}

write.csv(exposure, paste0(paf_directory, location,  "_", cause, "_", sex, ".csv"), row.names=F)

cat(format(Sys.time(), "%a %b %d %X %Y"))
cat("Finished!")

```

# 4. Appendix

### 4a. Functions


library(data.table)

attributable_risk <- function(d, exposure, relative_risk, l=0, u=150){

    #Read in exposure dataframe, relative risk curves, and set lower and upper bounds for integral.
    #Exposure dataframe requires a column identifying mean population "dose" (in this case, g/day), the standard error of that
    #measure, as well as the percentage of the population exposure (in this case, current_drinkers).
    
    #Peg relative risk draw to draw of exposure passed.
    rr <- relative_risk[draw == d,]
    
    #Calculate individual distribution, multiply by relative risk curve. Integrate over this plane to get total attributable
    #risk within the population
    
    attribute <- function(dose){
            individual_distribution(dose, exposure$drink_gday, exposure$drink_gday_se) * construct_rr(dose, rr)
        }
    
    #Multiply risk by percentage of current drinkers
    attribute <- exposure$current_drinkers * integrate(attribute, lower=l, upper=u)$value
    
    #Integral isn't working for small amounts of drinking (<1 g/day). Assume the risk is 1.
    if (exposure$drink_gday < 1){
        attribute <- exposure$current_drinkers
    }
    
    return(attribute)
}

construct_rr <- function(dose, relative_risk){
    
    #Returns a complete set of relative risk exposures for a given draw. 
    
    #From spline points estimated, interpolate between to construct curve at non-integers.
    rr <- approx(relative_risk$exposure, relative_risk$rr, xout=dose)$y
    
    return(rr)
}

construct_tmrel <- function(d, tmrel, relative_risk){
    
    t <- tmrel[draw == d,]
    rr <- relative_risk[draw == d,]
    
    t <- construct_rr(t$tmrel, rr)
    return(t)
    
}

individual_distribution <- function(dose, exposure_mean, exposure_se, distribution="gamma", l=0, u=150){
    
    #Gamma is the dUSERt, can calculate custom distribution from weights if desired.
    if (distribution == "gamma"){
        
        alpha <- exposure_mean^2/((exposure_se^2))
        beta <- exposure_se^2/exposure_mean
        
        #Since Gamma is exponential, we need a normalizing constant to scale total area under the curve = 1. If mean is small,
        #just set normalizing constant = 1
        
        normalizing_constant <- 1
        
        if (exposure_mean >= 1){
            normalizing_constant <- integrate(dgamma, shape=alpha, scale=beta, lower=l, upper=u)$value
        }
            
        result <- (dgamma(dose, shape = alpha, scale = beta))/normalizing_constant
        
        return(result)
    }
    
    #To be added later
    if (distribution == "custom"){
        return(NULL)
    }
}

mva_adjust <- function(df){
    
    source("")
    library(zoo)

    df <- df[, c("location_id", "year_id", "sex_id", "age_group_id", "cause_id", "draw", "paf")]

    #Add empty rows for missing age groups (Below age 15)
    new_ages <- expand.grid(location_id = location,
                            year_id = unique(df$year_id), 
                            sex_id = unique(df$sex_id),
                            draw   = unique(df$draw),
                            age_group_id = c(5, 6, 7),
                            cause_id = 688,
                            paf = 0)

    df <- rbind(df, new_ages)

    #Read in avg fatalities by age and percent victims by age

    fatalities <- fread("")
    victims    <- fread("")

    #Get DALY draws for MVA and format to match paf dataframe

    mva_dalys <- get_draws('cause_id', 
                            688, 
                            'dalynator', 
                            location_id = location,
                            metric_id = 1,
                            measure_id = 2,
                            status = 'best')

    mva_dalys <- data.table(mva_dalys)
    mva_dalys <- mva_dalys[, c("location_id", "year_id", "sex_id", "age_group_id", "cause_id", c(paste0("draw_", 0:999)))]
    mva_dalys <- melt(mva_dalys, id.vars = c("location_id", "year_id", "sex_id", "age_group_id", "cause_id"), 
                    value.name = "dalys", 
                    variable.name ='draw')
    mva_dalys <- mva_dalys[, draw := as.integer(gsub("draw_", "", draw))]

    #Make merged dataframe of all the objects we need
    df <- join(df, mva_dalys, by=c("location_id", "year_id", "sex_id", "age_group_id", "cause_id", "draw"), type = 'left')
    
    df <- df[, dalys := na.locf(dalys), by=c("location_id", "sex_id", "age_group_id", "cause_id", "draw")]

    df <- join(df, fatalities, by=c("age_group_id", "sex_id"))
    df[is.na(avg_fatalities), avg_fatalities := 1]

    #Multiply PAF by average fatalities
    df[, attributable_dalys := paf*dalys]
    df[, excess_dalys := attributable_dalys*(avg_fatalities)]
    
    #Reapportion fatalities to average victims by drunk driver's age/sex
    reapportion <- df[, .(year_id, sex_id, age_group_id, draw, excess_dalys)]
    reapportion <- data.table(join(reapportion, victims, by=c("sex_id", "age_group_id"), type="full"))

    reapportion[, new_dalys := excess_dalys * pct_deaths]
    reapportion[, new_dalys := sum(.SD$new_dalys), by = c("victim_sex_id", "victim_age_group_id", "year_id", "draw")]
    reapportion <- unique(reapportion[, .(year_id, victim_sex_id, victim_age_group_id, draw, new_dalys)])
    setnames(reapportion, c("victim_sex_id", "victim_age_group_id"), c("sex_id", "age_group_id"))

    #Set as new PAF
    df <- join(df, reapportion, by = c("year_id", "sex_id", "age_group_id", "draw"), type="left")
    df[, new_paf := new_dalys/dalys]
    df[, paf := new_paf]
    
    return(df)
}

```

### 4b. Shells


## Upload to database


%%R

#Compile results to upload to the database
for (location in locations){
        
        #Build qsub
        name <- paste0("format_alcohol_paf_",location)
    
        script <- paste0(code_directory,"format_results.R")
    
        arguments <- paste(paf_directory,
                           location)
    
        qsub <- paste0("qsub -N ", name, 
                       project,
                       " -pe multi_slot ", cluster_cores,
                       " -l mem_free=", 2*cluster_cores,
                       " -e ", errors, " -o ", errors)
    
        system(paste(qsub, shell, script, arguments))
}

```

rm(list=ls())
arg <- commandArgs()[-(1:3)]

#Source packages
library(plyr)
library(data.table)

#Read arguments and make necessary variables

paf_directory <- arg[1]
location      <- arg[2]
years         <- c(1990, 1995, 2000, 2005, 2006, 2010, 2016)
sexes         <- c(1, 2)

setwd(paf_directory)
files <- list.files(paf_directory)
files <- files[grep(paste0("^", location, "_"), files)]

#Read in all causes & sexes, collapse into single dataframe, then add in injury sequela.
#Then reshape wide, and save in the particular format required for save_results

compiler <- function(f){
    
    df <- fread(f)
    df <- df[, .(location_id, year_id, sex_id, age_group_id, cause_id, draw, paf)]
    return(df)
}

compiled <- data.table(rbindlist(lapply(files, compiler)))

unintentional <- compiled[cause_id==696, ] 
intentional   <- compiled[cause_id==724, ]
mva           <- compiled[cause_id==688, ]
self_harm     <- compiled[cause_id==718, ]

#For MVA, ages 80+, set PAF = 75-79 age group.
hold <- mva[age_group_id == 30,]
hold[, age_group_id := NULL]
setnames(hold, "paf", "new_paf")

mva <- join(mva, hold, by=c("location_id", "year_id", "sex_id", "cause_id", "draw"))
mva[age_group_id %in% c(31, 32, 235), paf := new_paf]
mva[, new_paf := NULL]

mva[paf > 0.9, paf := 0.9]

#Map parent injury causes to child injury causes
mva_map           <- 689:695
unintentional_map <- c(698, 699, 700, 704, 705, 716)
intentional_map   <- 724:727
self_harm_map     <- 719:723

child_cause <- function(c, df){
    df$cause_id <- c
    return(df)
}

mva           <- rbindlist(lapply(mva_map, child_cause, df=mva))
intentional   <- rbindlist(lapply(intentional_map, child_cause, df=intentional))
unintentional <- rbindlist(lapply(unintentional_map, child_cause, df=unintentional))
self_harm     <- rbindlist(lapply(self_harm_map, child_cause, df=self_harm))

children <- rbind(mva, intentional, unintentional, self_harm)

#Write each cause in save_results format
compiled <- compiled[(!cause_id %in% c(688, 696, 718, 724))]
compiled <- rbind(compiled, children)

compiled[, draw := paste0("paf_", draw)]
compiled <- dcast(compiled, location_id + year_id + sex_id + age_group_id + cause_id ~ draw, value.var='paf')

for (sex in sexes){
    for (year in years){
        
        df <- compiled[(sex_id == sex & year_id == year)]
        df <- df[, names(df)[!names(df) %in% c("location_id", "sex_id")], with=FALSE]
        
        write.csv(df, paste0(paf_directory, "epi_upload/paf_yll_", location, "_", year, "_", sex, ".csv"), row.names = F)
        write.csv(df, paste0(paf_directory, "epi_upload/paf_yld_", location, "_", year, "_", sex, ".csv"), row.names = F)
    }
}

```
