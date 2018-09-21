

## Calculate TMREL for alcohol

At the draw level, weight each relative risk based on the share of overall DALYs. Do this at each exposure level from 1-150 grams and take the minimum as the estimate for TMREL. 


```R
#Grab packages
library(data.table)
library(plyr)
library(parallel)

setwd(")
source("get_draws.R")

#Set options for TMREL calculation
sex <- 2
directory <- ""

setwd(directory)

files <- list.files(directory)
sex_hold <- paste0("^rr_\\d*\\.csv|(?<=_)", sex, "(?=.csv)")
files <- grep(sex_hold, files, perl=TRUE, value=TRUE)

causes <- as.numeric(unique(regmatches(files, regexpr("\\d{3}", files))))

id_field <- rep("cause_id", length(causes))

#Go get Daly draws from GBD 2016, reshape long, then by draw create weight factors as share of daly rates
daly_draws <- data.table(get_draws(gbd_id_field=id_field, gbd_id=causes, source="Dalynator", location_ids=1, sex_ids=sex, age_group_ids=24, year_ids=2016, measure_ids=2, metric_id=3, num_workers=5, gbd_round_id=4, status='latest'))
daly_draws <- melt(daly_draws, c("location_id", "year_id", "age_group_id", "sex_id", "cause_id", "metric_id", "measure_id"), value.name ="dalys", variable.name="draw")

daly_draws[, total_dalys := sum(.SD$dalys), by=c("metric_id", "draw")]
daly_draws[, weight_factor := dalys/total_dalys, by=c("metric_id", "draw")]

#Clean dataframe to match relative risks
daly_draws <- daly_draws[, .(draw, cause_id, weight_factor)]
daly_draws[, draw:=as.numeric(gsub("draw_", "", draw))]

```
```R
dose  <- seq(0,150,1)
draws <- seq(0,999,1)

#For each cause, pull in rr. Use approx to construct curve and combine results together.
make_rr <- function(d, df){
    
    df <- df[draw==d,]
    
    #m <- loess(rr ~ exposure, data=df)
    #m <- predict(m, newdata=dose)
    m <- approx(df$exposure, df$rr, xout=dose)$y
    
    df <- data.table(rr=m)
    
    df[, exposure:=dose]
    df[, draw:=d]
    
    return(df)
}

#Read in the files, then bind the files together
grab_and_format <- function(filepath){
    
    df <- fread(filepath)
    
    #df[, rr:=sort(rr), by="exposure"]
    df <- rbindlist(mclapply(draws, make_rr, df=df, mc.cores=20))
    
    setnames(df, "rr", regmatches(filepath, regexpr("\\d{3}", filepath)))
    df[, `:=`(exposure = NULL, draw = NULL)]
    return(df)
}

rr <- lapply(files, grab_and_format)
rr <- do.call(cbind, rr)

#Expand long by draw, exposure, and cause. Merge with daly weights and calculate average rr for each exposure and draw.
id <- expand.grid(exposure=dose, draw=draws)
rr <- data.table(cbind(id, rr))
rr <- melt(rr, c("exposure", "draw"), value.name="rr", variable.name="cause_id")

rr <- data.table(join(rr, daly_draws, by=c("draw", "cause_id")))
rr[, weight_rr:=rr*weight_factor]
rr[, avg_rr:=(sum(.SD$weight_rr)), by=c("draw", "exposure")]

rr[, min_avg_rr:=min(.SD$avg_rr), by="draw"]
rr[, min_exp:=.SD[avg_rr==min(.SD$avg_rr), exposure], by="draw"]

tmrel <- unique(rr[, .(draw, min_exp, min_avg_rr)])
setnames(tmrel, "min_exp", "tmrel")

```


```R
rr[, `:=`(mean_avg_rr=mean(.SD$avg_rr),
          lower_avg_rr=quantile(.SD$avg_rr, 0.025),
          upper_avg_rr=quantile(.SD$avg_rr, 0.975)), by=c("exposure")]

rr[exposure==0, `:=`(mean_avg_rr=1,
                    lower_avg_rr=1,
                    upper_avg_rr=1)]

rr[, exposure := exposure/12]
test <- unique(rr[, .(exposure, mean_avg_rr, lower_avg_rr, upper_avg_rr)])

```


```R
library(ggplot2)

pdf("")

ggplot(test, aes(y=mean_avg_rr, x=exposure)) + 
    geom_line() + 
    geom_ribbon(aes(ymin=lower_avg_rr, ymax=upper_avg_rr), fill='grey', alpha=0.6) + 
    coord_cartesian(xlim=c(0,12), ylim=c(0.9,3)) +
    scale_x_continuous(breaks=seq(0,12,1)) +
    labs(x="Drinks per day", y="Weighted \nall-cause risk", title="Weighted all-cause risk \nof alcoholic drinks consumed daily,\n amongst women aged 15-49") +
    theme(plot.title = element_text(hjust=0.5), axis.title.y = element_text(angle=0, vjust=0.5)) 
```


```R
#Plot density of TMREL

sum_stats <- summary(tmrel$tmrel)

ggplot(tmrel, aes(x=tmrel)) + 
    geom_histogram(bins=20, color='black') +
    labs(x="TMREL \n(in drinks per day)", y="Number of draws", title="Distribution of TMREL \n in g/day") +
    theme(plot.title = element_text(hjust=0.5), axis.title.y = element_text(angle=0, vjust=0.5))
```
