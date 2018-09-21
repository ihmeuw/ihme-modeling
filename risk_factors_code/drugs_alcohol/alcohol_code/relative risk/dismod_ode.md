
### Run dismod ODE to produce relative risks for alcohol


```python
%load_ext rpy2.ipython
```
```python
%%R

#Prepare necessary inputs for Dismod ODE
#Need:
# data_in, effect_in, plain_in, rate_in, value_in

#Load packages
library(data.table)

#Set up code options
cause <- 
version   <- 

inputs    <- 
outputs   <- 
templates <- 

dose            <- c(0, 30, 50, 70, 100, 150)   #Points for desired output
covariates      <- c("ones", "measure", "former_drinkers")  #Need at minimum sex and intercept
midpoint        <- F #Use midpoint for comparing

smoothing <- c(.05, .05, .05)

#Theo recommended scaling values between 0 & 100
adjust <- TRUE

if (adjust == TRUE){
    adjustment_factor   <- 100/max(dose)
    dose                <- dose*adjustment_factor
}

#Set up Dismod ODE options

sample          <- 5000
sample_interval <- 20

#Make necessary dismod files

#Read raw data
raw <- fread(paste0(inputs, "data_in_", cause, ".csv"))

#Format for dismod
raw <- raw[gday_lower != "EX",]
raw <- raw[is_outlier != 1, ]
raw <- raw[source_type !="case-control",]

#Pull out list of studies to produce random effects in rate_in
studies <- unique(raw$study)

raw[, gday_lower := as.numeric(gday_lower)]
raw[, gday_upper := as.numeric(gday_upper)]

#Transform values to be between 0-100
if (adjust==TRUE){
    raw[, gday_lower := gday_lower*adjustment_factor]
    raw[, gday_upper := gday_upper*adjustment_factor]
}

raw[is.na(gday_lower), gday_lower:=0]
raw[is.na(gday_upper), gday_upper:=gday_lower+30]

raw[, ]

raw[, super:="none"]
raw[, region:="none"]
raw[, subreg:=study]
raw[, age_lower:=gday_lower]
raw[, age_upper:=gday_upper]

#Use midpoint instead of upper/lower if setting is on
if (midpoint == T){
    raw[, hold := age_lower + (age_upper - age_lower)/2]
    raw[, `:=`(age_lower = hold, age_upper = hold)]
}

#Set covariates
raw[, x_sex:=0]
raw[, x_ones:=1]
raw[, x_former_drinkers:=former_drinkers]

#raw[, x_source_type:=0]

raw[, x_measure:=0]
raw[measure=="OR", x_measure:=1]
raw[measure=="HR", x_measure:=2]
raw[measure=="SMR", x_measure:=3]

#Add necessary generic variables for dismod
raw[sex=="Male", x_sex:=0.5]
raw[sex=="Female", x_sex:=-0.5]
raw[, time_lower:=2000]
raw[, time_upper:=2000]
raw[, data_like:= "log_gaussian"]
raw[, integrand:= "incidence"]
raw[, meas_value:=value]
raw[, meas_stdev:= (log(upper) - log(meas_value))/1.96]

keep <- c("age_lower", "age_upper", "time_lower", "time_upper", "super","region","subreg","data_like","integrand","meas_value",
          "meas_stdev", names(raw)[names(raw) %like% "x_"])

raw <- raw[, keep, with=FALSE]

#Make data_in
data_in <- data.table(age_lower = dose[1:length(dose)-1],
                      age_upper = dose[2:length(dose)],
                      time_lower = 2000,
                      time_upper = 2000,
                      super = "none",
                      region = "none",
                      subreg = "none",
                      data_like = "log_gaussian",
                      integrand = "mtall",
                      meas_value = 0.01,
                      meas_stdev = "inf")
    
for (cov in covariates){
    data_in[, paste0("x_", cov) := 0]
}

data_in <- rbind(raw, data_in, fill=TRUE)
data_in[meas_stdev <= 0, meas_stdev:=0.01]

#Make rate_in

rate_vars <- c("iota", "chi", "omega", "rho")
deriv_vars <- c("diota", "dchi", "domega", "drho")

rate_in <- data.table(expand.grid(type=rate_vars, age=dose))
deriv_df <- data.table(expand.grid(type=deriv_vars, age=dose[1:length(dose)-1]))

rate_in <- rbind(rate_in[order(type)], deriv_df[order(type)])

rate_in[, `:=` (lower = "_inf",
               upper = "inf",
               mean = 0,
               std = "inf")]

    #Change derivatives to be (-inf, inf)
rate_in[!type %in% c("dchi", "diota", "domega", "drho"), `:=`(lower = 0, upper = 0)]

    #Make sure iota is bounded from (0, inf) and choose reasonable mean
rate_in[type=="iota", `:=`(lower = 0,
                            upper = "inf",
                            mean = 1)]
    
    #Bind iota at 0 to 1 (since this is a RR and 0 is the counterfactual)
rate_in[(type=="iota") & (age==0), `:=`(lower = 1,
                                        upper = 1,
                                        mean = 1)]

#Make effect_in
effect_in <- fread(paste0(templates, "effect_in.csv"), colClasses=c(std="object"))

    #Add on random effects
add_on_studies <- data.table(integrand = "incidence",
                     name = studies,
                     effect = "subreg",
                     lower = -2,
                     upper = 2,
                     mean = 0,
                     std = 1)

add_on_covariates <- data.table(integrand = "incidence",
                               effect = "xcov",
                               name = paste0("x_", covariates[!covariates %in% c("sex", "ones")]),
                               lower = -2,
                               upper = 2,
                               mean = 0,
                               std = "inf")

if (add_on_covariates[, name] == "x_"){
    add_on_covariates <- data.table()
}

effect_in <- rbind(effect_in, add_on_studies)
effect_in <- rbind(effect_in, add_on_covariates)

#Make plain_in
plain_in <- data.table(name = c("p_zero", "xi_omega", "xi_chi", "xi_iota", "xi_rho"), lower = c(0, 0.1, 0.1, smoothing[1], 0.1),
                       upper = c(1, 0.1, 0.1, smoothing[3], 0.1), mean = c(0.1, 0.1, 0.1, smoothing[2], 0.1), std = c("inf","inf","inf","inf","inf"))

#Make value_in
value_in <- fread(paste0(templates, "value_in.csv"))
value_in[name == "num_sample",  value := sample]
value_in[name == "sample_interval", value := sample_interval]

#Ship off final datasets
dir.create(outputs, showWarnings = FALSE, recursive=TRUE)

write.csv(data_in, paste0(outputs, "/data_in.csv"), row.names=FALSE)
write.csv(rate_in, paste0(outputs, "/rate_in.csv"), row.names=FALSE)
write.csv(effect_in, paste0(outputs, "/effect_in.csv"), row.names=FALSE)
write.csv(plain_in, paste0(outputs, "/plain_in.csv"), row.names=FALSE)
write.csv(value_in, paste0(outputs, "/value_in.csv"), row.names=FALSE)

print(outputs)

```python
import os
os.chdir("")

! 
!  scale_beta=false


```python


%%R 

library(data.table)

df <- fread()

#Only hold onto last 1000 draws and iota's

df <- df[(dim(df)[1] - 999):(dim(df)[1])]
df <- df[, (colnames(df) %like% "iota") & !(colnames(df) %like% "xi"), with=FALSE ]

#Reshape long for easier manipulation, add on draw column, rename columns and remove strings from exposure column
df <- melt(df)
names(df) <- c("exposure", "rr")

df[, exposure := gsub("iota_", "", df$exposure)]
df[, exposure := as.numeric(exposure)]

#Modify exposure to match adjustment factor
if (adjust==TRUE){
    df[, exposure := exposure/adjustment_factor]
}

draws <- rep(seq(0, 999), length(unique(df$exposure)))
df[, draw :=draws]

#Make supplemental dataset holding mean, upper, lower for graphing below.

df[, `:=`(mean  = mean(.SD$rr),
          lower = quantile(.SD$rr, 0.025),
          upper = quantile(.SD$rr, 0.975)),
    by=c("exposure")]

df_compressed <- unique(df[, .(exposure, mean, lower, upper)])

write.csv(df[, .(exposure, draw, rr)], paste0(outputs, "/rr_draws.csv"), row.names=FALSE)
write.csv(df_compressed, paste0(outputs, "/rr_summary.csv"), row.names=FALSE)
```


```python
%%R

library(ggplot2)

df_compressed <- fread(paste0(outputs, "/rr_summary.csv"))
df_data       <- fread(paste0(outputs, "/data_in.csv"))

#Format raw data
df_data <- df_data[integrand != "mtall",]
df_data[, meas_stdev:=as.numeric(meas_stdev)]

setnames(df_data, c("age_lower", "age_upper", "meas_value", "subreg"), c("exposure_lower", "exposure_upper", "mean", "study"))

if (adjust==TRUE){
    df_data[, exposure_lower := exposure_lower/adjustment_factor]
    df_data[, exposure_upper := exposure_upper/adjustment_factor]
}

df_data[, lower:= log(mean) - 1.96*meas_stdev]
df_data[, upper:= log(mean) + 1.96*meas_stdev]

df_data[, `:=`(lower = exp(lower),
              upper = exp(upper))]

df_data <- df_data[, .(study, exposure_lower, exposure_upper, mean, lower, upper)]
df_data[, exposure := exposure_lower + (exposure_upper - exposure_lower)/2]

plot <- ggplot(df_compressed, aes(x=exposure, y=mean)) +
        geom_point() + 
        geom_line() + 
        geom_point(data=df_data, aes(color=study), alpha=0.5, size=0.5) +
        geom_errorbar(data=df_data, aes(ymin=lower, ymax=upper, color=study), alpha=0.3) +
        geom_errorbarh(data=df_data, aes(xmin=exposure_lower, xmax=exposure_upper, color=study), alpha=0.3) +
        geom_ribbon(data=df_compressed, aes(ymin=lower, ymax=upper), alpha=0.2, fill='blue') +
        geom_line(y=1, color='red') +
        coord_cartesian(xlim=c(0, 150), ylim=c(0.5, 4)) +
        ylab("RR") +
        ggtitle(paste(cause)) +
        guides(color=F) +
        #guides(col = guide_legend(nrow=50, byrow=TRUE)) +
        theme(legend.direction='vertical', legend.key.size= unit(.2, 'cm'), legend.text = element_text(size=10), legend.justification = 'top')
        
print(plot)

#Calculate RMSE for values lower than 100
pred <- approx(df_compressed$exposure, df_compressed$mean, df_data$exposure)$y
df_data[(exposure > 0), pred:=pred]

rmse <- sqrt(mean((df_data$mean - df_data$pred)^2, na.rm=TRUE))
print(paste0("RMSE: ", rmse))

#Calculate in-sample coverage, for points within range
pred_lower <- approx(df_compressed$exposure, df_compressed$lower, df_data$exposure)$y
pred_upper <- approx(df_compressed$exposure, df_compressed$upper, df_data$exposure)$y

df_data[, `:=`(pred_lower = pred_lower, pred_upper = pred_upper)]
df_data <- df_data[, is := ifelse((mean >= pred_lower) & (mean <= pred_upper), 1, 0)]

is_coverage <- sum(df_data$is, na.rm=T)/dim(df_data)[1]
print(paste0("In-sample coverage: ", is_coverage))

#Save to PDF
pdf("FILEPATH")
print(plot)      
dev.off()