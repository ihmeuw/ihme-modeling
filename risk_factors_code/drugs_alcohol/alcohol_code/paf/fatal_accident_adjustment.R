#Clean FARS data, calculate harm to others

rm(list=ls())
library(data.table)
library(ggplot2)
library(plyr)

years <- seq(1985, 2015, 5)

clean_data <- function(year){
  

  df <- fread(
  
  #Hold onto relevant columns
  if (year > 1990){
    df <- df[, .(STATE, ST_CASE, PER_TYP, AGE, SEX, ALC_RES, INJ_SEV)]
    setnames(df, names(df), c("state", "crash_id", "driver", "age", "sex", "bac", "fatal"))
  }else{
    df <- df[, .(STATE, ST_CASE, PER_TYP, AGE, SEX, TEST_RES, INJ_SEV)]
    setnames(df, names(df), c("state", "crash_id", "driver", "age", "sex", "bac", "fatal"))
  }

  #Set driver to a binary, as well as fatal
  df[driver!=1, driver:=0]
  df[fatal != 4, fatal := 0]
  df[fatal == 4, fatal := 1]
  
  #Convert BAC to decimal
  if (year == 2015){
    df[bac > 940, bac := 0]
    df[, bac := bac/1000]
  }else {
    df[bac > 94, bac := 0]
    df[, bac := bac/100]
  }

  #Drop observations not reporting age. Put ages into 5 year bins.
  if (year >= 2010){
    df <- df[age < 120,]
    df <- df[, age := ceiling(age/5) * 5]
  }else{
    df <- df[age <= 97]
    df <- df[, age := ceiling(age/5) * 5]
  }
  
  df <- df[(sex==1 | sex==2),]
  df[age >= 100, age := 95]
  
  #Peg crash id to alcohol intoxicated driver. Some amount of crashes had both drivers intoxicated
  #so only hold onto unique alc_crash
  
  alc_crash <- unique(df[(driver==1 & bac >= 0.01), .(crash_id)])$crash_id
  df[(df$crash_id %in% alc_crash), alc_involved := 1]
  df[!crash_id %in% alc_crash, alc_involved := 0]
  
  #calculate total fatal in crash & age of driver
  df[, tot_fatal := sum(.SD$fatal), by="crash_id"]
  df[, year := year]
  
  df[, driver_age := .SD[driver==1, .(age)], by=c("crash_id")]
  df[, driver_sex := .SD[driver==1, .(sex)], by=c("crash_id")]
  
  return(df)
}

#Load in person-level data
df <- data.table(rbindlist(lapply(years, clean_data)))

#Separate into dd driver and non-dd driver datasets
dd <- df[alc_involved==1, ]
nd <- df[alc_involved==0, ]

#By driver age, calculate mean total fatalities
dd[driver==1, avg_fatal := mean(.SD$tot_fatal), by=c("age", "sex")]
dd[driver==0, tot_victim := sum(.SD$fatal), by=c("driver_age", "driver_sex", "age", "sex")]

victims <- unique(dd[!is.na(tot_victim), .(driver_age, driver_sex, sex, age, tot_victim)])
setorder(dd, driver_age, driver_sex, age, sex)

victims <- victims[(driver_age <= 95 & driver_age >= 15)]

victims[, tot_deaths := sum(.SD$tot_victim), by=c("driver_age", "driver_sex")]
victims[, pct_deaths := tot_victim/tot_deaths]
victims[is.na(pct_deaths), pct_deaths := 0]

dd <- unique(dd[!is.na(avg_fatal), .(sex, age, avg_fatal)])
setorder(dd, sex, age)

dd <- dd[(age <= 95 & age >= 15),]
dd[, type := "Driver BAC >= 0.01"]

#By driver age, calculate mean total fatalities
nd[driver==1, avg_fatal := mean(.SD$tot_fatal), by=c("age", "sex")]
nd <- unique(nd[!is.na(avg_fatal), .(age, sex, avg_fatal)])
setorder(nd, sex, age)

nd <- nd[(age < 95 & age > 15),]
nd[, type := "No alcohol involved"]

#Make new df, avg kills by age & alc_involvment
t <- rbind(dd, nd)

#Graph 
pdf("")

print(ggplot(t, aes(x=as.factor(age), y=avg_fatal, color=type)) + 
  geom_point() + 
  ggtitle("Average number of deaths in crash \ngiven driver's age, sex, & alcohol's involvment") +
  labs(x="Age", y="Average # \n of deaths", color="Alcohol's involvement") +
  facet_wrap(~sex) +
  theme(legend.position = "bottom", 
        axis.title.y = element_text(angle=0, vjust=0.5), 
        plot.title = element_text(hjust=0)))

print(ggplot(victims[driver_sex==1,], aes(y=pct_deaths, x=age, color=as.factor(sex))) + 
  geom_point() + 
  facet_wrap(~driver_age) +
  scale_color_manual(values=c("Green", "Purple")) +
  scale_color_brewer(palette="Dark2") +
  labs(x="Victim's age", y="Percentage of \ntotal victims", color="Sex") +
  ggtitle("Percentage of total victims by age & sex, \ngiven the male drunk driver's age") +
  scale_color_discrete(labels = c("Male", "Female")) +
  theme(legend.position = "bottom",
        plot.title = element_text(hjust=0),
        axis.title.y = element_text(angle=0, vjust=0.5)) +
  scale_color_manual(values=c("#4daf4a", "#984ea3")))

print(ggplot(victims[driver_sex==2,], aes(y=pct_deaths, x=age, color=as.factor(sex))) + 
  geom_point() + 
  facet_wrap(~driver_age) +
  labs(x="Victim's age", y="Percentage of \ntotal victims", color="Sex") +
  ggtitle("Percentage of total victims by age & sex, \ngiven the female drunk driver's age") +
  scale_color_discrete(labels = c("Male", "Female")) +
  theme(legend.position = "bottom",
        plot.title = element_text(hjust=0),
        axis.title.y = element_text(angle=0, vjust=0.5)) +
  scale_color_manual(values=c("#4daf4a", "#984ea3")))

dev.off()

t <- t[type == "Driver BAC >= 0.01",]
t <- t[, .(sex, age, avg_fatal)]
names(t) <- c("sex_id", "age", "avg_fatalities")

age_legend <- data.table(age_group_id = c(1, seq(6,20), 30, 31, 32, 235),
                         age = seq(0,95,5))
t <- join(t, age_legend)
t <- t[, .(sex_id, age_group_id, avg_fatalities)]

victims <- victims[, .(driver_sex, driver_age, sex, age, pct_deaths)]
setnames(victims, c("driver_sex", "sex"), c("driver_sex_id", "sex_id"))
victims <- join(victims, age_legend)

setnames(age_legend, c("age", "age_group_id"), c("driver_age", "driver_age_group_id"))
victims <- join(victims, age_legend)

victims <- victims[, .(driver_sex_id, driver_age_group_id, sex_id, age_group_id, pct_deaths)]
setnames(victims, c("driver_sex_id", "driver_age_group_id", "sex_id", "age_group_id"), 
         c("sex_id", "age_group_id", "victim_sex_id", "victim_age_group_id"))

write.csv()
write.csv()
