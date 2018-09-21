
## Plot density of individual observations for g/day


```R
library(data.table)
library(haven)

df <- data.table(read_dta("))
df2 <- data.table(read_dta(""))

df <- df[, .(ihme_loc_id, ihme_male, ihme_age_yr, grams_per_day)]
df <- df[!is.na(grams_per_day)]
df <- df[grams_per_day <= 250 & grams_per_day >= 0]

df2 <- df2[, .(ihme_loc_id, ihme_male, ihme_age_yr, grams_per_day)]
df2 <- df2[!is.na(grams_per_day)]
df2 <- df2[grams_per_day <= 250 & grams_per_day >= 0]

final <- data.table(rbind(df, df2))
setnames(final, "grams_per_day", "gday")

fwrite(final, "")
```
```R
library(ggplot2)
library(data.table)

final <- fread("")

mean <- mean(final$gday)
sd   <- sd(final$gday)

alpha <- mean^2/sd^2
beta  <- sd^2/mean

ggplot(final, aes(x=gday)) + geom_histogram(binwidth=2) + stat_function(fun=dgamma, args=list(shape=alpha, rate=beta))
```
