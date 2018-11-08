
library(metafor)
library(data.table)
library(ggplot2)
rlogit <- function(x){exp(x)/(1+exp(x))}
logit <- function(x){log(x/(1-x))}
rr <- function(bullied_past_now, bullied_past_total, bullied_notpast_now, bullued_notpast_total){(bullied_past_now/bullied_past_total)/(bullied_notpast_now/bullued_notpast_total)}
seq_c <- function(start, finish){for(i in c(1:length(start))){
  temp_c <- seq(start[i], finish[i], 1)
  if(i==1){c <- temp_c} else {c <- cbind(c, temp_c)}
}
  return(unname(c))
}

### Build dataset ###
sourander_2000 <- data.table(author="sourander_2000", time=8, age_start = 8, age_end = 16, age_mean = (16+8)/2, past_future=27+16, past_nofuture=92+88, nopast_future=3+17, nopast_nofuture=102+170)

kumpulainen_1999 <- data.table(author="kumpulainen_1999", time=4, age_start = 8, age_end = 12, age_mean = (12+8)/2, past_future=22+6+10+21, past_nofuture=21+13+37+95, nopast_future=17+4+81+36, nopast_nofuture=26+61+40+778)

lereya_2015 <- data.table(author="lereya_2015", time=3, age_start = 10, age_end = 13, age_mean = (13+10)/2, past_future=47+97+71+337, past_nofuture=209+841-97-47-71-337, nopast_future=356+82+7+15, nopast_nofuture=65+1887-7-15-82-356)

winsper_2012 <- data.table(author="winsper_2012", time=2, age_start = 8, age_end = 10, age_mean = (10+8)/2, past_future=661, past_nofuture=4829*((344+1637)/(344+1637+3011+55))-661, nopast_future=4829*((301+1036)/(301+1036+4162+51))-611, nopast_nofuture=2454)

bowes_2013 <- data.table(author="bowes_2013", time=12-(7+10)/2, age_start = (7+10)/2, age_end = 12, age_mean = (12+5)/2, past_future=157+129, past_nofuture=112+109+95+84, nopast_future=61+49+36+59, nopast_nofuture=253+335+71+79+153+176+93+95)

baly_2014_1 <- data.table(author="baly_2014_1", time=1, age_start = 11, age_end = 13, age_mean = (13+11)/2, past_future=55, past_nofuture=43, nopast_future=38, nopast_nofuture=170)
baly_2014_2 <- data.table(author="baly_2014_2", time=1, age_start = 12, age_end = 14, age_mean = (14+12)/2, past_future=36+12, past_nofuture=19+26, nopast_future=8+19, nopast_nofuture=35+151)
baly_2014_3 <- data.table(author="baly_2014_3", time=2, age_start = 11, age_end = 14, age_mean = (14+11)/2, past_future=8+36, past_nofuture=35+19, nopast_future=19+12, nopast_nofuture=151+26)

paul_2003_1 <- data.table(author="paul_2003_1", time=1, age_start = 9, age_end = 11, age_mean = (9+11)/2, past_future=((658+638)/2)*0.1587*0.65, past_nofuture=((658+638)/2)*0.1587*(1-0.65), nopast_future=NA, nopast_nofuture=NA)
paul_2003_2 <- data.table(author="paul_2003_2", time=2, age_start = 9, age_end = 12, age_mean = (9+12)/2, past_future=((658+600)/2)*0.1587*0.49, past_nofuture=((658+600)/2)*0.1587*(1-0.49), nopast_future=NA, nopast_nofuture=NA)
paul_2003_3 <- data.table(author="paul_2003_3", time=3, age_start = 9, age_end = 13, age_mean = (9+13)/2, past_future=((658+600)/2)*0.1587*0.34, past_nofuture=((658+600)/2)*0.1587*(1-0.34), nopast_future=NA, nopast_nofuture=NA)
paul_2003_4 <- data.table(author="paul_2003_4", time=1, age_start = 10, age_end = 12, age_mean = (10+12)/2, past_future=((638+600)/2)*0.1587*0.42, past_nofuture=((658+600)/2)*0.1587*(1-0.42), nopast_future=NA, nopast_nofuture=NA)
paul_2003_5 <- data.table(author="paul_2003_5", time=2, age_start = 10, age_end = 13, age_mean = (10+13)/2, past_future=((638+600)/2)*0.1587*0.43, past_nofuture=((658+600)/2)*0.1587*(1-0.43), nopast_future=NA, nopast_nofuture=NA)
paul_2003_6 <- data.table(author="paul_2003_6", time=1, age_start = 11, age_end = 13, age_mean = (11+13)/2, past_future=68*0.48, past_nofuture=68*(1-0.48), nopast_future=NA, nopast_nofuture=NA)

lien_2013 <- data.table(author="lien_2013", time=3, age_start = 15, age_end = 19, age_mean = (15+19)/2, past_future=48, past_nofuture=289, nopast_future=NA, nopast_nofuture=NA)

kim_2009 <- data.table(author="kim_2009", time=10/12, age_start = 13, age_end = 14, age_mean = (14+13)/2, past_future=63+32+20+5+13+9+22+17, past_nofuture=55+46+11+6+24+14+31+17, nopast_future=24+34+16+12+6+3+16+9, nopast_nofuture=412+395+65+39+77+57+62+51)

bully_data <- rbind(sourander_2000, kumpulainen_1999, lereya_2015, winsper_2012, bowes_2013,
                    baly_2014_1, baly_2014_2, baly_2014_3, paul_2003_1, paul_2003_2,
                    paul_2003_3, paul_2003_4, paul_2003_5, paul_2003_6, lien_2013, kim_2009)
bully_data[, `:=` (current_bullying=past_future+nopast_future, past_bullying=past_future+past_nofuture, persistence=past_future/(past_future+past_nofuture))]
bully_data[, `:=` (cv_peer=0)]
bully_data[grep("paul_2003", author), `:=` (cv_peer=1)]
bully_data[grep("kim_2009", author), `:=` (cv_peer=1)]

### Meta Analysis ###

meta <- rma(measure="PLO", xi=past_future, ni=past_bullying, subset=(cv_peer==0), slab=author, data=bully_data, method="DL", mods=time)

predicted <- predict(meta, newmods=seq_c(c(1), c(11)), digits=6, transf=transf.ilogit)
reg_results <- data.table(time=c(1:11), prevalence=predicted$pred, lower=predicted$ci.lb, upper=predicted$ci.ub)
reg_results_pre <- data.table(time=c(0.9), prevalence=predict(meta, newmods=0.9, digits=6, transf=transf.ilogit)$pred, lower = predict(meta, newmods=0.9, digits=6, transf=transf.ilogit)$ci.lb, upper = predict(meta, newmods=0.9, digits=6, transf=transf.ilogit)$ci.ub)
reg_results <- rbind(reg_results_pre, reg_results)
write.csv(reg_results, "FILEPATH", row.names=F)

predicted_logit <- predict(meta, newmods=seq_c(c(0), c(25)), digits=6)
reg_results_logit <- data.table(time=c(0:25), prevalence=predicted_logit$pred, lower=predicted_logit$ci.lb, upper=predicted_logit$ci.ub)
write.csv(reg_results_logit, "FILEPATH", row.names=F)

