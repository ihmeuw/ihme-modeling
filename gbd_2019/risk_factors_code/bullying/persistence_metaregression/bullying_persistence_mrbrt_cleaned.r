#######################################################################################
### Date:     21th May 2019
### Purpose:  Run MR-BRT for persistence for Bullying
#######################################################################################

rm(list=ls())

source("/FILEPATH/mr_brt_functions.R")

library(data.table)
library(ggplot2)
rlogit <- function(x){exp(x)/(1+exp(x))}
logit <- function(x){log(x/(1-x))}
logit_se <- function(N, n){sqrt(1/(N-n)+1/n)}
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
baly_2014_1 <- data.table(author="baly_2014", time=1, age_start = 11, age_end = 13, age_mean = (13+11)/2, past_future=55, past_nofuture=43, nopast_future=38, nopast_nofuture=170)
baly_2014_2 <- data.table(author="baly_2014", time=1, age_start = 12, age_end = 14, age_mean = (14+12)/2, past_future=36+12, past_nofuture=19+26, nopast_future=8+19, nopast_nofuture=35+151)
baly_2014_3 <- data.table(author="baly_2014", time=2, age_start = 11, age_end = 14, age_mean = (14+11)/2, past_future=8+36, past_nofuture=35+19, nopast_future=19+12, nopast_nofuture=151+26)
paul_2003_1 <- data.table(author="paul_2003", time=1, age_start = 9, age_end = 11, age_mean = (9+11)/2, past_future=((658+638)/2)*0.1587*0.65, past_nofuture=((658+638)/2)*0.1587*(1-0.65), nopast_future=NA, nopast_nofuture=NA)
paul_2003_2 <- data.table(author="paul_2003", time=2, age_start = 9, age_end = 12, age_mean = (9+12)/2, past_future=((658+600)/2)*0.1587*0.49, past_nofuture=((658+600)/2)*0.1587*(1-0.49), nopast_future=NA, nopast_nofuture=NA)
paul_2003_3 <- data.table(author="paul_2003", time=3, age_start = 9, age_end = 13, age_mean = (9+13)/2, past_future=((658+600)/2)*0.1587*0.34, past_nofuture=((658+600)/2)*0.1587*(1-0.34), nopast_future=NA, nopast_nofuture=NA)
paul_2003_4 <- data.table(author="paul_2003", time=1, age_start = 10, age_end = 12, age_mean = (10+12)/2, past_future=((638+600)/2)*0.1587*0.42, past_nofuture=((658+600)/2)*0.1587*(1-0.42), nopast_future=NA, nopast_nofuture=NA)
paul_2003_5 <- data.table(author="paul_2003", time=2, age_start = 10, age_end = 13, age_mean = (10+13)/2, past_future=((638+600)/2)*0.1587*0.43, past_nofuture=((658+600)/2)*0.1587*(1-0.43), nopast_future=NA, nopast_nofuture=NA)
paul_2003_6 <- data.table(author="paul_2003", time=1, age_start = 11, age_end = 13, age_mean = (11+13)/2, past_future=68*0.48, past_nofuture=68*(1-0.48), nopast_future=NA, nopast_nofuture=NA)
lien_2013 <- data.table(author="lien_2013", time=3, age_start = 15, age_end = 19, age_mean = (15+19)/2, past_future=48, past_nofuture=289, nopast_future=NA, nopast_nofuture=NA)
kim_2009 <- data.table(author="kim_2009", time=10/12, age_start = 13, age_end = 14, age_mean = (14+13)/2, past_future=63+32+20+5+13+9+22+17, past_nofuture=55+46+11+6+24+14+31+17, nopast_future=24+34+16+12+6+3+16+9, nopast_nofuture=412+395+65+39+77+57+62+51)

bully_data <- rbind(sourander_2000, kumpulainen_1999, lereya_2015, winsper_2012, bowes_2013,
                    baly_2014_1, baly_2014_2, baly_2014_3, paul_2003_1, paul_2003_2,
                    paul_2003_3, paul_2003_4, paul_2003_5, paul_2003_6, lien_2013, kim_2009)
bully_data[, `:=` (current_bullying=past_future+nopast_future, past_bullying=past_future+past_nofuture, persistence=past_future/(past_future+past_nofuture))]
bully_data[, `:=` (cv_peer=0)]
bully_data[grep("paul_2003", author), `:=` (cv_peer=1)]
bully_data[grep("kim_2009", author), `:=` (cv_peer=1)]

bully_data[, sample_size := past_future+past_nofuture]

bully_data[, `:=` (logit_e = logit(persistence), logit_se = logit_se(past_future+past_nofuture, past_future)), by = c("author","persistence")]
## Run MR-BRT ##
dir.create(file.path("/FILEPATH/"), showWarnings = FALSE)

model <- run_mr_brt(
  output_dir = "/FILEPATH/",
  model_label = "#1",
  data = bully_data[cv_peer==0,],
  mean_var = "logit_e",
  se_var = "logit_se",
  covs = list(cov_info("time", "X", type = 'continuous')),
  method = "trim_maxL",
  trim_pct = 0.1,
  study_id = "author",
  overwrite_previous = TRUE,
  lasso = F)

predict_matrix <- data.table(intercept = 1, time = c(0:11))

outputs <- load_mr_brt_outputs(model)$model_coefs

write.csv(outputs, "/FILEPATH.csv", row.names=F)

pers_summaries <- as.data.table(predict_mr_brt(model, newdata = predict_matrix)["model_summaries"])
names(pers_summaries) <- gsub("model_summaries.", "", names(pers_summaries))
pers_summaries <- pers_summaries[,.(time = X_time, Y_mean, Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)), persistence = rlogit(Y_mean), lower = rlogit(Y_mean_lo), upper = rlogit(Y_mean_hi))]

pers_draws <- as.data.table(predict_mr_brt(model, newdata = predict_matrix, write_draws = TRUE)["model_draws"])
names(pers_draws) <- gsub("model_draws.", "", names(pers_draws))
setnames(pers_draws, "X_time", "time")

library(ggplot2)

plot <- ggplot(data=pers_summaries, aes(x=time, y=persistence))+
  geom_ribbon(data= pers_summaries, aes(x=time, ymin=lower, ymax=upper),  fill="lightgrey", alpha=.7) +
  geom_line(size=1) +
  ylab("Proportion still experiencing bullying") +
  xlab("Follow-up (years)") +
  theme_minimal() +
  scale_x_continuous(expand=c(0,0), breaks = c(0:11))+
  theme(axis.line=element_line(colour="black")) +
  geom_point(data=bully_data[cv_peer==0, ], aes(x=time, y=persistence) , color="blue", size=2)
plot

ggsave(plot, filename="/FILEPATH.pdf", width = 8, height = 4)

write.csv(pers_draws, "/FILEPATH.csv", row.names=F)


