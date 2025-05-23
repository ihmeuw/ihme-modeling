# Title: Scaling for age-sex

# input: birth sex-ratio, 5q0, sex-model, age-models
# format GPR output for scaling
# scale age in conditional probability space for sex-specific estimates
# calculate both-sex by age and scale age in conditional prob space
# output: draws and summaries for final age-sex estimates


# set-up ------------------------------------------------------------------

rm(list=ls())

# packages
library(data.table)
library(ggplot2)
library(argparse)
library(assertable)

# get arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type='integer', required=TRUE,
                   help='The version_id for this run of age-sex splitting')
parser$add_argument('--version_5q0_id', type='integer', required=TRUE,
                   help='The 5q0 version_id used for this run of age-sex splitting')
parser$add_argument('--ihme_loc_id', type='character', required=TRUE,
                   help='The ihme_loc_id for this run of age-sex splitting')
parser$add_argument('--location_id', type='integer', required=TRUE,
                   help='The location_id for this run of age-sex splitting')
args <- parser$parse_args()
version_id <- args$version_id
version_5q0_id <- args$version_5q0_id
loc_id <- args$location_id
loc <- args$ihme_loc_id

# directories
output_dir <- paste0("FILEPATH")
output_5q0_dir <- paste0("FILEPATH")
dir.create(paste0("FILEPATH"), showWarnings = FALSE)
dir.create(paste0("FILEPATH"), showWarnings = FALSE)
dir.create(paste0("FILEPATH"), showWarnings = FALSE)

# inputs ------------------------------------------------------------------

# location data
location_data <- fread(paste0("FILEPATH"))

# birth sex ratio
births <- fread(paste0("FILEPATH"))
if('ihme_loc_id' %in% names(births)) births[,ihme_loc_id:=NULL]
births <- merge(births, location_data[, c('location_id', 'ihme_loc_id')],
                all.x=T, by=c('location_id'))
births <- births[,c('location_name', 'sex_id', 'location_id'):=NULL]
births <- births[ihme_loc_id==loc]
births <- dcast.data.table(births, ihme_loc_id+year~sex, value.var='births')
births[,birth_sexratio:=male/female]
births <- births[,c('both', 'female', 'male'):=NULL]
births[,year:=year+0.5]

# both sex 5q0
ch <- fread(paste0("FILEPATH"))
ch <- ch[ihme_loc_id==loc]
setnames(ch, c('sim', 'mort'), c('simulation', 'q_u5_both'))
ch[,simulation:=as.numeric(simulation)]
setkey(ch, NULL)
ch <- unique(ch)

# sex model results
sexmod <- fread(paste0("FILEPATH"))
sexmod[,mort := exp(mort)/(1+exp(mort))]
sexmod[,mort := (mort * 0.7) + 0.8]
setnames(sexmod, 'sim', 'simulation')

# age model results
s3 <- list()
missing_files <- c()
for (sex in c('male','female')){
  for(age in c('enn','lnn','pnn', 'pna', 'pnb', 'inf','ch', 'cha', 'chb')){
    cat(paste(loc, sex, age, sep='_')); flush.console()
    file_dir = paste0("FILEPATH")
    file <- paste0("FILEPATH")
    if(file.exists(file)){
      s3[[paste0(file)]] <- fread(file)
      s3[[paste0(file)]]$age_group_name <- age
      s3[[paste0(file)]]$sex_name <- sex
    } else {
      missing_files <- c(missing_files, file)
    }
  }
}
if(length(missing_files)>0) stop('Files are missing.')
s3 <- rbindlist(s3)
s3[,mort:=exp(mort)]

# check GPR outputs
assertable::assert_values(s3, "mort", test = "not_na")

s3 <- dcast.data.table(s3, ihme_loc_id+year+sim+sex_name~age_group_name, value.var='mort')
setnames(s3, c('sex_name', 'sim', 'enn', 'lnn', 'pnn', 'pna', 'pnb', 'inf', 'ch', 'cha', 'chb'),
         c('sex', 'simulation', 'q_enn', 'q_lnn', 'q_pnn', 'q_pna', 'q_pnb', 'q_inf', 'q_ch', 'q_cha', 'q_chb'))

count <- nrow(s3[q_enn>0.99 | q_lnn>0.99 | q_pnn>0.99 | q_pna>0.99 | q_pnb>0.99 | q_inf>0.99 | q_ch>0.99 | q_cha>0.99 | q_chb>0.99, ])
message(paste0(count, ' rows where a qx draw is greater than 0.99'))
s3[q_enn>0.99, q_enn:=0.99]
s3[q_lnn>0.99, q_lnn:=0.99]
s3[q_pnn>0.99, q_pnn:=0.99]
s3[q_pna>0.99, q_pna:=0.99]
s3[q_pnb>0.99, q_pnb:=0.99]
s3[q_inf>0.99, q_inf:=0.99]
s3[q_ch>0.99, q_ch:=0.99]
s3[q_cha>0.99, q_cha:=0.99]
s3[q_chb>0.99, q_chb:=0.99]

# prep for scaling --------------------------------------------------------

# merge sexmod, birth sexratio, 5q0
dt <- merge(sexmod, births, all=T, by=c('ihme_loc_id','year'))
dt <- merge(dt, ch, by=c('ihme_loc_id','year','simulation'))

# calculate sex-specific 5q0
dt[,q_u5_female:= (q_u5_both*(1+birth_sexratio))/(1+(mort * birth_sexratio))]
dt[,q_u5_male:= q_u5_female * mort]

# melt
dt <- melt.data.table(dt, id.vars=c('ihme_loc_id', 'year', 'simulation', 'birth_sexratio'),
               measure.vars=c('q_u5_both', 'q_u5_female', 'q_u5_male'),
               value.name='q_u5',
               variable.name='sex')
dt[, sex:=tstrsplit(sex,'_',keep=3)]

# merge on age models
dt <- merge(s3, dt, by=c('ihme_loc_id', 'year', 'simulation', 'sex'), all=T)

# save scaling input for reference
write.csv(dt, paste0("FILEPATH"), row.names=F)

# scale age in conditional prob space --------------------------------------

# convert to conditional probability space
dt[, prob_enn := q_enn/q_u5]
dt[, prob_lnn := (1-q_enn)*q_lnn/q_u5]
dt[, prob_pnn := (1-q_enn)*(1-q_lnn)*q_pnn/q_u5]
dt[, prob_pna := (1-q_enn)*(1-q_lnn)*q_pna/q_u5]
dt[, prob_pnb := (1-q_enn)*(1-q_lnn)*(1-q_pna)*q_pnb/q_u5]
dt[, prob_ch  := (1-q_enn)*(1-q_lnn)*(1-q_pnn)*q_ch/q_u5]
dt[, prob_cha  := (1-q_enn)*(1-q_lnn)*(1-q_pnn)*q_cha/q_u5]
dt[, prob_chb  := (1-q_enn)*(1-q_lnn)*(1-q_pnn)*(1-q_cha)*q_chb/q_u5]
dt[, prob_inf := q_inf/q_u5]

# scale each sim
dt[, scale:=prob_inf+prob_ch]
dt[, prob_inf:=prob_inf/scale]
dt[, prob_ch:=prob_ch/scale]

dt[, scale:= (prob_cha + prob_chb)/prob_ch]
dt[, prob_cha:=prob_cha/scale]
dt[, prob_chb:=prob_chb/scale]

dt[, scale:=(prob_enn + prob_lnn + prob_pnn)/prob_inf]
dt[, prob_enn:=prob_enn/scale]
dt[, prob_lnn:=prob_lnn/scale]
dt[, prob_pnn:=prob_pnn/scale]

dt[, scale:=(prob_pna + prob_pnb)/prob_pnn]
dt[, prob_pna:=prob_pna/scale]
dt[, prob_pnb:=prob_pnb/scale]

# go back into qx space
dt[, q_enn := q_u5*prob_enn]
dt[, q_lnn := q_u5*prob_lnn / (1-q_enn)]
dt[, q_pnn := q_u5*prob_pnn / ((1-q_enn)*(1-q_lnn))]
dt[, q_pna := q_u5*prob_pna / ((1-q_enn)*(1-q_lnn))]
dt[, q_pnb := q_u5*prob_pnb / ((1-q_enn)*(1-q_lnn)*(1-q_pna))]
dt[, q_ch  := q_u5*prob_ch / ((1-q_enn)*(1-q_lnn)*(1-q_pnn))]
dt[, q_cha  := q_u5*prob_cha / ((1-q_enn)*(1-q_lnn)*(1-q_pnn))]
dt[, q_chb  := q_u5*prob_chb / ((1-q_enn)*(1-q_lnn)*(1-q_pnn)*(1-q_cha))]
dt[, q_inf := q_u5*prob_inf]

dt[,c('prob_enn','prob_lnn','prob_pnn', 'prob_pna', 'prob_pnb', 'prob_inf','prob_ch','prob_cha', 'prob_chb', 'scale'):=NULL]

# calculate and scale both-sex --------------------------------------------

# reshape to prep to generate both-sex estimates
dt <- dcast(dt, ihme_loc_id + year + simulation + birth_sexratio ~ sex,
            value.var=c('q_enn','q_lnn','q_pnn','q_pna', 'q_pnb', 'q_inf','q_ch','q_cha', 'q_chb', 'q_u5'))

# ratio of live males to females at the beginning of each period
dt[, r_enn := birth_sexratio]
dt[, r_lnn := r_enn*(1-q_enn_male)/(1-q_enn_female)]
dt[, r_pnn := r_lnn*(1-q_lnn_male)/(1-q_lnn_female)]
dt[, r_pna := r_lnn*(1-q_lnn_male)/(1-q_lnn_female)]
dt[, r_pnb := r_pna*(1-q_pna_male)/(1-q_pna_female)]
dt[, r_ch  := r_pnn*(1-q_pnn_male)/(1-q_pnn_female)]
dt[, r_cha  := r_pnn*(1-q_pnn_male)/(1-q_pnn_female)]
dt[, r_chb  := r_cha*(1-q_cha_male)/(1-q_cha_female)]

# calculate both-sex qx by age
dt[, q_enn_both := (q_enn_male) * (r_enn/(1+r_enn)) + (q_enn_female) * (1/(1+r_enn))]
dt[, q_lnn_both := (q_lnn_male) * (r_lnn/(1+r_lnn)) + (q_lnn_female) * (1/(1+r_lnn))]
dt[, q_pnn_both := (q_pnn_male) * (r_pnn/(1+r_pnn)) + (q_pnn_female) * (1/(1+r_pnn))]
dt[, q_pna_both := (q_pna_male) * (r_pna/(1+r_pna)) + (q_pna_female) * (1/(1+r_pna))]
dt[, q_pnb_both := (q_pnb_male) * (r_pnn/(1+r_pnb)) + (q_pnb_female) * (1/(1+r_pnb))]
dt[, q_ch_both  := (q_ch_male)  * (r_ch/(1+r_ch))   + (q_ch_female)  * (1/(1+r_ch))]
dt[, q_cha_both  := (q_cha_male)  * (r_cha/(1+r_cha))   + (q_cha_female)  * (1/(1+r_cha))]
dt[, q_chb_both  := (q_chb_male)  * (r_chb/(1+r_chb))   + (q_chb_female)  * (1/(1+r_chb))]
dt[, q_inf_both := (q_inf_male) * (r_enn/(1+r_enn)) + (q_inf_female) * (1/(1+r_enn))]

# convert to conditional probability space
dt[, prob_enn_both := q_enn_both/q_u5_both]
dt[, prob_lnn_both := (1-q_enn_both)*q_lnn_both/q_u5_both]
dt[, prob_pnn_both := (1-q_enn_both)*(1-q_lnn_both)*q_pnn_both/q_u5_both]
dt[, prob_pna_both := (1-q_enn_both)*(1-q_lnn_both)*q_pna_both/q_u5_both]
dt[, prob_pnb_both := (1-q_enn_both)*(1-q_lnn_both)*(1-q_pna_both)*q_pnb_both/q_u5_both]
dt[, prob_ch_both  := (1-q_enn_both)*(1-q_lnn_both)*(1-q_pnn_both)*q_ch_both/q_u5_both]
dt[, prob_cha_both  := (1-q_enn_both)*(1-q_lnn_both)*(1-q_pnn_both)*q_cha_both/q_u5_both]
dt[, prob_chb_both  := (1-q_enn_both)*(1-q_lnn_both)*(1-q_pnn_both)*(1-q_cha_both)*q_chb_both/q_u5_both]
dt[, prob_inf_both := q_inf_both/q_u5_both]

# scale ch and inf to 1
dt[, scale := prob_inf_both + prob_ch_both]
dt[, prob_inf_both := prob_inf_both/scale]
dt[, prob_ch_both  := prob_ch_both/scale]

# scale cha and chb to ch
dt[, scale := (prob_cha_both + prob_chb_both)/prob_ch_both]
dt[, prob_cha_both := prob_cha_both/scale]
dt[, prob_chb_both := prob_chb_both/scale]

dt[, prob_cha_chb_both := prob_cha_both + prob_chb_both]

# scale enn, lnn, pnn to inf
dt[, scale := (prob_enn_both+prob_lnn_both+prob_pnn_both)/prob_inf_both]
dt[, prob_enn_both := prob_enn_both/scale]
dt[, prob_lnn_both := prob_lnn_both/scale]
dt[, prob_pnn_both := prob_pnn_both/scale]

# scale pna and pnb to pnn
dt[, scale := (prob_pna_both + prob_pnb_both)/prob_pnn_both]
dt[, prob_pna_both := prob_pna_both/scale]
dt[, prob_pnb_both := prob_pnb_both/scale]
dt[,scale:=NULL]

dt[, prob_pna_pnb_both := prob_pna_both + prob_pnb_both]

# convert back to qx space
dt[, q_enn_both := (q_u5_both * prob_enn_both)]
dt[, q_lnn_both := (q_u5_both * prob_lnn_both) / ((1-q_enn_both))]
dt[, q_pnn_both := (q_u5_both * prob_pnn_both) / ((1-q_enn_both)*(1-q_lnn_both))]
dt[, q_pna_both := (q_u5_both * prob_pna_both) / ((1-q_enn_both)*(1-q_lnn_both))]
dt[, q_pnb_both := (q_u5_both * prob_pnb_both) / ((1-q_enn_both)*(1-q_lnn_both)*(1-q_pna_both))]
dt[, q_inf_both := (q_u5_both * prob_inf_both)]
dt[, q_ch_both  := (q_u5_both * prob_ch_both)  / ((1-q_enn_both)*(1-q_lnn_both)*(1-q_pnn_both))]
dt[, q_cha_both  := (q_u5_both * prob_cha_both)  / ((1-q_enn_both)*(1-q_lnn_both)*(1-q_pnn_both))]
dt[, q_chb_both  := (q_u5_both * prob_chb_both)  / ((1-q_enn_both)*(1-q_lnn_both)*(1-q_pnn_both)*(1-q_cha_both))]

# reshape long by sex again
dt <- melt(dt, id.vars=c('ihme_loc_id','year','simulation','birth_sexratio'))
dt <- dt[variable %like% 'q_']
dt[, age:=tstrsplit(variable,'_',keep=2)]
dt[, sex:=tstrsplit(variable,'_',keep=3)]
dt[, variable:=paste0('q_',age)]
dt <- dcast(dt, ihme_loc_id+year+simulation+birth_sexratio+sex~variable, value.var='value')

# save sims ---------------------------------------------------------------

# keep columns
dt_sims <- dt[,.(ihme_loc_id, year, sex, simulation, q_enn, q_lnn, q_pnn, q_pna, q_pnb, q_inf, q_ch, q_cha, q_chb, q_u5)]

count <- nrow(dt_sims[q_enn>0.99 | q_lnn>0.99 | q_pnn>0.99 | q_pna>0.99 | q_pnb>0.99 | q_inf>0.99 | q_ch>0.99 | q_cha>0.99 | q_chb>0.99, ])
message(paste0(count, ' rows where a qx draw is greater than 0.99'))
dt_sims[q_enn>0.99, q_enn:=0.99]
dt_sims[q_lnn>0.99, q_lnn:=0.99]
dt_sims[q_pnn>0.99, q_pnn:=0.99]
dt_sims[q_pna>0.99, q_pna:=0.99]
dt_sims[q_pnb>0.99, q_pnb:=0.99]
dt_sims[q_inf>0.99, q_inf:=0.99]
dt_sims[q_ch>0.99, q_ch:=0.99]
dt_sims[q_cha>0.99, q_cha:=0.99]
dt_sims[q_chb>0.99, q_chb:=0.99]

# assert there are no negative values
ages <- c('enn','lnn','pnn', 'pna', 'pnb', 'inf','ch','cha', 'chb', 'u5')
ages <- paste0("q_", ages)
assert_values(dt_sims, ages, test = "gte", test_val = 0)

# save sims
write.csv(dt_sims, paste0("FILEPATH"), row.names=F)

# collapse ----------------------------------------------------------------

# collapse over sim
byvars <- c('ihme_loc_id','year','sex')
ages <- c('enn','lnn','pnn', 'pna', 'pnb', 'inf','ch','cha', 'chb', 'u5')
for(aa in ages){
  dt[, temp_m:=mean(get(paste0('q_',aa))), by=byvars]
  dt[, temp_l:=quantile(get(paste0('q_',aa)),0.025), by=byvars]
  dt[, temp_u:=quantile(get(paste0('q_',aa)),0.975), by=byvars]
  setnames(dt, c('temp_m','temp_l','temp_u'),
           c(paste0('q_',aa,'_med'), paste0('q_',aa,'_lower'),paste0('q_',aa,'_upper')))
}
dt <- unique(dt, by=byvars)
dt[, c('q_enn','q_lnn','q_pnn', 'q_pna', 'q_pnb', 'q_inf','q_ch', 'q_cha', 'q_chb', 'q_u5','simulation'):=NULL]

dt <- dt[order(ihme_loc_id, sex, year)]
write.csv(dt, paste0("FILEPATH"), row.names=F)



# =====================================================================================================
# END of main script ==================================================================================
# =====================================================================================================

plot_intermediate_est <- function(dt, age, parameter){

    temp <- copy(dt)
    setnames(temp, paste0(parameter,'_',age), 'yvar')
    gg <- ggplot(data=dt[sex!='both', yvar:=mean(yvar),by=c('year','sex')],
                    aes(y=yvar, x=year, color=sex)) + geom_line() +
                    ggtitle(paste0(parameter,'_',age))
    return(gg)
}

plot_stacked_bar <- function(dt){
    plot_dt <- copy(dt)
    plot_dt <- melt(plot_dt, id.vars=c('year','sex','ihme_loc_id','simulation','birth_sexratio'))
    plot_dt[,med:=mean(value),by=c('year','sex','ihme_loc_id','birth_sexratio','variable')]
    plot_dt <- unique(plot_dt, by=c('year','sex','ihme_loc_id','variable'))
    plot_dt[, group:=ifelse(variable %like% 'inf' | variable %like% 'ch', 'one', 'two')]
    gg0 <- ggplot(data=plot_dt[variable %like% 'q'], aes(x=as.factor(year),y=med,fill=variable)) +
      geom_bar(position='stack', stat='identity') +
      geom_abline(slope=0, intercept=1) + facet_grid(sex~group)
    return(gg0)
}

plot_scaling_factors <- function(dt){
    gg_scale <- ggplot(data=dt, aes(x=as.factor(year), y=scale)) +
      geom_abline(slope=0, intercept=1, color='red') +
      geom_boxplot() + theme(axis.text.x = element_text(angle=90, hjust=1)) +
      xlab('year')
    return(gg_scale)
}

