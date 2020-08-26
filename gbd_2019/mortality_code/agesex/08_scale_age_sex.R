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
output_dir <- "FILEPATH"
output_5q0_dir <- "FILEPATH"
dir.create(paste0(output_dir, '/age_model/scaling'), showWarnings = FALSE)
dir.create(paste0(output_dir, '/summary'), showWarnings = FALSE)
dir.create(paste0(output_dir, '/draws'), showWarnings = FALSE)

# inputs ------------------------------------------------------------------

# location data
location_data <- fread(paste0(output_dir, '/as_locs_for_stata.csv'))

# birth sex ratio
births <- fread(paste0(output_dir, '/live_births.csv'))
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
ch <- fread(paste0(output_5q0_dir, '/draws/', loc_id, '.csv'))
ch <- ch[ihme_loc_id==loc]
setnames(ch, c('sim', 'mort'), c('simulation', 'q_u5_both'))
ch[,simulation:=as.numeric(simulation)]
setkey(ch, NULL)
ch <- unique(ch)

# sex model results
sexmod <- fread(paste0(output_dir, '/sex_model/gpr/gpr_', loc, '_sim.txt'))
sexmod[,mort := exp(mort)/(1+exp(mort))]
sexmod[,mort := (mort * 0.7) + 0.8]
setnames(sexmod, 'sim', 'simulation')

# age model results
s3 <- list()
missing_files <- c()
for (sex in c('male','female')){
  for(age in c('enn','lnn','pnn','inf','ch')){
    cat(paste(loc, sex, age, sep='_')); flush.console()
    file_dir = paste0(output_dir, '/age_model/gpr/', sex, '/', age)
    file <- paste0(file_dir, '/gpr_', loc, '_', sex, '_', age, '_sim.txt')
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
s3 <- dcast.data.table(s3, ihme_loc_id+year+sim+sex_name~age_group_name, value.var='mort')
setnames(s3, c('sex_name', 'sim', 'enn', 'lnn', 'pnn', 'inf', 'ch'),
         c('sex', 'simulation', 'q_enn', 'q_lnn', 'q_pnn', 'q_inf', 'q_ch'))

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
write.csv(dt, paste0(output_dir,'/age_model/scaling/scaling_input_',loc,'.csv'), row.names=F)

# scale age in conditional prob space --------------------------------------

# convert to conditional probability space
dt[, prob_enn := q_enn/q_u5]
dt[, prob_lnn := (1-q_enn)*q_lnn/q_u5]
dt[, prob_pnn := (1-q_enn)*(1-q_lnn)*q_pnn/q_u5]
dt[, prob_ch  := (1-q_enn)*(1-q_lnn)*(1-q_pnn)*q_ch/q_u5]
dt[, prob_inf := q_inf/q_u5]

# scale each sim (prob_inf and prob_ch should sum to 1)
dt[, scale:=prob_inf+prob_ch]
dt[, prob_inf:=prob_inf/scale]
dt[, prob_ch:=prob_ch/scale]

# scale each sim (prob enn, lnn, and pnn should sum to prob_inf)
dt[, scale:=(prob_enn + prob_lnn + prob_pnn)/prob_inf]
dt[, prob_enn:=prob_enn/scale]
dt[, prob_lnn:=prob_lnn/scale]
dt[, prob_pnn:=prob_pnn/scale]

# go back into qx space
dt[, q_enn := q_u5*prob_enn]
dt[, q_lnn := q_u5*prob_lnn / (1-q_enn)]
dt[, q_pnn := q_u5*prob_pnn / ((1-q_enn)*(1-q_lnn))]
dt[, q_ch  := q_u5*prob_ch / ((1-q_enn)*(1-q_lnn)*(1-q_pnn))]
dt[, q_inf := q_u5*prob_inf]

dt[,c('prob_enn','prob_lnn','prob_pnn','prob_inf','prob_ch','scale'):=NULL]

# calculate and scale both-sex --------------------------------------------

# reshape to prep to generate both-sex estimates
dt <- dcast(dt, ihme_loc_id + year + simulation + birth_sexratio ~ sex,
            value.var=c('q_enn','q_lnn','q_pnn','q_inf','q_ch','q_u5'))

# ratio of live males to females at the beginning of each period
dt[, r_enn := birth_sexratio]
dt[, r_lnn := r_enn*(1-q_enn_male)/(1-q_enn_female)]
dt[, r_pnn := r_lnn*(1-q_lnn_male)/(1-q_lnn_female)]
dt[, r_ch  := r_pnn*(1-q_pnn_male)/(1-q_pnn_female)]

# calculate both-sex qx by age
dt[, q_enn_both := (q_enn_male) * (r_enn/(1+r_enn)) + (q_enn_female) * (1/(1+r_enn))]
dt[, q_lnn_both := (q_lnn_male) * (r_lnn/(1+r_lnn)) + (q_lnn_female) * (1/(1+r_lnn))]
dt[, q_pnn_both := (q_pnn_male) * (r_pnn/(1+r_pnn)) + (q_pnn_female) * (1/(1+r_pnn))]
dt[, q_ch_both  := (q_ch_male)  * (r_ch/(1+r_ch))   + (q_ch_female)  * (1/(1+r_ch))]
dt[, q_inf_both := (q_inf_male) * (r_enn/(1+r_enn)) + (q_inf_female) * (1/(1+r_enn))]

# convert to conditional probability space
dt[, prob_enn_both := q_enn_both/q_u5_both]
dt[, prob_lnn_both := (1-q_enn_both)*q_lnn_both/q_u5_both]
dt[, prob_pnn_both := (1-q_enn_both)*(1-q_lnn_both)*q_pnn_both/q_u5_both]
dt[, prob_ch_both  := (1-q_enn_both)*(1-q_lnn_both)*(1-q_pnn_both)*q_ch_both/q_u5_both]
dt[, prob_inf_both := q_inf_both/q_u5_both]

# scale ch and inf to 1
dt[, scale := prob_inf_both + prob_ch_both]
dt[, prob_inf_both := prob_inf_both/scale]
dt[, prob_ch_both  := prob_ch_both/scale]

# scale enn, lnn, pnn to inf
dt[, scale := (prob_enn_both+prob_lnn_both+prob_pnn_both)/prob_inf_both]
dt[, prob_enn_both := prob_enn_both/scale]
dt[, prob_lnn_both := prob_lnn_both/scale]
dt[, prob_pnn_both := prob_pnn_both/scale]
dt[,scale:=NULL]

# convert back to qx space
dt[, q_enn_both := (q_u5_both * prob_enn_both)]
dt[, q_lnn_both := (q_u5_both * prob_lnn_both) / ((1-q_enn_both))]
dt[, q_pnn_both := (q_u5_both * prob_pnn_both) / ((1-q_enn_both)*(1-q_lnn_both))]
dt[, q_inf_both := (q_u5_both * prob_inf_both)]
dt[, q_ch_both  := (q_u5_both * prob_ch_both)  / ((1-q_enn_both)*(1-q_lnn_both)*(1-q_pnn_both))]

# reshape long by sex again
dt <- melt(dt, id.vars=c('ihme_loc_id','year','simulation','birth_sexratio'))
dt <- dt[variable %like% 'q_']
dt[, age:=tstrsplit(variable,'_',keep=2)]
dt[, sex:=tstrsplit(variable,'_',keep=3)]
dt[, variable:=paste0('q_',age)]
dt <- dcast(dt, ihme_loc_id+year+simulation+birth_sexratio+sex~variable, value.var='value')

# save sims ---------------------------------------------------------------

dt_sims <- dt[,.(ihme_loc_id, year, sex, simulation, q_enn, q_lnn, q_pnn, q_inf, q_ch, q_u5)]

count <- nrow(dt_sims[q_enn>0.99 | q_lnn>0.99 | q_pnn>0.99 | q_inf>0.99 | q_ch>0.99, ])
message(paste0(count, ' rows where a qx draw is greater than 0.99'))
dt_sims[q_enn>0.99, q_enn:=0.99]
dt_sims[q_lnn>0.99, q_lnn:=0.99]
dt_sims[q_pnn>0.99, q_pnn:=0.99]
dt_sims[q_inf>0.99, q_inf:=0.99]
dt_sims[q_ch>0.99, q_ch:=0.99]

# save sims
write.csv(dt_sims, paste0(output_dir,'/draws/',loc_id,'.csv'), row.names=F)

# collapse ----------------------------------------------------------------

# collapse over sim
byvars <- c('ihme_loc_id','year','sex')
ages <- c('enn','lnn','pnn','inf','ch','u5')
for(aa in ages){
  dt[, temp_m:=mean(get(paste0('q_',aa))), by=byvars]
  dt[, temp_l:=quantile(get(paste0('q_',aa)),0.025), by=byvars]
  dt[, temp_u:=quantile(get(paste0('q_',aa)),0.975), by=byvars]
  setnames(dt, c('temp_m','temp_l','temp_u'),
           c(paste0('q_',aa,'_med'), paste0('q_',aa,'_lower'),paste0('q_',aa,'_upper')))
}
dt <- unique(dt, by=byvars)
dt[, c('q_enn','q_lnn','q_pnn','q_inf','q_ch','q_u5','simulation'):=NULL]

dt <- dt[order(ihme_loc_id, sex, year)]
write.csv(dt, paste0(output_dir,'/summary/',loc_id,'.csv'), row.names=F)
