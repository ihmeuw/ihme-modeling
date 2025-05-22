#################################
## Purpose: Create location specific plots
## Details: Reads in outputs from plotting_old_new
##          Creates a single file for all age groups in 
##          a given location
#################################

library(argparse)
library(data.table)
library(ggplot2)
library(mortcore)
library(mortdb)
library(stringr)

if (interactive()) {
  version1 <- 'Recent run id'
  version2 <- 'Bested run id'
  version3 <- 'Bested run id: previous GBD round'
  loc <- 'input location'
  gbd_year <- 2022
} else {
  parser <- argparse::ArgumentParser()
  parser$add_argument('--loc', type = 'character')
  parser$add_argument('--version1', type = 'integer')
  parser$add_argument('--version2', type = 'integer')
  parser$add_argument('--version3', type = 'integer')
  parser$add_argument('--gbd_year', type = 'integer')
  args <- parser$parse_args()
  loc <- args$loc
  version1 <- args$version1
  version2 <- args$version2
  version3 <- args$version3
  gbd_year <- args$gbd_year
}

username <- Sys.getenv('USER')
root <- 'FILEPATH'

input_dir <- "FILEPATH"
output_dir <- "FILEPATH"

loc_map <- fread('FILEPATH/loc_map.csv')

g <- fread(paste0(input_dir, '/plotting_data.csv'))
params <- fread(paste0(input_dir, '/plotting_params.csv'))
adj <- fread(paste0(input_dir, '/plotting_adj.csv'))

## Setting data point colors
cols <- rep(NA, 25)
cols[(grep('cbh', sort(unique(g[version == version1]$source_type)), 
           ignore.case = T))] <- 'cyan'
cols[(grep('sbh', sort(unique(g[version == version1]$source_type)), 
           ignore.case = T))] <- 'lightsteelblue1'
cols[(grep('vr', sort(unique(g[version == version1]$source_type)), 
           ignore.case = T))] <- c('slateblue1', 'slateblue4', 'mediumorchid1', 'mediumorchid4')
cols[(grep('srs', sort(unique(g[version == version1]$source_type)), 
           ignore.case = T))] <- 'yellow'
cols[(grep('dhs', sort(unique(g[version == version1]$source_type)), 
           ignore.case = T))] <- 'orange'
cols[which(!grepl('vr|cbh|sbh|srs|dhs', sort(unique(g[version == version1]$source_type)), 
                  ignore.case = T))] <-c('royalblue1')


colsnames <-  sort(unique(g[version == version1]$source_type))
colsnames <- colsnames[!is.na(colsnames)]
names(cols) <- colsnames
colfScale <- scale_fill_manual(name = '', values = cols)

## setting shaped based on spilt sbh and outliered data
shs <- c(22 ,21, 21, 24, 24)
shsnames <- c('outliered_data', 'adjusted_asfr_data', 'asfr_data', 'split_sbh',
              'adjusted_split_sbh')
names(shs) <- shsnames
shapescale <- scale_shape_manual(name = '', values = shs)

scale_values <- c('New'='#F8766D', 'Previous'='#7CAE00', 'GBD2021'='black',
                  'WPP' = '#00BFC4')
names(scale_values)[1] <- paste0(version1)
names(scale_values)[2] <- paste0(version2)
names(scale_values)[3] <- paste0(version3)

scale_labels <- c('New'='New', 'Previous'=version2, 'GBD2021'='GBD2021',
                  'WPP'='WPP')
names(scale_labels)[1] <- paste0(version1)
names(scale_labels)[2] <- paste0(version2)
names(scale_labels)[3] <- paste0(version3)

pdf(paste0(output_dir, loc, '_', version1, '.pdf'), width=15, height=10)
for(ages in c('tfr', seq(10,50,5))){
  temp <- g[ihme_loc_id == loc & age == ages]
  temp_params <- params[ihme_loc_id == loc & age == ages]
  
  ### FORMAT TITLE ###
  if (unique(temp$level) == 5) {
    parent_name <- loc_map[location_id == unique(temp$parent_id), location_name]
    loc_name <- paste(unique(temp$ihme_loc_id), 
                      unique(temp$location_name), 
                      parent_name, 
                      unique(temp[!is.na(region_name)]$region_name), sep =', ')
  } else {
    loc_name <- paste(unique(temp$ihme_loc_id), 
                      unique(temp$location_name), 
                      unique(temp[!is.na(region_name)]$region_name), sep=', ')
  }
  
  beta1 <- temp_params[version == version1]$beta
  beta2 <- temp_params[version == version2]$beta
  zeta1 <- temp_params[version == version1]$zeta
  zeta2 <- temp_params[version == version2]$zeta
  scale1 <- temp_params[version == version1]$scale
  scale2 <- temp_params[version == version2]$scale
  dd_score1 <- round(temp_params[version == version1]$dd, 2)
  dd_score2 <- round(temp_params[version == version2]$dd, 2)
  mse1 <- round(temp_params[version == version1]$mse, 4)
  mse2 <- round(temp_params[version == version2]$mse, 4)
  mse3 <- round(temp_params[version == version3]$mse, 4)
  
  title_string <-  paste0('ASFR, Age ',ages, ', ', loc_name,
                          '\n beta: ', beta1, ', (', version2, ' beta: ', beta2,
                          '), zeta: ', zeta1, ', (', version2, ' zeta: ', zeta2,
                          '), scale: ', scale1, ', (', version2, ' scale: ', scale2,
                          '), data density score: ', dd_score1, ', (', version2, 
                          ' data density score: ', dd_score2,
                          ') \n', version1, ' mse: ', mse1, ', ', 
                          version2, ' mse: ', mse2, ', ',
                          version3, ' mse: ', mse3)
  
  ### FORMAT CAPTION ###
  adj_loc <- adj[ihme_loc_id == loc & age == ages]
  adj_loc <- unique(adj_loc[, .(source_type, adjustment_factor)])
  caption_string <- list()
  for(sources in unique(adj_loc$source_type)){
    source_type <- adj_loc$source_type[adj_loc$source_type == sources]
    adjust <- adj_loc$adjustment_factor[adj_loc$source_type == sources]
    string <- paste0('source: ', sources, ', source type: ', source_type, ', adjustment: ', round(adjust, 3))
    caption_string[[paste0(sources)]] <- str_pad(string, width = 110, side='right')
  }
  caption_string3 <- paste(paste(paste0(unique(unlist(caption_string)),sep = c(' ', ' ', '\n')), sep = ''), collapse='')
  
  temp[type %in% c('stage1_pred', 'stage2_pred', 'loop2', 'unraked'), estimate_type := type]
  
  ### PLOT ESTIMATES ##
  p <- ggplot(data= temp, aes(x = year_id, y = asfr, color = version, linetype = estimate_type)) +
    geom_line(data= temp[(type =='stage1_pred' | type == 'stage2_pred' | type == 'loop2' | type == 'unraked')], aes(linetype = estimate_type)) +
    geom_ribbon(data=temp[(version == version1 & type == 'loop2')], aes(ymin = lower, ymax = upper), fill = '#F8766D', alpha = 0.25, color = NA) +
    geom_ribbon(data=temp[(version == version3 & type == 'loop2')], aes(ymin = lower, ymax = upper), fill = 'gray', alpha = 0.25, color = NA) +
    scale_color_manual(values = scale_values, 
                       labels = scale_labels) +
    scale_x_continuous(breaks = seq(1950, 2020, 10), expand = c(0.01, 0.01)) +
    theme_bw() + theme(plot.caption = element_text(size = 8, hjust = 0)) +
    labs(title = title_string, caption = caption_string3, 
         linetype = 'Estimate Stage', color = 'Version', x = 'Year', y = 'ASFR')
  
  ### PLOT DATA ###
  if(nrow(temp[!is.na(asfr)]) > 0){
    p <- p +
      geom_point(data = temp[version == version1 & (type == 'adjusted_asfr_data' | type == 'outliered_data' | type == 'adjusted_split_sbh')], aes(fill = source_type, shape = type), color = 'black', size = 4) +
      geom_point(data = temp[version == version1 & (type == 'asfr_data' | type == 'split_sbh')], aes(fill = source_type, shape = type), color = 'black', alpha = 0.3, size = 4) +
      geom_point(data = temp[version == version1 & (type == 'adjusted_asfr_data' | type == 'adjusted_split_sbh') & reference == 1], shape = 5, size = 5.5, color = 'black', stroke = 1.5) +
      colfScale +
      scale_shape_manual(name = 'Data Type', values = shs) +
      guides(fill = guide_legend(override.aes = list(shape = 21)), 
             linetype = guide_legend(override.aes = list(shape = NA, fill = NA))) + 
      theme_bw()
  }
  print(p)
}

dev.off()
