pdf('FILEPATH')
bad_bundles <- c(127,128,201,202,206,207,210,3065,3137,336,436,437,60,610,638)
#for(bundle in unique(df$bundle_id)){
for(bundle in unique(df$bundle_id)){
    c <- tryCatch({
    cfs <- c('cf1', 'cf2', 'cf3')
    bun_df <- fread(FILEPATH)
    train_data <- fread(paste0(FILEPATH, bundle, FILEPATH))
    min_age <- min(train_data[w == 1]$age_start)
    max_age <- max(train_data[w == 1]$age_start)
    a <- tryCatch(all_cfs <- rbindlist(lapply(cfs, function(cf){
      #print(cf)
      dt <- fread(paste0(FILEAPTH))
      dt[, cf_num := cf]
      dt[cf_num == 'cf1', Y_mean := inv.logit(Y_mean)][cf_num == 'cf1', Y_mean_lo := inv.logit(Y_mean_lo)][cf_num == 'cf1', Y_mean_hi := inv.logit(Y_mean_hi)]
      dt[cf_num != 'cf1', Y_mean := exp(Y_mean)][cf_num != 'cf1', Y_mean_lo := exp(Y_mean_lo)][cf_num != 'cf1', Y_mean_hi := exp(Y_mean_hi)]
      dt[, bundle_id := bundle]
      return(dt)
    }), fill = TRUE))
    train_data[w < 0.5, trimmed := 'Trimmed data'][w >= 0.5, trimmed := 'Untrimmed data']
    train_data[sex_id == 1, sex := 'Males'][sex_id == 2, sex := 'Females']
    data_plot <- ggplot(train_data, aes(x = age_start, y = exp(log_mean), color = factor(trimmed))) + 
      geom_point() + theme_bw() +
      ylab('GBD 2019 CF3 input data') + xlab('Age') + 
      ggtitle(paste0(bun_df[bundle_id == bundle]$bundle_name, ' input data')) +
      scale_color_manual(name = '', values = c('#33cc33', '#D354AC')) +
      facet_wrap(~sex, ncol = 2) +
      xlim(c(restrictions_map$min_age[1],restrictions_map$max_age[1])) +
      facet_wrap(~sex, ncol = 2*num_sex, scales = 'free_y')
    
    p <- ggplot(all_cfs[cf_num == 'cf3' | cf_num == 'cf2'], aes(x = X_age_start, y = Y_mean, color = cf_num)) +
      geom_point() + geom_line(aes(linetype = 'GBD 2019')) + 
      #geom_line(data = past_dt[cf_num == 'cf3' | cf_num == 'cf2'], aes(linetype = 'GBD 2017', x = age_start)) +
      geom_ribbon(aes(ymin = Y_mean_lo   , ymax = Y_mean_hi, fill = cf_num), alpha = 0.6) +
      #geom_ribbon(data = past_dt[cf_num == 'cf3'], aes(ymin = Y_mean_lo, ymax = Y_mean_hi, fill = cf_num, x = age_start), alpha = 0.6) +
      ylab('Correction factor value') + xlab('Age') +
      facet_wrap(~sex, scales = 'free') +
      theme_bw() +
      ggtitle(bun_df[bundle_id == bundle]$bundle_name) +
      scale_color_discrete(name = '') +
      scale_linetype_discrete(name = '')
    #xlim(c(min_age, max_age))
    grid.arrange(data_plot, p, nrow =  2)
    
  },
  error = function(cond){
    message(paste0(bundle, " broke"))
    return()
  })
  
}

