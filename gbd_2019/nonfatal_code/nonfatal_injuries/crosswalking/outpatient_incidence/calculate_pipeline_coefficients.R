bundle <- commandArgs()[6]
name <- commandArgs()[7]

repo_dir <- 'FILEPATH'
source(paste0(repo_dir, 'predict_mr_brt_function.R'))
source(paste0(repo_dir, 'check_for_preds_function.R'))
source(paste0(repo_dir, 'load_mr_brt_preds_function.R'))
source('FILEPATH/get_age_metadata.R')

ages <- get_age_metadata(age_group_set_id=12, gbd_round_id=6)

ages <- ages[,1:3]
colnames(ages) <- c('age_group_id', 'age_start', 'age_end')
ages$age_end <- ifelse(ages$age_end == 125, 99, ages$age_end)
ages$age_midpoint <- (ages$age_start + ages$age_end) / 2

fit1 <-
  readRDS(
    paste0(
      'FILEPATH',
      as.character(bundle),
      '_',
      name,
      '.rds'
    )
  )

pred1 <- predict_mr_brt(fit1, newdata = ages)
preds <- pred1$model_summaries
preds$out_coeff <- exp(preds$Y_mean)

preds <- preds[,c('X_age_midpoint', 'out_coeff')]
colnames(preds) <- c('age_midpoint', 'out_coeff')
preds[3, 1] <- ages[3, 4]

final <- merge(preds, ages[,c('age_group_id', 'age_start', 'age_midpoint')])

write.csv(final, paste0('FILEPATH', name, '.csv'), row.names = F)


