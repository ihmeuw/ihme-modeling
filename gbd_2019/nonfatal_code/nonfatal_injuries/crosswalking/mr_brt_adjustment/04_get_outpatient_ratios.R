rm(list = ls())

bundle <- commandArgs()[6]
print(bundle)

ci_data <-
  read.csv(
    paste0(
      'FILEPATH',
      as.character(bundle),
      '.csv'
    )
  )

if (bundle != 271) {
  ci_data <- subset(ci_data, location_id == 102)
} else {
  ci_data <- subset(ci_data, location_id %in% c(102, 93, 4914, 4911, 4918, 4915, 4928, 4912, 4921, 4923, 
                                                4926, 4913, 4910, 4920, 4922, 4917, 53432, 4927, 4919, 4916))
}

inpatient <-
  subset(
    ci_data,
    (clinical_data_type == 'inpatient') &
      (year_start %in% c(1993, 1998, 2003, 2008)),
    select = c(
      'location_id',
      'sex',
      'year_start',
      'year_end',
      'age_start',
      'age_end',
      'mean',
      'sample_size',
      'standard_error'
    )
  )
inpatient$year_id <- ifelse(inpatient$year_start == 1993,
                            1,
                            ifelse(
                              inpatient$year_start == 1998,
                              2,
                              ifelse(
                                inpatient$year_start == 2003,
                                3,
                                ifelse(inpatient$year_start == 2008, 4, NA)
                              )
                            ))
colnames(inpatient)[colnames(inpatient) == 'mean'] <-
  'inpatient_mean'
colnames(inpatient)[colnames(inpatient) == 'standard_error'] <-
  'inpatient_se'
inpatient[, c('year_start', 'year_end', 'sample_size')] <- NULL

outpatient <-
  subset(
    ci_data,
    (clinical_data_type == 'outpatient') &
      (year_start %in% seq(1993, 2013, 1)),
    select = c(
      'location_id',
      'sex',
      'year_start',
      'year_end',
      'age_start',
      'age_end',
      'mean',
      'sample_size'
    )
  )
outpatient$year_id <-
  ifelse(
    outpatient$year_start %in% seq(1993, 1997, 1),
    1,
    ifelse(
      outpatient$year_start %in% seq(1998, 2002, 1),
      2,
      ifelse(outpatient$year_start %in% seq(2003, 2007, 1), 3, 4)
    )
  )
outpatient$cases <- outpatient$mean * outpatient$sample_size
outpatient[, c('mean', 'year_start', 'year_end')] <- NULL
out_agg <-
  aggregate(. ~ location_id + sex + year_id + age_start + age_end, outpatient, sum)
out_agg$mean <- out_agg$cases / out_agg$sample_size
out_agg$standard_error <-
  ifelse(
    out_agg$cases < 5,
    ((5 - out_agg$mean * out_agg$sample_size) / out_agg$sample_size + out_agg$mean *
       out_agg$sample_size * sqrt(5 / out_agg$sample_size ^ 2)
    ) / 5,
    sqrt(out_agg$mean / out_agg$sample_size)
  )
colnames(out_agg)[colnames(out_agg) == 'mean'] <- 'outpatient_mean'
colnames(out_agg)[colnames(out_agg) == 'standard_error'] <-
  'outpatient_se'
out_agg[, c('cases', 'sample_size')] <- NULL

ratios <-
  merge(inpatient,
        out_agg,
        by = c('location_id', 'sex', 'year_id', 'age_start', 'age_end'))
ratios$ratio <- ratios$outpatient_mean / ratios$inpatient_mean
ratios$ratio_se <-
  sqrt((ratios$outpatient_mean ^ 2 / ratios$inpatient_mean ^ 2) * (
    ratios$outpatient_se ^ 2 / ratios$outpatient_mean ^ 2 + ratios$inpatient_se ^
      2 / ratios$inpatient_mean ^ 2
  )
  )

write.csv(
  ratios,
  paste0(
    'FILEPATH',
    as.character(bundle),
    '.csv'
  ),
  row.names = FALSE
)


