
library(data.table)
library(haven)
library(mortdb, lib.loc = FILEPATH)
library(ltcore, lib.loc = FILEPATH)


# Set parameters here
gbd_year <- 2020
end_year <- 2022


natlocs <- get_locations()
pop <- get_population()

# Look for locations where pop > 5 million
eligible_locs <- pop[mean > 5000000, location_id]

# Pull qx, mx, ax values, sex/age specific, for eligible locations and ages
ages <- get_age()

lifetable <- get_lifetable(eligible_locs)

lifetable <- dcast(lifetable, sex+year_id+location_id+location_name + age_group_id+age_group_years_start+age_group_years_end ~ life_table_parameter_id, value.var='mean')

# For each age group, find the minimum value of qx
tmrlt <- lifetable[age_group_id != 148, .SD[which.min(`3`)], by=c('age_group_id')]

# Exception: for terminal age, find highest ax instead
terminal <- lifetable[age_group_id==148, .SD[which.max(`2`)], by='age_group_id']
tmrlt <- rbind(tmrlt, terminal, use.names=T)

# rename params for clarity
setnames(tmrlt, c("1", "2", "3"), c("mx", "ax", "qx"))

# Step2: Fill out other life table params
# Preliminary LTs - pre ax correction
# Only need age sex and mean, and order by age start
tmrlt_preax <- tmrlt[order(age_group_years_start), .(age_group_id, age_group_years_start, age_group_years_end, sex, mx, ax, qx)]

# Generate age gap
tmrlt_preax[, age_gap := age_group_years_end - age_group_years_start]

# Generate starting lx
tmrlt_preax[1, lx := 1]

# And the rest iteratively
for (i in 2:nrow(tmrlt_preax)) {
  tmrlt_preax[i, lx := tmrlt_preax[i-1, lx*(1-qx)]]
}

# Calc dx
tmrlt_preax[, dx := lx*qx]
# Find Lx - capital L
tmrlt_preax[, Lx := shift(lx, type='lead', n=1)*age_gap + dx*ax]
tmrlt_preax[age_group_id==148, Lx := ax*lx] # Calc separately for terminal group

# Find Tx
tmrlt_preax[, Tx := rev(cumsum(rev(Lx)))]

# Find Ex - final param (except mx)
tmrlt_preax[, Ex := Tx/lx]

# Iteratively calculate ax
num_ages <- length(tmrlt$age_group_id)
cats = 1
stan = .0001
iter = 20

tmrlt_postax <- copy(tmrlt_preax)

while((cats <= iter) & (stan >= .0001)) {

  tmrlt_postax <- tmrlt_postax[order(age_group_years_start)]
  tmrlt_postax[, cax := ax]

  for (i in 4:(num_ages - 1)) {
    tmrlt_postax[i, cax := ((-5/24) *  tmrlt_postax[i-1, dx] + 2.5*dx + (5/24)* tmrlt_postax[i+1, dx]) / dx]
  }
  tmrlt_postax[, diffax := abs(ax-cax)]
  stan <- sum(tmrlt_postax$diffax, na.rm=T)
  cats <- cats + 1
  tmrlt_postax[4:(num_ages - 1), ax := cax]

}

# Bring in empirical values for older ages
emp_values <- setDT(read_dta(FILEPATH))

# refactor the sex variable
tmrlt_postax[, sex := tolower(sex)]


# Merge on empirical values
older_ages <- merge(tmrlt_postax, emp_values, by.x=c('sex', 'age_group_years_start'), by.y=c('sex', 'age'), all.x=T)
older_ages[, qx_2 := qx^2]


older_ages[!is.na(par_con) & age_group_years_start > 75, ax := par_qx * qx + par_sqx*qx_2 + par_con]

older_ages <- older_ages[, .(age_group_years_start, mx, ax)]

# For younger ages, use mx .107 cutoff
mx_min <- older_ages[age_group_years_start == 0, mx]

if (mx_min >= .107) {
  older_ages[1, ax := .34]
  older_ages[2, ax := 1.3565]
} else {
  older_ages[1, ax := .049+2.742*mx_min]
  older_ages[2, ax := 1.5865 - 2.167*mx_min]
}

older_ages[, mx := NULL]

ax_corrected <- merge(tmrlt_preax, older_ages, by='age_group_years_start', suffixes=c('_precorr', '_postcorr'))

# Use corrected ax to find Lx
ax_corrected[, Lx := shift(lx, type='lead') * age_gap + dx*ax_postcorr]
ax_corrected[age_group_id==148, Lx := lx/mx]


# Recalculate Tx/Ex
ax_corrected[, Tx := rev(cumsum(rev(Lx)))]
ax_corrected[, Ex := Tx/lx]

final_abridged_tmrlt <- ax_corrected[, .(age_group_years_start, sex, mx, ax=ax_postcorr, qx, lx, Tx, nLx = Lx, dx, ex = Ex)]

# Interpolation

interpolated_tmrlt <- approx(final_abridged_tmrlt$age_group_years_start, final_abridged_tmrlt$ex, xout=seq(0,105,.01))
final_tmrlt <- data.table(
  precise_age = round(interpolated_tmrlt$x,2),
  mean = interpolated_tmrlt$y,
  estimate_stage_id = 6,
  life_table_parameter_id = 5
)

final_abridged_tmrlt[, c('sex','ex') := NULL]
final_abridged_tmrlt <- melt(final_abridged_tmrlt, id.vars = 'age_group_years_start', variable.name = 'parameter_name', value.name = 'mean')

# merge on parameter ids
lt_ids <- get_mort_ids(type='life_table_parameter')
final_abridged_tmrlt <- merge(final_abridged_tmrlt, lt_ids[, .(life_table_parameter_id, parameter_name)], by='parameter_name')

# Format and bind on the interpolated table
tmrlt_for_upload <- final_abridged_tmrlt[, .(precise_age = age_group_years_start, mean, estimate_stage_id = 6, life_table_parameter_id)]
tmrlt_for_upload <- rbind(tmrlt_for_upload, final_tmrlt, use.names=T)

upload_results(tmrlt_for_upload)