library(haven)
library(data.table)
library(ggplot2)
library(ggrepel)
library(plyr)

library(mortdb)

rm(list=ls())

locs <- get_locations()

new_version <- x
old_version <- x

# Read in old and new versions
ddm_years <- setDT(read_dta(paste0('FILEPATH')))

new <- setDT(read_dta(paste0('FILEPATH')))
old <- setDT(read_dta(paste0('FILEPATH')))

new <- ddply(new, 'ihme_loc_id', function(x, check_dt) {
  x <- setDT(x)
  check_dt <- setDT(check_dt)
  loc_id <- x[1, ihme_loc_id][1]
  years <- union(unique(check_dt[ihme_loc_id == loc_id, year1]), 
                 unique(check_dt[ihme_loc_id==loc_id, year2]))
  
  return(x[year %in% years])
}, check_dt = ddm_years)

new <- setDT(new)

# Only plot countries with VR data, both sexes
new <- new[, if(any(source=="VR")) .SD, by='ihme_loc_id']
new <- new[sex=='both']

# Merge
combined <- merge(new, old, by=c('ihme_loc_id', 'sex', 'year', 'source'), all.x=T)

# drop where compx or compy is na
combined <- combined[!is.na(final_comp.x) & !is.na(final_comp.y)]

# Add in relative difference
combined[, rel_diff := abs(final_comp.x - final_comp.y) / final_comp.y]

# merge on region name
combined <- merge(combined, locs[, list(ihme_loc_id, super_region_name)], by='ihme_loc_id', all.x=T)
combined[is.na(super_region_name), super_region_name := 'South Asia']

# separate vr/dsp/srs/mccd from others
combined[, source_id := ifelse(source %in% c('VR', 'DSP', 'SRS', 'MCCD'), 1, 2)]

# Mark points where one >50%, one <50%
combined[, fifty := ifelse((final_comp.x>0.5 & final_comp.y<0.5) | (final_comp.x<0.5 & final_comp.y>0.5), 1, 0)]

# Mark points where one >95, one<95
combined[, ninetyfive := ifelse((final_comp.x>0.95 & final_comp.y<0.95) | (final_comp.x<0.95 & final_comp.y>0.95), 1, 0)]

# rank top 5 relative difference
combined[, rank := frank(-rel_diff, ties.method='first'), by=c('fifty', 'ninetyfive', 'source_id')]

pdf("FILEPATH", width=10)

for (i in c(1,2)) {
  source_type <- ifelse(i==1, "VR/DSP/SRS/MCCD Sources", "Other Sources")
  p <- ggplot(data=combined[source_id==i], aes(x=final_comp.x, y=final_comp.y)) +
    theme_bw() +
    geom_point() +
    xlab(paste0("GBD 2019 Final Estimate, Run ", new_version)) +
    ylab(paste0("GBD 2019 Final Estimate, Run ", old_version)) +
    ggtitle(paste0("Comparison v", new_version, " to v", old_version, ", Global, Both Sexes, ", source_type)) +
    geom_abline(intercept = 0, slope = 1) +
    geom_hline(yintercept=0.95) +
    geom_vline(xintercept=0.95) +
    # xlim(0.2, 1.5) +
    # ylim(0.2, 1.5) +
    geom_label_repel(aes(label=ifelse(rank < 20, paste0(ihme_loc_id, ", ", year, ", ", source), "")),
                     box.padding = 0.35,
                     point.padding = 1,
                     force = 3,
                     size = 3,
                     segment.color = 'grey50')
  plot(p)
}

dev.off()



#### Point estimate comparison ####
pt_new <- setDT(read_dta(paste0("FILEPATH")))
pt_old <- setDT(read_dta(paste0("FILEPATH")))

points_master <- merge(pt_new, pt_old, by=c('iso3_sex_source', 'year', 'detailed_comp_type', 'ihme_loc_id', 'nid', 'underlying_nid', 'year1', 'year2'), suffixes=c('_new', "_old"))
points_master <- points_master[comp_new < 6]

points_master[, pct_diff := abs(comp_new - comp_old) / comp_old]
points_master[, err_rank := frank(-pct_diff, ties.method = 'first')]

points_master[, label := ifelse(err_rank < 30, paste0(iso3_sex_source, ", ", year), "")]

colors <- list("u5"='red', "ggb"='green', "seg"='blue', "ggbseg"='purple', "CCMP_aplus_no_migration"='turquoise3', "CCMP_aplus_migration"='magenta')

pdf("FILEPATH")
plot(
  ggplot(data=points_master, aes(x=comp_new, y=comp_old, color=detailed_comp_type)) +
    scale_colour_manual(values=colors) +
    geom_point(size=.3) +
    theme_bw() +
    xlab(paste0("DDM Version ", new_version)) +
    ylab(paste0("DDM Version ", old_version)) +
    ggtitle(paste0("Comparison of DDM Point estimates, v", new_version, " to v", old_version)) +
    geom_label_repel(aes(label=label),
                     box.padding = 0.35,
                     point.padding = 1,
                     force = 3,
                     size = 3,
                     segment.color = 'grey50')
)
dev.off()

