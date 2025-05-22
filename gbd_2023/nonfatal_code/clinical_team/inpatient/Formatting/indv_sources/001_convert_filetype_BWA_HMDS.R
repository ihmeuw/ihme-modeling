library(haven)
# convert .sav files to csv


types = c("IG", "IP", "ID", "OG", "OP", "OE")
# convert 2007 files

base = "FILEPATH"

dat = read_sav(paste0(base, "FILEPATH"))

dat = data.table(read_sav(paste0(base, "FILEPATH")))
write.csv(dat, paste0(base, "FILEPATH"), row.names = FALSE)

for (m in c("Mortality", "Morbidity")) {
    dat = data.table(read_sav(paste0(base, "FILEPATH", m, ".sav")))
    write.csv(dat, paste0(base,  "FILEPATH", m, ".csv"), row.names = FALSE)
}

# now outpatient
dat = data.table(read_sav(paste0(base, "OE2007/SPSS/OE2007.sav")))
write.csv(dat, paste0(base,  "OE2007/SPSS/OE2007.csv"), row.names = FALSE)

dat = data.table(read_sav(paste0(base, "FILEPATH")))             # maybe no ICD
dat = data.table(read_sav(paste0(base, "FILEPATH")))  # maybe no ICD
dat = data.table(read_sav(paste0(base, "FILEPATH")))     # includes ICD
dat = data.table(read_sav(paste0(base, "FILEPATH")))            # includes ICD
dat = data.table(read_sav(paste0(base, "FILEPATH")))  # includes ICD
dat = data.table(read_sav(paste0(base, "FILEPATH")))  # maybe no ICD

# TG there are only 3 years of this stuff, structure is so random you can't loop over it
base8 = "FILEPATH"


# IG looks good
dat = data.table(read_sav(paste0(base8, "FILEPATH")))
write.csv(dat, paste0(base8, "FILEPATH"))
hist(dat$AGE[dat$AGE<100])

dat = data.table(read_sav(paste0(base8, "FILEPATH")))
write.csv(dat, paste0(base8, "FILEPATH"))
hist(dat$AGE[dat$AGE<100])

# IP also ok
dat = data.table(read_sav(paste0(base8, "FILEPATH")))
write.csv(dat, paste0(base8, "FILEPATH"), row.names = FALSE)

# now Outpatient, first Outpatient Eye
dat = data.table(read_sav(paste0(base8, "FILEPATH")))
write.csv(dat, paste0(base8, "FILEPATH"), row.names = FALSE)

# OG has a ton of files, similar to 2007

# Outpatient Psychiatric
dat = data.table(read_sav(paste0(base8, "FILEPATH")))
write.csv(dat, paste0(base8, "FILEPATH"), row.names = FALSE)

# Inpatient 2009
base9 = "FILEPATH"
# Inpatient General 2009
dat = data.table(read_sav(paste0(base9, "FILEPATH")))
write.csv(dat, paste0(base9, "FILEPATH"))

dat = data.table(read_sav(paste0(base9, "FILEPATH")))
write.csv(dat, paste0(base9, "FILEPATH"))

