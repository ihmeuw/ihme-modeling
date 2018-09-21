####
# Preprocess CBH data, and tabulate, prepare for mapping

######################################################################
# pull data at the individual child level
cbh<-fread(paste0(datadrive,'Africa_CBH_1998_2016','.csv'),stringsAsFactors = FALSE)

# load IHME region list for Africa
ihme_regions <- read.csv('<<<< FILEPATH REDACTED >>>>>/ihme_regions.csv',
                        stringsAsFactors = FALSE)

# define date in final month of period estimates
# 4x 5-year periods centred on 2000, 2005, 2010, 2015
# so the final period is 2013-2017 (inclusive),
# the final date should be in December 2017
period_end <- as.Date('2017-12-01')
periods <- data.frame(period = 1:4,
                      year = c(2015, 2010, 2005, 2000))
# define numbers of periods and age bins
n_period <- nrow(periods)
width <- 60
n_age <- 4



######################################################################
# Some final cleaning details here

# Most surveys ask for AOD in years after 24mo.
# Add 6 months, If death fell in year of survey then use halfway mark between age and DOS
cbh$aod_orig <- cbh$child_age_at_death_months
cbh[,marked := child_age_at_death_months %in% c(24,36,48) &
                              (age_of_death_units == 3 | is.na(age_of_death_units))]
cbh$child_age_at_death_months[cbh$marked] <- cbh$child_age_at_death_months[cbh$marked] + 6
cbh[,marked2 := child_age_at_death_months > birthtointerview_cmc & marked]
cbh$marked2[is.na(cbh$marked2)]=FALSE
cbh$child_age_at_death_months[cbh$marked2] <- (cbh$aod_orig[cbh$marked2]) +
      round((cbh$birthtointerview_cmc[cbh$marked2] - cbh$aod_orig[cbh$marked2])/2,0)


# and remove NAs.
nrow(cbh)
cbh[,isnabti := is.na(cbh$birthtointerview_cmc),]
cbh[,isnacad := is.na(cbh$child_age_at_death_months),]
tmp <- cbh[child_alive=='No',]

# check data here
cbind(table(tmp$nid,tmp$isnacad),table(tmp$nid,tmp$isnacad)[,2]/(table(tmp$nid,tmp$isnacad)[,1]+table(tmp$nid,tmp$isnacad)[,2]))

# check data here
cbind(table(tmp$nid,tmp$isnabti),table(tmp$nid,tmp$isnabti)[,2]/(table(tmp$nid,tmp$isnabti)[,1]+table(tmp$nid,tmp$isnabti)[,2]))

cbh <- cbh[!is.na(cbh$child_age_at_death_months), ]
nrow(cbh)


# remove records with no birth to interview time
nrow(cbh)
cbh <- cbh[!is.na(cbh$birthtointerview_cmc), ]
nrow(cbh)

# make unique survey ID, used later
cbh$survey_id <- cbh$nid

# make unique cluster IDs
nrow(cbh)
# dropping places with no location IDs (ie no pt or shapefile),
cbh   <- cbh[!is.na(cbh$point),]
cl_id <- rep(NA, nrow(cbh))
pts   <- cbh$point == 1
nrow(cbh)

# if a point, the survey ID and cluster number are the id
cl_id[pts] <- idx(cbh$survey_id[pts], cbh$cluster_number[pts])

# else, the survey ID, shapefile, and location code are the id
cl_id[!pts] <- idx(cbh$survey_id[!pts], cbh$shapefile[!pts], cbh$location_code[!pts])

# make it different for points and polygons
cbh$cluster_id <- idx(cbh$point, cl_id)

# if cluster_id is just an actual cluster, do not weight.
cbh$weight[cbh$point==1] <- 1

if(mean(sort(unique(cbh$cluster_id))==unique(cbh$cluster_id))!=1) stop('SORT THE CLUSTER IDS!')

# Period Tabulate wont find decimal months
cbh$child_age_at_death_months <- floor(cbh$child_age_at_death_months)

# ONE survey with no weight info (GF ETHIOPIA SURVEY)
cbh$weight[is.na(cbh$weight)]=1

######################################################################
# get monthly period mortality rates within each fraction
# from CBH data, using the adapted IHME method
stopifnot(packageVersion('seegMBG')=='0.1.2')
# tabulate monthly mortality rates
pt <- periodTabulate_w(age_death          = cbh$child_age_at_death_months,
                          birth_int       = cbh$birthtointerview_cmc,
                          cluster_id      = cbh$cluster_id,
                          pw              = cbh$weight,
                          # define windows for tabulation
                          windows_lower   = c(0, 1, 12, 36),
                          windows_upper   = c(0, 11, 35, 59),
                          period          = width,
                          nperiod         = n_period,
                          period_end      = period_end,
                          interview_dates = cmc2Date(cbh$interview_date_cmc),
                          # IHME's monthly estimation method
                          method          = 'monthly',
                          cohorts         = 'one',
                          inclusion       = 'enter',
                          # monthly mortality rates within each bin
                          mortality       = 'monthly',
                          n_cores         = 1,
                          verbose         = TRUE)

cbh_sry <- data.table(pt)

# get info back from cbh dt and merge it on
first <- function(x) x[1]

info <- cbh[, .(latitude       = first(latnum),
                longitude      = first(longnum),
                shapefile      = first(shapefile),
                location_code  = first(location_code),
                nid            = first(nid),
                source         = first(source),
                country        = first(country)),
              by = .(cluster_id)]

cbh_sry <- merge(cbh_sry,info,by='cluster_id',all=TRUE)

# to make distinct from imputed sbh data.
cbh_sry[,data_type := 'cbh']
cbh_sry[,sbh_wgt := 1]

# put data in terms of year not period
cbh_sry$year   <- periods$year[match(cbh_sry$period,periods$period)]
cbh_sry$period <- NULL

# no need for observations with no exposure months
cbh_sry <- subset(cbh_sry, exposed != 0)

# save to disk
dir.create(paste0(sharedir,'/input/dated/',run_date,'/'))
write.csv(cbh_sry,
            file = paste0(sharedir,'/input/dated/',run_date,'/mortality_cbh_w2.csv'),
            row.names = FALSE)
write.csv(cbh_sry,
            file = paste0(sharedir,'/input/mortality_cbh_w2.csv'),
            row.names = FALSE)


#
