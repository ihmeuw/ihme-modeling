# this function gives all the age.cause pairs currently used
# it assumes the full age range, but if you set that opt to F, it will return a lite version
ageCauseLister <- function(cause.code = c("cvd_ihd", "cvd_stroke", "neo_lung", "resp_copd", "lri","t2_dm"),
                           gbd.version = "GBD2015",
                           lri.version = NA,
                           full.age.range = T) {

  age.cause <- NULL

  for (cause.code in c("cvd_ihd", "cvd_stroke", "neo_lung", "resp_copd", "lri", "t2_dm")) {

    if (cause.code %in% c("cvd_ihd", "cvd_stroke")) {

      # CVD and Stroke have age specific results
      ages <- c(25, 50, 80)
      if (gbd.version=="GBD2015")all.ages <- seq(25, 80, by=5) # CVD and Stroke have age specific results
      if (gbd.version=="GBD2016")all.ages <- seq(25, 95, by=5) # CVD and Stroke have age specific results
      if (gbd.version=="GBD2017")all.ages <- seq(25, 95, by=5) # CVD and Stroke have age specific results

    } else {

      # LRI, COPD and Lung Cancer all have a single age RR (though they reference different ages...)
      ages <- c(99)
      all.ages <- c(99)
      if (cause.code=="lri" & lri.version=="split") {
        all.ages <- c(1, 25)
        ages <- c(1,25)
      }
    }

    if (full.age.range == T) ages <- all.ages

    for (age.code in ages) {

      age.cause <- rbind(age.cause, c(cause.code, age.code))

    }
  }

  return(age.cause)

}

# this function takes PAFs or RRs that are not age-specific and expands them to the appropriate ages so that they can be merged onto burden
expandAges <- function(input.table, cause.code,
                       gbd.version = "GBD2020") {

  # Take out this cause
  temp <- input.table[cause == cause.code, ]

  if (cause.code %in% c("cvd_ihd", "cvd_stroke")) {

    temp[, age_group_id := as.numeric(age)]

    # switch the ages from age_code to new age_group_id
    if (gbd.version=="GBD2015") {
      age.codes <- seq(25, 80, by=5)
      age.ids <- c(10:21)
    } else {
      age.codes <- seq(25, 95, by=5)
      age.ids <- c(10:20, 30:32, 235)
    }

    # then pass to your custom function
    temp <- findAndReplace(temp,
                           age.codes,
                           age.ids,
                           "age_group_id",
                           "age_group_id")

  } else {

    if (gbd.version=="GBD2015") {
      # Add back in with proper ages
      if (cause.code == "lri") age.ids <- c(2:21) # LRI is now being calculated for all ages based off the input data for LRI and smokers
      if (cause.code %in% c("neo_lung", "resp_copd")) age.ids <- c(10:21) # others are between 25 and 80
    } else if(gbd.version=="GBD2020"){
      # Add new <5 age groups for LRI
      # Add back in with proper ages
      if (cause.code == "lri") age.ids <- c(2:3,6:20,30,31,32,34,235,238,388,389) #add in 388, 389, 238, 34 for the new <5 age groups!
      if (cause.code %in% c("neo_lung", "resp_copd","t2_dm")) age.ids <- c(10:20, 30:32, 235) # others are between 25 and 80
    }else {
      # Add back in with proper ages
      if (cause.code == "lri") age.ids <- c(2:20, 30:32, 235) # LRI is now being calculated for all ages based off the input data for LRI and smokers
      if (cause.code %in% c("neo_lung", "resp_copd","t2_dm")) age.ids <- c(10:20, 30:32, 235) # others are between 25 and 80
    }

    temp <- lapply(age.ids, function(age.id) temp[, age_group_id := age.id] %>% copy) %>% rbindlist

    if(cause.code=='lri') {

      lri.threshold <- 10
      temp <- temp[(grouping=="child" & age_group_id < lri.threshold |
                    grouping!="child" & age_group_id >= lri.threshold)]

      #now split the child grouping into male children and female children (with same val)
      splitSex <- function(sex, dt) {

        out <- copy(dt)
        out[, grouping := sex]
        return(out)

      }

      split <- lapply(c('male', 'female'), splitSex, dt=temp[age_group_id < lri.threshold]) %>%
        rbindlist

      temp <- list(temp[grouping!="child"], split) %>% rbindlist

    }

  }

  #add sex ID
  temp[grouping=="male", sex_id := 1]
  temp[grouping=="female", sex_id := 2]

  return(temp)

}

