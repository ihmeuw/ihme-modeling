# Purpose: Chagas disease Population at risk modeling

# # Script purpose: Create a population at risk model based on PAHO estimates.
#### 
source("FILEPATH/get_draws.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/make_aggregates.R")
source("FILEPATH/get_age_weights.R")

summaries <- function(dt, draw_vars){
  draw_vars <- paste0("draw_", 0:999) #change to 999 if 1000draws
  sum <- as.data.table(copy(dt))
  sum[, mean := rowMeans(.SD), .SDcols = draw_vars]
  sum[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draw_vars]
  sum[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draw_vars]
  sum[, c(draw_vars) := NULL]
  return(sum)
}

####
#Read PaR data

par80 <- fread("FILEPATH")
par80<- subset(par80, !(is.na(percent)))
par80[, par_prop := percent/100]
par80[, par_count := as.numeric(par80$`Pop at Risk`)]
par80[, pop_paho := par_count/par_prop]
par80[, ihme_loc_id := par80$`Country ISO3 Code`]
par80[, year_id := par80$`Year Start`]

par80 <- subset(par80, select = c(ihme_loc_id, year_id, par_prop, par_count,  pop_paho))

par05 <- fread("FILEPATH")
par05<- subset(par05, !(is.na(par)))
par05[, par_count := as.numeric(par)]
par05[, par_prop :=par_count /pop_paho]
par05[, ihme_loc_id := par05$`Country ISO3 Code`]
par05[, year_id := par05$`Year Start`]

par05 <- subset(par05, select = c(ihme_loc_id, year_id, par_prop, par_count,  pop_paho))

par10 <- fread("FILEPATH")
par10<- subset(par10, !(is.na(pop_at_risk)))
par10[, pop_paho :=population]
par10[, par_count := as.numeric(pop_at_risk)]
par10[, par_prop :=par_count /pop_paho]
par10[, ihme_loc_id := par10$`Country ISO3 Code`]
par10[, year_id := par10$`Year Start`]

par10 <- subset(par10, select = c(ihme_loc_id, year_id, par_prop, par_count,  pop_paho))

par_data <- rbind(par80, par05, par10)

####
locs <- get_location_metadata(location_set_id = ADDRESS, release_id = ADDRESS)
locs <- subset(locs, (super_region_id == 103 | region_id == 96) & level >=3 )
locs_id <- locs$location_id


## Pull covariates , CSMR, urbanicity
chagas_deathsmale <- get_model_results(gbd_team = 'ADDRESS',
                                       gbd_id = ADDRESS,
                                       measure_id = 1, #deaths
                                       year_id = c(ADDRESS),
                                       location_id = locs_id,
                                       age_group_id = ADDRESS,
                                       release_id = ADDRESS,
                                       model_version_id = ADDRESS)

chagas_deathsfemale <- get_model_results(gbd_team = 'ADDRESS',
                                         gbd_id = ADDRESS,
                                         measure_id = 1, #deaths
                                         year_id = c(ADDRESS),
                                         location_id = locs_id,
                                         age_group_id = ADDRESS,
                                         release_id = ADDRESS,
                                         model_version_id = ADDRESS)


chagas_deaths <- rbind(chagas_deathsmale, chagas_deathsfemale)
chagas_deaths <- chagas_deaths[, lapply(.SD, sum), by = c('location_id', 'year_id', 'age_group_id' ),
                               .SDcols = c("mean_death", "population")]

chagas_deaths[, csmr := mean_death/population]


chagas_end <- subset(chagas_deaths, csmr>0 )
chagas_end <- unique(chagas_end$location_id)
## Urbanicity

urban <- get_covariate_estimates(covariate_id = ADDRESS,
                                 year_id = c(ADDRESS),
                                 location_id = locs_id,
                                 release_id = ADDRESS)
urban[ ,prop_urban := mean_value]
### Run model

par_data[ year_id < 1980, year_id := 1980]
par_data2 <- merge(par_data, locs, by = 'ihme_loc_id', all.x=T)
loc_par <- unique(par_data2$location_id)
par_data2 <- merge(par_data2, chagas_deaths, by = c('location_id', 'year_id' ), all.x=T)
par_data2 <- merge(par_data2, urban, by = c('location_id', 'year_id' ), all.x=T)
par_data2[, csmr :=  csmr*100]

par_data2 <- subset(par_data2, location_id %in% chagas_end)

par_data2[par_prop == 0, par_prop := 0.01]
par_data2[par_count == 0, par_count := 1]
par_data2[, year_cor := year_id - 1979]

library(lme4)

model <- glm(par_prop ~ year_id + logit(prop_urban) + ihme_loc_id, data = par_data2, family = binomial)

summary(model)


# plot residuals by year for model without year , if it is correlated by year
aa <- residuals(model)

plot(aa ~ year_id, data = par_data2)

qqnorm(residuals(model))

qqline(residuals(model))

plot(model)

# ### create draws of the prediction

aa <- predict(model, newdata = model_data, type = "response", se.fit = T)
apred <- aa$fit
ase <- aa$se.fit

aadis <- predict(model, newdata = model_data, type = "response")

aapredse <- data.table(apred, ase)

model_data <- merge(x=chagas_deaths, y= urban, by = c('location_id', 'year_id', 'age_group_id' ), all.x=T)
model_data <- merge(x=model_data, y = locs, by = c('location_id' ), all.x=T)
model_data[, orig_loc := location_id]
#model_data[level >3, location_id := parent_id]
model_data <- subset(model_data, location_id %in% loc_par)
model_data[,year_cor := year_id - 1980 ]

model_data$pred_results <- predict(model, newdata = model_data, type = "response")

a <- as.data.table(predict(model, newdata = model_data, type = "response"))

model_data2 <- cbind(model_data,aapredse )

#####################################
model_data[, location_id := orig_loc]

model_data2 <- merge(model_data, par_data, by = c('ihme_loc_id', 'year_id' ), all.x=T)

for(i in loc_par){
  popatrisk4 <- subset(model_data2, location_id == i)
  loc_name <- unique(popatrisk4$location_name.x)
  
  ggpplot1 <- ggplot(data=popatrisk4, aes(x=(year_id), y= pred_results)) +
    geom_line() +
    ylim(0, NA) +
    geom_point(data=popatrisk4, aes(y= par_prop, x=(year_id))) + 
    labs(title=paste0("Chagas disease population at risk - ", loc_name),  y="% Pop at risk", x="Years", fill="Model") +
    theme_bw() 
  
  png(filename = paste0("FILEPATH"),
      height = 7, 
      width = 12, 
      units = "in", 
      res = 300)
  print(ggpplot1)
  dev.off()
}
##############################################################################
## Bootstrap
set.seed(123) # Ensure reproducibility
sample.data <- copy(par_data2)

repeat {
  # Sample rows from the dataset
  sample_df <- df[sample(nrow(df), size = sample_size, replace = TRUE), ]
  
  # Check if the number of unique locations in the sample meets the desired number
  if (length(unique(sample_df$location)) == desired_unique_locations) {
    break # Exit the loop if the condition is met
  }
}

# Containers for the coefficients
sample_coef_intercept <- NULL
sample_coef_x1 <- NULL

output_list <- lapply(1:1000, function(i) {
  
  #Creating a resampled dataset from the sample data
  repeat {
    # Sample rows from the dataset
    sample_d = sample.data[sample(1:nrow(sample.data), nrow(sample.data), replace = TRUE), ]
    
    # Check if the number of unique locations in the sample meets the desired number
    if (length(unique(sample_d$location_id)) == 19) {
      break # Exit the loop if the condition is met
    }
  }

  #Running the regression on these data
  model_bootstrap <- glm(par_prop ~ year_id + logit(prop_urban) + ihme_loc_id, data = sample_d, family = binomial)
  
  #Predictions
  prediction <- predict(model_bootstrap, newdata = model_data, type = "response" ) # Would need to customize this
  
  return(list(preds = prediction))
})

# Then you can refer to the outputs like this, where [[1]] would be draw one

output_list[[1]]$preds

bootstrap_results <- data.table()

for(i in 1:1000) {
  dt <- as.data.table(output_list[[i]]$preds)
  setnames(dt, "V1", paste0("draw_", i-1))
  bootstrap_results <- cbind(bootstrap_results, dt)
}

model_data2 <- cbind(model_data,bootstrap_results )
model_data3m <- summaries(model_data2)
i=135
for(i in loc_par){
  popatrisk4 <- subset(model_data3m, location_id == i)
  loc_name <- unique(popatrisk4$location_name.x)
  par_data_plot <- subset(par_data2, location_id == i)
  par_data_plot <- subset(par_data_plot, select = c(location_id, year_id, par_prop ))
  popatrisk5 <- merge(popatrisk4,par_data_plot, by= c('location_id', 'year_id'), all.x=T )
  
  
  ggpplot1 <- ggplot(data=popatrisk5, aes(x=year_id, y=mean, ymin=lower, ymax=upper, fill=ihme_loc_id),) +
    geom_line( aes(x=(year_id), y= pred_results), color = "black") +
    geom_line( color = "red") + 
    geom_ribbon( alpha=0.5) + 
    ylim(0, NA) +
    geom_point(data=popatrisk5, aes(y= par_prop, x=(year_id))) + 
    labs(title=paste0("Chagas disease population at risk - ", loc_name),  y="% Pop at risk", x="Years", fill="Model") +
    theme_bw() 
  
  png(filename = paste0("FILEPATH"),
      height = 7, 
      width = 12, 
      units = "in", 
      res = 300)
  print(ggpplot1)
  dev.off()
}


#####################################################################################################################################################################

##################################################################################################################
#####
#### Subnat
## Pull covariates , CSMR, urbanicity

locs_subnat <- get_location_metadata(location_set_id = ADDRESS, release_id =ADDRESS)
locs_subnat <- subset(locs_subnat, parent_id == 135 | parent_id == 130)
locs_subnat <- locs_subnat$location_id


chagas_deathsmaledraws <- get_draws(gbd_id_type = 'cause_id',
                                    source= 'ADDRESS',
                                    gbd_id = ADDRESS,
                                    measure_id = 1, #deaths
                                    year_id = c(ADDRESS),
                                    location_id = locs_subnat,
                                    #age_group_id = ADDRESS,
                                    release_id = ADDRESS,
                                    version_id = ADDRESS)

chagas_deathsfemaledraws <- get_draws(gbd_id_type = 'cause_id',
                                      source= 'ADDRESS',
                                      gbd_id = ADDRESS,
                                      measure_id = 1, #deaths
                                      year_id = c(ADDRESS),
                                      location_id = locs_subnat,
                                      release_id = ADDRESS,
                                      version_id = ADDRESS)


chagas_deaths_draws <- rbind(chagas_deathsmaledraws, chagas_deathsfemaledraws, fill=T)
chagas_deaths_draws <- chagas_deaths_draws[, lapply(.SD, sum), by = c('location_id', 'year_id' ),
                                           .SDcols = c(paste0("draw_", c(0:999)))]

chagas_deaths_draws[, sex_id :=3]
chagas_deaths_draws[, age_group_id := 22]


population <- get_population(location_id = locs_subnat, year_id = c(ADDRESS), sex_id = ADDRESS, age_group_id =ADDRESS, release_id = ADDRESS)

chagas_deaths_draws2 <- merge(chagas_deaths_draws, population, by = c('location_id', 'year_id', 'age_group_id', 'sex_id'), all.x=T)

chagas_deaths_draws2 <- chagas_deaths_draws2[, paste0("draw_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x))/ population )]


ages_id <- get_age_weights(release_id =16)
ages_id <- ages_id$age_group_id

# Very simple logit raking function

# Logit functions
logit <- function(x) {
  log(x/(1-x))
}
ilogit <- function(x) {
  exp(x)/(1+exp(x))
}


###############################################################################################################################################

rake_draws <- function(unraked_draw, k) {
  return(ilogit((logit(unraked_draw) + k)))
}

# function to evaluate some value of k
# for a set of unraked_draws u_draws
# and some population vector p
# basically this says "for some value of k, 
# how close do I get to my raking target t?

eval_k <- function(k, t, p, u_draw) {
  
  # Create raked draws using the proposed k
  raked_draw <- rake_draws(u_draw, k)
  
  # Calculate the population-weighted mean of the draw
  wt_mean <- weighted.mean(raked_draw, w=p)
  
  # Return the absolute value of the difference between the proposed and 
  # target national coverage values
  return(abs(wt_mean - t))
  
}

# wrapper function to find the best value of k
# so that the difference between the target and the 
# population-weighted mean of the subnational 
# draws is as small as possible

# arguments include the interval to search over (int) and the
# tolerance, which is how close you need to be before

find_k = function(target, unraked_draw,
                  population,
                  tolerance = 1e-6,
                  int = c(-10, 10)) {
  
  e_k <- function(k) {
    eval_k(k, 
           t = target,
           p = population,
           u_draw = unraked_draw)
  }
  
  best_k <- optimize(e_k, interval = int, tol = tolerance)$minimum
  
  return(best_k)
  
}

# Convenience function to rake by draw

rake_by_draw <- function(target_vector,
                         unraked_draw_matrix,
                         pop_vector) {
  
  n_draws <- dim(unraked_draw_matrix)[2]
  
  raked_list <- lapply(1:n_draws, function(i) {
    
    best_k <- find_k(target = target_vector[i], 
                     unraked_draw = unraked_draw_matrix[,i],
                     population = pops)
    
    raked_draw_vector <- rake_draws(unraked_draw_matrix[,i], best_k)
    
    return(list(k = best_k, 
                raked_draw = raked_draw_vector))
    
  })
  
  # Extract objects from the list
  k_vector <- sapply(1:n_draws, function(i) {raked_list[[i]]$k})
  
  raked_draw_matrix <- sapply(1:n_draws, function(i) {raked_list[[i]]$raked_draw})
  
  return(list(raked_draws = raked_draw_matrix,
              k_values = k_vector))
  
}

# EXAMPLE -----------------------------
# could loop over countries


#loop here for draws
y = 1980
l =130

dt_final <- data.table()

for(l in c(135, 130)){
  for(y in 1980:2022){
    
    if (l == 135){
      test_csmr <- subset(chagas_deaths_draws2, location_id >4700 & year_id == y)
    }
    if (l == 130){
      test_csmr <- subset(chagas_deaths_draws2, location_id <4700 & year_id == y)
    }
    
    test_csmr <- test_csmr[order(location_id)]
    
    #multiplying csmr by 100
    test_csmr <- test_csmr[, paste0("draw_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x))*100 )]
    csmr_subnat <- as.matrix(subset(test_csmr, select = -c(location_id, year_id, sex_id, age_group_id, population, run_id )))
    
    #population for the subnational  
    
    #should be a vector in the same order of csmr
    if (l == 135){
      pops <-   subset(population , location_id >4700 & year_id ==y)
    }
    if (l == 130){
      pops <-   subset(population , location_id < 4700 & year_id ==y)
    }
    
    pops <- pops[order(location_id)]
    pops <- as.vector(pops$population)
    
    #Mexico/Brazil PAR for a giving year
    
    partest <- subset(model_data2, location_id == l & year_id ==y)  
    partest <- subset(partest, select = c(paste0("draw_",0:999)))
    
    par_national_target <- unname(unlist(partest))

    csmr_unraked <- copy(csmr_subnat)
    
    raking_output <- rake_by_draw(target_vector = par_national_target,
                                  unraked_draw_matrix = csmr_unraked,
                                  pop_vector = pops)
    
    raked_draws <- raking_output$raked_draws
    k_values <- raking_output$k_values
    
    dt_it <- as.data.table(raked_draws)
    
    ids <- subset(test_csmr, select = c(location_id, year_id))
    dt_it2 <- cbind(ids, dt_it)
    
    dt_final <- rbind(dt_final, dt_it2)
    
  }
}


##### PLOTs
setnames(dt_final, c(paste0("V",1:1000)), c(paste0("draw_",0:999))) 
model_data_bra4 <- merge(dt_final, locs_subnat, by = "location_id", all.x=T)
model_data_bra4 <- summaries(model_data_bra4)

model_data_bra_plot <- subset(model_data_bra4, location_id == 130 | parent_id == 130)
#plotdata <- subset(model_bramex, location_id == 135) 
popatrisk4 <- subset(model_data3m, location_id == 130)
par_data_plot <- subset(par_data2, location_id == 130)

ggpplot1 <- ggplot() +
  geom_line(data=model_data_bra_plot, aes(x=(year_id), y= mean,  color= location_name)) +
  geom_line(data=popatrisk4, aes(x=(year_id), y= mean),color = "black") +
  ylim(0, NA) +
  geom_point(data=par_data_plot, aes(y= par_prop, x=(year_id))) + 
  labs(title=paste0("Chagas disease population at risk - "),  y="Pop at risk", x="Years", fill="Model") +
  theme_bw() 

png(filename = paste0("FILEPATH"),
    height = 7, 
    width = 12, 
    units = "in", 
    res = 300)
print(ggpplot1)
dev.off()


dt_test <- raked_draws[,1]


test <- weighted.mean(dt_test, w=pops)



# create final df and save

final_model <- rbind(model_subnat_final, model_nat_final)


####
dt_final_subnat <- copy(dt_final)
dt_final_nat <- copy(model_data2)
dt_final_nat <- subset(dt_final_nat, select = c("location_id", "year_id", paste0("draw_",0:999)))



dt_final_par <- rbind(dt_final_nat, dt_final_subnat)


####

fwrite(dt_final_par, "FILEPATH")


