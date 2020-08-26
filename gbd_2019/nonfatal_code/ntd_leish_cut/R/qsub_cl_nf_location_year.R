arg1 = as.numeric(commandArgs()[4])
i<-arg1

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}

#load in saved workspace
load(file = paste0(prefix,'FILEPATH'))
library(data.table)


source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))




  if (i %in% unique_cl_locations){
    #if yes, load in its st-gpr all-age estimate
    if( i %in% unique_subnat){
      #if a subnational, pull in the national envelope and multiply the count by proportion
      loc_stgpr<-read.csv(paste0(prefix,'FILEPATH',
                                 model_id,'FILEPATH',subnat_prop$country_loc_id[which(subnat_prop$loc_id==i)],'.csv'))
      for(alpha in full_year_id){
        annual_stgpr<-subset(loc_stgpr,loc_stgpr$year_id==alpha)
        annual_stgpr_draws<-annual_stgpr[,c(-1:-4)]
        incidence_df<-empty_df
        prevalence_df<-empty_df
        population_pull_sex1<-get_population(location_id=i, year_id = alpha, age_group_id = full_age_set, sex_id=1)
        population_pull_sex2<-get_population(location_id=i, year_id = alpha, age_group_id = full_age_set, sex_id=2)
        population_pull<-rbind(population_pull_sex1, population_pull_sex2)
        full_population_pull<-get_population(location_id=i, year_id=alpha)
        full_country_pull<-get_population(location_id=subnat_prop$country_loc_id[which(subnat_prop$loc_id==i)], year_id=alpha)
        country_pop<-full_country_pull$population
        total_pop<-full_population_pull$population
        split_pop<-data.frame(population_pull$population)

        for(draw in 1:1000){
          envelope<-annual_stgpr_draws[1,draw]
          envelope_country_cases<-envelope*country_pop
          envelope_cases<-envelope_country_cases*subnat_prop$draw_0[which(subnat_prop$loc_id==i)]
          draw_ratio<-data.frame(draws_ratio[,draw])
          names(draw_ratio)<-paste0("draw_",draw-1)
          inflated_ratio<-draw_ratio*split_pop
          rescaled_ratio<-inflated_ratio*envelope_cases/sum(inflated_ratio)
          result<-rescaled_ratio/split_pop
          #add in acute prevalence at cohort propagation stage - set to zero for now so fields are present
          result_prevalence<-result*0
          
          incidence_df<-cbind(incidence_df,result)
          prevalence_df<-cbind(prevalence_df,result_prevalence)

        }

        incidence_df$year_id<-rep(alpha,nrow(incidence_df))
        incidence_df$location_id<-rep(i,nrow(incidence_df))
        incidence_df$measure_id<-rep(6,nrow(incidence_df))
        incidence_df$modelable_entity_id<-rep(ADDRESS,nrow(incidence_df))

        prevalence_df$year_id<-rep(alpha, nrow(prevalence_df))
        prevalence_df$location_id<-rep(i, nrow(prevalence_df))
        prevalence_df$measure_id<-rep(5, nrow(prevalence_df))
        prevalence_df$modelable_entity_id<-rep(ADDRESS, nrow(prevalence_df))

        inc_prev_df<-rbind(incidence_df, prevalence_df)

        write.csv(inc_prev_df,
                  file=paste0(prefix, "FILEPATH",i,"_",alpha,".csv")
                  )
      }
      }else{
        loc_stgpr<-read.csv(paste0(prefix,'FILEPATH',
                                   model_id,'FILEPATH',i,'.csv'))
        for(alpha in full_year_id){
          annual_stgpr<-subset(loc_stgpr,loc_stgpr$year_id==alpha)
          annual_stgpr_draws<-annual_stgpr[,c(-1:-4,-1005)]
          incidence_df<-empty_df
          prevalence_df<-empty_df

          population_pull_sex1<-get_population(location_id=i, year_id = alpha, age_group_id = full_age_set, sex_id=1)
          population_pull_sex2<-get_population(location_id=i, year_id = alpha, age_group_id = full_age_set, sex_id=2)
          population_pull<-rbind(population_pull_sex1, population_pull_sex2)
          full_population_pull<-get_population(location_id=i, year_id=alpha)
          total_pop<-full_population_pull$population
          split_pop<-data.frame(population_pull$population)

          # #IMPLEMENT AT DRAW LEVEL
          # inflated_value<-global_ratio*split_pop
          # #rescale to cases
          # rescaled_value<-inflated_value*envelope_cases/sum(inflated_value)
          # incidence_rate<-rescaled_value/split_pop

          for(draw in 1:1000){
            envelope<-annual_stgpr_draws[1,draw]
            envelope_cases<-envelope*total_pop
            draw_ratio<-data.frame(draws_ratio[[draw]])
            names(draw_ratio)<-paste0("draw_",draw-1)
            inflated_ratio<-draw_ratio*split_pop
            rescaled_ratio<-inflated_ratio*envelope_cases/sum(inflated_ratio)
            result<-rescaled_ratio/split_pop
            #add in acute prevalence at cohort propagation stage - set to zero for now so fields are present
            result_prevalence<-result*0

            incidence_df<-cbind(incidence_df,result)
            prevalence_df<-cbind(prevalence_df,result_prevalence)

          }
          incidence_df$year_id<-rep(alpha,nrow(incidence_df))
          incidence_df$location_id<-rep(i,nrow(incidence_df))
          incidence_df$measure_id<-rep(6,nrow(incidence_df))
          incidence_df$modelable_entity_id<-rep(ADDRESS,nrow(incidence_df))

          prevalence_df$year_id<-rep(alpha, nrow(prevalence_df))
          prevalence_df$location_id<-rep(i, nrow(prevalence_df))
          prevalence_df$measure_id<-rep(5, nrow(prevalence_df))
          prevalence_df$modelable_entity_id<-rep(ADDRESS, nrow(prevalence_df))

          inc_prev_df<-rbind(incidence_df, prevalence_df)

          write.csv(inc_prev_df,
                    file=paste0(prefix, "FILEPATH",i,"_",alpha,".csv")
          )
        }
      }
    }else{
      for(alpha in full_year_id){
        incidence_df<-zero_df
        incidence_df$year_id<-rep(alpha,nrow(incidence_df))
        incidence_df$location_id<-rep(i,nrow(incidence_df))
        incidence_df$measure_id<-rep(6,nrow(incidence_df))
        incidence_df<-incidence_df

        incidence_df$modelable_entity_id<-rep(ADDRESS,nrow(incidence_df))

        prevalence_df<-zero_df
        prevalence_df$year_id<-rep(alpha,nrow(prevalence_df))
        prevalence_df$location_id<-rep(i,nrow(prevalence_df))
        prevalence_df$measure_id<-rep(5,nrow(prevalence_df))

        prevalence_df$modelable_entity_id<-rep(ADDRESS,nrow(prevalence_df))

        inc_prev_df<-rbind(incidence_df, prevalence_df)

        write.csv(inc_prev_df,
                  file=paste0(prefix, "FILEPATH",i,"_",alpha,".csv")
        )
    }
    }
