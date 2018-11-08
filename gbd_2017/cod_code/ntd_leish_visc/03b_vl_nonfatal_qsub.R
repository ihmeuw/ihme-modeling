## this script takes in an argument, squares multiplies it by 1 through 4, and writes it to a matrix

arg1 = as.numeric(commandArgs()[4])
i<-arg1

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}

#load in saved workspace
load(file = paste0(prefix,'FILEPATH/qsub_workspace.RData'))

full_year_id<-seq(1980,2017,1)


source(sprintf("FILEPATH/get_demographics.R",prefix))
source(sprintf("FILEPATH/get_covariate_estimates.R",prefix))
source(sprintf("FILEPATH/get_location_metadata.R",prefix))
source(sprintf("FILEPATH/get_population.R",prefix))
source(sprintf("FILEPATH/get_draws.R",prefix))
source(sprintf("FILEPATH/save_results_cod.R",prefix))
source(sprintf("FILEPATH/save_results_epi.R",prefix))
source(sprintf("FILEPATH/get_model_results.R",prefix))
source(sprintf("FILEPATH/get_ids.R",prefix))

  if (i %in% unique_vl_locations){
    #if yes, load in its st-gpr all-age estimate
    if( i %in% unique_subnat){
      #if a subnational, pull in the national envelope and multiply the count by proportion
      loc_stgpr<-read.csv(paste0(prefix,'FILEPATH',
                                 model_id,'/draws_temp_1/',subnat_prop$country_loc_id[which(subnat_prop$loc_id==i)],'.csv'))
      for(alpha in full_year_id){
        annual_stgpr<-subset(loc_stgpr,loc_stgpr$year_id==alpha)
        annual_stgpr_draws<-annual_stgpr[,c(-1:-4,-1005)]
        incidence_df<-empty_df
        prevalence_1458_df<-empty_df
        prevalence_1459_df<-empty_df
        prevalence_1460_df<-empty_df
        population_pull_sex1<-get_population(location_id=i, year_id = alpha, age_group_id = full_age_set, sex_id=1)
        population_pull_sex2<-get_population(location_id=i, year_id = alpha, age_group_id = full_age_set, sex_id=2)
        population_pull<-rbind(population_pull_sex1, population_pull_sex2)
        full_population_pull<-get_population(location_id=i, year_id=alpha)
        full_country_pull<-get_population(location_id=subnat_prop$country_loc_id[which(subnat_prop$loc_id==i)], year_id=alpha)
        country_pop<-full_country_pull$population
        total_pop<-full_population_pull$population
        split_pop<-data.frame(population_pull$population)

        # #IMPLEMENT AT DRAW LEVEL
        # inflated_value<-global_ratio*split_pop
        # #rescale to cases
        # rescaled_value<-inflated_value*envelope_cases/sum(inflated_value)
        # incidence_rate<-rescaled_value/split_pop

        for(draw in 1:1000){
          envelope<-annual_stgpr_draws[1,draw]
          envelope_country_cases<-envelope*country_pop
          envelope_cases<-envelope_country_cases*subnat_prop$draw_0[which(subnat_prop$loc_id==i)]
          draw_ratio<-data.frame(draws_ratio[[draw]])
          names(draw_ratio)<-paste0("draw_",draw-1)
          inflated_ratio<-draw_ratio*split_pop
          rescaled_ratio<-inflated_ratio*envelope_cases/sum(inflated_ratio)
          result<-rescaled_ratio/split_pop
          result_prevalence_1458<-result*0.25
          result_prevalence_1459<-result*0.1875
          result_prevalence_1460<-result*(0.25-0.1875)
          incidence_df<-cbind(incidence_df,result)
          prevalence_1458_df<-cbind(prevalence_1458_df,result_prevalence_1458)
          prevalence_1459_df<-cbind(prevalence_1459_df,result_prevalence_1459)
          prevalence_1460_df<-cbind(prevalence_1460_df,result_prevalence_1460)
        }
        #aggregate for me_id 1458 = all VL

        incidence_df$year_id<-rep(alpha,nrow(incidence_df))
        incidence_df$location_id<-rep(i,nrow(incidence_df))
        incidence_df$measure_id<-rep(6,nrow(incidence_df))
        incidence_df$modelable_entity_id<-rep(1458,nrow(incidence_df))

        prevalence_1458_df$year_id<-rep(alpha,nrow(prevalence_1458_df))
        prevalence_1458_df$location_id<-rep(i,nrow(prevalence_1458_df))
        prevalence_1458_df$measure_id<-rep(5,nrow(prevalence_1458_df))
        prevalence_1458_df$modelable_entity_id<-rep(1458,nrow(prevalence_1458_df))

        inc_prev_1458_df<-rbind(incidence_df,prevalence_1458_df)

        write.csv(inc_prev_1458_df,
                  file=paste0(prefix,'FILEPATH',i,'_',alpha,'.csv')
        )
        #aggregate for me_id 1459 = moderate VL
        incidence_df$year_id<-rep(alpha,nrow(incidence_df))
        incidence_df$location_id<-rep(i,nrow(incidence_df))
        incidence_df$measure_id<-rep(6,nrow(incidence_df))
        incidence_df$modelable_entity_id<-rep(1459,nrow(incidence_df))

        prevalence_1459_df$year_id<-rep(alpha,nrow(prevalence_1459_df))
        prevalence_1459_df$location_id<-rep(i,nrow(prevalence_1459_df))
        prevalence_1459_df$measure_id<-rep(5,nrow(prevalence_1459_df))
        prevalence_1459_df$modelable_entity_id<-rep(1459,nrow(prevalence_1459_df))

        inc_prev_1459_df<-rbind(incidence_df,prevalence_1459_df)

        write.csv(inc_prev_1459_df,
                  file=paste0(prefix,'FILEPATH',i,'_',alpha,'.csv')
        )
        #aggregate for me_id 1460 = severe VL
        incidence_df$year_id<-rep(alpha,nrow(incidence_df))
        incidence_df$location_id<-rep(i,nrow(incidence_df))
        incidence_df$measure_id<-rep(6,nrow(incidence_df))
        incidence_df$modelable_entity_id<-rep(1460,nrow(incidence_df))

        prevalence_1460_df$year_id<-rep(alpha,nrow(prevalence_1460_df))
        prevalence_1460_df$location_id<-rep(i,nrow(prevalence_1460_df))
        prevalence_1460_df$measure_id<-rep(5,nrow(prevalence_1460_df))
        prevalence_1460_df$modelable_entity_id<-rep(1460,nrow(prevalence_1460_df))

        inc_prev_1460_df<-rbind(incidence_df,prevalence_1460_df)

        write.csv(inc_prev_1460_df,
                  file=paste0(prefix,'FILEPATH',i,'_',alpha,'.csv')
        )
      }
    }else{

    loc_stgpr<-read.csv(paste0(prefix,'FILEPATH',
                               model_id,'/draws_temp_1/',i,'.csv'))
    for(alpha in full_year_id){
      annual_stgpr<-subset(loc_stgpr,loc_stgpr$year_id==alpha)
      annual_stgpr_draws<-annual_stgpr[,c(-1:-4,-1005)]
      incidence_df<-empty_df
      prevalence_1458_df<-empty_df
      prevalence_1459_df<-empty_df
      prevalence_1460_df<-empty_df
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
        result_prevalence_1458<-result*0.25
        result_prevalence_1459<-result*0.1875
        result_prevalence_1460<-result*(0.25-0.1875)
        incidence_df<-cbind(incidence_df,result)
        prevalence_1458_df<-cbind(prevalence_1458_df,result_prevalence_1458)
        prevalence_1459_df<-cbind(prevalence_1459_df,result_prevalence_1459)
        prevalence_1460_df<-cbind(prevalence_1460_df,result_prevalence_1460)
      }
      #aggregate for me_id 1458 = all VL

      incidence_df$year_id<-rep(alpha,nrow(incidence_df))
      incidence_df$location_id<-rep(i,nrow(incidence_df))
      incidence_df$measure_id<-rep(6,nrow(incidence_df))
      incidence_df$modelable_entity_id<-rep(1458,nrow(incidence_df))

      prevalence_1458_df$year_id<-rep(alpha,nrow(prevalence_1458_df))
      prevalence_1458_df$location_id<-rep(i,nrow(prevalence_1458_df))
      prevalence_1458_df$measure_id<-rep(5,nrow(prevalence_1458_df))
      prevalence_1458_df$modelable_entity_id<-rep(1458,nrow(prevalence_1458_df))

      inc_prev_1458_df<-rbind(incidence_df,prevalence_1458_df)

      write.csv(inc_prev_1458_df,
                file=paste0(prefix,'FILEPATH',i,'_',alpha,'.csv')
                )
      #aggregate for me_id 1459 = moderate VL
      incidence_df$year_id<-rep(alpha,nrow(incidence_df))
      incidence_df$location_id<-rep(i,nrow(incidence_df))
      incidence_df$measure_id<-rep(6,nrow(incidence_df))
      incidence_df$modelable_entity_id<-rep(1459,nrow(incidence_df))

      prevalence_1459_df$year_id<-rep(alpha,nrow(prevalence_1459_df))
      prevalence_1459_df$location_id<-rep(i,nrow(prevalence_1459_df))
      prevalence_1459_df$measure_id<-rep(5,nrow(prevalence_1459_df))
      prevalence_1459_df$modelable_entity_id<-rep(1459,nrow(prevalence_1459_df))

      inc_prev_1459_df<-rbind(incidence_df,prevalence_1459_df)

      write.csv(inc_prev_1459_df,
                file=paste0(prefix,'FILEPATH',i,'_',alpha,'.csv')
      )
      #aggregate for me_id 1460 = severe VL
      incidence_df$year_id<-rep(alpha,nrow(incidence_df))
      incidence_df$location_id<-rep(i,nrow(incidence_df))
      incidence_df$measure_id<-rep(6,nrow(incidence_df))
      incidence_df$modelable_entity_id<-rep(1460,nrow(incidence_df))

      prevalence_1460_df$year_id<-rep(alpha,nrow(prevalence_1460_df))
      prevalence_1460_df$location_id<-rep(i,nrow(prevalence_1460_df))
      prevalence_1460_df$measure_id<-rep(5,nrow(prevalence_1460_df))
      prevalence_1460_df$modelable_entity_id<-rep(1460,nrow(prevalence_1460_df))

      inc_prev_1460_df<-rbind(incidence_df,prevalence_1460_df)

      write.csv(inc_prev_1460_df,
                file=paste0(prefix,'FILEPATH',i,'_',alpha,'.csv')
      )
    }
    }
  }else{
    for(alpha in full_year_id){
      incidence_df<-zero_df
      incidence_df$year_id<-rep(alpha,nrow(incidence_df))
      incidence_df$location_id<-rep(i,nrow(incidence_df))
      incidence_df$measure_id<-rep(6,nrow(incidence_df))
      incidence_1458_df<-incidence_df
      incidence_1459_df<-incidence_df
      incidence_1460_df<-incidence_df
      incidence_1458_df$modelable_entity_id<-rep(1458,nrow(incidence_df))
      incidence_1459_df$modelable_entity_id<-rep(1459,nrow(incidence_df))
      incidence_1460_df$modelable_entity_id<-rep(1460,nrow(incidence_df))

      prevalence_df<-zero_df
      prevalence_df$year_id<-rep(alpha,nrow(prevalence_df))
      prevalence_df$location_id<-rep(i,nrow(prevalence_df))
      prevalence_df$measure_id<-rep(5,nrow(prevalence_df))
      prevalence_1458_df<-prevalence_df
      prevalence_1459_df<-prevalence_df
      prevalence_1460_df<-prevalence_df
      prevalence_1458_df$modelable_entity_id<-rep(1458,nrow(prevalence_df))
      prevalence_1459_df$modelable_entity_id<-rep(1459,nrow(prevalence_df))
      prevalence_1460_df$modelable_entity_id<-rep(1460,nrow(prevalence_df))

      inc_prev_1458_df<-rbind(incidence_1458_df,prevalence_1458_df)
      inc_prev_1459_df<-rbind(incidence_1459_df,prevalence_1459_df)
      inc_prev_1460_df<-rbind(incidence_1460_df,prevalence_1460_df)

      write.csv(inc_prev_1458_df,
                file=paste0(prefix,'FILEPATH',i,'_',alpha,'.csv')
      )
      write.csv(inc_prev_1459_df,
                file=paste0(prefix,'FILEPATH',i,'_',alpha,'.csv')
      )
      write.csv(inc_prev_1460_df,
                file=paste0(prefix,'FILEPATH',i,'_',alpha,'.csv')
      )
    }
  }
