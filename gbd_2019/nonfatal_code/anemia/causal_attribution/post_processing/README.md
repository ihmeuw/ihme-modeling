# HIV Proportion Calculations

This code calculated the proportions of mild, moderate, severe, and no anemia with people having HIV using the estimates from anemia causal attribution. This proportions are then used to split child HIV me_ids using split_epi_model.


To submit_jobs.py script submits the necessary jobs in parallel by year_id. The compute_HIV_props.py script takes the prevalence of mild, moderate, and severe anemia and subtracts from the parent in order to get the prevalence of no anemia. In case where mild, moderate, and severe add up to larger that the parent, other is set to 0. 

All draws are scaled so that they add to the parent, the divided by the parent to get proportions.
