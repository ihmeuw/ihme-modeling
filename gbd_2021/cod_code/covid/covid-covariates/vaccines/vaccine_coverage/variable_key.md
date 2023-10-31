## Variable key
In general 'residual' corresponds to left over vaccine doses among locations that have pre-purchased vaccine.
   'global_residual' corresponds to left over vaccine among all locations.
 - secured_daily_doses = expected doses available by location-date
 - weighted_doses_per_course = (doses * doses_per_fully_vaccinated) / sum(doses_per_fully_vaccinated)
 - secured_effective_daily_doses = number of doses * vaccine efficacy [unique by vaccine]
 - residual_daily_doses = number of residual doses after accounting for pre-purchasing agreements
 - effective_daily_residual = number of effective residual daily doses [accounting for vaccine efficacy]
 - global_daily_residual = total number of residual doses [not accounting for pre-purchasing agreements]
 - global_effective_daily_residual = total number of residual doses accounting for vaccine efficacy [not accounting for pre-purchasing]
 - weighted_efficacy_location = the weighted vaccine efficacy by manufacturer by location-date
 - weighted_efficacy_residual = the weighted vaccine efficacy by manufacturer for residual doses
 - weighted_efficacy_global = the weighted vaccine efficacy by manufacturer for all residual doses [not accounting for pre-purchasing]
 - global_daily_residual_location = the number of doses that each location-date would have if residual doses were distributed to all global locations
 - global_effective_daily_residual_location = the number of doses * vaccine efficay that each location-date would have if 
   residual doses were distributed to all global locations
 - adult_population = population 20+ years (both sexes)
 - over65_population = population 65+ years (both sexes)
 - predicted_reject_all = proportion of the adult population that would reject vaccine (estimate)
 - predicted_reject_o60 = proportion of 60+ years that would reject vaccine (estimate)
 - essential = proportion of the population that are essential workers (estimate)
 - essential_workers = count of population that are essential workers (assumed under 65 years)
 - non_essential = count of the population that are non-essential workers (assumed under 65 years)
 - count_accept_o65 = count of the population 65+ years that would accept the vaccine
 - count_accept_all = count of the adult population that would accept the vaccine
 - count_accept_essential = count of the essential worker population that would accept the vaccine
 - ratio_elderly_essential = proportion of the vaccine that would initially be distributed to elderly
 - essential_ratio = proportion of the vaccine that would initially be distributed to essential workers
 - {elderly/essential/adults}_vaccinated = daily count of the population that are newly vaccinated (full course of vaccine)
 - {elderly/essential/adults}_effective = daily count of the population that are newly effectively protected (efficacy * vaccinated),
   this value is the number vaccinated * efficacy, shifted by 28 days (assumed time to protection)
 - {elderly/essential/adults}_protected = daily count of the population that are newly protected against infection (effective * 0.5)
 - {elderly/essential/adults}_non_infectious = daily count of the population that are newly protected and non-infectious (effective * 0.5)
 - {elderly/essential/adults}_unprotected = daily count of the populationt that are newly vaccinated but not protected 
   (vaccinated * (1-efficacy))
 - hr_vaccinated: daily high-risk (65+) people vaccinated
 - hr_unproteced: daily high-risk (65+) people who receive no benefit from vaccine
 - hr_effective_variant: daily high-risk (65+) people immune and protected from wildtype and variant
 - hr_effective_protected_variant: daily high-risk (65+) people protected severe disease from wildtype and variant
 - hr_effective_wildtype: daily high-risk (65+) people immune and protected from wildtype only
 - hr_effective_protected_wildtype: daily high-risk (65+) people protected severe disease from wildtype only
 - lr_vaccinated: daily low-risk (<65) people vaccinated
 - lr_unproteced: daily low-risk (<65) people who receive no benefit from vaccine
 - lr_effective_variant: daily low-risk (<65) people immune and protected from wildtype and variant
 - lr_effective_protected_variant: daily low-risk (<65) people protected severe disease from wildtype and variant
 - lr_effective_wildtype: daily low-risk (<65) people immune and protected from wildtype only
 - lr_effective_protected_wildtype: daily low-risk (<65) people protected severe disease from wildtype only
 - hr_protected_not_immune_wildtype: CUMULATIVE high-risk (65+) vaccinated and protected from disease but not immune, wildtype
 - hr_protected_not_immune_variant: CUMULATIVE high-risk (65+) vaccinated and protected from disease but not immune, variant
 - lr_protected_not_immune_wildtype: CUMULATIVE low-risk (<65) vaccinated and protected from disease but not immune, wildtype
 - lr_protected_not_immune_variant: CUMULATIVE low-risk (<65) vaccinated and protected from disease but not immune, variant