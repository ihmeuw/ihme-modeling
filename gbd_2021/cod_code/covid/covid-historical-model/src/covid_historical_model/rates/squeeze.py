import pandas as pd
import numpy as np

CEILING = 0.99

def squeeze(daily: pd.Series, rate: pd.Series,
            day_shift: int,
            population: pd.Series,
            cross_variant_immunity: float,
            escape_variant_prevalence: pd.Series,
            vaccine_coverage: pd.DataFrame,
            ceiling: float = CEILING,) -> pd.Series:
    daily_infections = (daily / rate).dropna().rename('infections')
    daily_infections += 1
    daily_infections = daily_infections.reset_index()
    daily_infections['date'] -= pd.Timedelta(days=day_shift)
    daily_infections = daily_infections.set_index(['location_id', 'date']).loc[:, 'infections']
    
    escape_variant_prevalence = (pd.concat([daily_infections,
                                            escape_variant_prevalence], axis=1))
    escape_variant_prevalence = escape_variant_prevalence.fillna(0)
    escape_variant_prevalence = (escape_variant_prevalence
                                 .loc[daily_infections.index, 'escape_variant_prevalence'])
    
    non_ev_infections = daily_infections * (1 - escape_variant_prevalence)
    ev_infections = daily_infections * escape_variant_prevalence
    repeat_infections = (1 - cross_variant_immunity) * (non_ev_infections.cumsum() / population).clip(0, 1) * ev_infections
    first_infections = daily_infections - repeat_infections
    
    cumul_infections = daily_infections.dropna().groupby(level=0).cumsum()
    seroprevalence = first_infections.dropna().groupby(level=0).cumsum()
    
    vaccinations = vaccine_coverage.join(daily, how='right')['cumulative_all_effective'].fillna(0)
    daily_vaccinations = vaccinations.groupby(level=0).diff().fillna(vaccinations)
    eff_daily_vaccinations = daily_vaccinations * (1 - seroprevalence / population).clip(0, 1)
    eff_vaccinations = eff_daily_vaccinations.groupby(level=0).cumsum()
    
    non_suscept = seroprevalence + eff_vaccinations
    max_non_suscept = non_suscept.groupby(level=0).max()
    max_sero = seroprevalence.groupby(level=0).max()

    limits = population * ceiling
    
    excess_non_suscept = (max_non_suscept - limits).clip(0, np.inf)
    excess_scaling_factor = (max_sero - excess_non_suscept).clip(0, np.inf) / max_sero
    
    rate = (rate / excess_scaling_factor).fillna(rate)
        
    return rate.dropna()
