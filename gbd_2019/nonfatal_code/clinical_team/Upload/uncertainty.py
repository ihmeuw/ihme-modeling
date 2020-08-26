import numpy as np
import warnings

import pandas as pd
from scipy import stats


measure_map = {
    3: ('proportion', 'non-ratio'),  
    5: ('proportion', 'non-ratio'),  
    6: ('rate', 'non-ratio'),        
    7: ('rate', 'non-ratio'),        
    8: ('rate', 'non-ratio'),        
    9: ('rate', 'non-ratio'),        
    10: ('rate', 'non-ratio'),       
    11: ('rate', 'ratio'),           
    12: ('rate', 'ratio'),           
    13: ('rate', 'non-ratio'),       
    14: ('rate', 'non-ratio'),       
    15: ('rate', 'non-ratio'),       
    16: ('rate', 'non-ratio'),       
    17: ('proportion', 'non-ratio'), 
    18: ('proportion', 'non-ratio'), 
    19: ('rate', 'non-ratio'),       
    41: ('rate', 'non-ratio'),       
    42: ('rate', 'non-ratio')}       


uncertainty_type_map = {
    1: "Standard error",
    2: "Effective sample size",
    3: "Confidence interval",
    4: "Sample size"
}


class UncertaintyError(Exception): pass


class Uncertainty(object):
    
    
    
    
    
    _required_columns = ["measure_id"]
    _additional_columns = [
        "cases", "standard_error", "effective_sample_size", "sample_size",
        "uncertainty_type", "uncertainty_type_value", "uncertainty_type_id",
        "lower", "upper", "mean"
    ]

    def __init__(self, df: pd.DataFrame):
        """ Generates uncertainty for dataframe. 

        :param df: a pd.Dataframe
        :returns: None
        :rtype: NoneType

        """
        self.df = df.copy()

        
        for column in Uncertainty._additional_columns:
            if column not in self.df:
                self.df[column] = None

        self._check_input()
        
    def _check_input(self) -> None:
        """Checks if self.df can be used to generate uncertainty. 
        
        :raises: UncertaintyError
        :returns: None
        :rtype: NoneType

        """
        
        missing_columns = [col for col in Uncertainty._required_columns
                           if col not in self.df.columns]
        if missing_columns:
            raise UncertaintyError(
                f"Missing required columns: {missing_columns}"
            )

        
        measure_ids = self.df.measure_id.unique()
        illegal_measures = set(measure_ids) - set(measure_map.keys())
        if illegal_measures:
            raise UncertaintyError(
                f"The following measures are not valid: {illegal_measures}\n"
                f"Valid measures: {list(measure_map.keys)}"
            )

        
        
        can_calc_mean = (
            self.df["mean"].notnull() |
            (self.df.cases.notnull() &
             (self.df.sample_size.notnull() | self.df.effective_sample_size.notnull())
            )
        )
        if not can_calc_mean.all():
            raise UncertaintyError(
                f"Can't calculate the mean for the following rows: "
                f"{self.df.index[~can_calc_mean]}"
            )
        
        
        can_calc_uncertainty = (self.df.standard_error.notnull() |
                                self.df.effective_sample_size.notnull() |
                                self.df.sample_size.notnull() |
                               (
                                   self.df.lower.notnull() &
                                   self.df.upper.notnull()
                               )
        )
        if not can_calc_uncertainty.all():
            raise UncertaintyError(
                f"Can't calculate the uncertainty for the following rows: "
                f"{self.df.index[~can_calc_uncertainty]}"
            )

    def fill(self) -> None:
        """Fills in uncertainty measurements.

        :returns: None
        :rtype: NoneType

        """
        if len(self.df) == 0:
            warnings.warn("Dataframe is empty.")
            return self.df
        
        self.fill_mean_ss_cases()
        self.fill_uncertainty_type()
        self.fill_ess_ss()
        self.df = fill_uncertainty(self.df)

    def fill_mean_ss_cases(self) -> None:
        """ Fill the mean / sample size / cases column based on the values
    	of the other two """

        self.df.loc[self.df["mean"].isnull(), "mean"] = (
            self.df.loc[self.df["mean"].isnull(), "cases"] /
            self.df.loc[self.df["mean"].isnull(), "sample_size"])

        self.df.loc[self.df.cases.isnull(), "cases"] = (
            self.df.loc[self.df.cases.isnull(), "mean"] *
            self.df.loc[self.df.cases.isnull(), "sample_size"])

        
        
        case_col = self.df.loc[self.df.sample_size.isnull(), "cases"]
        if case_col.dtype.char == 'O':
            case_col = case_col.astype('float')

        self.df.loc[self.df.sample_size.isnull(), "sample_size"] = (
            case_col / self.df.loc[self.df.sample_size.isnull(), "mean"])

    def fill_ess_ss(self) -> None:
        """
        fill in effective_sample_size from sample_size so that uncertainty
        can be recalculated
        """
        self.df.loc[self.df.effective_sample_size.isnull(),
                   "effective_sample_size"] = (
            self.df.loc[self.df.effective_sample_size.isnull(), "sample_size"])

    def fill_uncertainty_type(self) -> None:
        """
        fill in uncertainty_type and uncertainty_type_id for each row given
        the available uncertainties
        """

        self.df.loc[
            (self.df.uncertainty_type.isnull()) & (
                self.df.standard_error.notnull()),
            "uncertainty_type_id"] = 1

        self.df.loc[
            (self.df.uncertainty_type.isnull()) & (
                self.df.effective_sample_size.notnull()),
            "uncertainty_type_id"] = 2

        self.df.loc[
            (self.df.uncertainty_type.isnull()) & (
                self.df.lower.notnull()) & (self.df.upper.notnull()),
            "uncertainty_type_id"] = 3

        self.df.loc[
            (self.df.uncertainty_type.isnull()) & (
                self.df.sample_size.notnull()),
            "uncertainty_type_id"] = 4

        uncertainty = self.df.uncertainty_type_id.replace(uncertainty_type_map)
        self.df.loc[:, "uncertainty_type"] = uncertainty

  
def fill_uncertainty(df: pd.DataFrame) -> pd.DataFrame:
    """ Selects the correct uncertainty back-calculation method based
    	on the measure type and fills uncertainty in the input dataframe """
    
    df.loc[df.uncertainty_type_value.isnull(), "uncertainty_type_value"] = 95
    
    filled_outputs = []
    for measure_id, measure_df in df.groupby("measure_id"):
        ptype, rtype = measure_map[measure_id]
        filled_outputs.append(
            fill_single_measure(measure_df, measure_id, ptype, rtype)
        )
    return pd.concat(filled_outputs)


def se_from_ess(
        p: np.ndarray,
        ess: np.ndarray,
        param_type: str,
        quantile: float=0.975
) -> np.ndarray:
    """ Calculates standard error from effective sample size and mean."""
    if param_type not in ['proportion', 'rate']:
        raise UncertaintyError(
            f"Unknown paramater: {param_type} while calculating SE from ESS"
        )

    cases = p * ess

    if param_type == "proportion":
        if (p > 1).any():
            raise UncertaintyError(
                "Cannot create uncertainty for a proportion larger than 1."
            )
        
        
        z = stats.norm.ppf(quantile)
        return np.sqrt(p * (1 - p) / ess + z**2 / (4 * ess**2))
    elif param_type == "rate":
        
        
        interpolate_se = (
            ((5 - p * ess) / ess + p * ess * np.sqrt(5 / ess**2)) / 5
        )
        poisson_se = np.sqrt(p / ess)
        return np.where(cases > 5, poisson_se, interpolate_se)
    else:
        raise UncertaintyError(
            "Invalid param_type encountered while calculating SE from ESS"
        )


def ui_from_ess(
        p: np.ndarray,
        ess: np.ndarray,
        param_type: str,
        measure_id: int,
        quantile: float=0.975
) -> pd.DataFrame:
    """ Calculates the lower and upper bound of the uncertainty interval"""
    cases = p * ess
    se = se_from_ess(p, ess, param_type=param_type, quantile=quantile)
    z = stats.norm.ppf(quantile)

    lower = np.repeat(0., len(cases))
    upper = np.repeat(0., len(cases))

    
    use_wilsons_mask = np.repeat(False, len(cases))
    if measure_id == 17:  
        use_wilsons_mask = ~use_wilsons_mask
    elif param_type == "proportion":
        use_wilsons_mask = cases > 5
    if use_wilsons_mask.any():
        mid = 1 / (1 + z**2 / ess) * (p + z**2 / (2 * ess))
        diff = z * se
        np.putmask(lower, use_wilsons_mask, mid - diff)
        np.putmask(upper, use_wilsons_mask, mid + diff)

    
    use_se_mask = np.repeat(False, len(cases))
    if param_type == "rate":
        use_se_mask = ~use_se_mask
    else:
        use_se_mask = cases <= 5
    if use_se_mask.any():
        ui = ui_from_se(
            p=p[use_se_mask],
            se=se[use_se_mask],
            param_type=param_type,
        )
        lower[use_se_mask] = ui["lower"]
        upper[use_se_mask] = ui["upper"]

    lower[lower < 0] = 0

    return pd.DataFrame({
        "lower": lower,
        "upper": upper
    })

  
def se_ui_from_ess(
        p: np.ndarray,
        ess: np.ndarray,
        param_type: str,
        measure_id: int,
        confidence: float=0.95
) -> pd.DataFrame:
    """ calculates standard error and upper and lower estimates using the estimated sample size"""
    result = ui_from_ess(p, ess, param_type, measure_id)
    result["standard_error"] = se_from_ess(p, ess, param_type)
    return result[["lower", "upper", "standard_error"]]


def se_from_ui(
        p: np.ndarray,
        lower: np.ndarray,
        upper: np.ndarray,
        method: str,
        confidence: int=0.95
) -> np.ndarray:
    """ Calculates standard error from the uncertainty interval """

    if method not in ['ratio', 'non-ratio']:
        raise UncertaintyError("Received a bad method: {} while calculating SE from UI".format(method))

    quantile = 1 - (1 - confidence) / 2
    if method == 'ratio':
        n = np.log(upper) - np.log(lower)
        d = 2 * stats.norm.ppf(quantile)
        se = (np.exp(n / d) - 1) * p
    elif method == 'non-ratio':
        high = upper - p
        low = p - lower
        se = np.where(high > low, high, low)
        se = se / stats.norm.ppf(quantile)
    return se

  
def ui_from_se(
        p: np.ndarray,
        se: np.ndarray,
        param_type: str,
        confidence: int=0.95
) -> pd.DataFrame:
    """ Calculates Uncertainty interval based on standard error """
    lower_quantile = (1 - confidence) / 2
    upper_quantile = 1 - lower_quantile

    lower = p + stats.norm.ppf(lower_quantile) * se
    lower[lower < 0] = 0

    upper = p + stats.norm.ppf(upper_quantile) * se
    if param_type == "proportion":
        upper[upper > 1] = 1

    return pd.DataFrame({'lower': lower, 'upper': upper})

  
def fill_single_measure(
        df: pd.DataFrame,
        measure_id: int,
        param_type: str,
        ratio_type: str
) -> pd.DataFrame:
    """Fills in uncertainty for a single measure

    :param df: a pd.DataFrame
    :param measure_id int: the measure id of the data in the df
    :param_type str: "proportion" or "rate"
    :ratio_type str: "ratio" or "non-ratio"

    :return: the original df with uncertainty estimates filled in
    :rtype: pd.DataFrame

    """

    missing_se = pd.isnull(df['standard_error'])
    missing_ui = pd.isnull(df['lower']) | pd.isnull(df['upper'])

    
    df_ok = df[~(missing_se) & ~(missing_ui)]

    
    df_se_from_ui = df[(missing_se) & ~(missing_ui)]
    df_se_from_ui.reset_index(inplace=True, drop=True)
    if not df_se_from_ui.empty:
        df_se_from_ui['standard_error'] = se_from_ui(
            p=df_se_from_ui["mean"].astype(float).values,
            lower=df_se_from_ui["lower"].astype(float).values,
            upper=df_se_from_ui["upper"].astype(float).values,
            method=ratio_type,
            confidence=(df_se_from_ui["uncertainty_type_value"].astype(float).values / 100)
        )

    
    df_ui_from_se = df[~(missing_se) & (missing_ui)]
    df_ui_from_se.reset_index(inplace=True, drop=True)
    if not df_ui_from_se.empty:
        df_ui_from_se[['lower', 'upper']] = ui_from_se(
            p=df_ui_from_se["mean"].astype(float).values,
            se=df_ui_from_se["standard_error"].astype(float).values,
            param_type=param_type
        )

    
    df_se_ui_from_ess = df[(missing_se) & (missing_ui)]
    df_se_ui_from_ess.reset_index(inplace=True, drop=True)
    if not df_se_ui_from_ess.empty:
        result = se_ui_from_ess(
            p=df_se_ui_from_ess["mean"].astype(float).values,
            ess=df_se_ui_from_ess["effective_sample_size"].astype(float).values,
            param_type=param_type,
            measure_id=measure_id
        )
        df_se_ui_from_ess.upper = result.upper
        df_se_ui_from_ess.lower = result.lower
        df_se_ui_from_ess.standard_error = result.standard_error

    
    filled_df = pd.concat([
        df_ok,
        df_se_from_ui,
        df_ui_from_se,
        df_se_ui_from_ess
    ], ignore_index=True)

    if df.shape != filled_df.shape:
        raise UncertaintyError(
            "Creating uncertainty changed the shape of the data"
        )

    return filled_df
