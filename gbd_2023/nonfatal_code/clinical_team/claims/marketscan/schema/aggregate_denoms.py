import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import marketscan as constants


class MsSampleSize:
    def __init__(self, denom: pd.DataFrame):
        """Class to create aggregated sample_size values for use in the Marketscan pipeline
        using a denom dataframe of individual beneficiary level yearly records. Aggregates this
        denom df using demographic columns defined in constants into a new self.sample_size df
        This sample_size object is written to disk (in create_yearly_ms_schema.py) and used in
        the Marketscan pipeline to convert from count-space to rate-space.

        Args:
            denom: Beneficiary-Year level denominator data from Marketscan. Each
            row contains demographic data for a unique combination of bene_id and year.
        """
        self.denom = denom

    def remove_oos_benes(self) -> None:
        """Remove beneficiary IDs that do not meet our criteria for inclusion in final
        estimates. eg less than 12 months of enrollment in a year unless a record of death
        or age under 1."""
        pre = len(self.denom)
        self.denom = self.denom.query("in_std_sample == 1")
        post = len(self.denom)
        if pre <= post:
            raise RuntimeError("Out of sample benes were not removed")

    def aggregate_sample_size(self) -> None:
        """Groupby and sum the MS denominator counts."""
        exp_ss = len(self.denom)
        self.sample_size = self.denom.groupby(constants.SAMPLE_SIZE_DEMO).size().reset_index()
        self.sample_size = self.sample_size.rename(columns={0: "sample_size"})
        if exp_ss != self.sample_size["sample_size"].sum():
            raise ValueError("Sample size sum is unexpected value")

    def create_sample_size(self) -> None:
        """Main method to prep and sum MS sample size data."""
        self.remove_oos_benes()
        self.aggregate_sample_size()