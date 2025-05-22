class WarningLog:
    def __init__(self):
        self.text = """"""
        self.count = 0

    def __repr__(self):
        return self.text

    def add_warning(self, warning):
        """
        (self, str) -> None

        Add a warning to the WarningLog class object and add one to the warning
        counter.
        """
        self.count += 1
        self.text += "{count}. {message}\n".format(count=self.count, message=warning)

    def log_missingness(self, data_frame, covariates):
        """
        (self, data_frame, list of strings) -> None

        Log warnings that describe the number of missing values in the data
        frame, one for every covariate with missing values. Warnings are logged
        using the add_warning method.
        """
        message = "{na_count} missing values in variable {variable}, removing observations."
        missing = {x: data_frame[x].isnull().sum() for x in covariates}
        for m in list(missing.keys()):
            if missing[m] != 0:
                self.add_warning(message.format(na_count=missing[m], variable=m))

    def log_bad_cf(self, data_frame, var):
        """
        (self, data_frame, str) -> None

        Log warnings that describe the number of bad values you have
        (either 0 or 1) for a particular variable. In the case of CODEm this
        function is used for cause fraction values.
        """
        message = "{bad_cf_count} values of 0 or 1 in variable {variable}, converted to NaN."
        bad_value_count = ((data_frame[var] == 1) | (data_frame[var] == 0)).sum()
        self.add_warning(message.format(bad_cf_count=bad_value_count, variable=var))
