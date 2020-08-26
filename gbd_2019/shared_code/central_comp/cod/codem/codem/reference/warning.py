import codem.data.queryStrings as QS
import codem.reference.db_connect as db_connect
import sys


class WarningLog:
    def __init__(self, model_version_id, db_connection):
        self.text = ''''''
        self.count = 0
        self.model_version_id = model_version_id
        self.db_connection = db_connection

    def __repr__(self):
        return self.text

    def add_warning(self, warning):
        '''
        (self, str) -> None

        Add a warning to the WarningLog class object and add one to the warning
        counter.
        '''
        self.count += 1
        self.text += "{count}. {message}\n".format(count=self.count, message=warning)

    def log_missingness(self, data_frame, covariates):
        '''
        (self, data_frame, list of strings) -> None

        Log warnings that describe the number of missing values in the data
        frame, one for every covariate with missing values. Warnings are logged
        using the add_warning method.
        '''
        message = "{na_count} missing values in variable {variable}, removing observations."
        missing = {x: data_frame[x].isnull().sum() for x in covariates}
        for m in list(missing.keys()):
            if missing[m] != 0:
                self.add_warning(message.format(na_count=missing[m], variable=m))

    def log_bad_cf(self, data_frame, var):
        '''
        (self, data_frame, str) -> None

        Log warnings that describe the number of bad values you have
        (either 0 or 1) for a particular variable. In the case of CODEm this
        function is used for cause fraction values.
        '''
        message = "{bad_cf_count} values of 0 or 1 in variable {variable}, converted to NaN."
        bad_value_count = ((data_frame[var] == 1) | (data_frame[var] == 0)).sum()
        self.add_warning(message.format(bad_cf_count=bad_value_count, variable=var))

    def time_stamp(self, procedure):
        '''
        (self, str) -> None

        Write to the database when a particular part of the procedure has
        started so that it can be read by the CodViz tool. This enables users
        to see what stage a model is currently at (i.e. covariate selection,
        linear model building, space time smoothing, gaussian process, etc.).
        '''
        phrase = "%s started." % procedure
        call = QS.status_write.format(self.model_version_id, phrase)
        db_connect.query(call, self.db_connection)
        self.add_warning(phrase)

    def check_covariate_selection(self, model_dictionary):
        '''
        (dict) -> None

        Ends a session of CODEm if there are no covariates selected.
        '''
        if len(model_dictionary["mixed"]) == 0:
            message = "No covariates were selected, ending CODEm run"
            self.time_stamp(message)
            sys.exit()
