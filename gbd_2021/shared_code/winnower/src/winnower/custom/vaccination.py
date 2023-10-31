from winnower import errors
from winnower.globals import eg
from winnower.util.dates import datetime_to_numeric
from winnower.util.dtype import is_datetime_dtype
from winnower.util.stata import is_missing_str

from .base import TransformBase


class Transform(TransformBase):
    # TODO: when this is removed update test_vaccination's vacc_config fixture
    # to remove the unncessary file_path value.
    one_off_hacked_files = frozenset([
        "<FILEPATH>",
    ])

    # list of all vaccine-doses for reference
    # order of doses matter!
    vaccines = ("dpt1", "dpt2", "dpt3",
                "pent1", "pent2", "pent3",
                "tetra1", "tetra2", "tetra3",
                "polio1", "polio2", "polio3",
                "hepb1", "hepb2", "hepb3",
                "hib1", "hib2", "hib3",
                "pcv1", "pcv2", "pcv3",
                "rota1", "rota2", "rota3",
                "mcv1", "mcv2",
                "mmr1", "mmr2",
                "bcg",
                "yfv",
                )

    uses_extra_columns = None

    def execute(self, df):
        for out_column, source_col_or_val in self.vaccine_out:
            if isinstance(source_col_or_val, str):  # input column
                col = df[source_col_or_val]

                if eg.strict and is_datetime_dtype(col):
                    col = datetime_to_numeric(col)

                df[out_column] = col
            else:  # value
                df[out_column] = source_col_or_val

        return df

    def validate(self, input_columns):
        self._error_if_custom_code_has_unimplemented_file_specific_code()

        # sequence of (output_column, source_column_OR_constant)
        vaccine_out = []
        extra_columns = []

        dmy_suffixes = ['day', 'month', 'year']
        for vacc in self.vaccines:
            codebook_column = f"card_{vacc}_dmy"
            fields = self._get_input_fields(codebook_column)
            if fields is None:
                continue
            if len(fields) == 1:  # Single variable indicates a date column
                out_col = f"card_{vacc}_date"
                vaccine_out.append([out_col, fields[0]])
                extra_columns.append(fields[0])
            elif len(fields) == 3:  # comma-separated D/M/Y
                # D/M/Y vals may be numbers
                for col_or_val, suffix in zip(fields, dmy_suffixes):
                    out_col = f"card_{vacc}_{suffix}"
                    vaccine_out.append([out_col, col_or_val])
                    if isinstance(col_or_val, str):
                        extra_columns.append(col_or_val)
            else:
                msg = (f"Expected either 1 or 3 values in {codebook_column}: "
                       f"Instead found {len(fields)} values: {fields} "
                       "(NOTE: field parts by splitting on ',')")
                raise errors.ValidationError(msg)

        self.vaccine_out = vaccine_out
        if extra_columns:
            extra_columns = [X for X in extra_columns if X in input_columns]
            self.uses_extra_columns = extra_columns

    def output_columns(self, input_columns):
        res = input_columns
        res.extend(X for X, _ in self.vaccine_out)
        return res

    def _get_input_fields(self, config_key):
        raw = self.config.get(config_key)
        if raw is None:
            return None
        fields = [X.strip() for X in raw.split(",")]

        result = []
        for field in fields:
            try:
                resolved = self.get_column_name(field)
            except errors.ValidationError:
                if field.isdigit():
                    # TODO: warn on behavior
                    result.append(int(field))
                elif is_missing_str(field):
                    result.append(float('NaN'))
                else:
                    msg = (f"Can't find column {field!r} from input {raw!r} "
                           "from column {config_key!r}")
                    raise errors.ValidationError(msg)
            else:
                result.append(resolved)
        return result

    def _error_if_custom_code_has_unimplemented_file_specific_code(self):
        path = self.config['file_path'].name
        if path in self.one_off_hacked_files:
            msg = f"Hack exists for path {path} - cannot extract yet"
            raise NotImplementedError(msg)
