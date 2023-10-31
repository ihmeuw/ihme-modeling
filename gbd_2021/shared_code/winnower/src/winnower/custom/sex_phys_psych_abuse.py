"""
Definitions:

GS - Gold Standard
IPV - intimate partner violence
"""
from collections import Counter
import warnings

from attr import attrs, attrib, fields_dict
import pandas

from winnower import errors

from winnower.config.models.fields import (
    pandas_int,
    optional_varlist,
)
from winnower.util.dataframe import get_column_type_factory
from winnower.custom.base import TransformBase
from winnower.transform.indicators import Continuous

LIFETIME = frozenset(['lifetime'])
YEAR_MONTH = frozenset(['year', 'month'])


def logical_or(df):
    """
    Combine sequence of values as a logical or, returning integers or NaN.

    This is very similar to using `|` or numpy.logical_or except in how NaN
    values are handled. In this case, if a row is all NaN values it should be
    NaN. Otherwise it should be the logical OR of all non-NaN values.
    """
    result = pandas.Series(float('nan'), index=df.index)
    # compute row-wise logical OR as 1/0, ignoring NA values. skip all-NA rows
    sub_result = df.dropna(how='all').any(axis='columns').astype(int)
    # update result in-place. NOTE: it may be faster to .reindex sub_result
    result.update(sub_result)
    return result


def get_raw_abuse_data(df, evaluator, configs):
    """
    Returns the abuse data pertinent to the evaluator/configurations.

    Handles data masking issues where an AbuseConfig instance uses perp_categ,
    which indicates that some of the violence recorded may have a perpetrator
    that is not relevant to our evaluator.
    """
    columns = {}
    for config in configs:
        columns[config.indicator_name] = evaluator.get_abuse_data(df, config)
    return pandas.DataFrame(columns)

    columns = [c.indicator_name for c in configs]
    raw = df[columns]
    return raw


def set_gatewayed_abuse_data(df, evaluator, configs):
    """
    Sets (alters) the abuse data for GatewayEvaluator.
    """
    for config in configs:
        evaluator.set_abuse_data(df, config)


def get_filled_in_abuse(config):
    """
    Yields all abuse fields that are filled in.
    """
    i = 1  # ab1 thru abN.
    while True:
        key = f'ab{i}'
        if key in config:
            input = config[key]
            if input:
                yield key
            i += 1
        else:
            break


# eq=False allows us to hash this object. __eq__ comparisons DO NOT work
@attrs(eq=False)
class AbuseConfig:
    """
    Configuration for a specific abuse input.
    """

    # name of input column and values that indicate abuse did/did not occur
    # this is processed as a normal binary indicator as column indicator_name
    input_column = attrib()
    true = attrib()
    false = attrib()
    indicator_name = attrib()
    # type of abuse e.g., "sexual", "physical"
    # TODO: should we limit these?
    #       we could programmatically limit to ONLY those with Evaluators...
    type = attrib()
    # type of question e.g., "lifetime", "year", "month"
    # TODO: validate it is only one of these 3 values
    recall_type = attrib()
    # for recall_type == "year", the number of years
    # TODO: validate this is None or a positive integer
    recall_value = attrib(converter=pandas_int)
    # age limit imposed by response e.g., 15 indicates
    # "only answer yes if abuse happened before you turned 16"
    ch_quest_agelimit = attrib(converter=pandas_int)
    # identifies the perpetrator for all records. single value input by user
    perp_id = attrib()
    # identifies the perpetrator by row
    perp_categ = attrib()
    # column indicating the first experience of childhood sexual abuse
    # this should be processed as a continuous
    age_first_exp = attrib()
    age_first_exp_missing = attrib()

    # deals with challenges of dealing with encounters with e.g., a 15 and 16
    # year old vs a 15 and 25 year old
    # no current use, but may be included.
    perp_5yrs_older = attrib()
    perp_5yrs_older_true = attrib()
    perp_5yrs_older_false = attrib()

    # no known use for processing yet (useful to DA though)
    qtype = attrib()
    sample = attrib()
    freq_cont = attrib()
    freq_cont_missing = attrib()
    freq_categ = attrib()
    casedef = attrib(default='DEFAULT VALUE TODO REMOVE')

    # abN has two new fields. gateway has column and gateway_false
    # has associated false values.
    gateway = attrib(default=None)
    gateway_false = optional_varlist()

    @classmethod
    def from_config_dict(cls, topic_config, name: str):
        prefix = f"{name}_"
        offset = len(prefix)
        # collect all values for this abuse, stripping the prefix
        d = {field[offset:]: topic_config[field]
             for field in topic_config
             if field.startswith(prefix)}
        # special case: ab# is the input column
        d['input_column'] = topic_config[name]
        d['indicator_name'] = name

        try:
            return cls(**d)
        except TypeError:
            # allows updating of abuse columns without breaking things
            supported_fields = fields_dict(cls)
            deleted = []
            for field in d:
                if field not in supported_fields:
                    deleted.append(field)
            for key in deleted:
                del d[key]
                warnings.warn(f"Ignored fields: {deleted} "
                              "- contact <USERNAME> to get them working")
            result = cls(**d)
            return result
            # once this is all standardized just error instead
            raise RuntimeError(f"Failed for {name}")

    # this is sort of a hack since we declare we're validating one field but it
    # is in alignment with the x_smaller_than_y example in the docs
    # http://www.attrs.org/en/stable/examples.html#validators
    @perp_id.validator
    def one_of_perp_id_and_perp_categ_filled_in(self, attribute, value):
        """
        Validates that exactly one of perp_id or perp_categ is filled in.
        """
        pid = value is not None
        pc = self.perp_categ is not None
        if pid and pc:
            msg = (f"{self.input_column}: both perp_id and perp_categ are "
                   "filled in - fill in only one")
            raise errors.Error(msg)
        elif not pid and not pc:
            msg = (f"{self.input_column}: neither perp_id nor perp_categ are "
                   "filled in. Fill in exactly one")
            raise errors.Error(msg)


_ABUSE_EVALUATORS = []


def default_evaluators():
    return [evaluator() for evaluator in _ABUSE_EVALUATORS]


class Evaluator:
    """
    Evaluates whether this violence configuration matches given criteria.

    This is a base class that should never be instantiated.

    It is expected that two levels of subclasses will exist:

    1. A subclass specific to a general kind of abuse e.g., Violence, Childhood
    Sexual Abuse. This class should be defined with register=False as it is in
    an abstraction, and not an evaluator to be used on data.

    2. Subclasses of #1 which represent specific standards IHME is evaluating.
    """
    def __init_subclass__(cls, registry=_ABUSE_EVALUATORS, register=True):
        """
        This automatically registers all Evaluators.

        https://docs.python.org/3/reference/datamodel.html#object.__init_subclass__
        """
        if register:
            registry.append(cls)
        # TODO: remove these when done
        name = cls.__name__
        if hasattr(cls, 'RECALL_TYPE'):
            raise RuntimeError(f"{name} needs rename: RECALL_TYPE -> RECALL_TYPES")  # noqa

        if cls.RECALL_TYPES:
            if cls.RECALL_TYPES == LIFETIME:
                pass
            elif cls.RECALL_TYPES == YEAR_MONTH:
                if cls.RECALL_MAX_MONTHS == YEAR_MONTH:
                    raise RuntimeError(f"{name} needs RECALL_MAX_MONTHS")
            else:
                raise RuntimeError(f"{name} needs RECALL_TYPES to be constant")

    def _recall_matches(self, abuse):
        if abuse.recall_type not in self.RECALL_TYPES:
            return False

        if self.RECALL_TYPES == YEAR_MONTH:
            # compute configuration value in months
            recall_months = abuse.recall_value
            if abuse.recall_type == 'year':
                recall_months *= 12

            return recall_months <= self.RECALL_MAX_MONTHS
        elif self.RECALL_TYPES == LIFETIME:
            return True
        else:
            raise RuntimeError(f"No logic for recall type {self.RECALL_TYPES}")


class ViolenceEvaluator(Evaluator, register=False):
    """
    Evaluates whether an abuse input matches physical/sexual violence.
    """
    # types of abuse that qualify as this type of violence
    ABUSE_TYPES = None
    # question recall type to qualify as this type of violence
    RECALL_TYPES = None
    # max number of months to allow. Only used if RECALL_TYPE is not 'lifetime'
    RECALL_MAX_MONTHS: int = None
    # perpetrator type for this violence. may come from perp_id or perp_categ
    PERP_TYPES = None
    # name of output column
    OUTPUT_COLUMN = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # TODO: handle all class inputs correctly here. type check and convert
        # if necessary.
        if isinstance(cls.PERP_TYPES, str):
            cls.PERP_TYPES = {cls.PERP_TYPES}

    def __init__(self):
        "validate class."

    def is_input_abuse(self, abuse: AbuseConfig, df=None):
        "Predicate: return True if abuse_config is a valid input for this."
        if abuse.type not in self.ABUSE_TYPES:
            return False
        if not self._recall_matches(abuse):
            return False

        # TODO: IFF abuse_type == 'year' CHECK recall_value is 1!

        if abuse.perp_id:
            return abuse.perp_id in self.PERP_TYPES
        else:  # assumes perp_categ
            # tricker. perp_categ is used to decode the perpatrator id from
            # *within* the dataset. as such we can only validate whether that
            # data is a valid input during extraction when df is not None
            if df is None:
                return True
            return self.PERP_TYPES.intersection(df[abuse.perp_categ].unique())

    def get_abuse_data(self, df, config):
        col_data = df[config.indicator_name]
        if config.perp_categ:  # only values matching used
            mask = df[config.perp_categ].isin(self.PERP_TYPES)
            col_data = col_data[mask]
        return col_data


class GatewayEvaluator(Evaluator):
    """
    Evaluates abN for a given abN_gateway column and abN_gateway_false values.
    """
    ABUSE_TYPES = None
    RECALL_TYPES = None
    RECALL_MAX_MONTHS: int = None
    PERP_TYPES = None
    OUTPUT_COLUMN = None

    def is_input_abuse(self, abuse: AbuseConfig, df=None):
        "Predicate: return True if abuse_config has gateway and gateway_false field."  # noqa
        if abuse.gateway and abuse.gateway_false:
            return True
        return False

    def set_abuse_data(self, df, config):
        """
        Question in abN was gateway'd if gateway value matches any value in
        gateway_false. This means that the participant was not asked
        the question in abN.
        """
        gateway_col_name = f"{config.indicator_name}_gateway"
        df[gateway_col_name] = df[config.gateway]

        # gateway_false is usually a string tuple
        factory = get_column_type_factory(df[gateway_col_name], use_category_dtype=False)   # noqa
        gateway_false = tuple(factory(X) for X in config.gateway_false)

        def set_abn(abN, gateway, gateway_false):
            if gateway in gateway_false:
                return 0
            else:
                return abN

        df[config.indicator_name] = df.apply(
                lambda x: set_abn(x[config.indicator_name],
                                  x[gateway_col_name],
                                  gateway_false), axis=1)


class GSIPV(ViolenceEvaluator):
    "'Gold Standard' Intimate Partner Violence evaluator."
    OUTPUT_COLUMN = 'gs_ipv'
    ABUSE_TYPES = {'physical', 'sexual'}
    RECALL_TYPES = {'lifetime'}
    PERP_TYPES = {'partner', 'spouse'}


class GSIPV_Sexual(GSIPV):
    "Sexual-only variant of GSIPV"
    OUTPUT_COLUMN = 'sex_only_ipv'
    ABUSE_TYPES = {'sexual'}


class GSIPV_Physical(GSIPV):
    "Physical-only variant of GSIPV."
    OUTPUT_COLUMN = 'phys_only_ipv'
    ABUSE_TYPES = {'physical'}


class PastYearIPV(ViolenceEvaluator):
    OUTPUT_COLUMN = 'pastyr_ipv'
    ABUSE_TYPES = {'physical', 'sexual'}
    RECALL_TYPES = YEAR_MONTH
    RECALL_MAX_MONTHS = 12
    PERP_TYPES = {'partner', 'spouse'}


class PastYearIPV_Sexual(PastYearIPV):
    OUTPUT_COLUMN = 'sex_only_pastyr_ipv'
    ABUSE_TYPES = {'sexual'}


class PastYearIPV_Physical(PastYearIPV):
    OUTPUT_COLUMN = 'phys_only_pastyr_ipv'
    ABUSE_TYPES = {'physical'}


class CSAEvaluator(Evaluator, register=False):
    """
    Evaluates whether an abuse input matches childhood sexual abuse.
    """
    # types of abuse that qualify as this type of violence
    ABUSE_TYPES = None
    # question recall type to qualify as this type of violence
    RECALL_TYPES = None
    # age limit imposed on question about sexual abuse
    AGE_LIMIT: int = None
    # age of first sexual experience. This is an input column.
    AGE_FIRST: int = None
    # name of output column
    OUTPUT_COLUMN = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # TODO: handle all class inputs correctly here. type check and convert
        # if necessary.

    def __init__(self):
        "validate class."
        # get_column_name() on inputs

    def is_input_abuse(self, abuse: AbuseConfig, df=None):
        "Predicate: return True if abuse_config is a valid input for this."
        if abuse.type not in self.ABUSE_TYPES:
            return False

        if not self._recall_matches(abuse):
            return False

        if abuse.ch_quest_agelimit is None:
            return False

        if abuse.ch_quest_agelimit != self.AGE_LIMIT:
            return False

        # NOTE: because of how prevalence is computed we DO NOT include other
        # age groups.
        # In other words, if the question is "were you abused before age 17?"
        # and the answer is "yes" you are NOT marked as abused before 18
        return True

    def get_abuse_data(self, df, config):
        col_data = df[config.indicator_name]
        if config.age_first_exp is not None:
            if config.age_first_exp_missing is None:
                age_first = df[config.age_first_exp]
            else:
                # extract the age of first abuse as a Continuous to correctly
                # mask the underlying abuse data.

                # out_col can be anything EXCEPT config.age_first_exp
                out_col = config.age_first_exp + "__noconflict"
                sub_extractor = Continuous(output_column=out_col,
                                           map_indicator=0,
                                           code_custom=0,
                                           required=0,
                                           input_column=config.age_first_exp,
                                           missing=config.age_first_exp_missing)  # noqa

                # this avoids a SettingWithCopyWarning
                sub_df = df[[config.age_first_exp]].copy()

                age_first = sub_extractor.execute(sub_df)[out_col]

            # omit anything without an age first experienced reponse
            col_data = col_data[age_first.dropna().index]
            # set cases where abuse occurred AFTER the AGE_LIMIT to FALSE
            col_data[(col_data == 1) & (age_first > self.AGE_LIMIT)] = 0
        return col_data


# Generate "alternative age threshold" evaluators for ages 12-18
# NOTE: 15 absent - that is GCCSA
#
# This is a more succinct way of defining the 6 classes
# If you add/remove an age, be sure to add/remove a "CSA_Before_X"!
(CSA_Before_12, CSA_Before_13, CSA_Before_14,
 CSA_Before_16, CSA_Before_17, CSA_Before_18) = (
     type(f"CSA_Before_{age}", (CSAEvaluator,), {
         'ABUSE_TYPES': {"child sexual"},
         'RECALL_TYPES': LIFETIME,
         'AGE_LIMIT': age,
         'OUTPUT_COLUMN': f'csa_before_age{age}',
     })
     for age in [12, 13, 14, 16, 17, 18])


class GSCSA(CSAEvaluator):
    """
    'Gold Standard' childhood sexual abuse evaluator.

    This evaluator has two standards unlike other evaluators. It is the only
    CSA evaluator that does not require ch_quest_agelimit be filled in.
    """
    RECALL_TYPES = LIFETIME
    AGE_LIMIT = 15
    OUTPUT_COLUMN = 'gs_csa'

    def is_input_abuse(self, abuse, df=None):
        """
        Predicate: does this abuse constitute childhood sexual abuse?
        """
        return self._is_std(abuse, df=df) or self._is_alt_std(abuse, df=df)

    def _is_std(self, abuse, df):
        """
        Does this match the 'Gold Standard'?

        This matches child-specific sexual abuse IFF the question's responses
        are limited to only include events happening at/before 15 years of age.
        """
        if abuse.type != 'child sexual':
            return False
        if abuse.recall_type != 'lifetime':
            return False
        if abuse.ch_quest_agelimit is None:
            return False
        return abuse.ch_quest_agelimit == self.AGE_LIMIT

    def _is_alt_std(self, abuse, df):
        """
        Does this match the alternative 'Gold Standard'?

        This matches non-child specific sexual abuse IFF the "age first
        experienced" input is set. This is because it is possible the
        respondant provided a number <= 15, which would constitute CSA.
        """
        if abuse.type != 'sexual':
            return False
        if abuse.recall_type != 'lifetime':
            return False
        # any sexual abuse that occurs at or before 15
        return abuse.age_first_exp is not None


class PastYearCSA(CSAEvaluator):
    ABUSE_TYPES = {'child sexual'}
    RECALL_TYPES = YEAR_MONTH
    RECALL_MAX_MONTHS = 12
    AGE_LIMIT = 15
    OUTPUT_COLUMN = 'pastyr_csa'


(PastYearCSA_Before_12, PastYearCSA_Before_13, PastYearCSA_Before_14,
 PastYearCSA_Before_16, PastYearCSA_Before_17, PastYearCSA_Before_18) = (
     type(f"PastYearCSA_Before_{age}", (PastYearCSA,), {
         'ABUSE_TYPES': {'child sexual'},
         'RECALL_TYPES': YEAR_MONTH,
         'RECALL_MAX_MONTHS': 12,
         'AGE_LIMIT': age,
         'OUTPUT_COLUMN': f'pastyr_csa_before_age{age}',
     })
     for age in [12, 13, 14, 16, 17, 18]
 )


class Transform(TransformBase):
    def __init__(self, columns, config, extraction_metadata):
        super().__init__(columns, config, extraction_metadata)
        # retrieve all the filled in abuse indicators from config
        # keep track of which are used for logging purposes
        self.abuse_configs = [AbuseConfig.from_config_dict(self.config, name)
                              for name in get_filled_in_abuse(self.config)]

    def validate(self, input_columns):
        self._report_unused_configs()
        self._fix_input_columns()

    def output_columns(self, input_columns):
        res = list(input_columns)
        res.extend(e.OUTPUT_COLUMN for e, _ in self._evaluators_configs())
        res.extend(self._continuous_asked_columns())
        return res

    def execute(self, df):
        for evaluator, configs in self._evaluators_configs(df):
            if isinstance(evaluator, GatewayEvaluator):
                set_gatewayed_abuse_data(df, evaluator, configs)
            else:
                raw = get_raw_abuse_data(df, evaluator, configs)
                aggregate = logical_or(raw)
                df[evaluator.OUTPUT_COLUMN] = aggregate

        df = self._add_continuous_asked_columns(df)
        return df

    def _evaluators_configs(self, df=None):
        evaluators_configs = []
        for evaluator in default_evaluators():
            related_configs = [c for c in self.abuse_configs
                               if evaluator.is_input_abuse(c, df=df)]
            if related_configs:
                evaluators_configs.append([evaluator, related_configs])
        return evaluators_configs

    def _fix_input_columns(self):
        """
        Update values in self.config that refer to an input column by name.

        raises errors.ValidationError if an input column is not present.
        """
        # recall_value (???)
        # freq_categ (PROBABLY)
        for abuse_config in self.abuse_configs:
            for config_name in ('input_column', 'perp_categ', 'age_first_exp',
                                'freq_cont', 'freq_categ'):
                # NOTE: inputs are all 1 element tuples, except input_column
                value = getattr(abuse_config, config_name)
                if value is None:
                    continue

                if config_name == 'input_column':
                    assert isinstance(value, tuple), "input_column not tuple"
                    value = value[0]

                fixed = self.get_column_name(value)

                if fixed != value:
                    if config_name == 'input_column':
                        abuse_config.input_column = (fixed,)
                    else:
                        setattr(abuse_config, config_name, fixed)

    def _report_unused_configs(self):
        "Report abuse configs which ARE NOT used"
        ctr = Counter(self.abuse_configs)
        for _, related_configs in self._evaluators_configs():
            for rc in related_configs:
                ctr[rc] += 1
        unused = [config.indicator_name for config, count in ctr.items()
                  if count == 0]
        if unused:
            msg = "Unused abuse configs: {}".format(', '.join(unused))
            self.logger.warning(msg)

    def _continuous_asked_columns(self):
        return (asked_col for asked_col, _ in self._yield_asked_and_factory())

    def _add_continuous_asked_columns(self, df):
        for asked_col_name, cont_transform in self._yield_asked_and_factory():
            ind_col = cont_transform.output_column
            asked_col = df[ind_col].notna()
            asked_but_set_to_nan = cont_transform.missing_selector(df)
            asked_col[asked_but_set_to_nan] = True
            df[asked_col_name] = asked_col.astype(float)

        return df

    def _yield_asked_and_factory(self):
        """
        Yields "_asked" column names + factory to create them.

        These columns are meant to augment existing Continuous behavior by
        providing an additional "{ind_name}_asked" boolean column. This column
        signals whether *any* non-NaN data was provided for a column prior to
        the Continuous indicator logic setting values matching
        "{ind_name}_missing" to NaN.
        """
        for ind in self.extraction_metadata.indicators:
            if ind.topic_name != "sex_phys_psych_abuse":
                continue

            if ind.indicator_type != "cont":
                continue

            cont_transform = Continuous.indicator_factory(
                ind, self.extraction_metadata.indicator_config)
            if cont_transform is None:
                continue

            yield (f"{ind.indicator_name}_asked", cont_transform)


"""
Questions:
    IPVE alternative case definition #3:
        "Ever experienced severe physical violence by a partner since age of 15 years"

    how do we differentiate "severe" from regular physical violence?
"""  # noqa
