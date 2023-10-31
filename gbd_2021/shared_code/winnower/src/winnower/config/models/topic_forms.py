"""
Classes for customizing the forms that process configuration.

Topics without code_custom indicators should not need to define anything here.

Topics with code_custom indicators should define only those fields used by
their code_custom indicators.

See also __init__.py's overview.

Note: these forms *do not* support the full spectrum of attr features. These
classes should *only* define classes without superclases, and should only
define attributes that are attrib() values.

No other class attributes, and no class methods should be defined.
"""
from attr import attrib, attributes
import pandas

from winnower import errors
from .fields import (
    pandas_int,
    optional_varlist,
    value,
)


class TopicFormManagerBase:
    """
    Manager class for indicator-specific configuration processors.

    These are designed to process form data ala wtforms, django forms, etc.

    Our form data is the configuration entered into ubCov's database (either
    "basic" or topic-specific codebooks). The form is meant to be able to
    refect against the "Indicators" database, which define all current
    indicators

    Some indicators will have a "code_custom" value set to 1 (True). In this
    case we *can not* reflect appropriate form field processing and require the
    user to manually supply it.
    """
    def for_topic(self, topic):
        """
        Returns registered topic form or None if no form is registered.
        """
        return self.topic_forms.get(topic)

    def register_for_topic(self, topic):
        def register_func(form_cls):
            # Delay err so we can provide the full class path for the new class
            if topic in self.topic_forms:
                msg = (f"Cannot register {form_cls} for topic {topic!r} "
                       f"as class {self.topic_forms[topic]} is registered.")
                raise errors.Error(msg)
            # decorate as an attr class
            form_cls = attributes(form_cls)
            self.topic_forms[topic] = form_cls
            return form_cls

        return register_func

    def __init__(self):
        self.topic_forms = {}


TopicFormManager = TopicFormManagerBase()


# Classes defining customized form processing behavior
@TopicFormManager.register_for_topic('design')
class DesignForm:
    """
    Configuration related to survey design.

    Args:
        ubcov_id: the primary identifier for matching survey-specific config.
        smaller_site_unit (int): Indicates if data is representative at a
            location smaller than specified in ihme_loc_id. 1 == True
        strata: the column(s) used to identify survey strata.
        psu: the column(s) used to identify the primary sampling unit (AKA
            cluster) for the survey.
        pweight: the column(s) for sample weighting at the person level.
        hhweight: the column(s) for sample weighting at the household level.
        geospatial_id: special variable for pus/cluster to be merged against
            onto geospatial codebooks in instances where psu doesn't work.
        hh_id: household identifier.
        line_id: line identifier.
    """
    ubcov_id = attrib(converter=int)
    smaller_site_unit = value(converter=pandas_int)
    strata = optional_varlist()
    psu = optional_varlist()
    pweight = optional_varlist()
    hhweight = optional_varlist()
    geospatial_id = optional_varlist()
    hh_id = optional_varlist()
    line_id = optional_varlist()


@TopicFormManager.register_for_topic("sbh")
class SbhForm:
    """
    sbh (summary birth history) wants to generate ceb and ced. However the
    input columns to get impute these columns vary, hence why all are listed
    in the indicators codebook as custom code indicators. This form is to
    process the missing values
    - if ceb is not directly configured, it can be imputed so long as ced and
      children_alive are configured
        - likewise if ced is not directly configured, it can only be imputed
          if ceb and children_alive are configured
    - if children_alive is not configured, some surveys contain male_in_house
      and males_elsewhere, females_in_house and females_elsewhere
    Args:
        ceb: children_ever_born
        ced: children_ever_died
        children_alive: # of children still alive
            children_alive + ced (should equal) ceb
        _in_house: # of children living at home
        _elsewhere: # of children living away from home,
            _in_house + _elsewhere = children_alive_
    The following arguments are not used but are included in sbh.do:
        lastborn_cmc: date the last child was born, cmc format
        lastborn_year_ccyy: year the last child was born, 4 digit format
        lastborn_month_mm: month the last child was born
        lastborn_day_dd: day of the month the last child was born
        stillbirth_number: # of stillbirths
    """
    ceb_missing = optional_varlist()
    ceb_male_missing = optional_varlist()
    ceb_female_missing = optional_varlist()

    ced_missing = optional_varlist()
    ced_male_missing = optional_varlist()
    ced_female_missing = optional_varlist()

    children_alive_missing = optional_varlist()
    children_alive_male_missing = optional_varlist()
    children_alive_female_missing = optional_varlist()

    males_in_house_missing = optional_varlist()
    males_elsewhere_missing = optional_varlist()
    females_in_house_missing = optional_varlist()
    females_elsewhere_missing = optional_varlist()

    lastborn_cmc_missing = optional_varlist()
    lastborn_year_ccyy_missing = optional_varlist()
    lastborn_month_mm_missing = optional_varlist()
    lastborn_day_dd_missing = optional_varlist()

    stilbirth_number_missing = optional_varlist()


@TopicFormManager.register_for_topic('msk_gout')
class Msk_GoutForm:
    "allow multiple values for true/false inputs for msk_gout, which is custom"
    gout_true = optional_varlist()
    gout_false = optional_varlist()
    gout_missing = optional_varlist()
    gout_combined_case_1_true = optional_varlist()

@TopicFormManager.register_for_topic("msk_rheumarthritis")
class Msk_RheumarthritisForm:
    """
    msk_rheumarthritis generate several binary indicators in custom code
    configuring binary input variables to take in multiple values
    - all four of these variables are to generate 'rheum_arth', whether or not
      an individual has rheumatoid arthritis
    """
    rheum_arth_true = optional_varlist()
    rheum_arth_false = optional_varlist()
    rheum_arth_missing = optional_varlist()
    rheum_arth_combined_case_1_true = optional_varlist()


@TopicFormManager.register_for_topic('dem_vr')
class Dem_VrForm:
    "For custom code ind aod_units, allow inputs to have multiple values"
    aod_units_days_vals = optional_varlist()
    aod_units_months_vals = optional_varlist()
    aod_units_years_vals = optional_varlist()
    aod_units_missing_vals = optional_varlist()
    aod_units_missing_vals = optional_varlist()
    date_of_death_missing = optional_varlist()

@TopicFormManager.register_for_topic('education')
class Msk_Education:
    edu_level_cont_missing = optional_varlist()

@TopicFormManager.register_for_topic('msk_osteoarthritis')
class Msk_OsteoarthritisForm:
    "For custom code ind ost_arth, allow t/f inputs to have multiple values"
    ost_arth_true = optional_varlist()
    ost_arth_false = optional_varlist()
    ost_arth_missing = optional_varlist()

@TopicFormManager.register_for_topic('demographics')
class DemographicsForm:
    """
    Configuration related to demographics identifiers within a survey.

    Args:
        ubcov_id: the primary identifier for matching survey-specific config.
        sex: column containing the gender identifier for the person/row.
        sex_male: the value(s) which indicate male.
        sex_female: the value(s) which indicate female.
        age_categorical: column with the age identifier.
        age_categorical_type: Used to name a calculated column. See 2) below.
        age_categorical_parse: delimiter used to parse values in
            age_categorical.

        birth_date: column containing date of birth.
        birth_date_format: format specifier for how to parse birth_date column.

        int_date: column specifying the date of the survey interview.
        int_date_format: format specifier for how to parse int_date column.

        birth_day: column containing a number representing the day of the month
            that the survey subject was born (1 thru 28-31, depending on month)
        birth_month: column containing a number representing the month that the
            survey subject was born (1-12).
        birth_year: column containing a number representing the year that the
            survey subject was born.

        int_day: same as birth_day except representing the day of the interview
        int_month: same as birth_month except representing the interview month.
        int_year: same as birth_year except representing the interview year.

        age_month: column containing a number representing the age of the
            interview subject in months.
        age_month_missing: indicator of value(s) in age_month that indicate a
            missing value. See below.
        age_year: column containing a number representing the age of the
            interview subject in years.
        age_year_missing: indicator of value(s) in age_year that indicate a
            missing value. See below.

        year_adjust: numeric value to add to birth_year and interview_year (if
            defined).
        month_adjust: numeric value to add to birth_month and int_month.
    """
    ubcov_id = attrib(converter=int)
    sex = value()
    sex_male = optional_varlist()
    sex_female = optional_varlist()

    age_categorical = value()
    age_categorical_type = value()
    age_categorical_parse = value()

    age_year = value()
    # TODO: need optional_varlist_or_expression
    age_year_missing = optional_varlist()

    age_month = value()
    # TODO: need optional_varlist_or_expression
    age_month_missing = optional_varlist()

    # Birth date-related vars
    birth_date = value()
    birth_date_format = value()
    birth_day = value()
    birth_month = value()
    birth_year = value()

    # Interview date-related vars
    int_date = value()
    int_date_format = value()
    int_day = value()
    int_month = value()
    int_year = value()

    # Fixed adjustments
    year_adjust = value(converter=pandas_int)
    month_adjust = value(converter=pandas_int)

    @age_categorical_type.validator
    def validate_age_categorical_type(self, attribute, value):
        if value in {'month', 'year'} or pandas.isnull(value):
            return
        msg = f"{attribute.name!r}: provide 'year' or 'month'; not {value!r}"
        raise errors.ValidationError(msg)


@TopicFormManager.register_for_topic('ckd')
class CkdForm:
    acr_missing = optional_varlist()
    ckd_stage5 = value()
    ckd_stage4 = value()
    ckd_stage3 = value()
    ckd_stage3_5 = value()

    ckd_stage5_mdrd = value()
    ckd_stage4_mdrd = value()
    ckd_stage3_mdrd = value()
    ckd_stage3_5_mdrd = value()

    ckd_stage5_cg = value()
    ckd_stage4_cg = value()
    ckd_stage3_cg = value()
    ckd_stage3_5_cg = value()

    albuminuria = value()
    albuminuria_25 = value()
    albuminuria_20 = value()
    albuminuria_17 = value()

    albuminuria_mdrd = value()
    albuminuria_25_mdrd = value()
    albuminuria_20_mdrd = value()
    albuminuria_17_mdrd = value()

    albuminuria_cg = value()
    albuminuria_25_cg = value()
    albuminuria_20_cg = value()
    albuminuria_17_cg = value()

    no_ckd = value()
