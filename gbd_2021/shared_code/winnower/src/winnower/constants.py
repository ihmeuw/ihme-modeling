from enum import IntEnum

# Key for the "links" table in Google docs
GDOCS_LINKS_KEY = '<REDACTED>'

# String representation for "missing" in STATA. Like NaN, but not. See also
# https://www.stata.com/manuals13/u12.pdf#u12.2.1Missingvalues
MISSING_STR = '.'

# The following keys uniquely define IHME surveys.
UBCOV_KEY = ['ubcov_id', ]
# used for filtering rows from the "merge" configuration
MERGE_KEY = ('nid', 'ihme_loc_id', 'year_start', 'year_end', 'survey_module',
             'file_path')
# alternative method of generating an "ubcov_id" - see "winnower_id" in
# winnower.config.ubcov.fields
# NOTE: Key order matters for winnower id generation.
SURVEY_KEY = ['survey_name', 'nid', 'ihme_loc_id',
              'year_start', 'year_end',
              'survey_module', 'file_path']

# Constant for Levenshtein Ratio threshold, used for approximate string mapping
LEV_RATIO_THLD = 0.8


class Sex(IntEnum):
    "Male/Female codes used by IHME"
    MALE = 1
    FEMALE = 2


class SurveyModule:
    """
    Valid survey module types.

    See ubCov documentation at <ADDRESS>
    """
    BIRTH_RECORD = 'BR'
    CHILDREN = 'CN'
    WOMEN = 'WN'
    MEN = 'MN'
    HOUSEHOLD = 'HH'
    HOUSEHOLD_MEMBER = 'HHM'
    HOUSEHOLD_RESHAPED_HOUSEHOLD_MEMBER = 'HH_HHM'
    WOMEN_RESHAPED_CHILDREN = 'WN_CH'
