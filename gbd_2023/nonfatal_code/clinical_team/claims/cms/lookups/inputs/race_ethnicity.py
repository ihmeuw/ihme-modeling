"""
Nested dictionary of all the different methods for storing race in the CMS data

Structure is {cms_column_name: race_code: race_name} so you can directly map
from coded values to str values. See get_cms_flat_files.py for
a method to perform the mapping

"""

race_lookup = {
    # MEDICARE VARS PRESENT
    'bene_race_cd': {
        0: 'UNKNOWN',
        1: 'WHITE',
        2: 'BLACK',
        3: 'OTHER',
        4: 'ASIAN',
        5: 'HISPANIC',
        6: 'NORTH AMERICAN NATIVE'
        },

    'rti_race_cd': {
        0: 'UNKNOWN',
        1: 'NON-HISPANIC WHITE',
        2: 'BLACK',
        3: 'OTHER',
        4: 'ASIAN/PACIFIC ISLANDER',
        5: 'HISPANIC',
        6: 'AMERICAN INDIAN/ALASKA NATIVE'
        },

    ## MAX VARS PRESENT
    # matches bene_race_cd identically
    'mdcr_race_ethncy_cd': {
        0: 'UNKNOWN',
        1: 'WHITE',
        2: 'BLACK',
        3: 'OTHER',
        4: 'ASIAN',
        5: 'HISPANIC',
        6: 'NORTH AMERICAN NATIVE'
        },

    'el_race_ethncy_cd': {
        1: 'WHITE',
        2: 'BLACK',
        3: 'AMERICAN INDIAN OR ALASKAN NATIVE',
        4: 'ASIAN OR PACIFIC ISLANDER',
        5: 'HISPANIC',
        6: 'NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER',
        7: 'HISPANIC OR LATINO AND ONE OR MORE RACES (NEW CODE BEGINNING 10/98)',
        8: 'MORE THAN ONE RACE (HISPANIC OR LATINO NOT INDICATED)',
        9: 'UNKNOWN'
        },

    'el_sex_race_cd': {
        1: 'WHITE, MALE',
        2: 'WHITE, FEMALE',
        3: 'NON-WHITE, MALE',
        4: 'NON-WHITE, FEMALE',
        5: 'RACE UNKNOWN, MALE',
        6: 'RACE UNKNOWN, FEMALE',
        7: 'SEX UNKNOWN, WHITE',
        8: 'SEX UNKNOWN, NON-WHITE',
        9: 'SEX AND RACE UNKNOWN'
        },

    # now the 5 cols
    'race_code_1': {
        0: 'NON-WHITE OR RACE UNKNOWN',
        1: 'WHITE',
        9: 'NO ELIGIBILITY INFORMATION'
        },

    'race_code_2': {
        0: 'NON-BLACK/AFRICAN-AMERICAN OR RACE UNKNOWN',
        1: 'BLACK OR AFRICAN AMERICAN'
        },

    'race_code_3': {
        0: 'NON-AMERICAN INDIAN/ALASKA NATIVE OR RACE UNKNOWN',
        1: 'AMERICAN INDIAN/ALASKAN NATIVE'
        },

    'race_code_4': {
        0: 'NON-ASIAN OR RACE UNKNOWN',
        1: 'ASIAN'
        },

    'race_code_5': {
        0: 'NON-NATIVE HAWAIIAN/OTHER PACIFIC ISLANDER OR RACE UNKNOWN',
        1: 'NATIVE HAWAIIAN/OTHER PACIFIC ISLANDER'
        },

}
