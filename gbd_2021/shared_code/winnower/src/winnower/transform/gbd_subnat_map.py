import os
import pandas

from winnower import errors
from winnower.util.path import fix_survey_path
from winnower.util.stata import MISSING_STR_VALUE

from winnower.util.dtype import (
    is_string_dtype,
)

from winnower.util.categorical import (
    is_categorical,
)

from winnower.transform.map_values import (
    levenshtein_ratio,
    write_topic_suggestions,
)

from winnower.constants import LEV_RATIO_THLD

from .base import (
    attrs_with_logger,
    attrib,
    Component,
)

# The following list of subnational locals is used only in strict mode to force
# winnower to adopt ubCov behavior with regards to which GBD subnational
# mappings will take place. When strict mode is not enabled winnower will
# instead discover the list if unique subnational locals from the gbd_subnat
# configuration.
#
# This is currently used in winnower/config/ubcov.py see there for more
# information on how winnower determines the valid subnational locals to use.
# See:
# <ADDRESS>
STRICT_GBD_SUBNAT_LOCS = [
    'BRA', 'CHN', 'GBR', 'GBR_4749', 'IND', 'IDN', 'JPN', 'KEN',
    'MEX', 'SWE', 'ZAF', 'SAU', 'USA', 'RUS', 'UKR', 'NZL', 'NOR', 'IRN',
    'ETH', 'ITA', 'NGA', 'PAK', 'PHL', 'POL']

@attrs_with_logger
class MapSubnationals(Component):
    """
    Performs a left inner merge between the extraction chain and a table of
    subnationals string representing name of the subnational in the original
    microdata.

    Necessary for GBD modeling below admin0.

    Args:
    gbd_subnat_map: matches made manually by ubcov users between
        strings and location ids
    """
    gbd_subnat_map = attrib()
    input_columns = attrib(default=None)

    def execute(self, df):
        """
        Merges left (extraction) and
        right (gbd_subnat_string_matches) data frames
        """
        gbd_subnat_map = self.gbd_subnat_map
        admins_present = gbd_subnat_map['indicator'].unique()
        # should be admin_1 and/or admin_2. using this method for a
        # potential future where GBD estimates below admin_2
        # subset admins_present to only those contained in the extraction

        admins_present = [admin for admin in admins_present
                          # present in dataframe and of dtype Object (str)
                          if admin in df.columns and (is_string_dtype(df[admin]) or is_categorical(df[admin]))]

        suggested_gbd_map = pandas.DataFrame(columns=[X for X in gbd_subnat_map.keys()
                                                      if not X.lower().startswith('unmapped')]
                                                      + ['match_confidence'])

        for admin in admins_present:
            # lowercase relevant columns (admin_1 pretty much everywhere.
            # admin_2 in KEN and GBR)
            df[admin] = df[admin].str.lower()
            # subset gbd_subnat_map to just the relevant
            # admin level (indicator column)
            admin_level = gbd_subnat_map['indicator']
            current_subnat = gbd_subnat_map.loc[admin_level == admin]
            # drop indicator column
            current_subnat = current_subnat.drop('indicator', axis='columns')
            # rename location_name_short_ihme_loc_id to admin_*_id
            right = current_subnat
            # merge on:
            #      - left: admin_*
            #      - right: value
            df = pandas.merge(
                df, right,
                left_on=admin,
                right_on='value',
                how='left',
                indicator=True,
            )

            # Unmapped values are returned by unmerged left values
            unmapped = df.query('_merge == "left_only"')[admin].dropna().unique()

            suggested_gbd_map = self._gen_map_suggestions(unmapped, admin, gbd_subnat_map, suggested_gbd_map)

            # Query returns equivalent dataframe as pandas.merge with how='left'
            df = df.drop(columns='_merge')

            # use regex to parse out admin_*_id
            df[admin + '_id'] = df['location_name_short_ihme_loc_id']\
                .str.replace(r"^.*(\|)", "")
            # use regex to parse out admin_*_mapped
            df['location_name_short_ihme_loc_id'] = \
                df['location_name_short_ihme_loc_id']\
                .str.replace(r"\|(?<=\|).*$", "")
            df = df.rename(
                columns={'location_name_short_ihme_loc_id': admin + '_mapped'})
            del df['value']

            # clean up columns as mapping/merging may leave NaNs
            for col in (f"{admin}_id", f"{admin}_mapped"):
                if df[col].isna().any():
                    df[col] = df[col].fillna(MISSING_STR_VALUE)
        write_topic_suggestions('gbd_subnat', suggested_gbd_map, self.logger)
        return df

    def output_columns(self, input_columns):
        colnames = list(input_columns)
        new_cols = list(self.gbd_subnat_map['indicator'].unique())
        id_cols = [col + '_id' for col in new_cols]
        mapped_cols = [col + '_mapped' for col in new_cols]
        colnames.extend(id_cols)
        colnames.extend(mapped_cols)
        return colnames

    def _validate(self, input_columns):
        """
        Populate self.input_columns which will be used to selectively
        load columns from the data.
        """
        admins_present = self.gbd_subnat_map['indicator'].unique()
        self.input_columns = [admin for admin in admins_present
                              if admin in input_columns]

    def get_uses_columns(self):
        return self.input_columns

    def _gen_map_suggestions(self, unmapped, indicator,
                             gbd_subnat_map, suggested_gbd_map):
        # Iterate over unmapped values and find approximate matches
        # for new topic value map suggestions
        ind_df = gbd_subnat_map.query(f'indicator == "{indicator}"')

        for um in unmapped:
            mapped_to = set()
            for _, row in ind_df.iterrows():
                # Calculate Levenshtein Ratio, if over threashold,
                # add as suggestion
                lev = levenshtein_ratio(um, row['value'])

                if lev > LEV_RATIO_THLD and row['location_name_short_ihme_loc_id'] not in mapped_to:
                    # Include as suggested mapping
                    fixed_row = row.fillna("")

                    new_row = {}
                    for col, val in fixed_row.items():
                        if col == 'value':
                            new_row[col] = um
                        else:
                            new_row[col] = val
                    new_row['match_confidence'] = str(lev)

                    suggested_gbd_map = suggested_gbd_map.append(new_row, ignore_index=True)
                    mapped_to.add(row['location_name_short_ihme_loc_id'])

        return suggested_gbd_map


@attrs_with_logger
class MapIndiaUrbanRuralSubnationals(Component):
    map_file_path = ('<FILEPATH>')
    input_columns = attrib(default=None)

    def execute(self, df):
        # Map urban/rural India subnationals if possible
        if 'admin_1_id' not in df:
            iso3 = df['ihme_loc_id'].unique()[0]  # uniform value in dataset
            df['admin_1_id'] = iso3

        if 'admin_1_id' in df and 'urban' in df:
            ind_table = self.get_india_subnational_table()
            df = self.map_india_subnats(df, ind_table)

        return df

    def _validate(self, input_columns):
        """
        Populate self.input_columns which will be used to selectively
        load columns from the data.
        """
        extra_columns = []
        if 'admin_1_id' in input_columns:
            extra_columns.append('admin_1_id')
        elif 'ihme_loc_id' in input_columns:
            extra_columns.append('ihme_loc_id')

        if 'urban' in input_columns:
            extra_columns.append('urban')

        self.input_columns = extra_columns

        p = fix_survey_path(self.map_file_path)
        if not os.path.exists(p):
            msg = ("Cannot map Indian Urban/Rural subnationals - file "
                   f"{os.path.basename(p)} does not exist as {p}")
            raise errors.ValidationError(msg)

    def get_uses_columns(self):
        return self.input_columns

    def output_columns(self, input_columns):
        if 'admin_1_id' not in input_columns:
            input_columns.append('admin_1_id')
        input_columns.extend(['admin_1_urban_id', 'admin_1_urban_mapped'])
        return input_columns

    def get_india_subnational_table(self):
        return pandas.read_csv(fix_survey_path(self.map_file_path))

    def map_india_subnats(self, mapped, ind_table):
        """
        Merge IND subnat info with existing GBD subnat map.
        """
        left = mapped
        right = ind_table
        merge_cols = ["admin_1_id", 'urban']
        mapped = pandas.merge(
            left, right,
            on=merge_cols,
            how='left',
        )
        # clean up columns as merge may leave NaNs
        for col in ind_table:
            if col in merge_cols:
                continue
            mapped[col].fillna(MISSING_STR_VALUE, inplace=True)

        return mapped
