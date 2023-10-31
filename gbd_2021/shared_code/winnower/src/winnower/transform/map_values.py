import csv
import tempfile
import pandas
import numpy

from attr import attrib
from winnower.util.dtype import is_string_dtype
from winnower.constants import LEV_RATIO_THLD
from winnower import errors

from .base import (
    Component,
    attrs_with_logger
)


@attrs_with_logger
class MapValues(Component):
    """
    Performs a left inner merge between the extraction and a table of
    values which users manually match to categories.

    Necesarry for categorical indicators like water source, education level,
    disease treatment, etc.

    Args:
        topic_value_maps: a dict of value maps for each indicator. In the form
            {'topic': {'indicator': {'raw_val': 'mapped_val',
                                     'raw_val2': 'mapped_val_2'},
                       'indicator2': {...},
                       }, 'topic2': {...}}
        mapable_indicators: sequence of IndicatorForm instances.
        write_unmapped_to_tempdir: whether to write temporary files with
            unmapped values for easy copy/paste into value map database.
    """
    topic_value_maps = attrib()
    mapable_indicators = attrib()
    input_columns = attrib(default=None)
    write_unmapped_to_tempdir = attrib(default=True)
    # this should be left as default - simpler than using __attrs_post_init__
    unmapped_values = attrib(factory=dict)

    def mapped_indicators(self, input_columns):
        """
        Return list of all indicators this will map.
        """
        have_value_maps = set()
        for topic_value_map in self.topic_value_maps.values():
            have_value_maps.update(topic_value_map)

        mapable_names = {X.indicator_name for X in self.mapable_indicators}
        possible_to_map = have_value_maps & mapable_names

        return [f"{ind}_mapped"
                for ind in input_columns if ind in possible_to_map]

    def output_columns(self, input_columns):
        mapped_names = self.mapped_indicators(input_columns)
        input_columns.extend(mapped_names)
        return input_columns

    def get_uses_columns(self):
        return self.input_columns

    def execute(self, df):
        """
        Merges left (extraction) and
        right (value_map) data frames
        """
        ind_type = {
            ind.indicator_name: ind.indicator_type
            for ind in self.mapable_indicators}

        for topic, topic_value_maps in self.topic_value_maps.items():
            topic_map_suggestion = pandas.DataFrame({'topic': [],
                                                     'indicator': [],
                                                     'value': [],
                                                     'category_indicator_name': [],
                                                     'match_confidence': []})
            for indicator, ind_map in topic_value_maps.items():
                if indicator not in df or indicator not in ind_type:
                    continue

                if not is_string_dtype(df[indicator]):
                    msg = (f'Column {indicator} is not a str. '
                           'You must label numeric values not map them.')
                    raise errors.Error(msg)
                # normalize values to lower case for matching
                col = df[indicator].str.lower()
                mapped = f"{indicator}_mapped"
                if ind_type[indicator] == 'categ_str':
                    map_func = self._map_categ_str
                elif ind_type[indicator] == 'categ_multi_bin':
                    map_func = self._map_categ_multi_bin
                else:
                    msg = f"Unable to map {ind_type[indicator]}"
                    raise NotImplementedError(msg)

                df[mapped], unmapped_values = map_func(col, ind_map)

                if unmapped_values:
                    self.unmapped_values.setdefault(topic, [])
                    self.unmapped_values[topic].extend([indicator, value]
                                                       for value in unmapped_values)
                    # Get mapping sugestions for this topic/indicator
                    topic_map_suggestion = topic_map_suggestion.append(
                                                self._gen_map_suggestions(unmapped_values,
                                                                          topic, indicator, ind_map))
                # original ubcov lower()'d the unmapped column
                df[indicator] = col.str.lower()
            # Write suggestion mapping for each topic
            write_topic_suggestions(topic, topic_map_suggestion, self.logger)
        if self.unmapped_values:
            self._handle_unmapped_values(self.unmapped_values)

        return df

    def _validate(self, input_columns):
        """
        Populate self.input_columns which will be used to selectively
        load columns from the data.
        """
        extra_columns = []
        for indicator in self.topic_value_maps:
            if indicator in input_columns:
                extra_columns.append(indicator)
        self.input_columns = extra_columns

    def _map_categ_str(self, in_col, value_map):
        unmapped = [X for X in in_col.unique() if X not in value_map]
        replace_map = dict.fromkeys(unmapped, "")
        replace_map.update(value_map)
        return in_col.replace(replace_map), unmapped

    def _map_categ_multi_bin(self, in_col, value_map):
        unmapped = set()

        def replace_func(val):
            try:
                parts = val.split('###')
                unmapped.update(X for X in parts if X not in value_map)
                return '###'.join(value_map[part] for part in parts
                                  if part in value_map)
            except AttributeError:
                return ''

        return in_col.map(replace_func), list(unmapped)

    def _handle_unmapped_values(self, unmapped):
        # alert user; write temporary file(s) for convenience
        for topic, unmapped_info in unmapped.items():
            msg = f"{len(unmapped_info)} unmapped values for {topic}"
            self.logger.warning(msg)
            template = "({}) {} - {}"
            for indicator, value in unmapped_info:
                self.logger.warning(template.format(topic, indicator, value))

            if self.write_unmapped_to_tempdir:  # True when not testing
                with tempfile.NamedTemporaryFile(prefix=f"{topic}_",
                                                 mode='w',
                                                 delete=False) as outf:
                    outfile = csv.writer(outf)
                    outfile.writerows(unmapped_info)

                self.logger.error(f"Unmapped values for {topic} have been "
                                  f"written to a CSV file at {outf.name}")

    def _gen_map_suggestions(self, unmapped, topic, indicator, value_map):
        # Iterate over unmapped values and find approximate matches
        # for new topic value map suggestions
        d = {'topic': [],
             'indicator': [],
             'value': [],
             'category_indicator_name': [],
             'match_confidence': []}

        for um in unmapped:
            for val in value_map:
                # Calculate Levenshtein Ratio, if over threashold, add as suggestion
                lev = levenshtein_ratio(um, val)
                if lev > LEV_RATIO_THLD:
                    d['topic'].append(topic)
                    d['indicator'].append(indicator)
                    d['value'].append(um)
                    d['category_indicator_name'].append(value_map[val] + '|' + indicator)
                    d['match_confidence'].append(str(lev))

        return pandas.DataFrame(d)


def write_topic_suggestions(topic, df, logger):
    # Write the suggestions into a csv file
    if df.shape[0] == 0:
        return

    with tempfile.NamedTemporaryFile(prefix=f"{topic}_map_suggestions",
                                     mode='w',
                                     delete=False) as outf:
        df.to_csv(outf, index=False)
        logger.error(f"Map value suggestions for {topic} have been "
                     f"written to a CSV file at {outf.name}")


def levenshtein_ratio(str1, str2):
    # Calculate Levenshtein Ratio between two strings
    if not (isinstance(str1, str) and isinstance(str2, str)):
        return 0
    # We initialize a numpy matrix of zeros.
    distances = numpy.zeros((len(str1) + 1, len(str2) + 1))

    # Write out indeces for the matrix
    for i in range(len(str1) + 1):
        distances[i][0] = i

    for j in range(len(str2) + 1):
        distances[0][j] = j

    # Iterate over the grid and populate distances between string indeces
    for i in range(1, len(str1) + 1):
        for j in range(1, len(str2) + 1):
            if (str1[i-1] == str2[j-1]):
                # If the strings at these cell indeces are the same
                # character we keep the distance of the prior diagonal cell
                distances[i][j] = distances[i - 1][j - 1]
            else:
                # If characters are different, the distance at this cell
                # will be the smallest distance of the three prior
                # adjacent cells plus 1
                d1 = distances[i][j - 1]
                d2 = distances[i - 1][j]
                d3 = distances[i - 1][j - 1]

                distances[i][j] = min([d1, d2, d3]) + 1

    # We calculate the ratio using the combined lengths of both strings
    # and the levenshtein distance, representes as the right bottom
    # corner cell of the matrix
    lev_dist = distances[len(str1)][len(str2)]
    len_sum = len(str1) + len(str2)
    return (len_sum - lev_dist) / len_sum
