import os

import glob
import PyPDF2

from db_queries import get_location_metadata

class LocationSpecificGraphs:

    OUTPUT_DIRS = {
        'census_processing_total': 'FILEPATH/{v}/diagnostics/location_specific/{l}_total_pop.pdf',
        'census_processing_baseline': 'FILEPATH/{v}/diagnostics/location_specific/{l}_baseline_pop.pdf',
        'census_processing_age_pattern': 'FILEPATH/{v}/diagnostics/location_specific/{l}_age_pattern_pop.pdf',
    }

    def __init__(self, location_id,
                 version_census_processing_id = None,
                 output_dir = None,
                 ihme_loc_id_dict = None):
        self.location_id = location_id
        self.version_census_processing_id = version_census_processing_id

        self.output_dir = output_dir

        self.ihme_loc_id_dict = ihme_loc_id_dict

    @staticmethod
    def get_ihme_loc_dict():
        ihme_loc_id_dict = {}
        location_hierarchy = get_location_metadata(
            location_set_id = 93, gbd_round_id = 6)
        for i in location_hierarchy.index:
            location_id = location_hierarchy.loc[i, 'location_id']
            ihme_loc_id = location_hierarchy.loc[i, 'ihme_loc_id']
            ihme_loc_id_dict[location_id] = ihme_loc_id
        return ihme_loc_id_dict

    @staticmethod
    def verify_file_path(file_path):
        return os.path.exists(file_path)


    def _read_pdf(self, file_path):
        return PyPDF2.PdfFileReader(open(file_path, 'rb'))

    @property
    def ihme_loc_id(self):
        if not self.ihme_loc_id_dict:
            self.ihme_loc_id_dict = self.get_ihme_loc_dict()
        return self.ihme_loc_id_dict[self.location_id]

    @property
    def output_file(self):
        return "{}/{}.pdf".format(
            self.output_dir, self.ihme_loc_id)

    def get_tracking_sheet_file_paths(self):
        # Get the file path
        fp = self.OUTPUT_DIRS['tracking_sheet'].format(l = self.ihme_loc_id)
        # Return file path
        return [fp]

    def get_census_processing_diagnostics_file_paths(self):
        # Get the file path
        fp = [
            self.OUTPUT_DIRS['census_processing_total'].format(
                v = self.version_census_processing_id, l = self.ihme_loc_id),
            self.OUTPUT_DIRS['census_processing_baseline'].format(
                v = self.version_census_processing_id, l = self.ihme_loc_id),
            self.OUTPUT_DIRS['census_processing_age_pattern'].format(
                v = self.version_census_processing_id, l = self.ihme_loc_id)]
        # Return file path
        return fp


    def run(self):
        output_graphs = []
        if self.version_census_processing_id:
            for f in self.get_census_processing_diagnostics_file_paths():
                output_graphs.append(f)
        # Verify
        output_graphs = [f for f in output_graphs if self.verify_file_path(f)]
        # Append together
        if output_graphs:
            m = PyPDF2.PdfFileMerger()
            for f in output_graphs:
                m.append(self._read_pdf(f))
            m.write(self.output_file)
        # Retun file path
        return self.ihme_loc_id
