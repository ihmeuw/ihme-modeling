import pandas
import warnings

from winnower.config.ubcov import (UbcovConfigLoader,)
from winnower.transform import (MapValues, MapSubnationals)

from winnower.transform.base import (
    attrs_with_logger,
    attrib,
)

warnings.filterwarnings(action="ignore", category=UnicodeWarning)
warnings.filterwarnings(action="ignore",
                        category=pandas.io.stata.CategoricalConversionWarning)


@attrs_with_logger
class ApproximateMapper:
    # ApproximateMapper requires a ubcov_id and a root path
    # from which to load the ubcov configuration
    ubcov_id = attrib()
    root = attrib()

    def find_unmatched_outputs(self, topics=()):
        # Load configuration from specified root path.
        # For google sheets configuration, use UrlPaths.for_google_docs()
        # Also load the value mapper
        self.config_loader = UbcovConfigLoader.from_root(self.root)
        topic_value_map = self.config_loader.get_value_maps(topics)

        self._run_matcher(topics=topics,
                          value_map=topic_value_map,
                          topic_specific=True)

    def find_unmatched_gbd_subnat_outputs(self):
        # Load configuration from specified root path.
        # For google sheets configuration, use UrlPaths.for_google_docs()
        # Also load the value mapping to be used
        self.config_loader = UbcovConfigLoader.from_root(self.root)
        gbd_subnat_map = self.config_loader.data_frames['gbd_subnat_map']

        self._run_matcher(topics=('geography', 'migration'),
                          value_map=gbd_subnat_map,
                          topic_specific=False)

    def _run_matcher(self, topics, value_map, topic_specific=True):
        # Get extractor from the given topics and ubcov_id
        extractor = self.config_loader.get_extractor(self.ubcov_id,
                                                     topics=topics)
        extraction = extractor.get_extraction(
            keep_unused_columns=True,
            save_unmapped_values=True)

        # This next step is only necessary to capture logs within MapValues for
        # testing purposes
        for transform in extraction:
            if isinstance(transform, MapValues) \
               or isinstance(transform, MapSubnationals):
                transform.logger = self.logger

        # We execute the extraction but ignore the resulting dataframe, since we are
        # only looking to trigger the mappings which writes output files for the user
        _ = extraction.execute()
