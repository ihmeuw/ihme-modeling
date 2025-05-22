"""Identify which bundles map to a given cause_id, and all child causes,

Example use for injuries:
```
inj_bundles = BundleCause(
    cause_id=687, map_version=33
).bundles_by_cause()
```
"""

from typing import List

from crosscutting_functions.mapping import clinical_mapping_db
from db_queries import get_cause_metadata
from db_tools.ezfuncs import query


class BundleCause:
    """Given a cause_id and clinical map_version, return a list of bundle_ids mapping
    to that cause and all child causes.
    """

    def __init__(
        self, cause_id: int, map_version: int, release_id: int, cause_set_id: int = 9
    ):
        self.cause_id = cause_id
        self.map_version = map_version
        self.cause_set_id = cause_set_id
        self.release_id = release_id

    def bundles_by_cause(self) -> List[int]:
        """Returns a list of bundle_ids associated with any cause at or below the
        input cause_id for a given cause set and release.
        """
        self._cause_hierarchy()
        self._bundle_cause_map()
        self._clinical_bundles()

        # Get a list of all the bundles that map to a given cause and all its children.
        bc = self.bm[self.bm["bundle_id"].isin(self.bundle_cause["bundle_id"])]

        bundle_list = bc["bundle_id"].sort_values().unique().tolist()

        return bundle_list

    def _cause_hierarchy(self) -> None:
        """Get the cause hierarchy for a given cause set ID, and return
        that tuple with that cause and all subcauses."""

        if not isinstance(self.cause_id, int):
            raise TypeError("cause_id must be an integer")
        causes = get_cause_metadata(cause_set_id=self.cause_set_id, release_id=self.release_id)
        cond = (
            f"(causes.path_to_top_parent.str.contains(',{self.cause_id},') "
            f"| (causes.cause_id == {self.cause_id}))"
        )
        causes = causes[eval(cond)]
        cause_sub = tuple(causes["cause_id"].unique())
        if not cause_sub:
            raise RuntimeError("The bundle lookup will fail because here's no cause data.")
        self.cause_sub = cause_sub

    def _bundle_cause_map(self) -> None:
        """Get the bundle-to-cause map for given causes."""
        if len(self.cause_sub) == 1:
            where_clause = f"= {self.cause_sub[0]}"
        else:
            where_clause = f"in {self.cause_sub}"
        self.bundle_cause = query(
            (
                "QUERY"
            ),
            conn_def="CONN_DEF",
        )

    def _clinical_bundles(self) -> None:
        """Pull in all the bundles for a given map."""
        keeps = ["bundle_id", "map_version"]
        bm = clinical_mapping_db.get_clinical_process_data(
            "icg_bundle", map_version=self.map_version
        )
        bm = bm[keeps].drop_duplicates()

        # Retain only the bundles for that cause.
        bm = bm[bm.bundle_id.isin(self.bundle_cause["bundle_id"])]

        self.bm = bm
