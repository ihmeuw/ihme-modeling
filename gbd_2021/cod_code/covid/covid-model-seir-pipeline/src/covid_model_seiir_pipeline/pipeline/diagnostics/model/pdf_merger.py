from pathlib import Path
from typing import List

import pandas as pd
from PyPDF2 import PdfFileMerger as PdfFileMerger_


class PdfFileMerger(PdfFileMerger_):
    """Super annoying that the real class isn't a context manager."""
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def merge_pdfs(plot_cache: Path, output_path: Path, hierarchy: pd.DataFrame):
    """Merge together all pdfs in the plot cache and write to the output path.
    The final pdf will have locations ordered by a depth first search of the
    provided hierarchy with nodes at the same level sorted alphabetically.
    """
    parent_map = hierarchy.set_index('location_id').parent_id
    name_map = hierarchy.set_index('location_id').location_ascii_name

    sorted_locations = get_locations_dfs(hierarchy)
    merged = []
    with PdfFileMerger() as merger:
        current_page = 0
        for location_id in sorted_locations:
            result_page_path = plot_cache / f'{location_id}_results.pdf'
            variants_page_path = plot_cache / f'{location_id}_details.pdf'
            covariate_page_path = plot_cache / f'{location_id}_drivers.pdf'

            if not result_page_path.exists():
                # We didn't model the location for some reason.
                continue

            # Add the results page.
            merger.merge(current_page, str(result_page_path))

            # Bookmark it and add a reference to it's parent.
            parent_id = parent_map.loc[location_id]
            if parent_id != location_id and parent_id in merged:
                parent = name_map.loc[parent_id]
            else:
                parent = None
            merger.addBookmark(name_map.loc[location_id], current_page, parent)

            # Add the variants and covariates pages.
            merger.merge(current_page + 1, str(variants_page_path))
            merger.merge(current_page + 2, str(covariate_page_path))

            merged.append(location_id)
            current_page += 3

        if output_path.exists():
            output_path.unlink()
        merger.write(str(output_path))


def get_locations_dfs(hierarchy: pd.DataFrame) -> List[int]:
    """Return location ids sorted by a depth first search of the hierarchy.
    Locations at the same level are sorted alphabetically by name.
    """
    def _get_locations(location: pd.Series):
        locs = [location.location_id]

        children = hierarchy[(hierarchy.parent_id == location.location_id)
                             & (hierarchy.location_id != location.location_id)]
        for child in children.sort_values('location_ascii_name').itertuples():
            locs.extend(_get_locations(child))
        return locs

    top_locs = hierarchy[hierarchy.location_id == hierarchy.parent_id]
    locations = []
    for top_loc in top_locs.sort_values('location_ascii_name').itertuples():
        locations.extend(_get_locations(top_loc))

    return locations
