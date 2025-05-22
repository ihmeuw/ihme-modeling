"""
This module will provide the tables we need to define and track
bundle-to-bundle relationships starting in map version 25 and on.

This module handles initializing, updating and pulling the relationship tables
As of 4/23/2020 it is using the pre-database flat files following the
general structure we use on FILEPATH

Examples of relationship types:
    parent injuries: multiple origin bundle_ids mapping to a single output bundle
    bundle clones: 1:1 clones of bundle_ids

Please Note: The original version of this code used to create the first bundle
relationship tables was removed from this package. It relied heavily on a large
amount of bundle and map specific processing. In order to review that code please
find a commit from before August 2022 in clinical_info and go to the source code
for the Database.bundle_relationships folder.

Update the table like so:
    NOTE: This is what we're expecting to be the workhorse func within a
    map version
    df, lookup = set_existing_relationship(map_version=28,
                                        relationship_id=4,
                                        relationship_dict={1234: 2222})
This will update map_version 28 to add an existing relationship type
(relationship_id 4 is cloning) to create a clone of bundle 1234 into 2222.
Save it to the above directory:
    save(df, lookup)

When a new map is created we can inherit the relationships from an earlier map.
I think we'll want to be careful with this, if we end up adding previously
cloned bundles into a new map version. worse case I think we duplicate data.
Example Usage:
    df, lookup = inherit_relationships(origin_map_version=28, target_map_version=29)
    save(df, lookup)

"""

import datetime
from collections import namedtuple
from typing import Dict, List, Tuple

import pandas as pd
from db_tools.ezfuncs import query

from crosscutting_functions.mapping import clinical_mapping_db

Bundle = namedtuple("Bundle", ["id", "name"])


def create_injury_hierarchy(parent_data_path: str) -> Dict[Bundle, List[Bundle]]:
    """Create the parent-to-child injury mapping.

    Args:
        parent_data_path: Filepath to the child to parent mapping.

    Returns:
        A dictionary of parent bundle id keys with child bundle id values.
    """
    # Prep data
    data = pd.read_csv(parent_data_path)
    data = data.sort_values("e_code")
    data = data.rename(columns={"Level1-Bundle ID": "bundle_id"})
    data.child = data.child.notnull()
    data.parent = data.parent.notnull()

    # Create a hierarchy
    # - Since e codes are sorted we know that parent bundles are always
    # - directly above their children bundles.
    hierarchy: Dict[Bundle, List[Bundle]] = {}
    parent_bundle = None
    for _, row in data.iterrows():
        bundle = Bundle(row.bundle_id, row.e_code)
        child, parent = row.child, row.parent
        if parent:
            parent_bundle = bundle
            hierarchy[parent_bundle] = []
        elif child:
            if not (
                parent_bundle
                # We have found a child bundle that isn't directly below a parent
            ):
                raise ValueError("Structure of parent child injury cause file broken")
            hierarchy[parent_bundle].append(bundle)
        else:
            parent_bundle = None

    return hierarchy


def define_lookup() -> pd.DataFrame:
    """Create the lookup table defining bundle relationship types and descriptions.

    Returns:
        A DataFrame of the lookup table.
    """
    df = pd.DataFrame(
        {
            "relationship_id": [1, 2, 3, 4],
            "relationship_description": [
                "numerator for a maternal ratio bundle",
                "denominator for a maternal ratio bundle",
                "child-to-parent mapping for injuries",
                "1:1 clone of a bundle_id in clinical_mapping",
            ],
        }
    )
    return df


def get_parent_inj(map_version: int) -> pd.DataFrame:
    """Pulls the parent to child mapping from disk, performs some bundle ID modifications
    and then returns a relationship table of the parent-child mapping.

    Args:
        map_version: Map version to assign to the parent injury relationship.

    Returns:
        A DataFrame containing the child to parent mapping.
    """
    parent_data_path = "FILEPATH"
    hierarchy = create_injury_hierarchy(parent_data_path)

    df_list = []
    for parent, children in hierarchy.items():
        df_list.append(
            pd.DataFrame(
                {
                    "origin_bundle_id": [child.id for child in children],
                    "output_bundle_id": [parent.id] * len(children),
                }
            )
        )
    df = pd.concat(df_list, sort=True, ignore_index=True)
    # remove the deprecated bundle_id 362 from inj_mech
    df = df[df["origin_bundle_id"] != 362]

    # swap in the updated bundle_ids for map version 28
    if map_version >= 26:
        swap28 = {368: 6812, 751: 6824, 765: 640, 3044: 6830}
        for old, new in swap28.items():
            df.loc[df["origin_bundle_id"] == old, "origin_bundle_id"] = new

    df["relationship_id"] = 3
    df["map_version"] = map_version
    df["relationship_update"] = 1

    confirm_bundle_in_map(bundles=df["origin_bundle_id"], map_version=map_version)
    return df


def get_mat_ratios(map_version: int) -> pd.DataFrame:
    """Manual definitions of the numerators and denominators for maternal ratio bundles.

    Args:
        map_version: Version to assign to the data and also influences how the ratios are
                     created. A different bundle_id is used in one ration for map_version 25
                     vs any version larger than 25.

    Returns:
        A DataFrame containing the relationships between maternal ratio bundles and their
        numerator and denominator bundle IDs.
    """
    if map_version == 25:
        ec = 667
    elif (
        map_version >= 26
    ):  # if this assumption is wrong the validations will catch missing bundles
        ec = 825

    numer = pd.DataFrame(
        {
            "origin_bundle_id": [76, 76, 6107, 6110, ec],
            "output_bundle_id": [6113, 6116, 6119, 6122, 6125],
            "relationship_id": [1] * 5,
        }
    )

    denom = pd.DataFrame(
        {
            "origin_bundle_id": [ec, 6107, 75, 6107, 6107],
            "output_bundle_id": [6113, 6116, 6119, 6122, 6125],
            "relationship_id": [2] * 5,
        }
    )

    df = pd.concat([numer, denom], sort=False, ignore_index=True)
    df.sort_values(["output_bundle_id", "relationship_id"], inplace=True)

    df["map_version"] = map_version
    df["relationship_update"] = 1

    confirm_bundle_in_map(bundles=df["origin_bundle_id"], map_version=map_version)
    return df


def confirm_bundle_in_map(bundles: List[int], map_version: int) -> List[str]:
    """Checks an input list of bundles against the icg_bundle table from clinical mapping.
    If any bundle is not present in the table for the input map version then this fails.

    Args:
        bundles: Input bundles to check.
        map_version: Map version to check bundles against.

    Returns:
        A list of bundles not present in the map or an empty string.
    """
    failures = []

    if map_version < 30:
        print(
            "Unable to validate bundles against DATABASE for versions"
            " less than 30."
        )
    else:
        m = clinical_mapping_db.get_clinical_process_data(
            "icg_bundle", map_version=map_version
        )
        map_bundles = m["bundle_id"].unique().tolist()

        for b in bundles:
            if b not in map_bundles:
                failures.append(f"Bundle_id {b} is not in map_version {map_version}.")

    return failures


def validate(df: pd.DataFrame, lookup: pd.DataFrame) -> None:
    """Run some checks on the relationship table (df) and its lookup.

    Args:
        df: A table of bundle relationship data.
        lookup: The lookup table for the relationship_ids.

    Raises:
        ValueError if nulls are found.
        ValueError if a relationship_id is present in the data table but not the lookup.
        ValueError if a map version is not present in the database.
        ValueError if a bundle_id is not present on bundle.bundle.
        ValueError if duplicates are found.
    """
    failures = []
    if df.isnull().sum().sum() > 0:
        failures.append("There are null values in df")

    for rid in df["relationship_id"].unique():
        if rid not in lookup["relationship_id"].tolist():
            failures.append(f"Relationship ID {rid} is not in the lookup table")

    db_map_versions = query(
        "QUERY", conn_def="CONN"
    )
    # should be map version 30 and up
    db_map_versions = db_map_versions["map_version"].unique().tolist()

    map_versions = df["map_version"].unique().tolist()
    for version in map_versions:
        if version >= 30 and version not in db_map_versions:
            failures.append(f"Map version {version} is not present in the database")
        df_v = df.query(f"map_version == {version}")
        failures += confirm_bundle_in_map(
            bundles=df_v["origin_bundle_id"], map_version=version
        )

    central_bundles = query("QUERY", conn_def="CONN")
    central_bundles = central_bundles["bundle_id"].tolist()

    df_bundles = df["origin_bundle_id"].tolist() + df["output_bundle_id"].tolist()
    for df_bundle in df_bundles:
        if df_bundle not in central_bundles:
            failures.append(f"bundle_id {df_bundle} is not in DATABASE")

    dupe_cols = ["origin_bundle_id", "output_bundle_id", "relationship_id", "map_version"]
    dupe_df = df[df.duplicated(subset=dupe_cols, keep=False)]
    if len(dupe_df) > 0:
        failures.append(f"It appears that some relationships are duplicated {dupe_df}")
    if failures:
        raise ValueError("\n".join(failures))


def save(df: pd.DataFrame, lookup: pd.DataFrame) -> None:
    """Write and archive the relationship and lookup tables."""
    parent_dir = "FILEPATH"

    df.to_csv("FILEPATH", index=False)
    lookup.to_csv("FILEPATH", index=False)

    # archive
    n = datetime.datetime.now()
    df.to_csv("FILEPATH", index=False)
    lookup.to_csv("FILEPATH", index=False)


def get_tables() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Pull in the currently active bundle relationship tables.

    Returns:
        A Tuple containing the relationship DataFrame and its corresponding lookup table.
    """

    parent_dir = "FILEPATH"

    df = pd.read_csv("FILEPATH")
    lookup = pd.read_csv("FILEPATH")

    # run validations everytime the tables are pulled
    validate(df, lookup)

    return df, lookup


def _validate_new_relationships(
    map_version: int,
    relationship_description: str,
    df: pd.DataFrame,
    lookup: pd.DataFrame,
    new_look: pd.DataFrame,
) -> None:
    """Run a few validation when new bundle to bundle relationships are defined."""

    failures = []
    if map_version not in df["map_version"].unique().tolist():
        failures.append(f"Map version {map_version} not recognized")
    if relationship_description in lookup["relationship_description"].tolist():
        failures.append(f"This relationship already exists {relationship_description}")
    rows = len(lookup) + len(new_look)
    ids = lookup["relationship_id"].tolist() + new_look["relationship_id"].tolist()
    if rows != len(set(ids)):
        failures.append("There's an issue with the relationship_ids.")

    if failures:
        raise ValueError(failures)


def set_new_relationship(
    map_version: int, relationship_description: str, save_new_id: bool
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Updates the lookup table to add a new relationship_id for the input relationship
    description.

    Args:
        map_version: A map version present in the relationship data table.
        relationship_description: The new type of relationship to create an ID for.
        save_new_id: If True, a set of df and lookup files will be saved to disk.

    Returns:
        A Tuple containing the relationship DataFrame and its corresponding lookup table.
    """

    df, lookup = get_tables()
    if not (map_version in df["map_version"].unique().tolist()):
        raise ValueError(f"map version {map_version} needs to be added to the existing data")

    # iterate up to get relationship id
    relationship_id = lookup["relationship_id"].max() + 1
    # get time and append to lookup
    n = datetime.datetime.now()
    new_look = pd.DataFrame(
        {
            "relationship_id": relationship_id,
            "relationship_description": relationship_description,
            "date_created": n,
        },
        index=[0],
    )

    _validate_new_relationships(
        map_version=map_version,
        relationship_description=relationship_description,
        df=df,
        lookup=lookup,
        new_look=new_look,
    )
    lookup = pd.concat([lookup, new_look], sort=False, ignore_index=True)
    lookup.sort_values("relationship_id", inplace=True)

    validate(df, lookup)
    if save_new_id:
        save(df, lookup)
    return df, lookup


def get_next_relationship_update(map_version: int) -> int:
    """Within a map version identify the most recent relationship update and
    increment that by 1."""

    df, _ = get_tables()
    mapdf = df.query(f"map_version == {map_version}")
    next_update = mapdf["relationship_update"].max() + 1
    return next_update


def set_update_df(
    map_version: int, relationship_id: int, relationship_dict: Dict[int, int]
) -> pd.DataFrame:
    """Create new bundle relationships in an existing map.

    Args:
        map_version: The map version to modify.
        relationship_id: The type of relationship to modify.
        relationship_dict: The bundle-to-bundle relationship values.

    Returns:
        A DataFrame containing the newly created relationships.
    """

    df = pd.DataFrame.from_dict(relationship_dict, orient="index").reset_index()
    df.rename(columns={"index": "origin_bundle_id", 0: "output_bundle_id"}, inplace=True)

    df["map_version"] = map_version
    df["relationship_id"] = relationship_id
    df["relationship_update"] = get_next_relationship_update(map_version)
    df["date_created"] = datetime.datetime.now()

    return df


def set_existing_relationship(
    map_version: int, relationship_id: int, relationship_dict: Dict[int, int]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Update the bundle relationship table with new rows for an already
    defined relationship.

    Args:
        map_version: The map version to modify.
        relationship_id: The type of relationship to modify.
        relationship_dict: The bundle-to-bundle relationship values.

    Returns:
        A Tuple containing the relationship DataFrame and its corresponding lookup table.
    """

    df, lookup = get_tables()
    if not (map_version in df["map_version"].unique().tolist()):
        raise ValueError(f"map version {map_version} needs to be added to the existing data")
    update_df = set_update_df(map_version, relationship_id, relationship_dict)

    df = pd.concat([df, update_df], sort=False, ignore_index=True)

    validate(df, lookup)
    return df, lookup


def inherit_relationships(
    origin_map_version: int, target_map_version: int, run_validation: bool = True
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Makes a copy of the old version and assigns it a new map version.

    Adds the new map version relationship table alongside the existing map version tables.

    Args:
        origin_map_version: The map version to pull relationships from.
        target_map_version: The new map version to create relationships for.

    Returns:
        A Tuple containing the relationship DataFrame and its corresponding lookup table.
    """

    df, lookup = get_tables()
    newdf = df.query(f"map_version == {origin_map_version}").copy()
    newdf["map_version"] = target_map_version

    # reset the update status
    newdf["relationship_update"] = 1
    newdf["date_created"] = datetime.datetime.now()

    df = pd.concat([df, newdf], sort=False, ignore_index=True)
    if run_validation:
        validate(df, lookup)

    return df, lookup
