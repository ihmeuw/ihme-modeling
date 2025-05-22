from dataclasses import dataclass
from typing import Tuple


@dataclass
class Tables:
    BUNDLE: str = "DATABASE"
    BUNDLE_METADATA: str = "DATABASE"
    CODE_SYSTEM: str = "DATABASE"
    CAUSE_CODE_ICG: str = "DATABASE"
    ICG: str = "DATABASE"
    ICG_PROPERTIES: str = "DATABASE"
    ICG_BUNDLE: str = "DATABASE"
    BUNDLE_PROPERTIES: str = "DATABASE"

    CAUSE_CODE_BUNDLE: str = "DATABASE"
    # The table below summarizes entities (bundles, cause codes, icgs) which have been
    # removed or added to the map
    ADDITIONS_REMOVALS: str = "DATABASE"


@dataclass
class MappingTables:
    CAUSE_CODE_ICG: str = "TABLE"
    CODE_SYSTEM: str = "TABLE"
    ICG: str = "TABLE"
    ICG_BUNDLE: str = "TABLE"
    ICG_PROPERTIES: str = "TABLE"
    BUNDLE_PROPERTIES: str = "TABLE"
    CAUSE_CODE_CONDITION: str = "TABLE"
    CONDITION_PROPERTIES: str = "TABLE"
    CONDITION: str = "TABLE"
    MAP_VERSION: str = "TABLE"
    MAP_VERSION_TYPE: str = "TABLE"


@dataclass
class VERSION_STATUS:
    CURRENT_BEST: int = 1
    USABLE: int = 2
    NOT_USABLE: int = 3
    VALID_STATUSES: Tuple[int, int, int] = (1, 2, 3)


# Ordered to ensure foreign key validations won't break insertions
ORDERED_CLINICAL_VERSION_METADATA_TABLES = [
    "TABLE",
    "TABLE",
    "TABLE",
    "TABLE",
    "TABLE",
    "TABLE",
]

# Tables which track values based mainly on refresh_id
REFRESH_INDEXED_TABLES = ["TABLE", "TABLE"]
