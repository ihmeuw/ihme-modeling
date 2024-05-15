from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, List, Tuple, Union

import yaml

ALL_CATEGORIES = "all"
BOTH_MODELS = "both"


class ModelRestrictions:
    """A class for capturing restrictions in which models get run for each entity/location."""

    def __init__(self, restrictions: Iterable[Tuple[str, Union[int, str], str]]) -> None:
        """Initializer.

        Args:
            restrictions: A list of tuples each of which specifies a particular restriction.
                The tuples contain an entity, a location and the model type to use (in that
                order). The entity and location can also be "all", to indicate that it
                applies to all entities. Entity-specific restrictions take precedence over
                location-specific restrictions.
        """
        self._original_specification = list(restrictions)
        self._map = defaultdict(dict)
        for restriction in self._original_specification:
            entity = restriction[0]
            location = (
                ALL_CATEGORIES if restriction[1] == ALL_CATEGORIES else int(restriction[1])
            )
            model_type = restriction[2]

            if location in self._map[entity]:
                raise ValueError(
                    f"Restriction list includes multiple restrictions for {entity}/{location}"
                )

            self._map[entity][location] = model_type

    def model_type(self, entity: str, location_id: int) -> str:
        """Get the model type to use for an entity and location, according to the restrictions.

        Args:
            entity: entity to look up the restriction for.
            location: location to look up the restriction for.

        Returns:
            Either a model type name to use for the entity/location_id, or "both", to indicate
                that both model types should be used.

        """
        if entity in self._map:
            if location_id in self._map[entity]:
                return self._map[entity][location_id]
            elif ALL_CATEGORIES in self._map[entity]:
                return self._map[entity][ALL_CATEGORIES]
            else:
                return BOTH_MODELS
        elif ALL_CATEGORIES in self._map:
            if location_id in self._map[ALL_CATEGORIES]:
                return self._map[ALL_CATEGORIES][location_id]
            elif ALL_CATEGORIES in self._map[ALL_CATEGORIES]:
                return self._map[ALL_CATEGORIES][ALL_CATEGORIES]
            else:
                return BOTH_MODELS
        else:
            return BOTH_MODELS

    def string_specifications(self) -> List[str]:
        """Returns a list of strings for the specifications used to initialize the object.

        Useful for serializing the object on the command-line, essentially a representation of
        the way the object was initialized.
        """
        return [
            " ".join([str(field) for field in spec]) for spec in self._original_specification
        ]

    def __eq__(self, other: ModelRestrictions) -> bool:
        """Do they have the same underlying dict?"""
        return self._map == other._map

    @staticmethod
    def yaml_representer(dumper: Any, data: ModelRestrictions) -> str:
        """Function for passing to pyyaml telling it how to represent ModelRestrictions.

        This specific tag used tells pyyaml not tuse a tag.

        Args:
            dumper: pyyaml dumper
            data: ModelRestrictions object ot serialize
        """
        return dumper.represent_sequence("tag:yaml.org,2002:seq", data._original_specification)


# register the yaml_representer with pyyaml's safedumper (which is what we use to write yaml)
yaml.SafeDumper.add_representer(
    ModelRestrictions, ModelRestrictions.yaml_representer
)
