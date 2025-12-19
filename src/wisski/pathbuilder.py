"""Module containing pathbuilder functionality"""

# spellchecker:words pathbuilder

from __future__ import annotations
from typing import Optional
from enum import Enum
from .drupal import Field, Bundle
from .util import pre, post, log
from dataclasses import dataclass, field

import logging

logger = logging.getLogger(__name__)


class Group:
    bundle: Bundle


@dataclass
class Path:
    """Class representing a path.

    Attributes:
        id:
            Machine name of the path.
        name:
            Human readable name of the path.
        field:
            Field configuration of the path.
        path_array:
            List representing the ontology path.
        enabled:
            Flag indicating if the path should be used.
        type:
            Type of the path. Either path or group.
        parent:
            The parent path of this path.
        children:
            A map of child paths, keyed by id.
        datatype_property:
            The datatype property of the path.
        disambiguation:
            Indicates which concept should be used for disambiguation.
            Index in the path_array is: disambiguation*2+1
        short_name:
            Short name of the path.
        description:
            A description of the path.
    """

    class Type(str, Enum):
        """Enum for specifying a path type."""

        PATH = "path"
        GROUP = "group"

    id: str
    name: str
    field: Field
    path_array: list
    enabled: bool = True
    type: Type = Type.PATH
    parent: Path = None
    children: dict[str, Path] = field(default_factory=dict)
    datatype_property: Optional[str] = None
    disambiguation: Optional[int] = 0
    short_name: Optional[str] = None
    description: Optional[str] = None


@dataclass
class Pathbuilder:
    """Class representing a pathbuilder."""

    id: str
    name: str
    adapter: str
    paths: dict[str, Path] = field(default_factory=dict)

    class NoSuchPathException(Exception):
        """Raised when this pathbuilder doesn't contain a particular path"""

    class ImportMode(str, Enum):
        """Enum for specifying a pathbuilder import mode."""

        KEEP = "keep"
        CONNECT_NO_FIELD = "1ae353e47a8aa3fc995220848780758a"
        GENERATE_NEW_FIELD = "ea6cd7a9428f121a9a042fe66de406eb"

    def validate(self):
        """Validate this pathbuilder"""

        for id, path in self.paths.items():
            print("Validate", path)

    @post(validate)
    @log(logger, logging.INFO, "Adding path {path.id}")
    def add_path(self, path: Path, children: bool = True, parent: bool = True) -> int:
        """Add a set of paths to this pathbuilder.

        Args:
            path (Path): The path to be added.
            children (bool): If children should be added.
            parent (bool): If the parent should be added.

        Returns:
            int: The number of paths that were added.
        """
        num = 0
        if path.id not in self.paths:
            self.paths[path.id] = path
            num += 1

        if path.parent and path.parent:
            num += self.add_path(path.parent, children, parent)

        if children:
            for child in path.children.values():
                num += self.add_path(child, children, parent)

        return num


class Old:
    def get_target_bundles_for_path_id(self, path_id: str) -> list:
        """Gets valid entity reference target bundles based on ontology classes.

        Note that this does not take into account subclassing!
        Args:
            path_id (str): The id of the entity reference path

        Returns:
            list: A list og bundle ids
        """
        path = self.paths.get(path_id)
        if not path:
            raise self.NoSuchPathException(path_id)

        if path.get("fieldtype") != "entity_reference":
            raise Exception(f"Path {path_id} is not an entity_reference!")

        path_array = path.get("path_array")
        assert (len(path_array) % 2) == 1
        assert path.get("datatype_property") == "empty"

        target_class = path_array[-1]

        return [
            path["bundle"]
            for path in self.paths.values()
            if path["id"] != path_id and path["path_array"][-1] == target_class
        ]

    def get_subtree_for_field_id(self, field_id: str) -> dict:
        """Get the path for a particular bundle ID from the provided search tree.

        Args:
            bundle_id (str): The bundle id for which the path should be returned.
            search_tree (dict): The tree that should be searched.

        Returns:
            dict: The path as dict.
        """

        def search_in_tree(needle: str, haystack: dict) -> dict:
            path_id = haystack["id"]

            if path_id in self.paths and needle in [
                self.paths[path_id]["field"],
                self.paths[path_id]["bundle"],
            ]:
                return haystack

            for child in haystack["children"].values():
                result = search_in_tree(needle, child)
                if result:
                    return result
            return None

        result = search_in_tree(field_id, self.tree)
        if result:
            return result
        raise Pathbuilder.NoSuchPathException(
            f"This pathbuilder has no path with field ID: {field_id}"
        )

    def get_path_for_id(self, field_id: str) -> dict:
        """Return the pbpath entry that belongs to a fieldId

        Args:
            field_id (str): The fieldId

        Returns:
            dict: The pbpath entry
        """
        for path in self.paths.values():
            if path["field"] == field_id or (
                path["field"] == path["bundle"] and path["bundle"] == field_id
            ):
                return path
        return None

    def add_path(self, new_path: dict) -> bool:
        """Add a path to this pathbuilder

        Args:
            new_path (dict): The path to be added

        Returns:
            bool: True if the path was added, False otherwise
        """

        def add_to_tree(element: dict, tree: dict) -> bool:
            if not element["enabled"]:
                return False

            parent = element["parent"]
            new_id = element["id"]

            # Don't try to add if the parent wasn't added yet
            if parent != "0" and parent not in self.paths:
                return False

            # Path belongs to this tree node or is root:
            if parent in [tree["id"], "0"]:
                # Skip if the path already exists
                if new_id in tree["children"]:
                    # print(f"path {new_id} exists, skipping")
                    return False
                tree["children"][new_id] = {"id": new_id, "children": {}}
                return True

            # Path does not belong to this tree node -> search in children
            for child in tree["children"].values():
                if add_to_tree(element, child):
                    return True
            return False

        # Only add if the path does not exist yet.
        if new_path["id"] not in self.paths:
            self.paths[new_path["id"]] = new_path
            return add_to_tree(new_path, self.tree)
        return False

    def combine(self, other) -> Pathbuilder:
        """Combine this pathbuilder with another pathbuilder"""
        # Add every path in every pathbuilder to the new one.
        for path in other.paths.values():
            # Add to pid_fid_map if it was added to the pb.
            self.add_path(path)

        return self
