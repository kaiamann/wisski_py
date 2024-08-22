"""Module description."""

from __future__ import annotations
from typing import Optional
from enum import Enum
import csv
import json
import os
import requests

WISSKI_INDIVIDUAL = "wisski_individual"
WISSKI_BUNDLE = "wisski_bundle"


class KeyType(Enum):
    """Enum for CSV column header types."""

    FIELD_ID = 1  # Columns are field IDs
    PATH_ID = 2  # Columns are path IDs
    NONE = 3  # No headers


class Pathbuilder:
    """Class representing a pathbuilder."""

    class NoSuchPathException(Exception):
        """Raised when this pathbuilder doesn't contain a particular path"""

    class ImportMode(str, Enum):
        """Enum for specifying a pathbuilder import mode."""

        KEEP = "keep"
        CONNECT_NO_FIELD = "1ae353e47a8aa3fc995220848780758a"
        GENERATE_NEW_FIELD = "ea6cd7a9428f121a9a042fe66de406eb"

    def __init__(self, pathbuilder_id: str, paths: dict, name: str = None, adapter: str = None) -> None:
        self.pathbuilder_id = pathbuilder_id
        self.name = name
        self.adapter = adapter
        self.tree = {
            'id': '0',
            'children': {}
        }
        self.paths = {}
        self.add_paths(paths)

    def add_paths(self, paths: list) -> int:
        """Add a set of paths to this pathbuilder.

        Args:
            paths (list): A list of paths

        Returns:
            int: The number of paths that were added
        """
        helper = paths.copy()

        # reorder the paths by group status: two passes, first push all groups, then all other
        groups = {}
        other = {}
        for k, v in helper.items():
            if v['is_group']:
                groups[k] = v
            else:
                other[k] = v

        groups.update(dict(sorted(other.items(), reverse=True)))
        helper = groups.copy()

        added = 0
        while helper:
            new_parent_ids = []
            for path in list(helper.values()):
                if self.add_path(path):
                    added += 1
                    new_parent_ids.append(path['id'])
                helper.pop(path['id'], None)
        return added

    def get_subtree_for_field_id(self, field_id: str) -> dict:
        """Get the path for a particular bundle ID from the provided search tree.

        Args:
            bundle_id (str): The bundle id for which the path should be returned.
            search_tree (dict): The tree that should be searched.

        Returns:
            dict: The path as dict.
        """
        def search_in_tree(needle: str, haystack: dict) -> dict:
            path_id = haystack['id']

            if path_id in self.paths and needle in [self.paths[path_id]['field'], self.paths[path_id]['bundle']]:
                return haystack

            for child in haystack['children'].values():
                result = search_in_tree(needle, child)
                if result:
                    return result
            return None

        result = search_in_tree(field_id, self.tree)
        if result:
            return result
        raise Pathbuilder.NoSuchPathException(f"This pathbuilder has no path with field ID: {field_id}")

    def get_path_for_id(self, field_id: str) -> dict:
        """Return the pbpath entry that belongs to a fieldId

        Args:
            field_id (str): The fieldId

        Returns:
            dict: The pbpath entry
        """
        for path in self.paths.values():
            if path['field'] == field_id or (path['field'] == path['bundle'] and path['bundle'] == field_id):
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
                # print(f"{element['id']} not enabled, skipping")
                return False

            parent = element['parent']
            new_id = element['id']

            # Don't try to add if the parent wasn't added yet
            if parent != "0" and parent not in self.paths:
                return False

            # Path belongs to this tree node or is root:
            if parent in [tree['id'], "0"]:
                # Skip if the path already exists
                if new_id in tree['children']:
                    # print(f"path {new_id} exists, skipping")
                    return False
                tree['children'][new_id] = {
                    'id': new_id,
                    'children': {}
                }
                return True

            # Path does not belong to this tree node -> search in children
            for child in tree['children'].values():
                if add_to_tree(element, child):
                    return True
            return False

        # Only add if the path does not exist yet.
        if new_path['id'] not in self.paths:
            self.paths[new_path['id']] = new_path
            return add_to_tree(new_path, self.tree)
        return False

    def combine(self, other) -> Pathbuilder:
        """Combine this pathbuilder with another pathbuilder"""
        # Add every path in every pathbuilder to the new one.
        for path in other.paths.values():
            # Add to pid_fid_map if it was added to the pb.
            self.add_path(path)

        return self


class Entity:
    """A WissKI entity."""

    class MissingUriException(Exception):
        """Raised when this entity has no URI"""

    def __init__(
        self,
        api: Api,
        bundle_id: str,
        fields: dict,
        uri: str = None,
    ) -> None:
        self.api = api
        self.bundle_id = bundle_id
        self.fields = fields
        self.uri = uri
        self._saved_hash = None
        # Fields from an unused pathbuilder
        self.unused_fields = {}

    def _mark_unmodified(self) -> 'Entity':
        """
            Marks this entity as not modified since last retrieved from the server.
            Returns the entity for convenience.
        """
        self._saved_hash = self._hash()
        return self

    @property
    def modified(self) -> bool:
        """
            Checks if the client modified this entity since it was last retrieved from the server.
        """

        # never saved
        if self._saved_hash is None:
            return True

        # get current and saved serializations
        return self._saved_hash != self._hash()

    def _hash(self) -> str:
        """ Serializes this entity as a string that can be used to determine if the content and ids of two entities are identical.
            Callers must not rely on internal structure of the string, use serialize instead for that.
        """

        return json.dumps(self.serialize(), sort_keys=True)

    def flatten(self) -> dict:
        """Flatten this entity and all its sub-entities.

        Returns:
            dict: The values of this and all sub-entities in a flat dict.
        """
        flattened = {}
        for field_id, values in self.fields.items():
            for value in values:
                if not isinstance(value, Entity):
                    flattened[field_id] = value
                else:
                    flattened.update(value.flatten())
        return flattened

    def serialize(self) -> dict:
        """Serialize the passed entity into the format that is expected by the remote API endpoint.

        Args:
            entity (Entity): The entity to be serialized.

        Returns:
            dict: The serialized entity.
        """
        entity_data = {
            "bundle": [
                {
                    "target_id": self.bundle_id,
                    "target_type": WISSKI_BUNDLE,
                }
            ]
        }
        # Attach URI if one was specified
        if self.uri:
            entity_data["wisski_uri"] = [{"value": self.uri}]

        bundle_path = self.api.pathbuilder.get_subtree_for_field_id(self.bundle_id)

        # Abort if not a bundle.
        if "children" not in bundle_path:
            # TODO: throw descriptive error here
            return None

        for path_id in bundle_path["children"]:
            path = self.api.pathbuilder.paths[path_id]
            field_id = path["field"]
            field_data = []

            # Skip this path if we do not have a value for this field in our mapped data.
            if field_id not in self.fields.keys():
                continue

            # Build a the field data for each provided field value
            # TODO: check for path cardinality here.
            # TODO: maybe move the check for entity reference type and the following check for distinguishing between URI/Entity to FieldTypeFormatter?
            for value in self.fields[field_id]:
                if (path['is_group'] or path['fieldtype'] == "entity_reference") and isinstance(value, Entity):
                    # Skip sub-entities with no field values.
                    if len(value.fields) == 0:
                        continue
                    child_values = value.serialize()
                    # Wrap the child data in the 'entity' key for the API to recognize the sub-entity.
                    field_data.append({"entity": child_values})
                else:
                    field_data.append(FieldTypeFormatter.format_value(path['fieldtype'], value))

            entity_data[field_id] = field_data

        # Copy over the unused field values
        entity_data.update(self.unused_fields)

        return entity_data

    def save(self, force: bool = False) -> bool:
        """ Saves this entity if it has been modified since the last server save.

        Args:
            force (Bool): Force saving even if the entity doesn't appear unmodified.

        Returns:
            True if saved, False otherwise. """
        if not force and not self.modified:
            return False

        self.api.save(self)
        return True

    def load(self, data: dict, modified: bool = True) -> Entity:
        """ De-serializes the values in this entity from data.
        See deserialize for format details.

        Modified indicates if the newly modified entity is marked as being modified since the last server save.
        """

        self.fields = {}
        self.unused_fields = {}
        for field_id, values in data.items():
            # Extract bundle and URI
            if field_id == "bundle":
                self.bundle_id = data["bundle"][0]["target_id"]
                continue
            if field_id == "wisski_uri":
                self.uri = data["wisski_uri"][0]["value"]
                continue

            new_field_value = []
            path = self.api.pathbuilder.get_path_for_id(field_id)
            # In case the path is not in one of the used pathbuilders, save the values and continue
            if not path:
                self.unused_fields[field_id] = values
                continue

            for field_value in values:
                # Catch sub-entities and deserialize them.
                if "entity" in field_value:
                    new_field_value.append(
                        __class__.deserialize(self.api, field_value["entity"], modified=modified)
                    )
                    continue
                new_field_value.append(FieldTypeFormatter.get_value(path['fieldtype'], field_value))

            # Initialize with empty list if no value is present.
            if field_id not in self.fields:
                self.fields[field_id] = []
            if len(new_field_value) != 0:
                self.fields[field_id] = new_field_value
        if not modified:
            self._mark_unmodified()
        return self


    @staticmethod
    def deserialize(api: Api, data: dict, modified: bool = True) -> Entity:
        """Builds a new entity from the tree representation.

        Args:
            tree (dict): The entity in tree representation.
            modified (bool): Should this entity be marked as being modified since being fetched from the server.
        Returns:
            Entity: The new entity.
        """

        new = Entity(api=api, bundle_id="", fields={})
        new.load(data, modified=modified)
        return new

    def to_csv(self, folder: str) -> None:
        """Convert this entity into csv.

        Also generates a csv for potential sub-entities.
        Since the main entity needs to reference the sub-entities, all

        Args:
            folder (str): The directory where the csv files should be stored.

        Raises:
            MissingUriException: _description_
            AttributeError: _description_
        """
        # this only works with entities that have uris
        if not self.uri:
            raise Entity.MissingUriException(f"{self} does not have a URI")

        filename = f"{folder}/{self.bundle_id}.csv"

        file_exists = os.path.isfile(filename)
        if file_exists:
            # Get headers
            # TODO: add potentially missing headers
            with open(filename, mode="r", encoding="utf-8") as file:
                reader = csv.reader(file)
                headers = next(reader)
        else:
            # Just take the order of values
            headers = ["uri"]
            headers.extend(self.fields.keys())


        with open(filename, mode="a", encoding="utf-8") as file:
            writer = csv.writer(file)
            row = []
            for field_id in headers:
                if field_id == "uri":
                    row.append(self.uri)
                    continue

                field_values = self.fields[field_id]
                pb_path = self.api.pathbuilder.get_path_for_id(field_id)
                field_type = pb_path['fieldtype']
                # we have a sub-bundle
                # print(self.api.pathbuilder.pb_paths[path_id])
                if pb_path["is_group"]:
                    uris = []
                    # Save every sub-entity to CSV
                    for sub_entity in field_values:
                        if not isinstance(sub_entity, Entity):
                            raise AttributeError(
                                f"Field value of a sub-bundle field {field_id} is not an Entity!"
                            )
                        # append URI as the field value
                        uris.append(sub_entity.uri)
                        # export the sub-entity to csv
                        sub_entity.to_csv(folder)
                    row.append("|".join(str(x) for x in uris))
                    continue
                row.append("|".join(field_values))
            if not file_exists:
                writer.writerow(headers)
            writer.writerow(row)

class Api:
    """Class for interacting with a remote WissKI system."""

    def __init__(self, base_url: str, auth: list, headers: dict, timeout: int = 60):
        self.base_url = base_url
        self.auth = auth
        self.headers = headers
        self.timeout = timeout

    def __setattr__(self, __name: str, __value: any) -> None:
        super().__setattr__(__name, __value)
        # Rebuild the pb every time the active pathbuilders are set.
        if __name == "pathbuilders":
            self.pathbuilder = self.__rebuild_pathbuilder()

    def __rebuild_pathbuilder(self) -> Pathbuilder:
        """Rebuild the current pathbuilder by pulling the relevant pathbuilders from the remote and combining them."""
        # Get available pathbuilders IDs from remote.
        pathbuilders = {}
        for pathbuilder_id in self.pathbuilders:
            # Get the pathbuilder object from the remote.
            pathbuilders[pathbuilder_id] = self.get_pathbuilder(pathbuilder_id)
        # Build the combined pathbuilder.
        return self.combine_pathbuilders(pathbuilders)

    def init_pathbuilders(self) -> None:
        self.pathbuilders = self.get_pathbuilder_ids()
        self.pathbuilder = self.__rebuild_pathbuilder()

    def save(
        self, obj: Entity | Pathbuilder | list
    ) -> str | Entity | Pathbuilder | list:
        """Save an entity to the remote.

        Args:
            obj (Entity | Pathbuilder): The entity to be saved

        Returns:
            str: The response.
        """
        match obj:
            case Entity() as entity:
                return self.save_entities([entity])[0]
            case Pathbuilder() as pathbuilder:
                # TODO: implement? or see if the flat path format is better suited for im/export...
                pass
            case list() as objects:
                if all(isinstance(x, Entity) for x in objects):
                    return self.save_entities(objects)
                if all(isinstance(x, Pathbuilder) for x in objects):
                    # TODO: implement?
                    pass

    # ----------------------------
    # --- Pathbuilder Handling ---
    # ----------------------------

    def Entity(self, bundle_id: str, values: dict, uri: str = None):
        return Entity(api=self, bundle_id=bundle_id, fields=values, uri=uri)

    def get_pathbuilder(self, pathbuilder_id: str) -> Optional[Pathbuilder]:
        """Get a particular pathbuilder in normalized form.

        Args:
            pathbuilder_id (str): The ID of the desired pathbuilder.

        Returns:
            dict: The normalized pathbuilder.
        """
        # Do the API request.
        url = f"{self.base_url}/pathbuilder/{pathbuilder_id}/get"
        response = self.get(url)
        if response.status_code != 200:
            print(response.text)
            return None
        args = json.loads(response.text)
        pathbuilder = Pathbuilder(pathbuilder_id=args['id'], paths=args["paths"], name=args['name'], adapter=args['adapter'])
        return pathbuilder

    def delete_pb(self, pathbuilder_id: str) -> str:
        """Delete a pathbuilder from the remote.

        Args:
            pathbuilder_id (str): The ID of the pathbuilder to be deleted.

        Returns:
            str: The response.
        """
        url = f"{self.base_url}/pathbuilder/{pathbuilder_id}/delete"
        response = self.delete(url=url)
        # If the pb was successfully deleted from the remote update the local pb.
        if response.status_code == 200 and pathbuilder_id in self.pathbuilders:
            # Remove the pathbuilder from the
            self.pathbuilders.remove(pathbuilder_id)
            # Recompute the combined pathbuilder
            self.pathbuilder = self.__rebuild_pathbuilder()
        return response.text

    def get_pathbuilder_ids(self) -> dict:
        """Get all available pathbuilders from the WissKI API.

        Returns:
            dict: The pathbuilders keyed by their ID.
        """
        # Get the IDs of the available pathbuilders via API.
        url = f"{self.base_url}/pathbuilder/list"
        response = self.get(url)
        if response.status_code != 200:
            print(response.text)
            return {}
        return json.loads(response.text)

    def combine_pathbuilders(self, pathbuilders) -> Pathbuilder:
        """Combine all available pathbuilders into a single one.

        Returns:
            Pathbuilder: the combined pathbuilder.
        """
        combined = Pathbuilder("combined", {})

        for pathbuilder in pathbuilders.values():
            combined.combine(pathbuilder)

        return combined

    def import_pathbuilder(
        self,
        pathbuilder_id: str,
        name: str,
        path_to_xml: str,
        adapter: str = "default",
        mode: str = Pathbuilder.ImportMode.KEEP,
    ) -> str:
        """Import a pathbuilder in XML format into the remote.

        Args:
            id (str): The machine name that the pathbuilder should get.
            name (str): The display name that the pathbuilder should get.
            path_to_xml (str): The path to the XML file.
            adapter (str, optional): The adapter the pathbuilder belongs to. Defaults to "default".
            mode (str, optional): The import mode. Defaults to Pathbuilder.ImportMode.KEEP.

        Returns:
            str: The response.
        """
        url = f"{self.base_url}/pathbuilder/import"
        with open(path_to_xml, encoding="utf-8") as file:
            xml = file.read()
            data = {
                "id": pathbuilder_id,
                "name": name,
                "adapter": adapter,
                "xml": xml,
                "mode": mode,
            }
            response = self.post(url=url, json_data=data)

            if response.status_code == 200:
                # If the pb was successfully added to the remote, update the local pb.
                # TODO: this may not be intended...
                # maybe someone uploads a pb but does not want to use it here?
                # Add the pathbuilder to the locally used pbs.
                self.pathbuilders.append(pathbuilder_id)
                # Recompute the combined pathbuilder
                self.pathbuilder = self.__rebuild_pathbuilder()
            return response.text

    def export_pathbuilder(self, pathbuilder_id: str) -> dict:
        """Export a pathbuilder from the remote to XML.

        Args:
            pathbuilder_id (str): The id of the pathbuilder to export.

        Returns:
            dict: A dict containing the pathbuilder information:
            - id: The machine name of the pathbuilder
            - name: The display name of the pathbuilder.
            - adapter: The adapter the pathbuilder belongs to.
            - xml: The path to the XML file.
        """
        url = f"{self.base_url}/pathbuilder/{pathbuilder_id}/export"
        response = self.get(url=url)
        if response.status_code != 200:
            return response.text
        return json.loads(response.text)

    def generate_bundles_and_fields(self) -> None:
        """Generates bundles and fields for the selected pathbuilders"""
        # Generate bundles and fields for every pathbuilder.
        for pathbuilder_id in self.pathbuilders:
            url = f"{self.base_url}/pathbuilder/{pathbuilder_id}/generate"
            self.get(url)

    # -----------------------
    # --- Entity Handling ---
    # -----------------------


    def build_entity(self, bundle_id: str, values: dict) -> Entity:
        """Build an entity from a flat list of values.

        This builds the entity including nested sub-entities.
        Does not generate URIs for the new entities.

        Args:
            bundle_id (str): The bundle ID of the entity.
            values (dict): The values as a field_id -> field_value map
            uri (str, optional): The URI of the entity. Defaults to None.

        Returns:
            Entity: The entity.
        """
        bundle = self.pathbuilder.get_subtree_for_field_id(bundle_id)

        sub_bundles = {}
        entity_values = {}

        for path_id in bundle["children"]:
            path = self.pathbuilder.paths[path_id]
            if path["is_group"]:
                # Initialize values when there aren't any yet.
                if path["bundle"] not in entity_values:
                    entity_values[path["bundle"]] = []

                entity_values[path["bundle"]].append(
                    self.build_entity(path["bundle"], values)
                )
                sub_bundles[path_id] = path
                continue

            # If we do not have a value for this path set it to empty.
            if path["field"] not in values.keys():
                entity_values[path["field"]] = []
                continue

            entity_values[path["field"]] = values[path["field"]]

        return Entity(self, bundle_id, entity_values)


    def get_entity(self, uri: str, meta: int = 0, expand: int = 1) -> Entity:
        """Get an entity from the WissKI API.

        Args:
            uri (str): The URI of the entity that should be returned.

        Returns:
            Entity: The WissKI entity.
        """
        url = f"{self.base_url}/entity/get?uri={uri}&meta={meta}&expand={expand}"
        response = self.get(url)
        if response.status_code != 200:
            print(response.text)
            return None

        return Entity.deserialize(self, json.loads(response.text), modified=False)

    def save_entities(
        self, entities: list[Entity], create_if_new: bool = True
    ) -> list[Entity]:
        """Update an existing WissKI entity on the remote.

        Args:
            entity (WisskiEntity): The entity containing the new values for the remote.
            create_if_new (bool, optional): Create a new entity if it does not exist
            on the remote yet. Defaults to True.

        Returns:
            list[Entity]: The list of entities
        """
        # Skip when no new entities should be created and no URI was specified.
        # TODO: This check should ultimately be done on PHP side, since there we
        # know which URIs exist and which don't.
        # TODO: also introduce three save modes:
        # 1: create and update
        # 2: only update
        # 3: only create
        if not create_if_new and all(entity.uri is None for entity in entities):
            return None

        url = f"{self.base_url}/entity/create?overwrite={1 if create_if_new else 0}"
        data = []
        for entity in entities:
            data.append(entity.serialize())

        # TODO: find out when this post request fails.
        # Either due to timeout or request size.
        response = self.post(url=url, json_data=data, timeout=1200)

        # Something went wrong...
        if response.status_code != 200:
            return print(response.text)

        # Replace the entities with the ones from the API
        # This is necessary to also set the URIs for sub-entities
        # since we only have the handle to the main entity.
        for i, entity_data in enumerate(json.loads(response.text)):
            entities[i].load(entity_data, modified=False)

        return entities

    # -----------------------
    # --- Bundle Handling ---
    # -----------------------

    def get_bunde_ids(self) -> map[str, str]:
        """Get a list of all available bundle_ids and their labels.
        """
        url = f"{self.base_url}/bundle/list"
        response = self.get(url)
        if response.status_code != 200:
            print(response.text)
            return None

        return response.json()


    def get_uris_for_bundle(self, bundle_id: str) ->list[str]:
        """Get a list of all URIs for a specific bundle.

        Args:
            bundle_id (str): The ID of the bundle to get.

        Returns:
            list[str]: A list of URIs.
        """
        url = f"{self.base_url}/entity/{bundle_id}/list"
        response = self.get(url)
        if response.status_code != 200:
            print(response.text)
            return None

        return response.json()

    # ----------------------
    # --- File Utilities ---
    # ----------------------

    def load_csv(
        self,
        directory: str,
        bundle_id: str,
        separator: str = "|",
        key_type: KeyType = KeyType.FIELD_ID,
    ) -> list[Entity]:
        """Import a CSV file.

        Args:
            bundle_id (str): The bundle_id of the entities that should be loaded.
            directory (str): The path to the csv files directory.
            separator (str, optional): The separator that separates multiple values for a column. Defaults to '|'.
            key_type (KeyType, optional): The type of header that is used in the CSV table. Defaults to KeyType.FIELD_ID.
        """
        # Get all the data from the directory and key it by bundle_id
        csv_files = os.listdir(directory)
        csv_data = {}
        for csv_file in csv_files:
            file_path = f"{directory}/{csv_file}"
            bundle = os.path.splitext(os.path.basename(file_path))[0]
            # TODO: check if the file is actually a csv file.
            data = self.parse_csv(file_path, separator, key_type)
            csv_data[bundle] = data

        # This function recursively builds entities from a csv row.
        # Referenced sub-entities are fetched from their respective tables.
        def build_entity_from_row(bun, uri, row):
            entity_values = {}
            for field_id, values in row.items():
                # we have a bundle and values to go with it
                pb_path = self.pathbuilder.get_path_for_id(field_id)
                if pb_path["is_group"] and field_id in csv_data:
                    sub_entities = []
                    # build the sub-entity
                    for sub_uri in values:
                        sub_entities.append(
                            build_entity_from_row(
                                field_id, sub_uri, csv_data[field_id][sub_uri]
                            )
                        )
                    entity_values[field_id] = sub_entities
                else:
                    formatted_values = []
                    for value in values:
                        formatted_values.append(value)
                    entity_values[field_id] = formatted_values
            return Entity(self, bun, entity_values, uri)

        entities = []
        for uri, row in csv_data[bundle_id].items():
            entities.append(build_entity_from_row(bundle_id, uri, row))
        return entities

    def parse_csv(
        self, csv_path: str, separator: str = "|", key_type: KeyType = KeyType.FIELD_ID
    ):
        """Parse a csv file and potentially remap the pathID column headers to field IDs.

        Args:
            csv_path (str): The path to the CSV.
            separator (str, optional): A set of characters separating values in a single column. Defaults to "|".
            key_type (KeyType, optional): Specifies the type of headers used in the CSV. Defaults to KeyType.FIELD_ID.

        Returns:
            dict: The parsed CSV.
        """
        data = {}
        headers = []
        with open(csv_path, "r", encoding="utf-8") as file:
            reader = csv.reader(file)
            headers = []
            if key_type is KeyType.FIELD_ID:
                headers = next(reader)
            elif key_type is KeyType.PATH_ID:
                headers = next(reader)
                # Remap from path id to field id.
                new_headers = []
                for header in headers:
                    if header not in self.pathbuilder.paths:
                        continue
                    new_headers.append(self.pathbuilder.paths[header]["field"])
                headers = new_headers

            for line, row in enumerate(reader):
                row_data = {}
                uri = None
                for i, header in enumerate(headers):
                    if header == "uri":
                        uri = row[i]
                        continue
                    # Get values separated by the delimiter.
                    values = row[i].split(separator)
                    # remove empty strings from the values.
                    values = [x for x in values if x]
                    # Do not set the field if no values are present
                    if not values:
                        continue
                    row_data[header] = values
                # pathbuilder.build_entity_data(bundle_id, row)
                if uri:
                    data[uri] = row_data
                else:
                    raise Entity.MissingUriException(f"No URI in line {line}")
        return data

    # --------------------
    # --- HTTP Helpers ---
    # --------------------

    def get(self, url: str):
        """Send a HTTP GET request to a URL.

        Args:
            url (str): The URL to send the request to.

        Returns:
            Response: The response
        """
        return requests.get(
            url, auth=self.auth, headers=self.headers, timeout=self.timeout
        )

    def post(
        self, url: str, json_data: dict = None, data: str = None, timeout: int = None
    ):
        """Send a HTTP POST request to a URL.

        Args:
            url (str): The URL to send the request to.
            json_data(dict): The data to be sent in JSON format.
            data (dict): The data to be sent in plaintext format.
            timeout(int): How many seconds until the request times out.

        Returns:
            Response: The response
        """
        return requests.post(
            url,
            data=data,
            json=json_data,
            auth=self.auth,
            headers=self.headers,
            timeout=timeout if timeout else self.timeout,
        )

    def delete(self, url: str):
        """Send a HTTP DELETE request to a URL.

        Args:
            url (str): The URL to send the request to.

        Returns:
            Response: The response
        """
        return requests.delete(
            url, auth=self.auth, headers=self.headers, timeout=self.timeout
        )


class FieldTypeFormatter:

    @staticmethod
    def format_value(field_type: str, value: str) -> dict:
        """Format a string value to the expected Drupal format.

        List of available field types:
        comment
        datetime
        file_uri
        file
        geofield
        image
        link
        list_integer
        list_float
        list_string
        path
        text_with_summary
        text
        text_long
        integer
        email
        changed
        string_long
        uri
        password
        string
        boolean
        decimal
        language
        uuid
        float
        entity_reference
        created
        map
        timestamp

        Args:
            field_type (str): The field type.
            value (str): The value as a string.

        Returns:
            dict: The value wrapped in the Drupal field format.
        """
        # Assume sane default
        formatted_value = {"value": value}

        if not field_type or field_type == "entity_reference":
            formatted_value = {
                "target_uri": value,
                "target_type": WISSKI_INDIVIDUAL,
            }
        elif field_type == "text_long":
            formatted_value = {
                "value": value,
                "format": "basic_html",
            }
        elif field_type == "image":
            # TODO: see what of these is needed/correct
            formatted_value = {
                "target_id": value,
                "alt": None,
                "title": None,
                "target_type": "file",
                "url": "some URL",
            }
        elif field_type == "link":
            formatted_value = {
                "uri": value,
                "title": value,
                "options": []
            }
        return formatted_value

    @staticmethod
    def get_value(field_type: str, value: dict) -> str:
        """Extract the value from a Drupal field item depending on field type.

        Args:
            field_type (str): The field type.
            value (dict): The field item dict.

        Returns:
            str: The value as string.
        """
        if not field_type or field_type == "entity_reference":
            return str(value['target_uri'])
        if field_type == "image":
            return str(value['target_id'])
        if field_type == "link":
            return f"<a href={value['uri']}>{value['title'] if 'title' in value else value['uri']}</a>"
        if field_type in ["string"]:
            return value['value']
        if field_type in ["text_long"]:
            return value['value']
        if field_type == "file":
            return value['url']

        try:
            return value['value']
        except KeyError:
            # Default to str representation of the value
            return repr(value)
