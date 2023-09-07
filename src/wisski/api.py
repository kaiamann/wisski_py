"""Module description."""

from __future__ import annotations
from enum import Enum
import csv
import copy
import json
import os
from typing import Any
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

    class ImportMode(str, Enum):
        """Enum for specifying a pathbuilder import mode."""

        KEEP = "keep"
        CONNECT_NO_FIELD = "1ae353e47a8aa3fc995220848780758a"
        GENERATE_NEW_FIELD = "ea6cd7a9428f121a9a042fe66de406eb"

    def __init__(self, pathbuilder_id: str, paths: dict) -> None:
        self.pathbuilder_id = pathbuilder_id
        self.paths = paths
        self.setup_metadata()

    def setup_metadata(self, search_tree: dict = None) -> None:
        """Set up metadata like the path_id -> field id mapping and the list of contained bundles.

        Args:
            search_tree (dict, optional): The pathbuilder in tree form. Defaults to None.
        """
        if search_tree is None:
            search_tree = self.paths
            self.pb_paths = {}
        if len(search_tree) == 0:
            return

        # Do BFS search here to get the correct
        # order for importing pb_paths later.
        children = []
        for path_id, path in search_tree.items():
            self.pb_paths[path_id] = path
            children.append(path["children"])

        for child in children:
            self.setup_metadata(child)

    def get_group_for_bundle_id(self, bundle_id: str, search_tree: dict = None) -> dict:
        """Get the path for a particular bundle ID from the provided search tree.

        Args:
            bundle_id (str): The bundle id for which the path should be returned.
            search_tree (dict): The tree that should be searched.

        Returns:
            dict: The path as dict.
        """
        if search_tree is None:
            search_tree = self.paths

        if len(search_tree) == 0:
            return None

        for path in search_tree.values():
            if path["bundle"] == bundle_id:
                return path
            if "children" in path:
                child_res = self.get_group_for_bundle_id(bundle_id, path["children"])
                if child_res:
                    return child_res
        return None

    def add_path(self, new_path: dict, tree: dict = None) -> dict:
        """Add a path to this pathbuilder.

        Args:
            new_path (dict): The path to be added.
            tree (dict, optional): The search tree. Defaults to None.

        Returns:
            dict: _description_
        """
        if new_path["enabled"] != "1":
            return False

        if tree is None:
            tree = self.paths

        self.pb_paths[new_path["id"]] = new_path

        # Path is a top level group:
        if new_path["is_group"] and new_path["parent"] == "0":
            self.paths[new_path["id"]] = new_path
            return True

        for path in tree.values():
            # Cannot add a path to a field => skip.
            if not path["is_group"]:
                continue

            # Bundle has no children yet => make a dict out of the empty list.
            if isinstance(path["children"], list):
                path["children"] = {}

            # New path belongs to this bundle.
            if self.pb_paths[new_path["parent"]]["bundle"] == path["bundle"]:
                # Do only add if the path is not there yet.
                if new_path["id"] not in path["children"]:
                    path["children"][new_path["id"]] = new_path
                return True

            # New path does not belong to the bundle.
            # => check if it belongs to the children.
            if self.add_path(new_path, path["children"]):
                return True

        # New path does not belong to any bundle:
        return False

    def combine(self, other) -> Pathbuilder:
        """Combine this pathbufile_exists = os.path.isfile(filename)ombined pathbuilder.
        """
        # Add every path in every pathbuilder to the new one.
        for path in other.pb_paths.values():
            # Remove children from path.
            # TODO: This is not really necessary here and should rather
            # happen when initializing the metadata in the pb
            new_path = copy.copy(path)
            new_path["children"] = {}
            # Add to pid_fid_map if it was added to the pb.
            self.add_path(new_path)

        return self


class Entity:
    """A WissKI entity."""

    def __init__(
        self,
        bundle_id: str,
        values: dict,
        uri: str = None,
    ) -> None:
        self.bundle_id = bundle_id
        self.values = values
        self.uri = uri

    def flatten(self) -> dict:
        """Flatten this entity and all its sub-entities.

        Returns:
            dict: The values of this and all sub-entities in a flat dict.
        """
        flattened = {}
        for field_id, values in self.values.items():
            for value in values:
                if not isinstance(value, Entity):
                    flattened[field_id] = value
                else:
                    flattened.update(value.flatten())
        return flattened

    def to_csv(self, folder: str, parent = None):
        # TODO THIS ONLY WORKS WITH ENTITIES THAT HAVE URIs
        filename = f"{folder}/{self.bundle_id}.csv"

        file_exists = os.path.isfile(filename)
        uri = 0
        if file_exists:
            # Get headers fs file exists.
            with open(filename, mode="r", encoding="utf-8") as file:
                reader = csv.reader(file)
                headers = next(reader)

        else:
            # Just take the order of values
            headers = ['uri']
            if parent:
                headers.append('parent')
            headers.extend(self.values.keys())


        with open(filename, mode="a", encoding="utf-8") as file:
            writer = csv.writer(file)
            row = []
            for field_id in headers:
                if field_id == "uri":
                    row.append(self.uri)
                    continue
                if field_id == "parent":
                    row.append(parent)
                    continue

                field_values = self.values[field_id]
                # we have a sub-bundle
                if field_id.startswith('b'):
                    # Save every sub-entity to CSV
                    for field_value in field_values:
                        if not isinstance(field_value, Entity):
                            raise Exception(f"Field value of a sub-bundle field {field_id} is not an Entity!")
                        field_value.to_csv(folder, self.uri)
                    continue
                row.append("|".join(str(x) for x in field_values))
            if not file_exists:
                writer.writerow(headers)
            writer.writerow(row)

    @staticmethod
    def build_from_tree(tree: dict) -> Entity:
        """Builds a new entity from the tree representation.

        Args:
            tree (dict): The entity in tree representation.

        Returns:
            WisskiEntity: The new entity.
        """
        entity_values = {}
        for field_id, values in tree.items():
            # Extract bundle and URI
            if field_id == "bundle":
                bundle_id = tree["bundle"][0]["target_id"]
                continue
            if field_id == "wisski_uri":
                uri = tree["wisski_uri"][0]["value"]
                continue

            # Initialize with empty list if no value is present.
            if field_id not in entity_values:
                entity_values[field_id] = []
            new_field_value = []
            for field_value in values:
                if "value" in field_value:
                    new_field_value.append(field_value["value"])
                    continue
                if "target_uri" in field_value:
                    new_field_value.append(field_value["target_uri"])
                    continue
                if "entity" in field_value:
                    new_field_value.append(
                        Entity.build_from_tree(field_value["entity"])
                    )
                    continue
            if len(new_field_value) != 0:
                entity_values[field_id] = new_field_value

        return Entity(bundle_id=bundle_id, values=entity_values, uri=uri)


class Api:
    """Class for interacting with a remote WissKI system."""

    def __init__(self, base_url: str, auth: list, headers: dict, timeout: int = 60):
        self.base_url = base_url
        self.auth = auth
        # TODO: maybe allow setting headers separate for each request.
        self.headers = headers
        self.timeout = timeout
        # Per default we use ALL pathbuilders that are on the remote.
        self.pathbuilders = self.get_pathbuilder_ids()
        self.pathbuilder = self.__rebuild_pathbuilder()

    def __setattr__(self, __name: str, __value: Any) -> None:
        super().__setattr__(__name, __value)
        # Rebuild the pb every time the active pathbuilders are set.
        if __name == "pathbuilders":
            self.pathbuilder = self.__rebuild_pathbuilder()

    def __rebuild_pathbuilder(self):
        """Rebuild the current pathbuilder by pulling the relevant pathbuilders from the remote and combining them."""
        # Get available pathbuilders IDs from remote.
        pathbuilders = {}
        for pathbuilder_id in self.pathbuilders:
            # Get the pathbuilder object from the remote.
            pathbuilders[pathbuilder_id] = self.get_pathbuilder(pathbuilder_id)
        # Build the combined pathbuilder.
        return self.combine_pathbuilders(pathbuilders)

    def save(self, obj: Entity | Pathbuilder) -> str:
        """Save an entity to the remote.

        Args:
            obj (Entity | Pathbuilder): The entity to be saved

        Returns:
            str: The response.
        """
        match obj:
            case Entity():
                obj.uri = self.save_entities(obj)
                return obj.uri
            case Pathbuilder():
                # TODO: implement? or see if the flat path format is better suited for im/export...
                pass

    # ----------------------------
    # --- Pathbuilder Handling ---
    # ----------------------------

    def get_pathbuilder(self, pathbuilder_id: str) -> Pathbuilder:
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
            return response.text
        args = json.loads(response.text)
        pathbuilder = Pathbuilder(args["id"], args["paths"])
        return pathbuilder

    def get_pb_test(self, pathbuilder_id):
        url = f"{self.base_url}/pathbuilder/{pathbuilder_id}/get"
        return self.get(url)

    def save_pathbuilder(self, pathbuilder: Pathbuilder) -> Pathbuilder:
        url = f"{self.base_url}/pathbuilder/create/delete"
        response = self.delete(url=url)
        pass

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

    def get_pathbuilder_ids(self):
        """Get all available pathbuilders from the WissKI API.

        Returns:
            dict: The pathbuilders keyed by their ID.
        """
        # Get the IDs of the available pathbuilders via API.
        url = f"{self.base_url}/pathbuilder/list"
        response = self.get(url)
        if response.status_code != 200:
            return response.text
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

    def export_pathbuilder(self, pathbuilder_id: str):
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

    def generate_bundles_and_fields(self):
        # Generate bundles and fields for every pathbuilder.
        for pathbuilder_id in self.pathbuilders:
            url = f"{self.base_url}/pathbuilder/{pathbuilder_id}/generate"
            self.get(url)

    # -----------------------
    # --- Entity Handling ---
    # -----------------------

    def build_entity(self, bundle_id: str, values: dict, uri: str = None) -> Entity:
        """Build an entity from a list of values.

        This builds the entity including nested sub-entities.

        Args:
            bundle_id (str): The bundle ID of the entity.
            values (dict): The values as a field_id -> field_value map
            uri (str, optional): The URI of the entity. Defaults to None.

        Returns:
            Entity: The entity.
        """
        bundle = self.pathbuilder.get_group_for_bundle_id(bundle_id)

        sub_bundles = {}
        entity_values = {}

        for path_id, path in bundle["children"].items():
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

        return Entity(bundle_id, entity_values, uri)

    def serialize_entity(self, entity: Entity) -> dict:
        """Serialize the passed entity into the format that is expected by the remote API endpoint.

        Args:
            entity (Entity): The entity to be serialized.

        Returns:
            dict: The serialized entity.
        """
        entity_data = {
            "bundle": [
                {
                    "target_id": entity.bundle_id,
                    "target_type": WISSKI_BUNDLE,
                }
            ]
        }
        # Attach URI if one was specified
        if entity.uri:
            entity_data["wisski_uri"] = [{"value": entity.uri}]

        bundle_path = self.pathbuilder.get_group_for_bundle_id(entity.bundle_id)

        # Abort if not a bundle.
        if "children" not in bundle_path:
            return None

        for path in bundle_path["children"].values():
            field_id = path["field"]
            field_data = []

            # Skip this path if we do not have a value for this field in our mapped data.
            if field_id not in entity.values.keys():
                continue

            # Build a the field data for each provided field value
            # TODO: check for path cardinality here.
            for value in entity.values[field_id]:
                if path["is_group"]:
                    # Skip sub-entities with no field values.
                    if len(value.values) == 0:
                        continue
                    child_values = self.serialize_entity(value)
                    # Wrap the child data in the 'entity' key for the API to recognize the sub-entity.
                    field_data.append({"entity": child_values})
                else:
                    field_data.append(Api.__build_field_data(path, value))

            entity_data[field_id] = field_data
        return entity_data

    def get_entity(self, uri: str, meta=0, expand=1) -> Entity:
        """Get an entity from the WissKI API.

        Args:
            uri (str): The URI of the entity that should be returned.

        Returns:
            WisskiEntity: The WissKI entity.
        """
        url = f"{self.base_url}/entity/get?uri={uri}&meta={meta}&expand={expand}"
        response = self.get(url)
        if response.status_code != 200:
            print(response.status_code)
            return None

        return Entity.build_from_tree(json.loads(response.text))


    def save_entities(self, entities: list[Entity], create_if_new: bool = True) -> str:
        """Update an existing WissKI entity on the remote.

        Args:
            entity (WisskiEntity): The entity containing the new values for the remote.
            create_if_new (bool, optional): Create a new entity if it does not exist
            on the remote yet. Defaults to True.

        Returns:
            str: The response.
        """
        # Skip when no new entities should be created and no URI was specified.
        # TODO: This check should ultimately be done on PHP side, since there we
        # know which URIs exist and which don't.
        # TODO: also introduce three save modes:
        # 1: create and update
        # 2: only update
        # 3: only create
        if not create_if_new and entities.uri is None:
            return None

        url = f"{self.base_url}/entity/create?overwrite={1 if create_if_new else 0}"
        data = []
        for entity in entities:
            data.append(self.serialize_entity(entity))

        # response = self.post(url=url, json_data=data)
        response = requests.post(
            url = url,
            json=data,
            headers=self.headers,
            auth=self.auth,
            timeout=1200
            )

        # Something went wrong...
        if response.status_code != 200:
            return response.text


        # Replace the entities with the one from the API
        entities = []

        for entity_data in json.loads(response.text):
            print(entity_data)
            entities.append(Entity.build_from_tree(entity_data))

        return entities


    # ----------------------
    # --- File Utilities ---
    # ----------------------

    def import_csv(
        self,
        bundle_id: str,
        file: str,
        separator: str = "|",
        key_type: KeyType = KeyType.PATH_ID,
    ) -> list[Entity]:
        """Import a whole CSV file, row by row.

        Args:
            bundle_id (str): The bundle ID to which the entities in the CSV table belong to.
            file (str): The path to the CSV file.
            separator (str, optional): The separator that separates multiple values for a column. Defaults to '|'.
            key_type (KeyType, optional): The type of header that is used in the CSV table. Defaults to KeyType.PATH_ID.
        """
        # Remap the csv headers from path_id to field_id.
        csv_data = self.parse_csv(file, separator, key_type)

        # Post every csv row
        entities = []
        for row in csv_data:
            entities.append(self.build_entity(bundle_id, row))
            # TODO: add save mode codes here only save/update/ both
        return self.save_entities(entities)

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
        data = []
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
                    if header not in self.pathbuilder.pb_paths:
                        continue
                    new_headers.append(self.pathbuilder.pb_paths[header]["field"])
                headers = new_headers

            for row in reader:
                row_data = {}
                for i, header in enumerate(headers):
                    # Get values separated by the delimiter.
                    values = row[i].split(separator)
                    row_data[header] = values
                # pathbuilder.build_entity_data(bundle_id, row)
                data.append(row_data)
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

    def post(self, url: str, json_data: dict = None, data: str = None, timeout: int = None):
        """Send a HTTP POST request to a URL.

        Args:
            url (str): The URL to send the request to.
            data (dict): The body of the request.

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

    @staticmethod
    def __build_field_data(path: dict, value: any) -> dict:
        """Build the field data for a particular path and value.

        Args:
            path (dict): The path that the value should be stored to.
            value (any): The value to be stored.

        Returns:
            dict: The field data as a dict.
        """
        content = []

        field_type = path["fieldtype"]
        if field_type == "string":
            content = {"value": value}
        elif field_type == "entity_reference":
            content = {
                "target_uri": value,
                "target_type": WISSKI_INDIVIDUAL,
            }
        elif field_type == "text_long":
            content = {
                "value": value,
                "format": "basic_html",
            }
        elif field_type == "image":
            # TODO: see what of these is needed/correct
            content = {
                "target_id": value,
                "alt": None,
                "title": None,
                "target_type": "file",
                "url": "some URL",
            }
        return content
