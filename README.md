# WissKI API
This project provides a minimal python library to easily interact with WissKI systems through the WissKI API.

## Installation:
For now clone this repo and install locally using pip.
```
git clone git@github.com:kaiamann/wisski_py.git
cd wisski_py
pip install .
```
After installing you should be able to import the module simply by doing:
```py
from wisski.api import Api, Pathbuilder, Entity
```

## API Initialization:
For initializing the API you have to supply it with a URL and some credentials.
If your WissKI systems API is configured to be accessible without authentication may not need any credentials, although this is not recommended.
In case you want to specify some headers you can do so by supplying them with the `headers` parameter:
```py
api_url = "https://example.wisski.url/wisski/api/v0"
auth = ("some_username", "super_secure_password")
headers = {"Cache-Control": "no-cache"}
api = Api(api_url, auth, headers)
```

## Pathbuilders
To be able to import/edit WissKI entities the API also needs to load a pathbuilder for context.
The context tells the API which fields are present in which Bundle, and thus which values have to be mapped to which path/field.

The API wrapper can either initialize **ALL** pathbuilders that are a available in the system for you, but must be told to do so explicitly; or you configure a (set of) pathbuilder(s) to use.
By default, no pathbuilders are initialized, so **make sure to choose a setup prior to interacting with the API**.

Initializing all available pathbuilders may lead to problems, e.g., when WissKI linkblock pathbuilders are present (?).
This is not tested yet, but it is likely that problems will occur.

**Note:** you must either explicitly initialize all pathbuilders, or configure which pathbuilder to use:

```python
# Check which pathbuilders are present in the system.
print(api.get_pathbuilder_ids()) 
# >>> ['pathbuilder1', 'pathbuilder2', 'linkblock_pathbuilder']

# Initialize all available pathbuilders:
api.init_pathbuilders()

# Or configure the pathbuilder explicitly; this internalizes the pathbuilder under
# the hood, no further processing is required:
api.pathbuilders = ['pathbuilder1']
```

### Multiple Pathbuilders:
The API can also handle multiple pathbuilders, by combining several pathbuilders.
Note that the combining only happens on the client (Python) side.
This functionality allows assigning values to paths from multiple pathbuilders:
E.g.
- `pathbuilder1` has a path that documents the name of a person.
- `pathbuilder2` has a path that documents the occupation of this person.
Assuming that the pathbuilders are configured in a way that these paths belong to the same `person` bundle we can now build a combined pathbuilder like this:
```py
# Make sure to use direct assignment!
# Altering the pathbuilder list with list functions (e.g. pop, append, etc.) won't work properly for now.
self.api.pathbuilders = ['pathbuilder1', 'pathbuilder2']
```

## Entities

### Loading Entities:
Entities can be easily loaded by:
```py
entity = api.get_entity("https://some.random.uri")
```
Accessing information about the entities is easily possible with:
```py
entity.fields # Field values of the entity
entity.uri # The URI of the entity
entity.bundle_id # The ID of the bundle that the entity belongs to.
```

### Editing Entities:
Entities can be easily edited by:
```py
entity.fields["some_field_id"] = ["This value comes from Python!"]
# Save to remote
api.save(entity)
```
Field values are always encapsulated in arrays.


In case there are sub entities:
```py
# Get the first sub-entity
sub_entity = entity.fields["sub_bundle_id"][0]
# Change the field values
sub_entity.fields["sub_bundle_field_id"] = ["This value also comes from Python!"]
# Save to remote
api.save(entity)
```

### Creating new Entities:
To create new entities you just need to supply an entity with a dict that contains the corresponding `field_id` &rarr; `value` mapping.

Let's look at the following example pathbuilder structure to illustrate:
- **Collection Object**: `object_bundle_id`
  - Inventory number: `inventory_number_field_id`
  - Title: `title_field_id`
  - **Production**: `production_bundle_id`
    - Date: `date_field_id`

Format:
- PATH_NAME: `BUNDLE/FIELD_ID`
- **bold** font denotes that the path belongs to a bundle.

Code for creating a new entity:
```py
# First set up the production sub-entity
production_values = {
    'date_field_id': ["11.11.1111"]
}
production = Entity(api=api, fields=production_values, bundle_id="production_bundle_id")

# Set up the collection object entity
object_values = {
    'inventory_number_field_id': ["I1234"],
    'title_field_id': ["some Title", "another Title"],
    'production_bundle_id': [production]
}
collection_object = Entity(api=api, fields=object_values, bundle_id="object_bundle_id")
```
As of now this entity does not have a URI.
Upon saving the entity to the remote, the api returns a new entity with updated URIs.
```py
collection_object = api.save(collection_object)
```

You can also create Entities from a flat data structure like this:
```py
values = {
    'date_field_id': ["11.11.1111"]
    'inventory_number_field_id': ["I1234"],
    'title_field_id': ["some Title", "another Title"],
}
collection_object = api.build_entity('object_bundle_id','object_bundle_id',  values)
collection_object = api.save(collection_object)
```
Just keep in mind that this approach cannot create multiple sub-entities for a specific sub-bundle. (In this case you are only able to create a single `Production` sub-entity)

## Entities and CSV

### Export
If you want you can export an entity to `.csv` format like this:
```py
entity = api.get_entity("http://some.random.uri")
out_dir = os.path.join(os.path.expanduser("~"), "csv_data")
entity.to_csv(out_dir)
```
This will export the entity into potentially multiple `csv` files and place them in the `example_csv` folder in your home directory. (make sure that this directory actually exists)


### Import
You can also import entities from `.csv` files.
A `.csv` file containing entities of a specific bundle should be named like the corresponding bundle id.

For our previous example this would result in:
- `object_bundle_id.csv`
- `production_bundle_id.csv`

These files feature the corresponding field ids as keys, as well as the uri of the entity.
e.g. `object_bundle_id.csv`:
```
uri,inventory_number_field_id,title_field_id,production_bundle_id
```

Here the column `production_bundle_id` would contain URIs of the referenced productions in `production_bundle_id.csv`.

To load these files call the corresponding API functions:
```py
in_dir = os.path.join(os.path.expanduser("~"), "csv_data")
entities = api.load_csv(in_dir, 'object_bundle_id')
entities = api.save(entities)
```
You only need to specify the top-level bundle here.
The API will automatically include all referenced sub-entities from the other `csv` files.