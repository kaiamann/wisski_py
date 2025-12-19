import json
import sqlite3


def normalize_path_name(name: str) -> str:
    """Normalizes a path name"""
    return name.replace("-", "_")


def get_column_name(meta: dict, fallback: str) -> str:
    return normalize_path_name(meta.get("id", fallback))


def get_column_label(meta: dict) -> str:
    return meta.get("name", "")


def collect_tables(node, tables, metadata):
    children = node.get("children", {})
    if not children:
        return

    is_root = node["id"] == "0"
    table_name = normalize_path_name(node["id"])

    if not is_root:
        meta = metadata.get(node["id"], {})

        tables.setdefault(
            table_name,
            {
                "leaf_fields": [],
                "ref_fields": [],
                "column_meta": [],
                "table_meta": {
                    "bundle": meta.get("bundle", ""),
                    "label": meta.get("name", ""),
                    "description": meta.get("description", ""),
                },
            },
        )

    for child in children.values():
        child_id = child["id"]
        meta = metadata.get(child_id, {})

        if child.get("children"):
            ref_table = normalize_path_name(child_id)
            ref_column = f"{ref_table}_ref"

            if not is_root:
                tables[table_name]["ref_fields"].append((ref_column, ref_table))

            collect_tables(child, tables, metadata)

        else:
            if not is_root:
                meta = metadata.get(child_id, {})
                column_name = get_column_name(meta, child_id)
                column_label = get_column_label(meta)
                column_type = infer_sqlite_type_from_metadata(meta)

                # TODO handle this... For this we have to know which bundle the
                # Entity reference field references, which requires a new API route.
                # We could also do it just like WissKI and infer from the paths array
                if meta.get("fieldtype") == "entity_reference":
                    # The referenced table name could be stored in some metadata field, or
                    # for simplicity, assume it uses the ID of the target group.
                    # Here we'll store a generic placeholder and the user can adjust later
                    ref_table_name = meta.get(
                        "reference_target", "UNKNOWN_TARGET"
                    )  # optional field
                    tables[table_name]["ref_fields"].append(
                        (column_name, ref_table_name)
                    )
                else:
                    # Regular column
                    tables[table_name]["leaf_fields"].append((column_name, column_type))

                tables[table_name]["leaf_fields"].append((column_name, column_type))

                tables[table_name]["column_meta"].append(
                    (
                        column_name,
                        column_label,
                        meta.get("field"),
                        meta.get("description", ""),
                    )
                )


def infer_sqlite_type_from_metadata(meta: dict) -> str:
    datatype = meta.get("datatype_property", "").lower()
    fieldtype = meta.get("fieldtype", "").lower()

    if datatype in {"integer", "int"}:
        return "INTEGER"
    if datatype in {"decimal", "float", "double"}:
        return "REAL"
    if datatype in {"boolean", "bool"}:
        return "INTEGER"
    if datatype in {"date", "datetime"}:
        return "TEXT"

    if fieldtype in {"text_long", "textarea"}:
        return "TEXT"

    return "TEXT"


def create_schema(tables, db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")

    # Main tables
    for table, fields in tables.items():
        columns = ["uri TEXT PRIMARY KEY"]

        for name, col_type in fields["leaf_fields"]:
            columns.append(f"{name} {col_type}")

        for ref_name, ref_table in fields["ref_fields"]:
            columns.append(f"{ref_name} TEXT REFERENCES {ref_table}(uri)")

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {", ".join(columns)}
        );
        """
        conn.execute(ddl)

    # Table metadata
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS table_metadata (
            table_name TEXT PRIMARY KEY,
            bundle TEXT,
            label TEXT,
            description TEXT
        );
    """
    )

    # Column Metadata
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS column_metadata (
            table_name TEXT,
            column_name TEXT,
            label TEXT,
            field TEXT,
            description TEXT,
            PRIMARY KEY (table_name, column_name)
        );
    """
    )

    # Insert table metadata
    for table, fields in tables.items():
        for bundle, label, description in fields["table_meta"]:
            conn.execute(
                """
                INSERT OR REPLACE INTO table_metadata
                (table_name, bundle, label, description)
                VALUES (?, ?, ?, ?)
                """,
                (table, bundle, label, description),
            )

    # Insert column metadata
    for table, fields in tables.items():
        for col, label, field, desc in fields["column_meta"]:
            conn.execute(
                """
                INSERT OR REPLACE INTO column_metadata
                (table_name, column_name, label, field, description)
                VALUES (?, ?, ?, ?, ?)
                """,
                (table, col, label, field, desc),
            )

    conn.commit()
    conn.close()


def json_tree_to_sqlite_schema(tree, metadata, db_path):
    tables = {}
    collect_tables(tree, tables, metadata)
    create_schema(tables, db_path)


with open("tree.json") as f:
    tree = json.load(f)

with open("paths.json") as f:
    metadata = json.load(f)  # already keyed by id

json_tree_to_sqlite_schema(tree, metadata, "schema.db")
