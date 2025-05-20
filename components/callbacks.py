import dash
from dash import Input, Output, State, ALL, ctx, no_update, html
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import time # Added for timestamp generation
import os # <-- Add os import
from utils.db import (
    create_project, update_project, get_projects, get_datasets, create_dataset, 
    update_dataset, delete_project, delete_dataset,
    get_training_job, create_training_job_record, update_training_job_id,
    get_dataset_details, get_project_git_details, get_dataset_name_by_job_id
)
from utils.databricks_connect import get_databricks_connection, execute_sql # Renamed import
# --- Add imports for training and JSON --- #
import json
from utils.training import create_training_job as create_databricks_job, run_training_job
# --- Add MLflow import --- #
from utils.mlflow_utils import get_experiment_runs # Ensure correct function is imported
from utils.mlflow_utils import register_model_version # Added for model registration
# --- End Add imports --- #

# --- Import for fetching notebook files --- #
from components.tabs.project_tab import fetch_notebook_files_from_github
# --- End Import --- #

# --- Helper function for datetime formatting (optional) --- #
from datetime import datetime
def format_timestamp(ts):
    if ts is None:
        return "N/A"
    # Convert milliseconds timestamp (common in MLflow) to datetime
    try:
        dt_object = datetime.fromtimestamp(ts / 1000)
        return dt_object.strftime("%Y-%m-%d %H:%M:%S")
    except TypeError:
        return str(ts) # Fallback if it's not a standard timestamp
# --- End Helper --- #

def register_callbacks(app):

    @app.callback(
        Output('list-store', 'data', allow_duplicate=True),
        Input('url', 'pathname'),  # Trigger on page load
        prevent_initial_call=True
    )
    def update_store_on_refresh(_):
        print("update_store_on_refresh")
        df = get_projects()
        items = []
        if not df.empty:
            try:
                records = df.to_dict(orient='records')
                # Validate and transform records
                items = []
                for rec in records:
                    if isinstance(rec, dict) and 'id' in rec:
                        items.append({
                            'id': int(rec['id']),
                            'text': str(rec.get('name', '')),
                            'description': str(rec.get('description', '')),
                            'catalog': str(rec.get('catalog', '')),
                            'schema': str(rec.get('schema', '')),
                            'git_url': str(rec.get('git_url', '')),
                            'training_notebook': str(rec.get('training_notebook', ''))
                        })
            except Exception as e:
                print(f"Error processing project records: {e}")
                items = []
                
        # Set active project to first item if available, otherwise None
        active_project_id = items[0]['id'] if items else None
        print("Processed items:", items)
        print("Active project ID:", active_project_id)
        
        # Always return a dictionary with both items and active_project_id
        return {'items': items, "active_project_id": active_project_id}

    @app.callback(
        Output("list-group", "children", allow_duplicate=True),
        Input("list-store", "data"),
        prevent_initial_call=True
    )
    def refresh_project_list(store_data):
        print("refresh_project_list")
        print("store_data type:", type(store_data))
        print("store_data:", store_data)
        
        # Handle both list and dictionary data structures
        if isinstance(store_data, dict):
            new_items = store_data.get('items', []) or []
            active_project_id = store_data.get('active_project_id', None)
        elif isinstance(store_data, list):
            new_items = store_data
            # More robust handling of active_project_id for list case
            try:
                active_project_id = new_items[0].get('id') if new_items and isinstance(new_items[0], dict) else None
            except (IndexError, AttributeError, KeyError) as e:
                print(f"Error getting active_project_id from list: {e}")
                active_project_id = None
        else:
            print(f"Unexpected store_data type: {type(store_data)}")
            new_items = []
            active_project_id = None
            
        # Validate items structure
        valid_items = []
        for item in new_items:
            if isinstance(item, dict) and 'id' in item and 'text' in item:
                valid_items.append(item)
            else:
                print(f"Skipping invalid item: {item}")
        
        # Create list items only from valid items
        list_items = [
            dbc.ListGroupItem(
                itm['text'],
                id={"type": "list-group-item", "index": itm['id']},
                action=True,
                active=(itm['id'] == active_project_id)
            ) for itm in valid_items
        ]
        
        # If no valid items, show a message
        if not list_items:
            list_items = [
                dbc.ListGroupItem(
                    "No projects found.",
                    id={"type": "list-group-item", "index": -1},
                    disabled=True
                )
            ]
       
        return list_items


    @app.callback(
        Output("list-store", "data"),
        Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks'),
        State("list-store", "data"),
        prevent_initial_call=True
    )
    def select_project_callback(_, store_data):
        if not ctx.triggered_id or not isinstance(ctx.triggered_id, dict) or ctx.triggered_id.get('type') != 'list-group-item':
            raise PreventUpdate
        project_id = ctx.triggered_id["index"]
        
        # Handle both list and dictionary cases for store_data
        if isinstance(store_data, dict):
            items = store_data.get('items', []) or []
        elif isinstance(store_data, list):
            items = store_data
        else:
            items = []
            
        return {'items': items, "active_project_id": project_id}
     
   


    @app.callback(
        Output("list-store", "data", allow_duplicate=True),
        Input("create-project-button", "n_clicks"),
        State("project-notebook-dropdown", "value"),
        State("list-store", "data"),
        prevent_initial_call=True
    )
    def create_project_callback(create_clicks, training_notebook_file, store_data):
        print("create_project_callback")
        print("store_data type:", type(store_data))
        print("store_data:", store_data)
        
        # Ensure store_data is a dictionary
        if isinstance(store_data, list):
            store_data = {'items': store_data, 'active_project_id': store_data[0]['id'] if store_data else None}
        
        # Create project with default values for required fields
        project_id = create_project(
            name="New Project",  # Default name
            description="No description",  # Default description
            catalog="default_catalog",  # Default catalog
            schema="default_schema",  # Default schema
            git_url="https://github.com/example/repo",  # Default git URL
            training_notebook=training_notebook_file or "default_notebook.py"  # Use provided notebook or default
        )
        
        if project_id is None:
            print("Failed to create project")
            return no_update
            
        # Get existing items, ensuring we have a list
        existing_items = store_data.get('items', []) if isinstance(store_data, dict) else store_data
        if not isinstance(existing_items, list):
            existing_items = []
            
        # Append new item with the same default values
        new_items = existing_items + [{
            'id': project_id,
            'text': 'New Project',  # Match the name used in create_project
            'description': 'No description',
            'catalog': 'default_catalog',
            'schema': 'default_schema',
            'git_url': 'https://github.com/example/repo',
            'training_notebook': training_notebook_file or 'default_notebook.py'
        }]
        print("New items after project creation:", new_items)  # Debug print
     
        return {'items': new_items, "active_project_id": project_id}
    
    @app.callback(
        Output("list-store", "data", allow_duplicate=True),
        Input("update-project-button", "n_clicks"),
        State("project-name", "value"),
        State("project-description", "value"),
        State("project-catalog", "value"),
        State("project-schema", "value"),
        State("project-git-url", "value"),
        State("project-notebook-dropdown", "value"),
        State("list-store", "data"),
        State({"type": "list-group-item", "index": ALL}, "active"),
        prevent_initial_call=True
    )
    def update_project_callback(update_clicks, name, description, catalog, schema, git_url,
                              training_notebook_file,
                              store_data, active_states):
        # choose index
        try:
            idx = active_states.index(True)
        except (ValueError, TypeError):
            idx = len(store_data.get('items', [])) - 1
        if idx < 0:
            return no_update
        project_item = store_data.get('items', [])[idx]
        project_id = project_item.get('id')
        updated = update_project(project_id, name, description, catalog, schema, git_url, training_notebook_file)
        if updated is None:
            return no_update
        # Refresh list
        df = get_projects()
        items = []
        if not df.empty:
            for rec in df.to_dict(orient='records'):
                items.append({
                    'id': int(rec.get('id')),
                    'text': rec.get('name'),
                    'description': rec.get('description'),
                    'catalog': rec.get('catalog'),
                    'schema': rec.get('schema'),
                    'git_url': rec.get('git_url'),
                    'training_notebook': rec.get('training_notebook')
                })
       
        return {'items': items, "active_project_id": project_id}
    
    def get_project_from_store(store_data, project_id):
        """
        Get a project record from the store data by its ID.
        Returns the project dictionary if found, None otherwise.
        """
        # Handle both list and dictionary cases for store_data
        if isinstance(store_data, dict):
            items = store_data.get('items', [])
        elif isinstance(store_data, list):
            items = store_data
        else:
            return None

        # Debug print to see what we're working with
        print("Store data items:", items)
        
        for item in items:
            # Handle both dictionary and object cases, and check for both 'id' and 'index' keys
            item_id = None
            if isinstance(item, dict):
                item_id = item.get('id') or item.get('index')
            elif hasattr(item, 'id'):
                item_id = item.id
            elif hasattr(item, 'index'):
                item_id = item.index
                
            if item_id is not None and item_id == project_id:
                return item
        return None

    @app.callback(
        Output("project-name", "value"),
        Output("project-description", "value"),
        Output("project-catalog", "value"),
        Output("project-schema", "value"),
        Output("project-git-url", "value"),
        Output("project-notebook-dropdown", "value"),
        Input("list-store", "data"),
        prevent_initial_call=True
    )
    def populate_form(store_data):
        # Populate the form inputs based on the selected project
        print("populate_form - store_data:", store_data)
        
        # Handle empty or invalid store_data
        if not store_data:
            # Return empty values for all form fields
            return '', '', '', '', '', None
            
        # Get active project ID, handling both dict and list cases
        if isinstance(store_data, dict):
            active_project_id = store_data.get("active_project_id")
            items = store_data.get('items', [])
        else:
            # If it's a list, try to get the first item's ID
            items = store_data if isinstance(store_data, list) else []
            active_project_id = items[0].get('id') if items and isinstance(items[0], dict) else None
            
        # If no active project or no items, return empty values
        if not active_project_id or not items:
            print("No active project ID or items found")
            return '', '', '', '', '', None
            
        # Use the helper function to get the project based on active_project_id
        project = get_project_from_store(store_data, active_project_id)
        if not project:
            print(f"No project found for ID {active_project_id}")
            return '', '', '', '', '', None
            
        # Handle both dictionary and object cases for project
        if isinstance(project, dict):
            return (
                project.get('text', ''),
                project.get('description', ''),
                project.get('catalog', ''),
                project.get('schema', ''),
                project.get('git_url', ''),
                project.get('training_notebook', None)
            )
        else:
            # Handle object case
            return (
                getattr(project, 'text', ''),
                getattr(project, 'description', ''),
                getattr(project, 'catalog', ''),
                getattr(project, 'schema', ''),
                getattr(project, 'git_url', ''),
                getattr(project, 'training_notebook', None)
            )

    @app.callback(
        [Output("dataset-list-group", "children"),
         Output("dataset-store", "data")],
        Input("tabs", "active_tab"),
        Input({"type": "list-group-item", "index": ALL}, "active"),
        Input("create-dataset-button", "n_clicks"),
        Input("update-dataset-button", "n_clicks"),
        Input("delete-dataset-button", "n_clicks"),
        State("list-store", "data"),
        State("dataset-store", "data"),
        State({"type": "dataset-group-item", "index": ALL}, "active"),
        State({"type": "dataset-group-item", "index": ALL}, "id"),
        # Dataset form fields
        State("dataset-name", "value"),
        State("dataset-source-type", "value"),
        State("dataset-eol-definition", "value"),
        State("dataset-feature-lookup-definition", "value"),
        State("dataset-source-table", "value"),
        State("dataset-timestamp-col", "value"),
        State("dataset-evaluation-type", "value"),
        State("dataset-percentage", "value"),
        State("dataset-source-table-eval", "value"),
        State("dataset-split-time-column", "value"),
        State("dataset-training-table-name", "value"),
        State("dataset-eval-table-name", "value"),
        State("dataset-materialized", "value"),
        State("dataset-target", "value"),
        prevent_initial_call=True
    )
    def manage_datasets(active_tab, proj_active, create_ds, update_ds,
                        delete_ds,
                        proj_store, ds_store, ds_active, ds_ids,
                        name, source_type, eol_def, feat_lookup_def,
                        source_table, timestamp_col,
                        eval_type, percentage,
                        source_table_eval_input, split_time_column,
                        training_table_name, eval_table_name_input,
                        materialized, target):
        trigger = ctx.triggered_id
        # Convert comma-separated string fields into Python lists for Postgres array columns
        def _to_list(val):
            if not val:
                return []
            return [item.strip() for item in val.split(',') if item.strip()]
        eol_list = _to_list(eol_def)
        feat_list = _to_list(feat_lookup_def)
        # source_table is VARCHAR in the DB; use the raw string rather than an array
        # Load datasets when entering Display tab or changing project
        if ((trigger == "tabs" or (isinstance(trigger, dict) and trigger.get("type") == "list-group-item"))
            and active_tab == "tab-dataset"):
            try:
                pidx = proj_active.index(True)
            except (ValueError, TypeError):
                return no_update, no_update
            proj = proj_store.get("items", [])[pidx]
            pid = proj.get("id")
            df = get_datasets(pid)
            items = []
            if not df.empty:
                for rec in df.to_dict(orient="records"):
                    items.append({
                    "id": int(rec.get("id")),
                    "text": rec.get("name"),
                    "source_type": rec.get("source_type"),
                    "eol_definition": rec.get("eol_definition"),
                    "feature_lookup_definition": rec.get("feature_lookup_definition"),
                    "source_table": rec.get("source_table"),
                    "timestamp_col": rec.get("timestamp_col"),
                    "evaluation_type": rec.get("evaluation_type"),
                    "percentage": rec.get("percentage"),
                    "source_table_eval": rec.get("source_table_eval"),
                    "split_time_column": rec.get("split_time_column"),
                    "materialized": rec.get("materialized"),
                    "training_table_name": rec.get("training_table_name"),
                    "eval_table_name": rec.get("eval_table_name"),
                    "target": rec.get("target")
                })
            list_items = []
            for i, it in enumerate(items):
                list_items.append(
                    dbc.ListGroupItem(
                        it["text"],
                        id={"type": "dataset-group-item", "index": it["id"]},
                        action=True,
                        active=(i == 0)
                    )
                )
            if not list_items:
                list_items = [
                    dbc.ListGroupItem(
                        "No datasets found.",
                        id={"type": "dataset-group-item", "index": -1},
                        disabled=True
                    )
                ]
            return list_items, {"items": items}
        # Create a new dataset: give a placeholder 'New' entry with defaults
        if trigger == "create-dataset-button" and create_ds:
            try:
                pidx = proj_active.index(True)
            except (ValueError, TypeError):
                return no_update, no_update
            proj = proj_store.get("items", [])[pidx]
            pid = proj.get("id")
            # Insert with minimal defaults (DB and UI will show 'New')
            dsid = create_dataset(
                pid,
                "New",
                "static_table",
                None, # eol_definition
                None, # feature_lookup_definition
                None, # source_table
                "random", # evaluation_type
                None, # percentage
                None, # source_table_eval
                None, # split_time_column
                None, # timestamp_col (Correct position)
                False, # materialized (Correct position)
                None, # training_table_name
                None,  # eval_table_name (generated)
                "" # <-- Add empty target for new dataset
            )
            if dsid is None:
                return no_update, no_update
            new_items = ds_store.get("items", []) + [{
                "id": dsid,
                "text": "New",
                "source_type": "static_table",
                "eol_definition": [],
                "feature_lookup_definition": [],
                "source_table": "",
                "timestamp_col": "",
                "evaluation_type": "random",
                "percentage": None,
                "source_table_eval": "",
                "split_time_column": "",
                "materialized": False,
                "training_table_name": "",
                "eval_table_name": "",
                "target": "" # <-- Add empty target to store item
            }]
            # Render list without selecting any item (form cleared separately)
            list_items = [
                dbc.ListGroupItem(
                    itm["text"],
                    id={"type": "dataset-group-item", "index": itm["id"]},
                    action=True,
                    active=False
                ) for itm in new_items
            ]
            return list_items, {"items": new_items}
        # Update existing dataset
        if trigger == "update-dataset-button" and update_ds:
            # Determine project index
            try:
                pidx = proj_active.index(True)
            except (ValueError, TypeError):
                return no_update, no_update
            # Determine dataset index, fallback to the last item if none selected
            try:
                didx = ds_active.index(True)
            except (ValueError, TypeError):
                items_prior = ds_store.get("items", [])
                didx = len(items_prior) - 1
                if didx < 0:
                    return no_update, no_update
            proj = proj_store.get("items", [])[pidx]
            pid = proj.get("id")
            ds_it = ds_store.get("items", [])[didx]
            dsid = ds_it.get("id")
            # Persist update to database
            upd = update_dataset(
                dsid,
                name,
                source_type,
                eol_list,
                feat_list,
                source_table,
                eval_type,
                percentage,
                source_table_eval_input,
                split_time_column,
                timestamp_col,
                bool(materialized),
                training_table_name,
                eval_table_name_input,
                target
            )
            if upd is None:
                return no_update, no_update
            # Reload all datasets for this project from DB
            df = get_datasets(pid)
            items = []
            if not df.empty:
                for rec in df.to_dict(orient="records"):
                    items.append({
                        "id": int(rec.get("id")),
                        "text": rec.get("name"),
                        "source_type": rec.get("source_type"),
                        "eol_definition": rec.get("eol_definition"),
                        "feature_lookup_definition": rec.get("feature_lookup_definition"),
                        "source_table": rec.get("source_table"),
                        "timestamp_col": rec.get("timestamp_col"),
                        "evaluation_type": rec.get("evaluation_type"),
                        "percentage": rec.get("percentage"),
                        "source_table_eval": rec.get("source_table_eval"),
                        "split_time_column": rec.get("split_time_column"),
                        "materialized": rec.get("materialized"),
                        "training_table_name": rec.get("training_table_name"),
                        "eval_table_name": rec.get("eval_table_name"),
                        "target": rec.get("target")
                    })
            list_items = [
                dbc.ListGroupItem(
                    itm["text"],
                    id={"type": "dataset-group-item", "index": itm["id"]},
                    action=True,
                    active=(itm["id"] == dsid)
                ) for itm in items
            ]
            return list_items, {"items": items}
        # Delete the selected dataset
        if trigger == "delete-dataset-button" and delete_ds:
            print("Delete dataset triggered")
            # Determine project index
            try:
                pidx = proj_active.index(True)
            except (ValueError, TypeError):
                print("No project selected for delete")
                return no_update, no_update
            proj = proj_store.get("items", [])[pidx]
            pid = proj.get("id")
            
            # Determine dataset index
            try:
                didx = ds_active.index(True)
            except (ValueError, TypeError):
                print("No dataset selected for delete")
                # Optionally show a message to the user here
                return no_update, no_update
            
            ds_it = ds_store.get("items", [])[didx]
            dsid = ds_it.get("id")
            print(f"Attempting to delete dataset with ID: {dsid}")

            # Call delete function
            success = delete_dataset(dsid)
            if not success:
                print(f"Failed to delete dataset {dsid}")
                # Optionally show an error message
                return no_update, no_update
            
            print(f"Successfully deleted dataset {dsid}")
            # Reload datasets for this project from DB
            df = get_datasets(pid)
            items = []
            if not df.empty:
                for rec in df.to_dict(orient="records"):
                    items.append({
                        "id": int(rec.get("id")),
                        "text": rec.get("name"),
                        "source_type": rec.get("source_type"),
                        "eol_definition": rec.get("eol_definition"),
                        "feature_lookup_definition": rec.get("feature_lookup_definition"),
                        "source_table": rec.get("source_table"),
                        "timestamp_col": rec.get("timestamp_col"),
                        "evaluation_type": rec.get("evaluation_type"),
                        "percentage": rec.get("percentage"),
                        "source_table_eval": rec.get("source_table_eval"),
                        "split_time_column": rec.get("split_time_column"),
                        "materialized": rec.get("materialized"),
                        "training_table_name": rec.get("training_table_name"),
                        "eval_table_name": rec.get("eval_table_name"),
                        "target": rec.get("target")
                    })
            list_items = []
            for i, itm in enumerate(items):
                 list_items.append(
                    dbc.ListGroupItem(
                        itm["text"],
                        id={"type": "dataset-group-item", "index": itm["id"]},
                        action=True,
                        active=(i == 0) # Select first item after delete
                    )
                )
            if not list_items:
                list_items = [
                    dbc.ListGroupItem(
                        "No datasets found.",
                        id={"type": "dataset-group-item", "index": -1},
                        disabled=True
                    )
                ]
            
            print("Refreshed dataset list after delete")
            # Clear the form fields as well after delete
            # TODO: This might be better handled by triggering the form population callback
            # For now, returning no_update for the form fields.
            return list_items, {"items": items}
        return no_update, no_update
    # -- Dataset list item active-state callback --------------------------------
    @app.callback(
        Output({"type": "dataset-group-item", "index": ALL}, "active"),
        Input({"type": "dataset-group-item", "index": ALL}, "n_clicks"),
        State({"type": "dataset-group-item", "index": ALL}, "active"),
        State({"type": "dataset-group-item", "index": ALL}, "id"),
        prevent_initial_call=True
    )
    def update_dataset_active(n_clicks_list, active_states, ids):
        # Only one dataset item active at a time
        new_states = [False] * len(active_states)
        triggered = ctx.triggered_id
        # Extract index of clicked dataset
        if isinstance(triggered, dict):
            clicked = triggered.get('index')
        else:
            clicked = getattr(triggered, 'index', None)
        # Map ids to indices
        id_list = []
        for id_obj in ids:
            if isinstance(id_obj, dict):
                id_list.append(id_obj.get('index'))
            else:
                id_list.append(getattr(id_obj, 'index', None))
        try:
            pos = id_list.index(clicked)
        except (ValueError, TypeError):
            return active_states
        new_states[pos] = True
        return new_states

    # -- Dataset form population & clear callback -----------------------------
    @app.callback(
        Output("dataset-name", "value"),
        Output("dataset-source-type", "value"),
        Output("dataset-eol-definition", "value"),
        Output("dataset-feature-lookup-definition", "value"),
        Output("dataset-source-table", "value"),
        Output("dataset-timestamp-col", "value"),
        Output("dataset-evaluation-type", "value"),
        Output("dataset-percentage", "value"),
        Output("dataset-source-table-eval", "value"),
        Output("dataset-split-time-column", "value"),
        Output("dataset-training-table-name", "value"),
        Output("dataset-eval-table-name", "value"),
        Output("dataset-materialized", "value"),
        Output("materialize-dataset-button", "disabled"),
        Output("dataset-target", "value"),
        Input({"type": "dataset-group-item", "index": ALL}, "active"),
        Input("create-dataset-button", "n_clicks"),
        State("dataset-store", "data"),
        prevent_initial_call=True
    )
    def manage_dataset_form(active_states, create_clicks, ds_store):
        # Determine trigger: either a dataset item was selected or 'Create' was clicked
        trig = ctx.triggered_id
        # Create: clear form to defaults, enable Materialize button
        if trig == "create-dataset-button":
            return (
                "",              # name
                "static_table",  # source type
                "",              # eol definition
                "",              # feature lookup definition
                "",              # source table
                "",              # timestamp_col
                "random",        # evaluation type
                None,             # percentage
                "",              # source_table_eval
                "",              # split time column
                "",              # training table name
                "",              # eval_table_name
                [],                # materialized
                False,             # materialize button disabled
                ""               # <-- Clear target on create
            )
        # Selection: populate from store, set button disabled based on materialized state
        if isinstance(trig, dict) and trig.get('type') == 'dataset-group-item':
            if not ds_store or not active_states:
                raise PreventUpdate
            try:
                idx = active_states.index(True)
            except ValueError:
                raise PreventUpdate
            ds = ds_store.get('items', [])[idx]
            # Checklist expects list of values
            materialized_status = ds.get('materialized', False)
            mat_checklist_value = [True] if materialized_status else []
            # EOL/feature strings
            eol_str = ', '.join(ds.get('eol_definition') or [])
            feat_str = ', '.join(ds.get('feature_lookup_definition') or [])
            return (
                ds.get('text', ''),
                ds.get('source_type', ''),
                eol_str,
                feat_str,
                ds.get('source_table', ''),
                ds.get('timestamp_col', ''),
                ds.get('evaluation_type', ''),
                ds.get('percentage', None),
                ds.get('source_table_eval', ''),
                ds.get('split_time_column', ''),
                ds.get('training_table_name', ''),
                ds.get('eval_table_name', ''),
                mat_checklist_value,
                materialized_status,  # Set button disabled state
                ds.get('target', '') # <-- Populate target on selection
            )
        # Other triggers: no update
        raise PreventUpdate
    
    @app.callback(
        Output("list-store", "data", allow_duplicate=True),
        Input("delete-project-button", "n_clicks"),
        State("list-store", "data"),
        State({"type": "list-group-item", "index": ALL}, "active"),
        prevent_initial_call=True
    )
    def delete_project_callback(delete_clicks, store_data, active_states):
        print("delete_project_callback")
        if not store_data or not active_states:
            raise PreventUpdate
        try:
            idx = active_states.index(True)
        except ValueError:
            raise PreventUpdate
        project_item = store_data.get('items', [])[idx]
        project_id = project_item.get('id')
        # Delete project and its datasets
        success = delete_project(project_id)
        if not success:
            return no_update
        # Refresh list
        df = get_projects()
        items = []
        if not df.empty:
            for rec in df.to_dict(orient='records'):
                items.append({
                    'id': int(rec.get('id')),
                    'text': rec.get('name'),
                    'description': rec.get('description'),
                    'catalog': rec.get('catalog'),
                    'schema': rec.get('schema'),
                    'git_url': rec.get('git_url'),
                    'training_notebook': rec.get('training_notebook')
                })
        # Set active project to first item if available, otherwise None
        active_project_id = items[0]['id'] if items else None
        return {'items': items, "active_project_id": active_project_id}
    
    # -- Callback to control visibility of conditional form fields ---
    @app.callback(
        Output("div-eol-definition", "style"),
        Output("div-feature-lookup-definition", "style"),
        Output("div-source-table", "style"),
        Output("div-timestamp-col", "style"),  # Added timestamp col div
        Output("div-percentage", "style"),
        Output("div-eval-table-name", "style"),
        Output("div-split-time-column", "style"),
        Input("dataset-source-type", "value"),
        Input("dataset-evaluation-type", "value")
    )
    def toggle_form_fields(source_type, eval_type):
        # Default styles (hidden)
        eol_style = {"display": "none"}
        feat_lookup_style = {"display": "none"}
        source_table_style = {"display": "none"}
        timestamp_col_style = {"display": "none"}
        percentage_style = {"display": "none"}
        eval_table_style = {"display": "none"}
        split_time_style = {"display": "none"}

        # Show fields based on source_type
        if source_type == "static_table":
            source_table_style = {}
        elif source_type == "dynamic_table":
            source_table_style = {}
            timestamp_col_style = {}
        elif source_type == "feature_lookup":
            eol_style = {}
            feat_lookup_style = {}
            timestamp_col_style = {}

        # Show fields based on evaluation_type
        if eval_type == "random":
            percentage_style = {}
        elif eval_type == "table":
            eval_table_style = {}
        elif eval_type == "timestamp":
            split_time_style = {}

        return (
            eol_style,
            feat_lookup_style,
            source_table_style,
            timestamp_col_style,
            percentage_style,
            eval_table_style,
            split_time_style
        )
    
    # --- Helper to get dataset from store by ID ---
    def get_dataset_from_store(ds_store, ds_id):
        items = ds_store.get('items', [])
        for item in items:
            if item.get('id') == ds_id:
                return item
        return None

    # -- Materialize Dataset Callback -----------------------------------------
    @app.callback(
        Output("dataset-store", "data", allow_duplicate=True),
        Output("dataset-training-table-name", "value", allow_duplicate=True),
        Output("dataset-eval-table-name", "value", allow_duplicate=True),
        Output("dataset-materialized", "value", allow_duplicate=True),
        # Consider adding an Output for user feedback (e.g., an Alert)
        Input("materialize-dataset-button", "n_clicks"),
        # Get necessary state: selected project/dataset, form values for logic
        State("list-store", "data"),
        State("dataset-store", "data"),
        State({"type": "list-group-item", "index": ALL}, "active"), # To find selected project
        State({"type": "dataset-group-item", "index": ALL}, "active"), # To find selected dataset
        State("dataset-source-table", "value"),
        State("dataset-source-type", "value"),
        State("dataset-evaluation-type", "value"),
        State("dataset-percentage", "value"),
        State("dataset-source-table-eval", "value"),
        State("dataset-split-time-column", "value"),
        State("dataset-timestamp-col", "value"),
        State("dataset-target", "value"),
        # Get dataset ID directly (needed for update_dataset)
        State({"type": "dataset-group-item", "index": ALL}, "id"),
        prevent_initial_call=True
    )
    def materialize_dataset_callback(n_clicks, list_store, ds_store, proj_active, ds_active,
                                   source_table, source_type, eval_type, percentage,
                                   source_table_eval_input, split_time_input, timestamp_col,
                                   target, ds_ids):
        if not n_clicks or n_clicks < 1:
            raise PreventUpdate

        print("Materialize button clicked")
        conn_db = None # Databricks connection
        sql_success = False

        # --- 1. Get Selected Project and Dataset --- 
        if not list_store or not ds_store or not proj_active or not ds_active:
             print("Missing store or active state")
             raise PreventUpdate

        try:
            proj_idx = proj_active.index(True)
            ds_idx = ds_active.index(True)
        except ValueError:
            print("No active project or dataset selected")
            # TODO: Show user feedback (e.g., dbc.Alert)
            raise PreventUpdate
            
        # Extract the ID of the selected dataset using its index
        selected_ds_id_obj = ds_ids[ds_idx]
        selected_ds_id = selected_ds_id_obj.get('index') if isinstance(selected_ds_id_obj, dict) else None
        if selected_ds_id is None or selected_ds_id == -1: # Check for placeholder ID
             print("Invalid dataset selected")
             raise PreventUpdate

        # Get full project and dataset details from store
        project = get_project_from_store(list_store, list_store.get('active_project_id'))
        dataset = get_dataset_from_store(ds_store, selected_ds_id)

        if not project or not dataset:
            print("Could not find project or dataset in store")
            raise PreventUpdate
            
        project_catalog = project.get('catalog')
        project_schema = project.get('schema')
        ds_id = dataset.get('id')
        ds_name = dataset.get('text') # Use the stored name
        
        if not all([project_catalog, project_schema, source_table, ds_name]): # Added ds_name check
            print("Missing required info: catalog, schema, source table, or dataset name")
            # TODO: Show user feedback
            raise PreventUpdate

        # --- 2. Generate Table Names --- 
        timestamp_suffix = int(time.time())
        # Sanitize dataset name for use in table names
        sanitized_ds_name = ds_name.replace(' ', '_')
        
        # Use sanitized dataset name as the base for generated tables
        target_base = f"{project_catalog}.{project_schema}.{sanitized_ds_name}"
        training_table_name = f"{target_base}_training_{timestamp_suffix}"
        generated_eval_table_name = f"{target_base}_eval_{timestamp_suffix}"

        # Determine qualified source table name (needed for SELECT)
        source_parts = source_table.split('.')
        if len(source_parts) == 1:
            qualified_source_table = f"{project_catalog}.{project_schema}.{source_parts[0]}"
        elif len(source_parts) == 2:
            qualified_source_table = f"{project_catalog}.{source_parts[0]}.{source_parts[1]}"
        else: # Assume already fully qualified
            qualified_source_table = source_table
            
        print(f"Using Dataset Name: {ds_name} (Sanitized: {sanitized_ds_name})")
        print(f"Source Table (Qualified): {qualified_source_table}")
        print(f"Generated training table: {training_table_name}")
        print(f"Generated eval table: {generated_eval_table_name}")

        # --- 3. Execute Databricks SQL --- 
        print("Attempting to connect to Databricks...")
        conn_db = get_databricks_connection()
        if conn_db is None:
            print("Databricks connection failed")
            # TODO: Show user feedback
            raise PreventUpdate # Or return specific error state
        
        try:
            print(f"Generating SQL for eval_type: {eval_type}")
            sql_train = ""
            sql_eval = ""

            if eval_type == "random":
                if percentage is None or not (0 < percentage < 1):
                    print("Invalid percentage for random split.")
                    # TODO: User feedback
                    raise PreventUpdate 
                # Use random split approach
                temp_table_name = f"{target_base}_temp_split_{timestamp_suffix}"
                sql_create_temp = f"CREATE TABLE {temp_table_name} AS SELECT *, rand() as __rand_split FROM {qualified_source_table}"
                sql_train = f"CREATE TABLE {training_table_name} AS SELECT * EXCEPT (__rand_split) FROM {temp_table_name} WHERE __rand_split >= {percentage}"
                sql_eval = f"CREATE TABLE {generated_eval_table_name} AS SELECT * EXCEPT (__rand_split) FROM {temp_table_name} WHERE __rand_split < {percentage}"
                sql_drop_temp = f"DROP TABLE IF EXISTS {temp_table_name}"
                
                print("Executing: Create Temp Table")
                if not execute_sql(conn_db, sql_create_temp):
                     raise Exception("Failed to create temporary split table")
                print("Executing: Create Training Table")
                if not execute_sql(conn_db, sql_train):
                    raise Exception("Failed to create training table")
                print("Executing: Create Eval Table")
                if not execute_sql(conn_db, sql_eval):
                    raise Exception("Failed to create eval table")
                print("Executing: Drop Temp Table")
                execute_sql(conn_db, sql_drop_temp) # Try to cleanup even if subsequent steps fail
                sql_success = True # If all steps passed

            elif eval_type == "table":
                if not source_table_eval_input:
                    print("Missing evaluation table name for 'table' split type.")
                    # TODO: User feedback
                    raise PreventUpdate
                # Assume eval_table_input is qualified or in the same catalog/schema context
                # Training table is the full source
                sql_train = f"CREATE TABLE {training_table_name} AS SELECT * FROM {qualified_source_table}"
                # Eval table is a copy of the user-provided table (use renamed state variable)
                sql_eval = f"CREATE TABLE {generated_eval_table_name} AS SELECT * FROM {source_table_eval_input}"
                
                print("Executing: Create Training Table")
                if not execute_sql(conn_db, sql_train):
                     raise Exception("Failed to create training table")
                print("Executing: Create Eval Table from Input")
                if not execute_sql(conn_db, sql_eval):
                     raise Exception("Failed to create eval table")
                sql_success = True

            elif eval_type == "timestamp":
                if not timestamp_col or not split_time_input:
                    print("Missing timestamp column or split timestamp for 'timestamp' split type.")
                    # TODO: User feedback
                    raise PreventUpdate
                # Ensure split_time_input is appropriately quoted if it's a string timestamp
                # Assuming it's a standard timestamp format recognized by Databricks SQL
                sql_train = f"CREATE TABLE {training_table_name} AS SELECT * FROM {qualified_source_table} WHERE {timestamp_col} < '{split_time_input}'"
                sql_eval = f"CREATE TABLE {generated_eval_table_name} AS SELECT * FROM {qualified_source_table} WHERE {timestamp_col} >= '{split_time_input}'"
                
                print("Executing: Create Training Table")
                if not execute_sql(conn_db, sql_train):
                     raise Exception("Failed to create training table")
                print("Executing: Create Eval Table")
                if not execute_sql(conn_db, sql_eval):
                     raise Exception("Failed to create eval table")
                sql_success = True

            else:
                print(f"Unknown evaluation type: {eval_type}")
                raise PreventUpdate

        except Exception as e:
            print(f"Databricks SQL execution failed: {e}")
            sql_success = False
            # TODO: Show user feedback 
            # Optional: Attempt cleanup like dropping partially created tables
            # execute_sql(conn_db, f"DROP TABLE IF EXISTS {training_table_name}")
            # execute_sql(conn_db, f"DROP TABLE IF EXISTS {generated_eval_table_name}")

        finally:
            if conn_db:
                conn_db.close()
                print("Databricks connection closed.")

        if not sql_success:
            print("Materialization failed due to SQL execution error.")
            raise PreventUpdate
            
        print("Databricks SQL execution successful")

        # --- 4. Update Database Record --- 
        print(f"Updating dataset ID: {ds_id} in database")
        updated = update_dataset(
            ds_id,
            name=ds_name,
            source_type=source_type,
            eol_definition=dataset.get('eol_definition'),
            feature_lookup_definition=dataset.get('feature_lookup_definition'),
            source_table=source_table,
            evaluation_type=eval_type,
            percentage=percentage,
            source_table_eval=source_table_eval_input,
            split_time_column=split_time_input,
            timestamp_col=timestamp_col,
            materialized=True,
            training_table_name=training_table_name,
            eval_table_name=generated_eval_table_name,
            target=target
        )

        if updated is None:
            print("Database update failed")
            # TODO: Show user feedback
            raise PreventUpdate
            
        print("Database update successful")

        # --- 5. Update Store and Form --- 
        # Find the dataset in the store and update it
        updated_items = []
        for item in ds_store.get('items', []):
            if item.get('id') == ds_id:
                item['materialized'] = True
                item['training_table_name'] = training_table_name
                item['eval_table_name'] = generated_eval_table_name
                item['target'] = target
            updated_items.append(item)

        # Update form fields and checklist
        new_materialized_value = [True] # Checklist expects a list

        return ({'items': updated_items}, training_table_name, generated_eval_table_name, new_materialized_value)
    
    @app.callback(
        Output("train-status-output", "children"),
        Input("train-run-button", "n_clicks"),
        State("list-store", "data"), # Get project ID
        State("train-dataset-dropdown", "value"), # Get selected dataset ID from dropdown
        State("train-parameters-input", "value"), # Get user-provided params
        prevent_initial_call=True
    )
    def handle_train_button_click(n_clicks, proj_store, selected_ds_id, params_json_str):
        if n_clicks == 0:
            raise PreventUpdate

        # --- 1. Get Selected Project and Dataset IDs --- #
        project_id = proj_store.get("active_project_id")
        # selected_ds_id is now directly from the dropdown state
        
        # --- Remove Debugging related to ds_store and active states --- #
        print("--- Debug: handle_train_button_click ---")
        print(f"Project ID: {project_id}")
        print(f"Selected Dataset ID from Dropdown: {selected_ds_id}")
        # print(f"Dataset Active States: {ds_active_states}") # Removed
        # ds_items_count = len(ds_store.get("items", [])) # Removed
        # print(f"Items in dataset-store: {ds_items_count}") # Removed
        # --- End Debugging Removal --- #

        # --- Add Validation for Dropdown Selection --- #
        if not selected_ds_id:
             return dbc.Alert("Error: Please select a dataset from the dropdown.", color="danger")
        # --- End Validation --- #
        
        print("----------------------------------------")

        if not project_id: # selected_ds_id already checked
            return dbc.Alert("Error: Project not selected properly.", color="danger")

        # --- 2. Parse Parameters --- #
        try:
            parameters = json.loads(params_json_str or '{}') # Default to empty dict if no input
        except json.JSONDecodeError as e:
            return dbc.Alert(f"Error parsing parameters JSON: {e}", color="danger")

        # --- 3. Check Existing Training Job Record --- #
        training_record = get_training_job(project_id, selected_ds_id)
        db_job_id = None
        training_record_id = None

        if training_record:
            training_record_id = training_record.get('id')
            db_job_id = training_record.get('job_id')
            print(f"Found existing training record ID: {training_record_id} with Job ID: {db_job_id}")
        else:
            print("No existing training record found, will create one.")

        # --- 4. Get Job Details (Dataset Target, Git Info, etc.) --- #
        dataset_details = get_dataset_details(selected_ds_id)
        project_details = get_project_from_store(proj_store, project_id)
        
        # Get base git details (provider, branch, notebook_path from env/fallbacks in db.py)
        # and potentially a DB-fetched or default git_url if not in project_details
        raw_git_details = get_project_git_details(project_id)

        if not dataset_details or not project_details:
            return dbc.Alert("Error: Could not retrieve necessary project/dataset details.", color="danger")

        # Prioritize git_url from the project form (via store) if available
        git_url_from_store = project_details.get('git_url')
        final_git_url = git_url_from_store if git_url_from_store else raw_git_details.get('git_url')
        
        # Construct final git_details, prioritizing store's git_url
        final_git_details = {
            "git_url": final_git_url,
            "git_provider": raw_git_details.get('git_provider'),
            "git_branch": raw_git_details.get('git_branch'),
            "notebook_path": raw_git_details.get('notebook_path')
        }

        target_variable = dataset_details.get('target')
        training_table = dataset_details.get('training_table_name')
        # --- Add retrieval for eval table --- #
        eval_table = dataset_details.get('eval_table_name') 
        # --- End retrieval --- #
        dataset_name = dataset_details.get('name')
        project_name = project_details.get('text')

        if not training_table:
             return dbc.Alert(f"Error: Dataset '{dataset_name}' has not been materialized yet (no training table name).", color="danger")
        
        # --- Add check for eval table if needed by the job --- #
        # Depending on your notebook logic, you might need the eval table even if created.
        # Add a check here if the eval table is strictly required.
        if not eval_table:
             print(f"Warning: Dataset '{dataset_name}' does not have a generated evaluation table name.")
             # Optionally return an error if eval_table is mandatory:
             # return dbc.Alert(f"Error: Dataset '{dataset_name}' is missing a generated evaluation table.", color="danger")
        # --- End check --- #

        # --- 5. Logic: Run Existing or Create New Job --- #
        try:
            if db_job_id:
                # --- 5a. Run Existing Job --- #
                print(f"Running existing Databricks Job ID: {db_job_id}")
                run_info = run_training_job(db_job_id)
                return dbc.Alert(f"Started existing job {db_job_id}. Run ID: {run_info.run_id}", color="info")
            else:
                # --- 5b. Create New Job --- # 
                job_name = f"{project_name}_{dataset_name}"
                # --- Use project name for experiment path --- #
                experiment_name = project_name
                # --- End change --- #
                print(f"Creating new Databricks Job: {job_name}")
                
                # Prepare base parameters for the notebook task
                # --- Remove base_params dict, pass directly --- #
                # base_params = {
                #     "experiment_name": experiment_name,
                #     "target": target_variable,
                #     "table_name": training_table # Old parameter
                # }
                # Merge user parameters, user params take precedence
                # base_params.update(parameters) 
                # --- End Removal --- #

                new_job = create_databricks_job(
                    job_name=job_name,
                    experiment_name=experiment_name,
                    target=target_variable,
                    # --- Pass correct table names --- #
                    training_table_name=training_table,
                    eval_table_name=eval_table, # Pass the retrieved eval table name
                    # --- End pass correct table names --- #
                    git_url=final_git_details['git_url'],
                    git_provider=final_git_details['git_provider'],
                    git_branch=final_git_details['git_branch'],
                    notebook_path=final_git_details['notebook_path']
                    # Pass merged parameters to the notebook task
                    # This assumes create_training_job is updated or handles extra kwargs
                    # to pass them to the notebook_task.base_parameters
                    # --- Modification needed in utils/training.py --- #
                    # **base_params # This line needs adjustment based on function signature
                )
                new_job_id = new_job.job_id
                print(f"Created Databricks Job ID: {new_job_id}")

                # --- 5c. Update DB --- #
                if training_record_id:
                    # Update existing record that didn't have a job_id
                    success = update_training_job_id(training_record_id, new_job_id)
                    print(f"Updated existing training record {training_record_id} with job ID {new_job_id}: {success}")
                else:
                    # Create new training record
                    training_record_id = create_training_job_record(project_id, selected_ds_id, json.dumps(parameters))
                    if training_record_id:
                         success = update_training_job_id(training_record_id, new_job_id)
                         print(f"Created new training record {training_record_id}, updated with job ID {new_job_id}: {success}")
                    else:
                         print("Failed to create training record in DB.")
                         # Decide how to handle this - maybe don't run the job?

                if not training_record_id or not success:
                     return dbc.Alert("Error: Failed to update database with new Job ID. Job was created but not run.", color="warning")

                # --- 5d. Run New Job --- #
                print(f"Running newly created Databricks Job ID: {new_job_id}")
                run_info = run_training_job(new_job_id)
                return dbc.Alert(f"Created job {new_job_id} and started run. Run ID: {run_info.run_id}", color="success")

        except Exception as e:
            import traceback
            print(f"Error during training job creation/run: {traceback.format_exc()}")
            return dbc.Alert(f"An error occurred: {e}", color="danger")
    
    @app.callback(
        Output("train-mlflow-runs-list", "children"),
        Input("tabs", "active_tab"),
        Input("list-store", "data"), # Trigger when project list/selection changes
        prevent_initial_call=True
    )
    def update_mlflow_runs_list(active_tab, proj_store):
        if active_tab != 'tab-train' or not proj_store:
            raise PreventUpdate

        project_id = proj_store.get('active_project_id')
        if not project_id:
            return html.Tr(html.Td("Select a project to view MLflow runs.", colSpan=7))

        project_details = get_project_from_store(proj_store, project_id)
        if not project_details:
            print(f"Error: Could not find details for project ID {project_id} in store.")
            return html.Tr(html.Td("Error retrieving project details.", colSpan=7))

        base_experiment_name = project_details.get('text')
        catalog = project_details.get('catalog')
        schema = project_details.get('schema')

        if not base_experiment_name:
            return html.Tr(html.Td("Project name is missing.", colSpan=7))

        # --- Determine target_model_name --- START ---
        # First, get runs without model info to find a dataset name
        pre_runs, _, pre_error_msg = get_experiment_runs(base_experiment_name) # No target_model_name yet
        
        target_model_name = None
        dataset_name_for_model = None

        if pre_error_msg:
            print(f"MLflow fetch error (pre-fetch for dataset name): {pre_error_msg}")
            # Proceed without target_model_name, table will show N/A for model columns
        elif pre_runs:
            for pre_run in pre_runs:
                pre_run_data = pre_run.get('data', {})
                pre_tags = {tag['key']: tag['value'] for tag in pre_run_data.get('tags', [])}
                job_id_str = pre_tags.get('mlflow.databricks.jobID')
                if job_id_str:
                    try:
                        dataset_name_for_model = get_dataset_name_by_job_id(int(job_id_str))
                        if dataset_name_for_model:
                            break # Found a dataset name
                    except ValueError:
                        pass # Invalid job ID format
                    except Exception as e:
                        print(f"Error looking up dataset by job ID {job_id_str} for model naming: {e}")
            
            if dataset_name_for_model and catalog and schema:
                sanitized_dataset_name = dataset_name_for_model.replace(" ", "_").replace("-", "_") # Basic sanitization
                target_model_name = f"{catalog}.{schema}.{sanitized_dataset_name}"
                print(f"Constructed target_model_name: {target_model_name}")
            else:
                print("Could not determine dataset name for model, or catalog/schema missing.")
        # --- Determine target_model_name --- END ---

        print(f"Fetching MLflow runs for base experiment: {base_experiment_name}, target model: {target_model_name}")
        runs, experiment_id, error_msg = get_experiment_runs(base_experiment_name, target_model_name=target_model_name)

        if error_msg:
            print(f"MLflow fetch error: {error_msg}")
            return html.Tr(html.Td(error_msg, colSpan=7))
        
        if runs is None:
            print("MLflow runs list is None unexpectedly after main fetch.")
            return html.Tr(html.Td("Error fetching runs.", colSpan=7))

        if not runs:
            return html.Tr(html.Td("No runs found for this experiment.", colSpan=7))

        table_rows = []
        host = os.getenv("DATABRICKS_HOST") 
        if host and not host.startswith('https://'):
             host = 'https://' + host 
        
        for run in runs: 
            run_info = run.get('info', {})
            run_data = run.get('data', {})
            run_id = run_info.get('run_id', 'N/A')
            
            tags = {tag['key']: tag['value'] for tag in run_data.get('tags', [])}
            job_id_str = tags.get('mlflow.databricks.jobID')
            job_run_id_str = tags.get('mlflow.databricks.jobRunID')

            source_link = "#" 
            link_text = "Source Info Missing"
            if host and job_id_str and job_run_id_str:
                host_cleaned = host.rstrip('/') 
                source_link = f"{host_cleaned}/jobs/{job_id_str}/runs/{job_run_id_str}"
                link_text = f"run {job_run_id_str} of job {job_id_str}"
            elif job_id_str and job_run_id_str: 
                 link_text = f"run {job_run_id_str} of job {job_id_str} (Link Unavailable)"

            mlflow_run_link = "#" 
            if host and experiment_id and run_id != 'N/A':
                 host_cleaned = host.rstrip('/')
                 mlflow_run_link = f"{host_cleaned}/ml/experiments/{experiment_id}/runs/{run_id}"

            source_cell = html.Td(html.A(link_text, href=source_link, target="_blank"))
            run_id_cell = html.Td(html.A(run_id, href=mlflow_run_link, target="_blank")) 

            metrics_str = "N/A"
            metrics_list = run_data.get('metrics', []) 
            if metrics_list:
                metrics_str = " | ".join([
                    f"{metric.get('key')}: {metric.get('value'):.4f}"
                    for metric in metrics_list 
                    if metric.get('key') and metric.get('value') is not None
                ])
            metrics_cell = html.Td(metrics_str)

            dataset_name_display = "N/A" 
            if job_id_str:
                try:
                    dataset_name_from_db = get_dataset_name_by_job_id(int(job_id_str))
                    dataset_name_display = dataset_name_from_db if dataset_name_from_db else "Dataset Not Found"
                except ValueError:
                    dataset_name_display = "Invalid Job ID Tag"
                except Exception as e:
                    print(f"Error looking up dataset by job ID {job_id_str}: {e}")
                    dataset_name_display = "DB Lookup Error"
            else:
                 dataset_name_display = "Job ID Tag Missing"
                 
            dataset_cell = html.Td(dataset_name_display)

            # --- Get Registered Model Info --- 
            reg_model_name = run.get('registered_model_name', 'N/A')
            reg_model_version = run.get('registered_model_version', 'N/A')
            model_name_cell = html.Td(reg_model_name)
            model_version_cell = html.Td(reg_model_version)
            # --- End Registered Model Info --- 

            model_source = f"runs:/{run_id}/model"
            button_dataset_name = dataset_name_display if isinstance(dataset_name_display, str) else "UnknownDataset"

            # Disable button if model name and version are already populated for this run
            button_disabled = bool(reg_model_name and reg_model_name != 'N/A')

            register_button = html.Button(
                "Register Model",
                id={
                    'type': 'register-model-button', 
                    'index': run_id, 
                    'datasetname': button_dataset_name,
                    'modelsource': model_source
                },
                n_clicks=0,
                disabled=button_disabled, # Updated disabled state
                className="btn btn-primary"
            )
            actions_cell = html.Td(register_button)

            # Reorder: Dataset, Metrics, Run ID, Source, Reg. Model, Version, Actions
            table_rows.append(html.Tr([dataset_cell, metrics_cell, run_id_cell, source_cell, model_name_cell, model_version_cell, actions_cell]))

        return table_rows
    
    # --- Callback to populate the dataset dropdown on the Train tab --- #
    @app.callback(
        Output('train-dataset-dropdown', 'options'),
        Output('train-dataset-dropdown', 'value'),
        Input("tabs", "active_tab"),
        Input("list-store", "data"), # Trigger when project changes
        # Also consider Input("dataset-store", "data") if datasets can change while on train tab
        prevent_initial_call=True
    )
    def populate_train_dataset_dropdown(active_tab, list_store):
        if active_tab != 'tab-train' or not list_store:
            # Don't update if not on the right tab or store is empty
            return no_update, no_update

        project_id = list_store.get('active_project_id')
        if not project_id:
            # No project selected, clear dropdown
            return [], None 

        print(f"Populating dataset dropdown for project ID: {project_id}")
        df_datasets = get_datasets(project_id)
        options = []
        value = None
        if not df_datasets.empty:
            # Only include materialized datasets? Or all? Let's include all for now.
            # Might want to filter: df_datasets = df_datasets[df_datasets['materialized'] == True]
            options = [
                {'label': row['name'], 'value': row['id']}
                for index, row in df_datasets.iterrows()
            ]
            if options: # Set default value to the first dataset
                value = options[0]['value']
        
        return options, value
    # --- End Dropdown Population Callback --- #
    
    @app.callback(
        Output("train-status-output", "children", allow_duplicate=True), # Output for feedback
        Input({'type': 'register-model-button', 'index': ALL, 'datasetname': ALL, 'modelsource': ALL}, 'n_clicks'),
        State("list-store", "data"), # For catalog/schema
        prevent_initial_call=True
    )
    def handle_register_model_click(n_clicks_list, proj_store):
        if not ctx.triggered_id:
            # This condition might be met if n_clicks_list is all None or 0, prevent update.
            raise PreventUpdate

        # Check if the sum of n_clicks (for all buttons that could have triggered) is 0
        # This filters out initial calls or callbacks where no button was actually clicked
        # (e.g. if a button was dynamically removed and Dash still tried to process its callback)
        if not any(n_click for n_click in n_clicks_list if n_click is not None):
            raise PreventUpdate

        clicked_button_id_dict = ctx.triggered_id 
        
        run_id = clicked_button_id_dict.get('index') 
        dataset_name = clicked_button_id_dict.get('datasetname')
        model_source = clicked_button_id_dict.get('modelsource')

        if not dataset_name or not model_source or not run_id:
            return dbc.Alert("Error: Missing necessary data from the button (run_id, dataset name, or model source).", color="danger")

        active_project_id = proj_store.get("active_project_id")
        if not active_project_id:
            return dbc.Alert("Error: No active project selected.", color="danger")
        
        project_details = get_project_from_store(proj_store, active_project_id) 
        if not project_details:
            return dbc.Alert(f"Error: Could not retrieve details for project ID {active_project_id}.", color="danger")

        catalog = project_details.get('catalog')
        schema = project_details.get('schema')

        if not catalog or not schema:
            return dbc.Alert("Error: Project catalog or schema is not defined. Please set them in the Project tab.", color="danger")
        
        if dataset_name == "UnknownDataset" or dataset_name == "Dataset Not Found" or dataset_name == "DB Lookup Error" or dataset_name == "Invalid Job ID Tag" or dataset_name == "Job ID Tag Missing":
            return dbc.Alert(f"Error: Cannot register model. The dataset name associated with run ID '{run_id}' is '{dataset_name}'. Please ensure the run is correctly tagged with a job ID and the job ID is linked to a valid dataset.", color="danger")

        # Sanitize dataset_name by replacing spaces with underscores
        sanitized_dataset_name = dataset_name.replace(" ", "_")

        model_name_str = f"{catalog}.{schema}.{sanitized_dataset_name}"
        
        print(f"Attempting to register model: {model_name_str} from source: {model_source} for run: {run_id}")
        
        response_data, error_msg = register_model_version(model_name=model_name_str, model_source=model_source)

        if error_msg:
            return dbc.Alert(f"Error registering model '{model_name_str}': {error_msg}", color="danger")
        
        # Check if response_data itself is the model version information
        if response_data and response_data.get("version"): 
            version = response_data.get("version")
            status_message = response_data.get("status_message", "Status not available.")
            # mlflow_web_url = proj_store.get("mlflow_web_url") # Assuming you might store this
            # model_link = f"{mlflow_web_url}/#/models/{model_name_str}/versions/{version}" # Example link
            
            return dbc.Alert(f"Successfully registered model '{model_name_str}' as version {version}. Source: {model_source}. Status: {status_message}", color="success")
        else:
            # This case should ideally not be hit if error_msg is None and registration was successful
            return dbc.Alert(f"Model registration for '{model_name_str}' returned an unexpected response format. API Response: {response_data}", color="warning")
    
    # --- Callback to update notebook dropdown in Project Tab --- #
    @app.callback(
        Output("project-notebook-dropdown", "options"),
        Input("project-git-url", "value"),
        prevent_initial_call=True
    )
    def update_notebook_dropdown_options(git_url):
        if not git_url:
            return [] # Return empty options, value will be handled by populate_form or remain as is
        
        notebook_options = fetch_notebook_files_from_github(git_url, folder_path="notebooks")
        
        if not notebook_options:
            return [{"label": "No files found in 'notebooks' folder or error", "value": "", "disabled": True}]

        return notebook_options
    # --- End Notebook Dropdown Callback --- #
    