from dash import Input, Output, State, ALL, ctx, no_update
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import time # Added for timestamp generation
from utils.db import create_project, update_project, get_projects, get_datasets, create_dataset, update_dataset, delete_project, delete_dataset
from utils.databricks import get_databricks_connection, execute_sql # Import databricks utils

def register_callbacks(app):

    @app.callback(
        Output('list-store', 'data', allow_duplicate=True),
        Input('url', 'pathname'),  # Trigger on page load
        prevent_initial_call=True
    )
    def update_store_on_refresh(_):
        print("update_store_on_refresh")
        df = get_projects()
        # Convert to items for storage and display
        
        items = []
        if not df.empty:
            records = df.to_dict(orient='records')
            # Build list of projects with full details
            items = [
                {
                    'id': int(rec['id']),
                    'text': rec.get('name'),
                    'description': rec.get('description'),
                    'catalog': rec.get('catalog'),
                    'schema': rec.get('schema'),
                }
                for rec in records
            ]
        active_project_id = items[0]['id'] if items else None# Store component to maintain the list of projects
        print("items", items)
        return {'items': items, "active_project_id": active_project_id  }

    @app.callback(
        Output("list-group", "children", allow_duplicate=True),
        Input("list-store", "data"),
        prevent_initial_call=True
    )
    def refresh_project_list(store_data):
        print("refresh_project_list")
      
        new_items = (store_data.get('items', []) or []) 
        active_project_id = store_data.get('active_project_id', None)
        list_items = [
            dbc.ListGroupItem(
                itm['text'],
                id={"type": "list-group-item", "index": itm['id']},
                action=True,
                active=(itm['id'] == active_project_id)
            ) for itm in new_items
        ]
       
        return list_items


    @app.callback(
        Output("list-store", "data"),
        Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks'),
        State("list-store", "data"),
        prevent_initial_call=True
    )
    def select_project_callback(_, store_data):
        project_id = ctx.triggered_id["index"]
        items = (store_data.get('items', []) or []) 
        return {'items': items, "active_project_id": project_id}
     
   


    @app.callback(
        Output("list-store", "data", allow_duplicate=True),
        Input("create-project-button", "n_clicks"),
        State("list-store", "data"),
        prevent_initial_call=True
    )
    def create_project_callback(create_clicks, store_data):
        print("create_project_callback")
        project_id = create_project("New", "", "", "")
        if project_id is None:
            return no_update, no_update
        # Append stub
        new_items = (store_data.get('items', []) or []) + [{
            'id': project_id,
            'text': 'New',
            'description': '',
            'catalog': '',
            'schema': ''
        }]
        print("New items after project creation:", new_items)  # Debug print
     
       
        return  {'items': new_items, "active_project_id": project_id}
    
    @app.callback(
        Output("list-store", "data", allow_duplicate=True),
        Input("update-project-button", "n_clicks"),
        State("project-name", "value"),
        State("project-description", "value"),
        State("project-catalog", "value"),
        State("project-schema", "value"),
        State("list-store", "data"),
        State({"type": "list-group-item", "index": ALL}, "active"),
        prevent_initial_call=True
    )
    def update_project_callback(update_clicks, name, description, catalog, schema,
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
        updated = update_project(project_id, name, description, catalog, schema)
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
                    'schema': rec.get('schema')
                })
       
        return {'items': items, "active_project_id": project_id}
    
    def get_project_from_store(store_data, project_id):
        """
        Get a project record from the store data by its ID.
        Returns the project dictionary if found, None otherwise.
        """
        items = store_data.get('items', [])
        for item in items:
            if item['id'] == project_id:
                return item
        return None

    @app.callback(
        Output("project-name", "value"),
        Output("project-description", "value"),
        Output("project-catalog", "value"),
        Output("project-schema", "value"),
        Input({"type": "list-group-item", "index": ALL}, "active"),
        State("list-store", "data"),
        prevent_initial_call=True
    )
    def populate_form(active_states, store_data):
        # Populate the form inputs based on the selected project
        print("active_states", active_states)
        if not store_data or not active_states:
            raise PreventUpdate
        try:
            idx = active_states.index(True)
        except ValueError:
            raise PreventUpdate
        # Use the helper function to get the project based on active_project_id
        project = get_project_from_store(store_data, store_data.get('active_project_id'))
        if not project:
            print("No active project found in store")
            raise PreventUpdate
        
        return (
            project.get('text', ''),
            project.get('description', ''),
            project.get('catalog', ''),
            project.get('schema', '')
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
        State("dataset-eval-table-name", "value"),
        State("dataset-split-time-column", "value"),
        State("dataset-training-table-name", "value"),
        State("dataset-eval-table-generated", "value"),
        State("dataset-materialized", "value"),
        prevent_initial_call=True
    )
    def manage_datasets(active_tab, proj_active, create_ds, update_ds,
                        delete_ds,
                        proj_store, ds_store, ds_active, ds_ids,
                        name, source_type, eol_def, feat_lookup_def,
                        source_table, timestamp_col,
                        eval_type, percentage,
                        eval_table_name, split_time_column,
                        training_table_name, eval_table_generated,
                        materialized):
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
                    "eval_table_name": rec.get("eval_table_name"),
                    "split_time_column": rec.get("split_time_column"),
                    "materialized": rec.get("materialized"),
                    "training_table_name": rec.get("training_table_name"),
                    "eval_table_name_generated": rec.get("eval_table_name_generated")
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
                None, # eval_table_name
                None, # split_time_column
                None, # timestamp_col (Correct position)
                False, # materialized (Correct position)
                None, # training_table_name
                None  # eval_table_name_generated
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
                "eval_table_name": "",
                "split_time_column": "",
                "materialized": False,
                "training_table_name": "",
                "eval_table_name_generated": ""
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
                eval_table_name,
                split_time_column,
                timestamp_col,
                bool(materialized),
                training_table_name,
                eval_table_generated
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
                        "eval_table_name": rec.get("eval_table_name"),
                        "split_time_column": rec.get("split_time_column"),
                        "materialized": rec.get("materialized"),
                        "training_table_name": rec.get("training_table_name"),
                        "eval_table_name_generated": rec.get("eval_table_name_generated")
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
                        "eval_table_name": rec.get("eval_table_name"),
                        "split_time_column": rec.get("split_time_column"),
                        "materialized": rec.get("materialized"),
                        "training_table_name": rec.get("training_table_name"),
                        "eval_table_name_generated": rec.get("eval_table_name_generated")
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
        Output("dataset-eval-table-name", "value"),
        Output("dataset-split-time-column", "value"),
        Output("dataset-training-table-name", "value"),
        Output("dataset-eval-table-generated", "value"),
        Output("dataset-materialized", "value"),
        Output("materialize-dataset-button", "disabled"),
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
                "",              # eval table name
                "",              # split time column
                "",              # training table name
                "",              # eval table generated
                [],                # materialized
                False             # materialize button disabled
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
                ds.get('eval_table_name', ''),
                ds.get('split_time_column', ''),
                ds.get('training_table_name', ''),
                ds.get('eval_table_name_generated', ''),
                mat_checklist_value,
                materialized_status  # Set button disabled state
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
                    'schema': rec.get('schema')
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
        Output("dataset-eval-table-generated", "value", allow_duplicate=True),
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
        State("dataset-eval-table-name", "value"),
        State("dataset-split-time-column", "value"),
        State("dataset-timestamp-col", "value"),
        # Get dataset ID directly (needed for update_dataset)
        State({"type": "dataset-group-item", "index": ALL}, "id"),
        prevent_initial_call=True
    )
    def materialize_dataset_callback(n_clicks, list_store, ds_store, proj_active, ds_active,
                                   source_table, source_type, eval_type, percentage,
                                   eval_table_input, split_time_input, timestamp_col,
                                   ds_ids):
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
        eval_table_name_generated = f"{target_base}_eval_{timestamp_suffix}"

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
        print(f"Generated eval table: {eval_table_name_generated}")

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
                sql_eval = f"CREATE TABLE {eval_table_name_generated} AS SELECT * EXCEPT (__rand_split) FROM {temp_table_name} WHERE __rand_split < {percentage}"
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
                if not eval_table_input:
                    print("Missing evaluation table name for 'table' split type.")
                    # TODO: User feedback
                    raise PreventUpdate
                # Assume eval_table_input is qualified or in the same catalog/schema context
                # Training table is the full source
                sql_train = f"CREATE TABLE {training_table_name} AS SELECT * FROM {qualified_source_table}"
                # Eval table is a copy of the user-provided table
                sql_eval = f"CREATE TABLE {eval_table_name_generated} AS SELECT * FROM {eval_table_input}"
                
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
                sql_eval = f"CREATE TABLE {eval_table_name_generated} AS SELECT * FROM {qualified_source_table} WHERE {timestamp_col} >= '{split_time_input}'"
                
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
            # execute_sql(conn_db, f"DROP TABLE IF EXISTS {eval_table_name_generated}")

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
            eval_table_name=eval_table_input,
            split_time_column=split_time_input,
            timestamp_col=timestamp_col,
            materialized=True,
            training_table_name=training_table_name,
            eval_table_name_generated=eval_table_name_generated
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
                item['eval_table_name_generated'] = eval_table_name_generated
            updated_items.append(item)

        # Update form fields and checklist
        new_materialized_value = [True] # Checklist expects a list

        return ({'items': updated_items}, training_table_name, eval_table_name_generated, new_materialized_value)
    