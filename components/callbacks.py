from dash import Input, Output, State, ALL, ctx, no_update
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from utils.db import create_project, update_project, get_projects, get_datasets, create_dataset, update_dataset, delete_project

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
        project = get_project_from_store(store_data, store_data.get('active_project_id'))
        #project = store_data.get('items', [])[idx]
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
                        proj_store, ds_store, ds_active, ds_ids,
                        name, source_type, eol_def, feat_lookup_def,
                        source_table, eval_type, percentage,
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
            and active_tab == "tab-display"):
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
                None,
                None,
                None,
                "random",
                None,
                None,
                None,
                False,
                None,
                None
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
        Output("dataset-evaluation-type", "value"),
        Output("dataset-percentage", "value"),
        Output("dataset-eval-table-name", "value"),
        Output("dataset-split-time-column", "value"),
        Output("dataset-training-table-name", "value"),
        Output("dataset-eval-table-generated", "value"),
        Output("dataset-materialized", "value"),
        Input({"type": "dataset-group-item", "index": ALL}, "active"),
        Input("create-dataset-button", "n_clicks"),
        State("dataset-store", "data"),
        prevent_initial_call=True
    )
    def manage_dataset_form(active_states, create_clicks, ds_store):
        # Determine trigger: either a dataset item was selected or 'Create' was clicked
        trig = ctx.triggered_id
        # Create: clear form to defaults
        if trig == "create-dataset-button":
            return (
                "",              # name
                "static_table",  # source type
                "",              # eol definition
                "",              # feature lookup definition
                "",              # source table
                "random",        # evaluation type
                None,             # percentage
                "",              # eval table name
                "",              # split time column
                "",              # training table name
                "",              # eval table generated
                []                # materialized
            )
        # Selection: populate from store
        if isinstance(trig, dict) and trig.get('type') == 'dataset-group-item':
            if not ds_store or not active_states:
                raise PreventUpdate
            try:
                idx = active_states.index(True)
            except ValueError:
                raise PreventUpdate
            ds = ds_store.get('items', [])[idx]
            # Checklist expects list of values
            mat = [True] if ds.get('materialized') else []
            # EOL/feature strings
            eol_str = ', '.join(ds.get('eol_definition') or [])
            feat_str = ', '.join(ds.get('feature_lookup_definition') or [])
            return (
                ds.get('text', ''),
                ds.get('source_type', ''),
                eol_str,
                feat_str,
                ds.get('source_table', ''),
                ds.get('evaluation_type', ''),
                ds.get('percentage', None),
                ds.get('eval_table_name', ''),
                ds.get('split_time_column', ''),
                ds.get('training_table_name', ''),
                ds.get('eval_table_name_generated', ''),
                mat
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
    