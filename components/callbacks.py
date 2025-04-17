from dash import Input, Output, State, ALL, ctx, no_update
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from utils.db import create_project, update_project, get_datasets, create_dataset, update_dataset

def register_callbacks(app):
    @app.callback(
        Output({"type": "list-group-item", "index": ALL}, "active"),
        Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks'),
        State({"type": "list-group-item", "index": ALL}, "active"),
        State({"type": "list-group-item", "index": ALL}, "id"),
        prevent_initial_call=True
    )
    def update_active_states(_, active_states, ids):
        # Create a new list of active states where only the clicked item is active
        new_active_states = [False] * len(active_states)
        # Determine which item was clicked via its id mapping
        triggered = ctx.triggered_id
        # Extract the 'index' value from the triggered id
        if isinstance(triggered, dict):
            clicked_value = triggered.get('index')
        else:
            clicked_value = getattr(triggered, 'index', None)
        # Build a list of index values for all items (in order)
        id_list = []
        for id_obj in ids:
            if isinstance(id_obj, dict):
                id_list.append(id_obj.get('index'))
            else:
                id_list.append(getattr(id_obj, 'index', None))
        # Find the position of the clicked item in the list
        try:
            pos = id_list.index(clicked_value)
        except (ValueError, TypeError):
            # If not found, do not change active states
            return active_states
        # Activate only the clicked item
        new_active_states[pos] = True
        return new_active_states


    @app.callback(
        [Output("list-group", "children"),
         Output("list-store", "data")],
        Input("create-project-button", "n_clicks"),
        Input("update-project-button", "n_clicks"),
        State("project-name", "value"),
        State("project-description", "value"),
        State("project-catalog", "value"),
        State("project-schema", "value"),
        State("list-store", "data"),
        State({"type": "list-group-item", "index": ALL}, "active"),
        State({"type": "list-group-item", "index": ALL}, "id"),
        prevent_initial_call=True
    )
    def create_or_update_project(create_clicks, update_clicks,
                                 name, description, catalog, schema,
                                 store_data, active_states, ids):
        triggered_id = ctx.triggered_id
        # CREATE
        if triggered_id == "create-project-button":
            if not create_clicks:
                return no_update, no_update
            project_id = create_project(name, description, catalog, schema)
            if project_id is None:
                return no_update, no_update
            new_items = store_data.get('items', []) + [{
                'id': project_id,
                'text': name,
                'description': description,
                'catalog': catalog,
                'schema': schema
            }]
            active_id = project_id
        # UPDATE
        elif triggered_id == "update-project-button":
            if not update_clicks:
                return no_update, no_update
            try:
                idx = active_states.index(True)
            except (ValueError, TypeError):
                return no_update, no_update
            project_item = store_data.get('items', [])[idx]
            project_id = project_item.get('id')
            updated = update_project(project_id, name, description, catalog, schema)
            if updated is None:
                return no_update, no_update
            new_items = store_data.get('items', []).copy()
            new_items[idx] = {
                'id': project_id,
                'text': name,
                'description': description,
                'catalog': catalog,
                'schema': schema
            }
            active_id = project_id
        else:
            return no_update, no_update
        # Build outputs
        new_store_data = {'items': new_items}
        new_list_items = [
            dbc.ListGroupItem(
                item['text'],
                id={"type": "list-group-item", "index": item['id']},
                action=True,
                active=(item['id'] == active_id)
            )
            for item in new_items
        ]
        return new_list_items, new_store_data
    
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
        if not store_data or not active_states:
            raise PreventUpdate
        try:
            idx = active_states.index(True)
        except ValueError:
            raise PreventUpdate
        project = store_data.get('items', [])[idx]
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
        State("dataset-name", "value"),
        State("dataset-source-type", "value"),
        State("dataset-evaluation-type", "value"),
        State("dataset-materialized", "value"),
        State("dataset-target", "value"),
        prevent_initial_call=True
    )
    def manage_datasets(active_tab, proj_active, create_ds, update_ds,
                        proj_store, ds_store, ds_active, ds_ids,
                        name, source_type, eval_type, materialized, target):
        trigger = ctx.triggered_id
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
                        "evaluation_type": rec.get("evaluation_type"),
                        "materialized": rec.get("materialized"),
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
        # Create a new dataset
        if trigger == "create-dataset-button" and create_ds:
            try:
                pidx = proj_active.index(True)
            except (ValueError, TypeError):
                return no_update, no_update
            proj = proj_store.get("items", [])[pidx]
            pid = proj.get("id")
            dsid = create_dataset(pid, name, source_type, eval_type, bool(materialized), target)
            if dsid is None:
                return no_update, no_update
            new_items = ds_store.get("items", []) + [{
                "id": dsid,
                "text": name,
                "source_type": source_type,
                "evaluation_type": eval_type,
                "materialized": bool(materialized),
                "target": target
            }]
            list_items = [
                dbc.ListGroupItem(
                    itm["text"],
                    id={"type": "dataset-group-item", "index": itm["id"]},
                    action=True,
                    active=(itm["id"] == dsid)
                ) for itm in new_items
            ]
            return list_items, {"items": new_items}
        # Update existing dataset
        if trigger == "update-dataset-button" and update_ds:
            try:
                didx = ds_active.index(True)
            except (ValueError, TypeError):
                return no_update, no_update
            ds_it = ds_store.get("items", [])[didx]
            dsid = ds_it.get("id")
            upd = update_dataset(dsid, name, source_type, eval_type, bool(materialized), target)
            if upd is None:
                return no_update, no_update
            new_items = ds_store.get("items", []).copy()
            new_items[didx] = {
                "id": dsid,
                "text": name,
                "source_type": source_type,
                "evaluation_type": eval_type,
                "materialized": bool(materialized),
                "target": target
            }
            list_items = [
                dbc.ListGroupItem(
                    itm["text"],
                    id={"type": "dataset-group-item", "index": itm["id"]},
                    action=True,
                    active=(itm["id"] == dsid)
                ) for itm in new_items
            ]
            return list_items, {"items": new_items}
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

    # -- Dataset form population callback --------------------------------------
    @app.callback(
        Output("dataset-name", "value"),
        Output("dataset-source-type", "value"),
        Output("dataset-evaluation-type", "value"),
        Output("dataset-materialized", "value"),
        Output("dataset-target", "value"),
        Input({"type": "dataset-group-item", "index": ALL}, "active"),
        State("dataset-store", "data"),
        prevent_initial_call=True
    )
    def populate_dataset_form(active_states, ds_store):
        # Populate form inputs for selected dataset
        if not ds_store or not active_states:
            raise PreventUpdate
        try:
            idx = active_states.index(True)
        except ValueError:
            raise PreventUpdate
        ds = ds_store.get('items', [])[idx]
        # Checklist expects list of values
        mat = [True] if ds.get('materialized') else []
        return (
            ds.get('text', ''),
            ds.get('source_type', ''),
            ds.get('evaluation_type', ''),
            mat,
            ds.get('target', '')
        )