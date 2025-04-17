from dash import Input, Output, State, ALL, ctx, no_update
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from utils.db import create_project, update_project

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
        Output("test", "children"),
        [Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks'),
         Input("tabs", "active_tab"),
         Input("list-store", "data")],
        State({"type": "list-group-item", "index": ALL}, "active"),
        prevent_initial_call=True
    )
    def update_display(_, active_tab, store_data, active_states):    
        if ctx.triggered_id == "tabs" and active_tab == "tab-display":
            # Find the active item in the list
            active_index = active_states.index(True)
            return store_data['items'][active_index]['text']
        return f"Clicked on Item {ctx.triggered_id.index}"

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