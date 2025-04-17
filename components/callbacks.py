from dash import Input, Output, State, ALL, ctx
import dash_bootstrap_components as dbc

def register_callbacks(app):
    @app.callback(
        Output({"type": "list-group-item", "index": ALL}, "active"),
        Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks'),
        State({"type": "list-group-item", "index": ALL}, "active"),
        prevent_initial_call=True
    )
    def update_active_states(_, active_states):    
        # Create a new list of active states where only the clicked item is active
        new_active_states = [False] * len(active_states)
        new_active_states[ctx.triggered_id.index] = True
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
        Input("add-button", "n_clicks"),
        State("list-store", "data"),
        prevent_initial_call=True
    )
    def add_item(n_clicks, store_data):
        if n_clicks is None:
            return dash.no_update
        
        # Get the current number of items
        current_items = store_data['items']
        new_index = len(current_items)
        
        # Create new item
        new_item = {
            'id': new_index,
            'text': f'Item {new_index}'
        }
        
        # Update store data
        new_store_data = {
            'items': current_items + [new_item]
        }
        
        # Create new list group items
        new_list_items = [
            dbc.ListGroupItem(
                item['text'],
                id={"type": "list-group-item", "index": item['id']},
                action=True,
                active=item['id'] == new_index  # Make the new item active
            )
            for item in new_store_data['items']
        ]
        
        return new_list_items, new_store_data 