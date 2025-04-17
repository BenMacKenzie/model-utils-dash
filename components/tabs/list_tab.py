import dash_bootstrap_components as dbc
from dash import html, dcc

def create_list_tab():
    # Store component to maintain the list of items
    store = dcc.Store(id='list-store', data={'items': [{'id': i, 'text': f'Item {i}'} for i in range(3)]})

    # Initial list group
    listgroup = dbc.ListGroup(
        [
            dbc.ListGroupItem(
                f"Item {i}",
                id={"type": "list-group-item", "index": i},
                action=True,
                active=i==0
            )
            for i in range(3)
        ],
        id="list-group",
    )

    return dbc.Tab([
        dbc.Row([
            dbc.Col(listgroup, width=8),
            dbc.Col([
                dbc.Button("Add Item", id="add-button", color="primary", className="mb-3")
            ], width=4)
        ])
    ], label="List Items", tab_id="tab-list"), store 