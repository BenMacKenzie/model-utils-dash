import dash_bootstrap_components as dbc
from dash import html, dcc

def create_display_tab():
    # Store for datasets of the selected project
    store = dcc.Store(id='dataset-store', data={'items': []})

    # List of datasets
    list_items = [
        dbc.ListGroupItem(
            "No datasets found.",
            id={"type": "dataset-group-item", "index": -1},
            disabled=True
        )
    ]
    listgroup = dbc.ListGroup(list_items, id="dataset-list-group", className="mb-4")

    # Form to create or update a dataset
    form = dbc.Form([
        dbc.Label("Name", html_for="dataset-name"),
        dbc.Input(type="text", id="dataset-name", placeholder="Enter dataset name"),
        html.Div(
            [dbc.Label("Source Type", html_for="dataset-source-type"),
             dbc.Input(type="text", id="dataset-source-type", placeholder="e.g. static_table")],
            className="mb-3"
        ),
        html.Div(
            [dbc.Label("Evaluation Type", html_for="dataset-evaluation-type"),
             dbc.Input(type="text", id="dataset-evaluation-type", placeholder="e.g. random")],
            className="mb-3"
        ),
        html.Div(
            [dbc.Label("Materialized", html_for="dataset-materialized"),
             dcc.Checklist(
                 options=[{"label": "", "value": True}],
                 value=[],
                 id="dataset-materialized",
                 inline=True
             )],
            className="mb-3"
        ),
        html.Div(
            [dbc.Label("Target", html_for="dataset-target"),
             dbc.Input(type="text", id="dataset-target", placeholder="Enter target field")],
            className="mb-3"
        ),
        html.Div([
            dbc.Button("Create Dataset", id="create-dataset-button", color="success", className="me-2"),
            dbc.Button("Update Dataset", id="update-dataset-button", color="primary")
        ], className="mt-3")
    ])

    # Layout: dataset list and form side by side
    content = [
        store,
        dbc.Row([
            dbc.Col(listgroup, width=6),
            dbc.Col(form, width=6)
        ])
    ]
    return dbc.Tab(content, label="Datasets", tab_id="tab-display")