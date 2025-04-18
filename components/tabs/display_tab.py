import dash_bootstrap_components as dbc
from dash import html, dcc

def create_display_tab():
    # Store for datasets of the selected project
    store = dcc.Store(id='dataset-store', data={'items': []}, storage_type='session')

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
        # Dataset Name
        dbc.Label("Name", html_for="dataset-name"),
        dbc.Input(type="text", id="dataset-name", placeholder="Enter dataset name"),

        # Source Type selection
        html.Div([
            dbc.Label("Source Type", html_for="dataset-source-type"),
            dbc.RadioItems(
                options=[
                    {"label": "Static Table", "value": "static_table"},
                    {"label": "Dynamic Table", "value": "dynamic_table"},
                    {"label": "Feature Lookup", "value": "feature_lookup"}
                ],
                value="static_table",
                id="dataset-source-type",
                inline=True
            )
        ], className="mb-3"),

        # eol_definition (only for feature_lookup)
        html.Div([
            dbc.Label("EOL Definition", html_for="dataset-eol-definition"),
            dbc.Textarea(id="dataset-eol-definition", placeholder="Enter eol definition (comma-separated)"),
        ], id="div-eol-definition", className="mb-3", style={"display": "none"}),

        # feature_lookup_definition (only for feature_lookup)
        html.Div([
            dbc.Label("Feature Lookup Definition", html_for="dataset-feature-lookup-definition"),
            dbc.Textarea(id="dataset-feature-lookup-definition", placeholder="Enter feature lookup definition (comma-separated)"),
        ], id="div-feature-lookup-definition", className="mb-3", style={"display": "none"}),

        # source_table (for static/dynamic)
        html.Div([
            dbc.Label("Source Table", html_for="dataset-source-table"),
            dbc.Textarea(id="dataset-source-table", placeholder="Enter source table(s) (comma-separated)"),
        ], id="div-source-table", className="mb-3"),

        # Evaluation Type selection
        html.Div([
            dbc.Label("Evaluation Type", html_for="dataset-evaluation-type"),
            dbc.RadioItems(
                options=[
                    {"label": "Random", "value": "random"},
                    {"label": "Table", "value": "table"},
                    {"label": "Timestamp", "value": "timestamp"}
                ],
                value="random",
                id="dataset-evaluation-type",
                inline=True
            )
        ], className="mb-3"),

        # Percentage (only for random)
        html.Div([
            dbc.Label("Percentage", html_for="dataset-percentage"),
            dbc.Input(type="number", id="dataset-percentage", min=0, max=1, step=0.01, placeholder="0-1"),
        ], id="div-percentage", className="mb-3"),

        # Eval Table Name (only for table)
        html.Div([
            dbc.Label("Eval Table Name", html_for="dataset-eval-table-name"),
            dbc.Input(type="text", id="dataset-eval-table-name", placeholder="Enter eval table name"),
        ], id="div-eval-table-name", className="mb-3", style={"display": "none"}),

        # Split Time Column (only for timestamp)
        html.Div([
            dbc.Label("Split Time Column", html_for="dataset-split-time-column"),
            dbc.Input(type="text", id="dataset-split-time-column", placeholder="Enter split time column"),
        ], id="div-split-time-column", className="mb-3", style={"display": "none"}),

        # Training Table Name (auto-generated from dataset name)
        html.Div([
            dbc.Label("Training Table Name (Generated)", html_for="dataset-training-table-name"),
            dbc.Input(type="text", id="dataset-training-table-name", readonly=True),
        ], className="mb-3"),

        # Eval Table Name Generated (always displayed, read-only)
        html.Div([
            dbc.Label("Eval Table Name (Generated)"),
            dbc.Input(type="text", id="dataset-eval-table-generated", readonly=True)
        ], className="mb-3"),

        # Materialized toggle
        html.Div([
            dbc.Label("Materialized", html_for="dataset-materialized"),
            dcc.Checklist(
                options=[{"label": "", "value": True}],
                value=[],
                id="dataset-materialized",
                inline=True
            )
        ], className="mb-3"),

        # Action buttons
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