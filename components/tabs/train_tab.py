import dash_bootstrap_components as dbc
from dash import dcc, html

def create_train_tab():
    return dbc.Tab(
        label="Train Model",
        id="tab-train",
        tab_id="tab-train",
        children=[
            dbc.Row([
                dbc.Col([
                    html.H5("Selected Dataset:"),
                    html.Div(id="train-selected-dataset-name", children="None Selected"),
                    html.Div(id="train-job-id-display", children="Job ID: Not Created Yet", style={'font-style': 'italic', 'margin-bottom': '1rem'}),
                    html.Hr(),
                    html.H5("Select Dataset for Training:"),
                    dcc.Dropdown(
                        id='train-dataset-dropdown',
                        placeholder="Select a dataset...",
                        style={'margin-bottom': '1rem'}
                    ),
                    html.H5("Training Parameters (JSON):"),
                    dbc.Textarea(
                        id="train-parameters-input",
                        placeholder='{\n  "learning_rate": 0.01,\n  "epochs": 10\n}',
                        style={'height': '150px'}
                    ),
                    html.Br(),
                    dbc.Button("Train / Run Job", id="train-run-button", color="primary", n_clicks=0),
                    html.Br(),
                    html.Br(),
                    dcc.Loading(
                        id="loading-train-output",
                        type="default",
                        children=html.Div(id="train-status-output")
                    ),
                    html.Hr(),
                    html.H5("MLflow Runs"),
                    dcc.Loading(
                        id="loading-mlflow-runs",
                        type="default",
                        children=dbc.Table([
                            html.Thead(html.Tr([
                                html.Th("Dataset"),
                                html.Th("Metrics"),
                                html.Th("Run ID"),
                                html.Th("Source")
                            ])),
                            html.Tbody(id="train-mlflow-runs-list", children=[
                                html.Tr(html.Td(children=["Select a project to see runs."], colSpan=4))
                            ])
                        ], bordered=True, hover=True, responsive=True, striped=True)
                    )
                ], width=12)
            ], className="p-3")
        ]
    )