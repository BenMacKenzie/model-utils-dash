import dash_bootstrap_components as dbc
from dash import html

def create_display_tab():
    return dbc.Tab(
        html.Div(id="test"),
        label="Display",
        tab_id="tab-display"
    ) 