import dash_bootstrap_components as dbc
from dash import Dash, html
from components.tabs.list_tab import create_list_tab
from components.tabs.display_tab import create_display_tab
from components.callbacks import register_callbacks

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Create tabs
list_tab, store = create_list_tab()
display_tab = create_display_tab()

app.layout = dbc.Container([
    store,
    dbc.Tabs([
        list_tab,
        display_tab
    ],
    id="tabs",
    active_tab="tab-list"
    )
])

# Register callbacks
register_callbacks(app)

if __name__ == "__main__":
    app.run(debug=True)