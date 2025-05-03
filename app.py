import dash_bootstrap_components as dbc
from dash import Dash, html
from dotenv import load_dotenv

load_dotenv()

from components.tabs.project_tab import create_project_tab
from components.tabs.dataset_tab import create_dataset_tab
from components.tabs.train_tab import create_train_tab
from components.callbacks import register_callbacks
from dash import dcc
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])



# Create tabs
project_tab, store = create_project_tab()
dataset_tab = create_dataset_tab()
train_tab = create_train_tab()

app.layout = dbc.Container([
    dcc.Location(id='url'),
    store,
    dbc.Tabs([
        project_tab,
        dataset_tab,
        train_tab
    ],
    id="tabs",
    active_tab="tab-project"
    )
])

# Register callbacks
register_callbacks(app)

if __name__ == "__main__":
    app.run(debug=True)