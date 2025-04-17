# app.py
import dash
from dash import dcc, html, dash_table, callback, ALL, Output, Input, State
import dash_bootstrap_components as dbc
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

# Load database credentials from .env file
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Database connection function
def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    conn = None # Initialize conn
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        if conn:
            conn.close() # Ensure connection is closed on error
        return None

# Function to fetch data
def fetch_data(query, params=None):
    """Fetches data from the database using the provided query."""
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            return df
        except Exception as e:
            print(f"Error fetching data: {e}")
            conn.close()
            return pd.DataFrame() # Return empty DataFrame on error
    else:
        return pd.DataFrame() # Return empty DataFrame if connection failed

# Function to execute insert/update/delete
def execute_query(query, params=None):
    """Executes an INSERT, UPDATE, or DELETE query."""
    conn = get_db_connection()
    success = False
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
            conn.commit()
            success = True
        except Exception as e:
            print(f"Error executing query: {e}")
            conn.rollback() # Rollback changes on error
        finally:
            conn.close()
    return success

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
app.title = "Model Utils Dashboard"

# MODIFIED: Main application layout - Now includes all stores/states and input fields
app.layout = dbc.Container([
    html.H1("Model Utils Dashboard", className="my-4 text-center"),
    dcc.Tabs(id="main-tabs", value='tab-projects', children=[
        dcc.Tab(label='Projects', value='tab-projects'),
        dcc.Tab(label='Datasets', value='tab-datasets'),
    ]),
    dcc.Location(id='url', refresh=False),
    # Add all Stores upfront in the main layout
    dcc.Store(id='data-updated-signal'),
    # Store for project edit state used by the form and callbacks
    dcc.Store(id='project-edit-state', data={'mode': 'new', 'id': None}),
    # Hidden fields for form state (to fix the callback issues)
    html.Div(
        [
            html.Div(id="project-form-title", style={"display": "none"}),
            html.Div(id="project-name-input", style={"display": "none"}),
            html.Div(id="project-desc-input", style={"display": "none"}),
            html.Div(id="project-catalog-input", style={"display": "none"}),
            html.Div(id="project-schema-input", style={"display": "none"}),
            html.Div(id="project-form-feedback", style={"display": "none"}),
        ],
        style={"display": "none"},
        id="hidden-form-fields"
    ),
    # Main content area that will be filled by the callback
    dbc.Spinner(html.Div(id='tab-content', className="mt-4"))
], fluid=True)

# Define the Projects tab layout structure (used in the callback)
def build_projects_layout(projects_df, initial_project=None):
    """Builds the layout STRUCTURE for the Projects tab with optional initial selection."""
    # Build project list items
    project_list_items = []
    if not projects_df.empty:
        project_list_items = [
            dbc.ListGroupItem(
                html.Span(f"{row['name']}", title=f"ID: {row['id']}"),
                id={'type': 'project-item', 'index': row['id']},
                n_clicks=0,
                action=True,
                # Mark active if this is the initially selected project
                active=initial_project is not None and initial_project['id'] == row['id']
            )
            for index, row in projects_df.iterrows()
        ]
    else:
         project_list_items = [dbc.ListGroupItem("No projects found.")]

    # Get initial form values based on initial_project
    initial_title = "Create New Project"
    initial_name = ""
    initial_desc = ""
    initial_catalog = ""
    initial_schema = ""
    
    if initial_project:
        initial_title = f"Edit Project (ID: {initial_project['id']})"
        initial_name = initial_project.get('name', '')
        initial_desc = initial_project.get('description', '')
        initial_catalog = initial_project.get('catalog', '')
        initial_schema = initial_project.get('schema', '')

    # Create the tab layout
    layout = dbc.Row([
        # Column 1: Project List and Create Button
        dbc.Col([
            html.H3("Projects"),
            dbc.ListGroup(project_list_items, id="project-list"),
            html.Br(),
            dbc.Button("Create New Project", id="create-project-btn", color="primary", className="mt-2", n_clicks=0)
        ], md=4),

        # Column 2: Project Form - Include form fields with initial values
        dbc.Col([
            html.H3(initial_title, id="project-form-title-display"),
            dbc.Form([
                dbc.Row([
                    dbc.Label("Name", html_for="project-name-input-field", width=2),
                    dbc.Col(dbc.Input(type="text", id="project-name-input-field", value=initial_name), width=10),
                ], className="mb-3"),
                 dbc.Row([
                    dbc.Label("Description", html_for="project-desc-input-field", width=2),
                    dbc.Col(dbc.Textarea(id="project-desc-input-field", rows=3, value=initial_desc), width=10),
                 ], className="mb-3"),
                 dbc.Row([
                    dbc.Label("Catalog", html_for="project-catalog-input-field", width=2),
                    dbc.Col(dbc.Input(type="text", id="project-catalog-input-field", value=initial_catalog), width=10),
                 ], className="mb-3"),
                 dbc.Row([
                    dbc.Label("Schema", html_for="project-schema-input-field", width=2),
                    dbc.Col(dbc.Input(type="text", id="project-schema-input-field", value=initial_schema), width=10),
                 ], className="mb-3"),
                 dbc.Button("Save Project", id="save-project-btn", color="success", n_clicks=0),
                 html.Div(id="project-form-feedback-display", className="mt-3")
            ])
        ], md=8)
    ])
    return layout

# ===== Callbacks =====

# Split the callbacks into two parts:
# 1. First callback to just handle which tab content to show
@callback(
    Output('tab-content', 'children'),
    Input('main-tabs', 'value'),
    Input('data-updated-signal', 'data'),
)
def render_content(tab, data_signal):
    ctx = dash.callback_context
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else 'No input'

    if tab == 'tab-projects':
        # Fetch projects
        projects_df = fetch_data("SELECT id, name FROM projects ORDER BY name ASC;")
        
        # Fetch details for the first project if available
        initial_project = None
        if not projects_df.empty:
            first_project_id = projects_df.iloc[0]['id']
            first_project_data_df = fetch_data(
                "SELECT id, name, description, catalog, schema FROM projects WHERE id = %s", 
                (first_project_id,)
            )
            if not first_project_data_df.empty:
                initial_project = first_project_data_df.iloc[0].to_dict()
        
        # Return the layout with initial project (if any)
        return build_projects_layout(projects_df, initial_project)

    elif tab == 'tab-datasets':
        # Fetch datasets data
        datasets_df = fetch_data("""
            SELECT d.id, p.name as project_name, d.name, d.source_type, d.evaluation_type, d.materialized, d.target, d.created_at
            FROM datasets d
            JOIN projects p ON d.project_id = p.id
            ORDER BY d.created_at DESC;
        """)
        
        # Build dataset layout
        if datasets_df.empty:
             datasets_layout = html.Div([
                html.H3("Datasets"),
                dbc.Alert("Could not load dataset data or no datasets found.", color="warning")
            ])
        else:
            datasets_layout = html.Div([
                html.H3("Datasets"),
                dbc.Table.from_dataframe(datasets_df, striped=True, bordered=True, hover=True, responsive=True)
            ])
        return datasets_layout

    return html.P("Select a tab")

# 2. Callback for handling project selection
@callback(
    # Update hidden states in app.layout (used for callback chaining)
    Output('project-edit-state', 'data'),
    # Actual form fields that user sees
    Output('project-form-title-display', 'children'),
    Output('project-name-input-field', 'value'),
    Output('project-desc-input-field', 'value'),
    Output('project-catalog-input-field', 'value'),
    Output('project-schema-input-field', 'value'),
    Output('project-form-feedback-display', 'children'),
    # Trigger on any project item click
    Input({'type': 'project-item', 'index': ALL}, 'n_clicks'),
    prevent_initial_call=True
)
def load_project_details(n_clicks_list):
    ctx = dash.callback_context
    if not ctx.triggered_id or not isinstance(ctx.triggered_id, dict):
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
    if ctx.triggered_id.get('type') != 'project-item':
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    project_id = ctx.triggered_id['index']
    triggered_n_click = ctx.triggered[0]['value']

    if triggered_n_click is None or triggered_n_click == 0:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    project_data_df = fetch_data("SELECT name, description, catalog, schema FROM projects WHERE id = %s", (project_id,))

    if not project_data_df.empty:
        project_data = project_data_df.iloc[0]
        new_state = {'mode': 'edit', 'id': project_id}
        form_title = f"Edit Project (ID: {project_id})"
        # Return data to populate the form fields
        return new_state, form_title, project_data['name'], project_data['description'], project_data['catalog'], project_data['schema'], None
    else:
        error_message = dbc.Alert(f"Could not find details for Project ID: {project_id}", color="danger")
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, error_message


# Callback to handle the "Create New Project" button click -> Clears the form
@callback(
    Output('project-edit-state', 'data', allow_duplicate=True),
    Output('project-form-title-display', 'children'),
    Output('project-name-input-field', 'value'),
    Output('project-desc-input-field', 'value'),
    Output('project-catalog-input-field', 'value'),
    Output('project-schema-input-field', 'value'),
    Output('project-form-feedback-display', 'children'),
    Input('create-project-btn', 'n_clicks'),
    prevent_initial_call=True
)
def prepare_create_form(n_clicks):
    if n_clicks > 0:
        return {'mode': 'new', 'id': None}, "Create New Project", '', '', '', '', None
    return dash.no_update


# Callback to save a new project OR update an existing one
@callback(
    Output('project-form-feedback-display', 'children'),
    Output('data-updated-signal', 'data'),
    Input('save-project-btn', 'n_clicks'),
    State('project-edit-state', 'data'),
    State('project-name-input-field', 'value'),
    State('project-desc-input-field', 'value'),
    State('project-catalog-input-field', 'value'),
    State('project-schema-input-field', 'value'),
    prevent_initial_call=True
)
def save_project(n_clicks, edit_state, name, description, catalog, schema):
    if n_clicks == 0:
        return dash.no_update, dash.no_update

    mode = edit_state.get('mode', 'new')
    project_id = edit_state.get('id')

    if not all([name, description, catalog, schema]):
        return dbc.Alert("All fields are required.", color="danger"), dash.no_update

    feedback_message = None
    signal_update = dash.no_update
    success = False
    action = "" # Initialize action

    if mode == 'new':
        query = """
            INSERT INTO projects (name, description, catalog, schema)
            VALUES (%s, %s, %s, %s);
        """
        params = (name, description, catalog, schema)
        success = execute_query(query, params)
        action = "created"

    elif mode == 'edit' and project_id is not None:
        query = """
            UPDATE projects
            SET name = %s, description = %s, catalog = %s, schema = %s
            WHERE id = %s;
        """
        params = (name, description, catalog, schema, project_id)
        success = execute_query(query, params)
        action = "updated"
    else:
        feedback_message = dbc.Alert("Invalid operation state.", color="warning")
        return feedback_message, dash.no_update

    if success:
        feedback_message = dbc.Alert(f"Project '{name}' {action} successfully!", color="success")
        signal_update = n_clicks
    else:
        feedback_message = dbc.Alert(f"Failed to {action.replace('ed','')} project. Check logs.", color="danger")

    return feedback_message, signal_update


# Run the app
if __name__ == '__main__':
    if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
        print("Error: Database environment variables (DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT) not set.")
        print("Please create a .env file with these details.")
    else:
        app.run(debug=True, host='0.0.0.0')