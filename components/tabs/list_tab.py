import dash_bootstrap_components as dbc
from dash import html, dcc
from utils.db import get_projects

def create_list_tab():
    # Retrieve projects from the database


   
    df = get_projects()
    # Convert to items for storage and display
    items = []
    if not df.empty:
        records = df.to_dict(orient='records')
        # Build list of projects with full details
        items = [
            {
                'id': int(rec['id']),
                'text': rec.get('name'),
                'description': rec.get('description'),
                'catalog': rec.get('catalog'),
                'schema': rec.get('schema'),
            }
            for rec in records
        ]

    active_project_id = items[0]['id'] if items else None# Store component to maintain the list of projects
    store = dcc.Store(id='list-store', data={'items': items, "active_project_id": active_project_id})


    # Create list group items
    list_items = []
    for idx, item in enumerate(items):
        list_items.append(
            dbc.ListGroupItem(
                item['text'],
                id={"type": "list-group-item", "index": item['id']},
                action=True,
                active=(idx == 0)
            )
        )
    # Show message if no projects found
    if not list_items:
        list_items = [
            dbc.ListGroupItem(
                "No projects found.",
                id={"type": "list-group-item", "index": -1},
                disabled=True
            )
        ]

    listgroup = dbc.ListGroup(list_items, id="list-group")

    # Form to create a new project
    create_form = dbc.Form([
        html.Div([
            dbc.Label("Name", html_for="project-name"),
            dbc.Input(type="text", id="project-name", placeholder="Enter project name"),
        ], className="mb-3"),
        html.Div([
            dbc.Label("Description", html_for="project-description"),
            dbc.Input(type="text", id="project-description", placeholder="Enter description"),
        ], className="mb-3"),
        html.Div([
            dbc.Label("Catalog", html_for="project-catalog"),
            dbc.Input(type="text", id="project-catalog", placeholder="Enter catalog"),
        ], className="mb-3"),
        html.Div([
            dbc.Label("Schema", html_for="project-schema"),
            dbc.Input(type="text", id="project-schema", placeholder="Enter schema"),
        ], className="mb-3"),
        html.Div([
            dbc.Button("Create Project", id="create-project-button", color="success", className="me-2"),
            dbc.Button("Update Project", id="update-project-button", color="primary", className="me-2"),
            dbc.Button("Delete Project", id="delete-project-button", color="danger"),
        ], className="mt-3")
    ])
    # Layout: project list and creation form side by side
    return dbc.Tab([
        dbc.Row([
            dbc.Col(listgroup, width=8),
            dbc.Col(create_form, width=4)
        ])
    ], label="Projects", tab_id="tab-list"), store