import os
import traceback
import requests # Import requests library
import json # Import json library
import mlflow # Added mlflow import

# Removed Databricks SDK imports


def get_runs(experiment_id, host, token):
    """
    Get all runs for a specific experiment.
    
    Args:
        experiment_id (str): The ID of the experiment
        host (str): Databricks workspace host
        token (str): Databricks access token
        
    Returns:
        list: List of runs or None if error occurs
    """
    try:
        # Construct the API URL
        url = f"{host}/api/2.0/mlflow/runs/search"
        
        # Set up the headers
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Set up the request body
        data = {
            "experiment_ids": [experiment_id],
            "max_results": 1000  # Adjust this if you need more runs
        }
        
        # Make the API call
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == 200:
            runs_data = response.json()
            return runs_data.get('runs', [])
        else:
            print(f"Error fetching runs: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"Error getting runs: {str(e)}")
        return None

def get_experiment_runs(experiment_name):
    """
    Get experiment details by name and its runs using the Databricks REST API.
    
    Args:
        experiment_name (str): The name of the experiment to retrieve
        
    Returns:
        tuple: (list_of_run_dicts, experiment_id, error_message_or_None)
               Returns ([], None, message) if experiment not found.
               Returns (None, None, message) if there was an API or other error.
               Returns (list_of_run_dicts, experiment_id, None) on success.
    """
    try:
        # Get the host and token from environment variables
        host = os.getenv('DATABRICKS_HOST')
        token = os.getenv('DATABRICKS_TOKEN')
        
        if not host or not token:
            msg = "Error: DATABRICKS_HOST and DATABRICKS_TOKEN environment variables must be set"
            print(msg)
            return None, None, msg # Return None for runs and exp_id on config error
            
        # Ensure host starts with https:// (important for URL construction)
        if not host.startswith('https://'):
            host = 'https://' + host

        # Construct the API URL
        url = f"{host}/api/2.0/mlflow/experiments/get-by-name"
        
        # Set up the headers
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        experiment_name = f"/Users/ben.mackenzie@databricks.com/{experiment_name}"
        
        # Set up the request body
        data = {
            "experiment_name": experiment_name
        }
        
        # Make the API call
        response = requests.get(url, headers=headers, json=data)
        
        # Check if the request was successful
        if response.status_code == 200:
            experiment = response.json()['experiment']
            experiment_id = experiment['experiment_id'] # Get experiment_id
            
            # Pass host and token to get_runs explicitly
            runs = get_runs(experiment_id, host, token) 
            
            if runs is None: # Handle error from get_runs
                return None, experiment_id, "Error fetching runs for the experiment."
                
            return runs, experiment_id, None # Return runs, experiment_id, and no error
            
        elif response.status_code == 404: # Specific handling for experiment not found
             msg = f"Experiment '{experiment_name}' not found."
             print(msg + " (API 404)")
             return [], None, msg # Return empty list, None exp_id, and message
        else:
            msg = f"Error getting experiment: {response.status_code} - {response.text}"
            print(msg)
            return None, None, msg # Return None for runs and exp_id on API error
            
    except Exception as e:
        msg = f"Error getting experiment: {str(e)}"
        print(msg + f"\n{traceback.format_exc()}")
        return None, None, msg # Return None for runs and exp_id on general error

def register_model_version(model_name: str, model_source: str, description: str = None, tags: list = None) -> tuple[dict | None, str | None]:
    """
    Registers a new model version in the Databricks Model Registry using MLflow.
    The model will be registered in Unity Catalog if a three-level name is provided (catalog.schema.model).
    MLflow will create the registered model entity if it doesn't exist.
    The run ID is expected to be part of the model_source URI.

    Args:
        model_name (str): The name of the model (e.g., 'catalog.schema.model_name').
        model_source (str): The source URI of the model, e.g., "runs:/<run_id>/model".
        description (str, optional): A description for the model version.
        tags (list, optional): A list of tag dictionaries (e.g., [{"key": "key1", "value": "value1"}])
                               for the model version. These will be converted to a flat dict for MLflow.

    Returns:
        tuple: (model_version_details_dict, error_message_or_None)
               model_version_details_dict is a dictionary representation of the registered model version on success.
               error_message_or_None is a string containing an error message if any step fails.
    """
    mlflow.set_registry_uri('databricks-uc')
    try:
        # Ensure MLflow is configured to use Databricks
        os.environ["MLFLOW_TRACKING_URI"] = "databricks"
        
        # Check for Databricks host and token (MLflow client will use these)
        host_env = os.getenv('DATABRICKS_HOST')
        token_env = os.getenv('DATABRICKS_TOKEN')

        if not host_env or not token_env:
            msg = "Error: DATABRICKS_HOST and DATABRICKS_TOKEN environment variables must be set."
            print(msg)
            return None, msg
        
        mlflow_tags = None
        if tags:
            current_tags = {}
            for tag_item in tags:
                if isinstance(tag_item, dict) and "key" in tag_item and "value" in tag_item:
                    # Ensure key is a string and value is converted to string for MLflow tags
                    current_tags[str(tag_item["key"])] = str(tag_item["value"])
            if current_tags:
                 mlflow_tags = current_tags
            
        print(f"Attempting to register model version using MLflow. Name: {model_name}, Source: {model_source}")
        if description:
            print(f"Description: {description}")
        if mlflow_tags:
            print(f"Tags: {mlflow_tags}")

        # Register model without description and tags initially
        model_version_initial = mlflow.register_model(
            model_uri=model_source,
            name=model_name
            # description and tags will be set using MlflowClient later
        )
        
        # Initialize MlflowClient to update description and tags
        client = mlflow.tracking.MlflowClient()
        
        # Update description if provided
        if description:
            client.update_model_version(
                name=model_version_initial.name,
                version=model_version_initial.version,
                description=description
            )
            print(f"Updated description for version {model_version_initial.version} of model '{model_name}'")

        # Set tags if provided
        if mlflow_tags:
            for key, value in mlflow_tags.items():
                client.set_model_version_tag(
                    name=model_version_initial.name,
                    version=model_version_initial.version,
                    key=key,
                    value=value
                )
            print(f"Set tags for version {model_version_initial.version} of model '{model_name}'")

        # Fetch the updated model version to get all details including description and tags
        model_version = client.get_model_version(
            name=model_version_initial.name,
            version=model_version_initial.version
        )
        
        # Convert ModelVersion object to a dictionary for consistent return type
        model_version_details = {
            "name": model_version.name,
            "version": model_version.version,
            "creation_timestamp": model_version.creation_timestamp,
            "last_updated_timestamp": model_version.last_updated_timestamp,
            "current_stage": model_version.current_stage,
            "description": model_version.description,
            "source": model_version.source,
            "run_id": model_version.run_id,
            "status": model_version.status,
            "status_message": model_version.status_message,
            "user_id": model_version.user_id,
            "run_link": model_version.run_link,
            "tags": model_version.tags # This will be a dict {'key1': 'val1'}
        }
        
        print(f"MLflow model version registration successful: {json.dumps(model_version_details)}")
        return model_version_details, None

    except mlflow.exceptions.MlflowException as e:
        msg = f"MLflow API error during model version registration for '{model_name}': {str(e)}"
        detailed_error = traceback.format_exc()
        print(f"{msg}\\n{detailed_error}")
        # Attempt to get more specific error details if available (e.g., from Databricks API response)
        # This part is tricky as MlflowException might wrap various error types
        error_details_str = ""
        if hasattr(e, 'get_json_body') and callable(e.get_json_body): # For RestException
            try:
                error_details_str = json.dumps(e.get_json_body())
            except:
                pass # Ignore if can't get json body
        elif hasattr(e, 'message'):
             error_details_str = e.message

        full_msg = f"{msg}. Details: {error_details_str if error_details_str else 'No additional details found in exception.'}"
        print(f"Returning error: {full_msg}") # For clearer logging
        return None, full_msg # Return the more detailed message
    except Exception as e:
        error_trace = traceback.format_exc()
        msg = f"An unexpected error occurred during MLflow model version registration for '{model_name}': {str(e)}\\n{error_trace}"
        print(msg)
        return None, msg