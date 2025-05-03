import os
import traceback
import requests # Import requests library
import json # Import json library

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