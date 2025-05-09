import requests

def get_files_from_github_repo_folder(github_repo_url: str, folder_path: str = "notebook") -> list | None:
    """
    Retrieves the names of files from a specified folder in a public GitHub repository.

    Args:
        github_repo_url (str): The full URL of the public GitHub repository 
                               (e.g., "https://github.com/owner/repo").
        folder_path (str): The path to the folder within the repository 
                           (e.g., "notebook", "src/notebooks"). Defaults to "notebook".

    Returns:
        list: A list of file names in the specified folder.
              Returns None if an error occurs or the folder is not found/accessible.
    """
    if not github_repo_url.startswith("https://github.com/"):
        print(f"Error: Invalid GitHub repository URL provided: '{github_repo_url}'. "
              "It must start with 'https://github.com/'.")
        return None

    # Attempt to parse owner and repo from URL
    try:
        parts = github_repo_url.strip("/").split("/")
        if len(parts) < 5: # e.g. https: , '', github.com, owner, repo
            raise ValueError("URL format is incorrect.")
        owner = parts[3]
        repo_name = parts[4]
    except (IndexError, ValueError):
        print(f"Error: Could not parse owner and repository from URL: '{github_repo_url}'. "
              "Expected format: https://github.com/owner/repository_name")
        return None

    api_url = f"https://api.github.com/repos/{owner}/{repo_name}/contents/{folder_path}"
    
    headers = {
        "Accept": "application/vnd.github.v3+json",
        # No token needed for public repos, but good to be aware of rate limits for unauthenticated requests
    }
    
    print(f"Fetching contents from: {api_url}")

    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()  # Raises HTTPError for bad responses (4XX or 5XX)
        
        contents = response.json()
        
        if isinstance(contents, list):
            # Filter for items of type 'file'
            file_names = [item['name'] for item in contents if item.get('type') == 'file']
            return file_names
        elif isinstance(contents, dict) and 'message' in contents:
            # Likely an error message from GitHub API (e.g., "Not Found")
            print(f"Error from GitHub API for folder '{folder_path}': {contents['message']}")
            return None
        else:
            print(f"Error: Unexpected response format from GitHub API for folder '{folder_path}'.")
            # print(f"Response: {contents}") # for debugging
            return None

    except requests.exceptions.HTTPError as http_err:
        if http_err.response.status_code == 404:
            print(f"Error: The folder '{folder_path}' was not found in the repository '{owner}/{repo_name}'. "
                  f"Or the repository itself was not found.")
        else:
            print(f"HTTP error occurred: {http_err}")
        return None
    except requests.exceptions.RequestException as req_err:
        print(f"Error making request to GitHub API: {req_err}")
        return None
    except KeyError:
        # If 'type' or 'name' key is missing in an item
        print("Error: Unexpected JSON structure in the list of contents from GitHub API.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

if __name__ == "__main__":
    print("Demonstration of retrieving file names from a GitHub repository's folder.\n")
    
    # Example 1: Using a repository that is known to have a "notebook" folder.
    # The jupyter/notebook repository has a 'notebook' directory.
    repo1_url = "https://github.com/BenMacKenzie/db-model-trainer"
    folder1_path = "notebooks" # This is an actual folder in that repo
    
    print(f"--- Example 1 ---")
    print(f"Repository: {repo1_url}")
    print(f"Folder: {folder1_path}")
    files1 = get_files_from_github_repo_folder(repo1_url, folder1_path)
    
    if files1 is not None:
        if files1:
            print(f"\nFiles in '{folder1_path}':")
            for file_name in files1:
                print(f"- {file_name}")
        else:
            print(f"\nNo files found in '{folder1_path}' or the folder is empty.")
    else:
        print(f"\nCould not retrieve files for {repo1_url}, folder '{folder1_path}'. Check logs for details.")
    
    print("\n" + "="*40 + "\n")
