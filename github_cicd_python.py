import sys
from datetime import timedelta, datetime
import json
import re
import requests
import getopt
import urllib3
import os
import time
import html
from jira import JIRA
from bs4 import BeautifulSoup
import markdown
import subprocess
import sqlparse

pr_number = os.getenv("PR_NUMBER")
repo_name = os.getenv("GITHUB_REPOSITORY")
# access_token = os.getenv("GITHUB_TOKEN")
access_token = os.getenv("GITHUB_TOKEN")
pr_url = os.getenv("PR_URL")

def extract_sql_queries(content):
    statements = sqlparse.split(content)

    # Filter out empty statements and remove comments
    sql_queries = [sqlparse.format(statement, strip_comments=True).strip() for statement in statements if statement.strip()]

    query_line_map = {}

    # Iterate through the statements and track line numbers
    current_line = 1
    for statement in statements:
        if statement.strip():
            lines = statement.splitlines()
            num_lines = len(lines)
            query_line_map[statement.strip()] = list(range(current_line, current_line + num_lines))
            current_line += num_lines

    print(query_line_map)

    return sql_queries

def extract_sql_statements(content):
    sql_queries = []
    current_query = ""
    if(content):
        in_comment_block = False
        in_procedure_block=False 
        for line in content.split("\n"):
            if not in_comment_block and not line.strip().startswith('--') and not line.strip().startswith('/*'):
                
                if 'BEGIN' in line:
                    in_procedure_block = True
                    current_query += line.strip() + ' '

                elif not line.strip().endswith(';'):
                    
                    current_query += line.strip() + ' '
                elif 'END' not in line:
                    
                    current_query += line.strip()
                
                    
                if 'END' in line:
                    in_procedure_block = False
                    current_query += line.strip()  
                    
                    sql_queries.append(current_query.strip())
                    
                    current_query = ""
                
                elif current_query.endswith(';') and in_procedure_block==False:
                    
                    sql_queries.append(current_query.strip())
                    
                    current_query = ""
            else:
                
                if '/*' in line:
                    in_comment_block = True
                
                if '*/' in line:
                    in_comment_block = False


    return sql_queries

def get_raw_file_content(get_file_name_flag=False):
    # Get the changed file paths from the pull request event payload
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    url = f'https://api.github.com/repos/{repo_name}/pulls/{pr_number}/files'
    response = requests.get(url, headers=headers)
    files = response.json()
    changed_files = [file['filename'] for file in files]
    if get_file_name_flag == True:
        return changed_files

    file_contents = {}
    for file in files:
        file_url = file['raw_url']
        file_response = requests.get(file_url, headers=headers)
        #print(file_response)
        file_content = file_response.text
        #print(file_content)
        file_contents[file['filename']] = file_content

    return file_contents

def get_pr_description():
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.github.v3+json",
    }

    url = f"https://api.github.com/repos/{repo_name}/pulls/{pr_number}"

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    pr_data = response.json()
    description = pr_data["body"]
    return description

def send_to_api(sql_queries, api_endpoint, platform_name, unravel_token):
    try:
        data = {"queries": sql_queries, "platform": platform_name}
        response = requests.post(api_endpoint, json=data, verify=False, headers={"Authorization": unravel_token})

        return {"status": response.status_code, "content": response.text}
    except Exception as e:
        return {"status": 500, "error": f"Error sending to API: {e}"}

def send_to_api_with_curl(sql_queries, api_endpoint):
    try:
        data = {"sql_queries": sql_queries}
        data_json = json.dumps(data).replace('"', r'\"')  # Escape double quotes for curl

        # Construct the curl command
        curl_command = f'curl -X POST -H "Content-Type: application/json" -d "{data_json}" {api_endpoint}'

        # Execute the curl command using subprocess
        result = subprocess.check_output(curl_command, shell=True)

        # Parse the result as JSON
        result_json = json.loads(result.decode('utf-8'))

        return result_json

    except Exception as e:
        print(f"Error sending to API: {e}")
        return None
        
def format_comment(query, insights):
    logo_url = 'https://www.unraveldata.com/wp-content/themes/unravel-child/src/images/unLogo.svg'
    
    comment = f"![Logo]({logo_url})\n\n📌**Query:**\n```sql\n{query}\n```\n\n<details>\n<summary>📊Insights</summary>\n\n"
    
    # Create a table header
    comment += "| # | Name | Action | Detail |\n"
    comment += "| --- | --- | --- | --- |\n"
    
    # Add insights to the table
    for idx, insight in enumerate(insights, start=1):
        name = insight.get('name', '')
        action = insight.get('action', '')
        detail = insight.get('detail', '')
        
        comment += f"| {idx} | {name} | {action} | {detail} |\n"
    
    comment += "</details>"
    return comment

def post_comment_on_pr(api_response, pr_number, github_token, repo_owner, repo_name):
    try:
        url = f"https://api.github.com/repos/{repo_name}/issues/{pr_number}/comments"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/vnd.github.v3+json",
        }
        # Parse the JSON-formatted string into a dictionary
        content_data = json.loads(api_response.get('content', {}))
        #print(content_data)
        
        for entry in content_data:
            query = entry.get('query', '')
            events = entry.get('insights', [])
            
            if query and events:
                comment = format_comment(query, events)
                payload = {"body": "{}".format(comment)}
                response = requests.post(url, headers=headers, json=payload)

        return {"status": response.status_code, "content": response.text}
    except Exception as e:
        return {"status": 500, "error": f"Error posting comment: {e}"}

def get_platform_details(pr_description):
    platforms_start = pr_description.find('**Platforms:')
    if platforms_start != -1:
        platforms_start += len('**Platforms:')
        platforms_end = pr_description.find('**Changes:', platforms_start)
        platforms_section = pr_description[platforms_start:platforms_end] if platforms_end != -1 else pr_description[platforms_start:]
    
        # Split the platforms section into lines
        platform_lines = platforms_section.split('\n')
    
        # Extract selected platforms with [x]
        selected_platform = [line.strip()[6:] for line in platform_lines if line.startswith('- [x]')]
    
        print(f'Selected Platform: {selected_platform}')
    else:
        print('Platforms information not found in the description, proceeding with snowflake as default platform.')
        
    if(len(selected_platform)!=0):
        return selected_platform[0].lower()
    else:
        print('Platforms information not found in the description, proceeding with snowflake as default platform.')
        return "snowflake"

def get_pr_comments():
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.github.v3+json",
    }

    url = f"https://api.github.com/repos/{repo_name}/issues/{pr_number}/comments"
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    comments = response.json()
    return comments

def update_comments(api_response, existing_comments):
    content_str = api_response.get('content', '{}')
    api_response_content = json.loads(content_str)
    extracted_queriesq = [item.get('query', '') for item in api_response_content]

    extracted_queries = []
    for comment in existing_comments:
        if "⚙️Status - ✅Resolved" not in comment['body']:
            # Extract the SQL query from the 'body' field
            match = re.search(r'```sql\n(.*?)\n```', comment['body'], re.DOTALL)
            if match:
                sql_query = match.group(1)
                extracted_queries.append(sql_query)
    for query in extracted_queries:
        if query not in extracted_queriesq:
            # Comment is resolved, update the comment with "Status - Resolved"
            update_comment_status(query, "✅Resolved")

def update_comment_status(query, status):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Get the comment ID using the GitHub API
    comments_url = f"https://api.github.com/repos/{repo_name}/issues/{pr_number}/comments"
    comments_response = requests.get(comments_url, headers=headers)
    comments = comments_response.json()
    comment_id = None
    for comment in comments:
        if query in comment['body']:
            comment_id = comment['id']
            break

    if comment_id:
        # Get the existing comment body
        existing_comment_url = f"https://api.github.com/repos/{repo_name}/issues/comments/{comment_id}"
        existing_comment_response = requests.get(existing_comment_url, headers=headers)
        existing_comment_body = existing_comment_response.json().get('body', '')

        # Add the status to the existing comment body
        updated_comment_body = f"{existing_comment_body}\n\n⚙️Status - {status}"

        # Update the comment body with the new content
        update_url = f"https://api.github.com/repos/{repo_name}/issues/comments/{comment_id}"
        update_payload = {"body": updated_comment_body}
        update_response = requests.patch(update_url, headers=headers, json=update_payload)

def post_comment_on_pr_query_wise(api_response, existing_comments):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    try:
        # Parse the JSON content from the api_response string
        content_data = json.loads(api_response.get('content', {}))
        
        extracted_queriesq = []
        for comment in existing_comments:
            if "⚙️Status - ✅Resolved" not in comment['body']:
                # Extract the SQL query from the 'body' field
                match = re.search(r'```sql\n(.*?)\n```', comment['body'], re.DOTALL)
                if match:
                    sql_query = match.group(1)
                    extracted_queriesq.append(sql_query)

        print('extracted_queriesq',extracted_queriesq)
        
        # Comment on the pull request for each extracted query
        for entry in content_data:
            query = entry.get('query', '')
            events = entry.get('insights', [])

            # Check if the query is not in existing_queries
            if query not in extracted_queriesq:

                if query and events:
                    # Create the comment body
                    comment_body = format_comment(query, events)
                    print(f"Comment Body:\n{comment_body}")
    
                    # Add the comment to the pull request
                    comments_url = f"https://api.github.com/repos/{repo_name}/issues/{pr_number}/comments"
                    comment_payload = {"body": comment_body}
                    comment_response = requests.post(comments_url, headers=headers, json=comment_payload)

                    # Check if the comment was successfully added
                    if comment_response.status_code == 201:
                        print(f"Comment added for query:\n{query}")
                    else:
                        print(f"Failed to add comment for query:\n{query}")
    
    except json.JSONDecodeError:
        print("Error decoding JSON from api_response content.")
            
    

if __name__ == "__main__":
    existing_comments = get_pr_comments()
    if not existing_comments:
        file_content=get_raw_file_content()
        file_names=get_raw_file_content(get_file_name_flag=True)
        file=file_names[0]
        platform="snowflake"
        if('snowflake' in file.lower() or 'sf' in file.lower()):
            platform="snowflake"
        elif('bigquery' in file.lower() or 'bq' in file.lower()):
            platform="bigquery"
        elif('databricks' in file.lower() or 'dbx' in file.lower()):
            platform="databricks"
        else:
            print('Platforms information not found in the description, proceeding with snowflake as default platform.')
    
        print(platform)
        # Get other details from GitHub Secrets
        api_endpoint = os.getenv("UNRAVEL_URL")
        repo_owner = os.getenv("REPO_OWNER")
        repo_name = os.getenv("GITHUB_REPOSITORY")
        github_token = os.getenv("GITHUB_TOKEN")
        unravel_url = os.getenv("UNRAVEL_URL")
        unravel_token = os.getenv("UNRAVEL_JWT_TOKEN")
        # Extract SQL queries
        for filename, content in file_content.items():
            
            sql_statements = extract_sql_queries(content)
        
        # Send SQL queries to API
        api_response = send_to_api(sql_statements, api_endpoint, platform, unravel_token)
        print(api_response)
    
        # Post comment on PR
        if api_response.get("status") == 200:
            print(f"SQL Queries successfully processed . API Response: {api_response}")
        else:
            print(f"SQL Queries processing failed. API Response: {api_response}")
        
        post_response = post_comment_on_pr(api_response, pr_number, github_token, repo_owner, repo_name)
        #print(post_response)
    else:
        file_content=get_raw_file_content()
        file_names=get_raw_file_content(get_file_name_flag=True)
        file=file_names[0]
        print(file)
        platform="snowflake"
        if('snowflake' in file.lower() or 'sf' in file.lower()):
            platform="snowflake"
        elif('bigquery' in file.lower() or 'bq' in file.lower()):
            platform="bigquery"
        elif('databricks' in file.lower() or 'dbx' in file.lower()):
            platform="databricks"
        else:
            print('Platforms information not found in the description, proceeding with snowflake as default platform.')
    
        print(platform)
        # Get other details from GitHub Secrets
        api_endpoint = os.getenv("UNRAVEL_URL")
        repo_owner = os.getenv("REPO_OWNER")
        repo_name = os.getenv("GITHUB_REPOSITORY")
        github_token = os.getenv("GITHUB_TOKEN")
        unravel_url = os.getenv("UNRAVEL_URL")
        unravel_token = os.getenv("UNRAVEL_JWT_TOKEN")
        # Extract SQL queries
        for filename, content in file_content.items():
            
            sql_statements = extract_sql_queries(content)
        
        # Send SQL queries to API
        api_response = send_to_api(sql_statements, api_endpoint, platform, unravel_token)
    
        # Post comment on PR
        if api_response.get("status") == 200:
            print(f"SQL Queries successfully processed . API Response: {api_response}")
        else:
            print(f"SQL Queries processing failed. API Response: {api_response}")
                
        update_comments(api_response, existing_comments)
        post_comment_on_pr_query_wise(api_response, existing_comments)
