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

pr_number = os.getenv("PR_NUMBER")
repo_name = os.getenv("GITHUB_REPOSITORY")
# access_token = os.getenv("GITHUB_TOKEN")
access_token = os.getenv("GITHUB_TOKEN")
pr_url = os.getenv("PR_URL")

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

def send_to_api(sql_queries, api_endpoint):
    try:
        data = {"sql_queries": sql_queries}
        response = requests.post(api_endpoint, json=data)

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

def format_comment(query, events):
    logo_url = 'https://www.unraveldata.com/wp-content/themes/unravel-child/src/images/unLogo.svg'
    
    comment = f"![Logo]({logo_url})\n\n📌**Query:**\n```sql\n{query}\n```\n\n<details>\n<summary>📊Events</summary>\n\n"
    
    # Create a table header
    comment += "| Event | Details |\n"
    comment += "| --- | --- |\n"
    
    # Add events to the table
    for key, value in events.items():
        comment += f'| **{key}** | {value} |\n'
    
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
        
        for query, events in content_data.items():
            comment = format_comment(query, events['events'])
            payload = {"body": "{}".format(comment)}
            response = requests.post(url, headers=headers, json=payload)
        

        return {"status": response.status_code, "content": response.text}
    except Exception as e:
        return {"status": 500, "error": f"Error posting comment: {e}"}

if __name__ == "__main__":
    file_content=get_raw_file_content()
    # Get other details from GitHub Secrets
    api_endpoint = os.getenv("API_ENDPOINT")
    repo_owner = os.getenv("REPO_OWNER")
    repo_name = os.getenv("GITHUB_REPOSITORY")
    github_token = os.getenv("GITHUB_TOKEN")
    # Extract SQL queries
    for filename, content in file_content.items():
        
        sql_statements = extract_sql_statements(content)
    
    # Send SQL queries to API
    api_response = send_to_api(sql_statements, api_endpoint)

    # Post comment on PR
    if api_response.get("status") == 200:
        print(f"SQL Queries extraction successful. API Response: {api_response}")
    else:
        print(f"SQL Queries extraction failed. API Response: {api_response}")

    post_response = post_comment_on_pr(api_response, pr_number, github_token, repo_owner, repo_name)
    print(post_response)
