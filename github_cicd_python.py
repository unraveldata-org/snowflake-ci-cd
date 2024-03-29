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
    end_line_queries=[]
    for statement in statements:
        if not statement.strip():
            continue
        
        matches = re.finditer(re.escape(statement), content)
        

        for match in matches:
            
            start_line = content.count('\n', 0, match.start()) + 1
            end_line = start_line + statement.count('\n')

            if end_line not in end_line_queries:
                end_line_queries.append(end_line)
                query_line_count=sqlparse.format(statement, strip_comments=True).count('\n')+1
                if sqlparse.format(statement, strip_comments=True) in query_line_map:
                    query_line_map[sqlparse.format(statement, strip_comments=True)].append((end_line, query_line_count))
                else:
                    query_line_map[sqlparse.format(statement, strip_comments=True)]=[(end_line, query_line_count)]
                    

    return sql_queries, query_line_map

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
        
def get_html_content(url):
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Return the HTML content of the page
            return response.text
        else:
            print(f"Failed to fetch HTML content from {url}. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"An error occurred while fetching HTML content from {url}: {e}")
        return None

def get_data_anchor(html_content, line_number):
    try:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        # Find the <td> element with the corresponding data-line-number attribute
        line_td = soup.find('td', {'data-line-number': str(line_number)})
        if line_td:
            # Extract the data-anchor value from the id attribute
            data_anchor = line_td.get('id')
            print("data_anchor",data_anchor)
            return data_anchor
        return None
    except Exception as e:
        print(f"An error occurred while extracting data-anchor value: {e}")
        return None

def generate_url_for_line_change(url, data_anchor):
    try:
        # Check if the URL already contains a fragment identifier
        if '#' in url:
            return f"{url}&#{data_anchor}"
        else:
            return f"{url}#{data_anchor}"
    except Exception as e:
        print(f"An error occurred while generating URL: {e}")
        return None

def format_comment(query, insights, profile_insights, query_line_map, details_map, url):
    logo_url = 'https://www.unraveldata.com/wp-content/themes/unravel-child/src/images/unLogo.svg'
    if(insights and not profile_insights):
        comment = f"![Logo]({logo_url})\n\n📌 **Query:**\n```sql\n{query}\n```\n\n<details>\n<summary>📊 Parser Based Insights</summary>\n\n"
        
        # Create a table header
        comment += "| # | Name | Action | Detail | Let's Navigate |\n"
        comment += "| --- | --- | --- | --- | --- |\n"
        
        # Get HTML content of the page
        
        html_content = get_html_content(url)
        
        # Add insights to the table
        for idx, insight in enumerate(insights, start=1):
            name = insight.get('name', '')
            action = insight.get('action', '')
            detail = insight.get('detail', '')
            
            navigate_button = ""  # Default value for the navigation column
            
            if 'at line' in detail:
                detail_parts = detail.split('at line')
                if len(detail_parts) == 2:
                    endline, count = query_line_map.get(query, [(0, 0)])[0]  # Default to [(0, 0)] if query not found in map
                    line_no = endline - (count - int(detail_parts[1].strip()))
                    detail = f"{detail_parts[0]}at line {line_no}"
    
            else:
                endline, count = query_line_map.get(query, [(0, 0)])[0]
                line_no = (endline - count) + 1
                    
            # Generate URL for navigating to the specific line
            data_anchor = get_data_anchor(html_content, line_no)
            if data_anchor:
                url_with_anchor = generate_url_for_line_change(url, data_anchor)
                print("url_with_anchor",url_with_anchor)
                navigate_button = f"[**↗️Navigate to line {line_no}**]({url_with_anchor})"
            else:
                navigate_button = f"(Line {line_no})"
            
            # Add a dash if the 'at line' condition is false
            if not navigate_button:
                navigate_button = "-"
                    
            comment += f"| {idx} | {name} | {action} | {detail} | {navigate_button} |\n"

        comment += "</details>"
        
    if(profile_insights and not insights):
        comment = f"![Logo]({logo_url})\n\n📌 **Query:**\n```sql\n{query}\n```\n\n<details>\n<summary>📊 Profile Based Insights</summary>\n\n"
        
        # Create a table header
        comment += "| # | Name | Action | Detail | Let's Navigate |\n"
        comment += "| --- | --- | --- | --- | --- |\n"
        
        # Get HTML content of the page
        
        html_content = get_html_content(url)
        
        # Add insights to the table
        for idx, insight in enumerate(profile_insights, start=1):
            name = insight.get('name', '')
            action = insight.get('action', '')
            detail = insight.get('detail', '')
            
            navigate_button = ""  # Default value for the navigation column
            
            if 'at line' in detail:
                detail_parts = detail.split('at line')
                if len(detail_parts) == 2:
                    endline, count = query_line_map.get(query, [(0, 0)])[0]  # Default to [(0, 0)] if query not found in map
                    line_no = endline - (count - int(detail_parts[1].strip()))
                    detail = f"{detail_parts[0]}at line {line_no}"
    
            else:
                endline, count = query_line_map.get(query, [(0, 0)])[0]
                line_no = (endline - count) + 1
                    
            # Generate URL for navigating to the specific line
            data_anchor = get_data_anchor(html_content, line_no)
            if data_anchor:
                url_with_anchor = generate_url_for_line_change(url, data_anchor)
                print("url_with_anchor",url_with_anchor)
                navigate_button = f"[**↗️Navigate to line {line_no}**]({url_with_anchor})"
            else:
                navigate_button = f"(Line {line_no})"
            
            # Add a dash if the 'at line' condition is false
            if not navigate_button:
                navigate_button = "-"
                    
            comment += f"| {idx} | {name} | {action} | {detail} | {navigate_button} |\n"

        comment += "</details>"

    if(profile_insights and insights):
        
        comment = f"![Logo]({logo_url})\n\n📌 **Query:**\n```sql\n{query}\n```\n\n<details>\n<summary>📊 Parser Based Insights</summary>\n\n"
        
        # Create a table header
        comment += "| # | Name | Action | Detail | Let's Navigate |\n"
        comment += "| --- | --- | --- | --- | --- |\n"
        
        # Get HTML content of the page
        
        html_content = get_html_content(url)
        
        # Add insights to the table
        for idx, insight in enumerate(insights, start=1):
            name = insight.get('name', '')
            action = insight.get('action', '')
            detail = insight.get('detail', '')
            
            navigate_button = ""  # Default value for the navigation column
            
            if 'at line' in detail:
                detail_parts = detail.split('at line')
                if len(detail_parts) == 2:
                    endline, count = query_line_map.get(query, [(0, 0)])[0]  # Default to [(0, 0)] if query not found in map
                    line_no = endline - (count - int(detail_parts[1].strip()))
                    detail = f"{detail_parts[0]}at line {line_no}"
    
            else:
                endline, count = query_line_map.get(query, [(0, 0)])[0]
                line_no = (endline - count) + 1
                    
            # Generate URL for navigating to the specific line
            data_anchor = get_data_anchor(html_content, line_no)
            if data_anchor:
                url_with_anchor = generate_url_for_line_change(url, data_anchor)
                print("url_with_anchor",url_with_anchor)
                navigate_button = f"[**↗️Navigate to line {line_no}**]({url_with_anchor})"
            else:
                navigate_button = f"(Line {line_no})"
            
            # Add a dash if the 'at line' condition is false
            if not navigate_button:
                navigate_button = "-"
                    
            comment += f"| {idx} | {name} | {action} | {detail} | {navigate_button} |\n"

        comment += "</details>"

        comment += f"<details>\n<summary>📊 Profile Based Insights</summary>\n\n"

        # Create a table header
        comment += "| # | Name | Action | Detail | Let's Navigate |\n"
        comment += "| --- | --- | --- | --- | --- |\n"
        
        # Get HTML content of the page
        
        html_content = get_html_content(url)
        
        # Add insights to the table
        for idx, insight in enumerate(profile_insights, start=1):
            name = insight.get('name', '')
            action = insight.get('action', '')
            detail = insight.get('detail', '')
            
            navigate_button = ""  # Default value for the navigation column
            
            if 'at line' in detail:
                detail_parts = detail.split('at line')
                if len(detail_parts) == 2:
                    endline, count = query_line_map.get(query, [(0, 0)])[0]  # Default to [(0, 0)] if query not found in map
                    line_no = endline - (count - int(detail_parts[1].strip()))
                    detail = f"{detail_parts[0]}at line {line_no}"
    
            else:
                endline, count = query_line_map.get(query, [(0, 0)])[0]
                line_no = (endline - count) + 1
                    
            # Generate URL for navigating to the specific line
            data_anchor = get_data_anchor(html_content, line_no)
            if data_anchor:
                url_with_anchor = generate_url_for_line_change(url, data_anchor)
                print("url_with_anchor",url_with_anchor)
                navigate_button = f"[**↗️Navigate to line {line_no}**]({url_with_anchor})"
            else:
                navigate_button = f"(Line {line_no})"
            
            # Add a dash if the 'at line' condition is false
            if not navigate_button:
                navigate_button = "-"
                    
            comment += f"| {idx} | {name} | {action} | {detail} | {navigate_button} |\n"

        comment += "</details>"
    

    # Add Details section
    comment += f"<details>\n<summary>📋 Details</summary>\n\n"
    
    # Create a table header for Details
    comment += "| # | Attribute | Value |\n"
    comment += "| --- | --- | --- |\n"
    
    # Add details map as a table
    for idx, (key, value) in enumerate(details_map.items(), start=1):
        # key=re.sub(r'\w+', lambda m:m.group(0).capitalize(), key)
        if key in ['minCost', 'maxCost']:
            if value[0]==0.0:
                value_html='NA'
            else:
                value_html = f'$ {round(value[0], 6):.6f}'
            if key =='minCost':
                key='Min Estimated Cost'
            else:
                key='Max Estimated Cost'
        elif key == 'bytesScanned':
            if(value[0]==-1):
                value_html='NA'
            else:
                value_html = bytes_to_human_readable(value[0])
            key='Bytes Scanned'
        elif value[0] == "SUCCESS":
            value_html = "✅"
            key='Compilation'
        elif value[0] == "FAILURE":
            value_html = "❌"
            key='Compilation'
        else:
            value_html = value[0]
        
        comment += f"| {idx} | {key} | **{value_html}** |\n"
    
    comment += "</details>"
    
    return comment

def bytes_to_human_readable(bytes):
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    unit_index = 0
    bytes = int(bytes)  # Convert to integer
    while bytes >= 1024 and unit_index < len(units) - 1:
        bytes /= 1024
        unit_index += 1
    return f'{bytes:.2f} {units[unit_index]}'

def post_comment_on_pr(api_response, pr_number, github_token, repo_owner, repo_name, query_line_map, url1):
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
            profile_events = entry.get('profileInsights', [])

            details_map = {}
            # Extract key-value pairs excluding 'query' and 'insights'
            key_value_pairs = {key: value for key, value in entry.items() if key not in ['query', 'insights', 'profileInsights']}
            
            # Add key-value pairs to the details_map
            for key, value in key_value_pairs.items():
                details_map.setdefault(key, []).append(value)
            
            if query and (events or profile_events):
                comment = format_comment(query, events, profile_events, query_line_map, details_map, url1)
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
    print(comments)
    for comment in comments:
        if query in comment['body'] and "⚙️Status - ✅Resolved" not in comment['body']:
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

def post_comment_on_pr_query_wise(api_response, existing_comments, query_line_map, url1):
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
            profile_events = entry.get('profileInsights', [])

            details_map = {}
            # Extract key-value pairs excluding 'query' and 'insights'
            key_value_pairs = {key: value for key, value in entry.items() if key not in ['query', 'insights', 'profileInsights']}
            
            # Add key-value pairs to the details_map
            for key, value in key_value_pairs.items():
                details_map.setdefault(key, []).append(value)

            # Check if the query is not in existing_queries
            if query not in extracted_queriesq:

                if query and (events or profile_events):
                    # Create the comment body
                    comment_body = format_comment(query, events, profile_events, query_line_map, details_map, url1)
    
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
        url1 = f'https://github.com/{repo_name}/pull/{pr_number}/files'
        
        for filename, content in file_content.items():
            
            sql_statements, query_line_map = extract_sql_queries(content)
        print("sql_statements",sql_statements)
        print("query_line_map",query_line_map)
        
        # Send SQL queries to API
        api_response = send_to_api(sql_statements, api_endpoint, platform, unravel_token)
        print(api_response)
    
        # Post comment on PR
        if api_response.get("status") == 200:
            print(f"SQL Queries successfully processed . API Response: {api_response}")
        else:
            print(f"SQL Queries processing failed. API Response: {api_response}")
        
        post_response = post_comment_on_pr(api_response, pr_number, github_token, repo_owner, repo_name, query_line_map, url1)
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

        url1 = f'https://github.com/{repo_name}/pull/{pr_number}/files'
        # Extract SQL queries
        for filename, content in file_content.items():
            
            sql_statements, query_line_map = extract_sql_queries(content)
        
        # Send SQL queries to API
        api_response = send_to_api(sql_statements, api_endpoint, platform, unravel_token)
    
        # Post comment on PR
        if api_response.get("status") == 200:
            print(f"SQL Queries successfully processed . API Response: {api_response}")
        else:
            print(f"SQL Queries processing failed. API Response: {api_response}")
                
        update_comments(api_response, existing_comments)
        post_comment_on_pr_query_wise(api_response, existing_comments, query_line_map, url1)
