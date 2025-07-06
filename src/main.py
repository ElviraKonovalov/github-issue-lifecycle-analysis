# uv run src/main.py

"""
GitHub Issues Data Collection Script

Simple approach with SQL queries extracted to database.py for better maintainability.
Uses DatabaseManager methods for cleaner database operations.
"""

from pathlib import Path
import requests
import yaml
import re
import time
import logging

# Load config
cfg = yaml.safe_load(Path("config.yaml").read_text())
is_debug = cfg['log_level'] == 'debug'

import polars as pl

from database import DatabaseManager

# Set up logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def handle_rate_limit_response(response):
    """
    Handle rate limiting based on GitHub's response headers
    Returns the number of seconds to wait before retrying
    """
    # Buffer to add to wait times to prevent immediate re-hitting of rate limits
    RATE_LIMIT_BUFFER = 2  # seconds
    
    # Check for retry-after header first
    retry_after = response.headers.get('retry-after')
    if retry_after:
        try:
            wait_seconds = int(retry_after) + RATE_LIMIT_BUFFER
            logger.warning(f"Rate limited - waiting {wait_seconds} seconds (retry-after: {retry_after} + {RATE_LIMIT_BUFFER}s buffer)")
            return wait_seconds
        except ValueError:
            logger.debug("Invalid retry-after header value")
            pass
    
    # Check for x-ratelimit-remaining header
    remaining = response.headers.get('x-ratelimit-remaining')
    if remaining == '0':
        reset_time = response.headers.get('x-ratelimit-reset')
        if reset_time:
            try:
                reset_timestamp = int(reset_time)
                current_timestamp = int(time.time())
                calculated_wait = max(0, reset_timestamp - current_timestamp)
                wait_seconds = calculated_wait + RATE_LIMIT_BUFFER
                logger.warning(f"Rate limit exhausted - waiting {wait_seconds} seconds (calculated: {calculated_wait} + {RATE_LIMIT_BUFFER}s buffer)")
                return wait_seconds
            except ValueError:
                logger.debug("Invalid x-ratelimit-reset header value")
                pass
    
    # Default: wait at least one minute for rate limit errors
    if response.status_code in [403, 429]:
        default_wait = 60 + RATE_LIMIT_BUFFER
        logger.warning(f"Rate limited - waiting {default_wait} seconds (default: 60 + {RATE_LIMIT_BUFFER}s buffer)")
        return default_wait
    
    return 0


def make_api_request_with_retry(url, headers, params=None, max_retries=4):
    """
    Make an API request with GitHub rate limiting aware retry logic
    
    Handles rate limits (403, 429) and server errors (5xx) with appropriate backoff
    """
    attempt = 0
    exponential_wait = 1  # Start with 1 second for exponential backoff
    
    logger.debug(f"Making API request to {url} with params: {params}")
    
    while attempt < max_retries:
        try:
            start_time = time.time()
            response = requests.get(url, headers=headers, params=params)
            response_time = time.time() - start_time
            
            # Log rate limit information
            remaining = response.headers.get('x-ratelimit-remaining')
            reset_time = response.headers.get('x-ratelimit-reset')
            if remaining:
                logger.debug(f"Rate limit remaining: {remaining}")
            
            # Success - return response
            if response.status_code == 200:
                logger.debug(f"API GET {url} - Status: {response.status_code} - Time: {response_time:.2f}s")
                return response
            
            # Handle rate limiting
            if response.status_code in [403, 429]:
                wait_seconds = handle_rate_limit_response(response)
                if wait_seconds > 0:
                    logger.warning(f"Attempt {attempt + 1}/{max_retries} failed with rate limit. Waiting {wait_seconds} seconds...")
                    time.sleep(wait_seconds)
                    attempt += 1
                    continue
            
            # Handle server errors (5xx) with exponential backoff
            elif response.status_code in [500, 502, 503, 504]:
                if attempt < max_retries - 1:
                    logger.warning(f"Attempt {attempt + 1}/{max_retries} failed with server error {response.status_code}. Waiting {exponential_wait} seconds...")
                    time.sleep(exponential_wait)
                    exponential_wait = min(exponential_wait * 2, 60)  # Cap at 60 seconds
                    attempt += 1
                    continue
            
            # For other errors, return the response to let caller handle it
            logger.error(f"API request failed with status {response.status_code}: {response.text[:200]}")
            return response
            
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed with exception: {e}. Waiting {exponential_wait} seconds...")
                time.sleep(exponential_wait)
                exponential_wait = min(exponential_wait * 2, 60)  # Cap at 60 seconds
                attempt += 1
                continue
            else:
                # Re-raise the exception on final attempt
                logger.error(f"API request failed after {max_retries} attempts: {e}")
                raise
    
    # If we've exhausted all retries, return the last response
    logger.error(f"API request exhausted all {max_retries} attempts")
    return response


def get_org_issues_and_timeline(org_name, github_token, db, repo_last_issue_updated_at=[], db_manager=None):
    """
    Fetch issues and their timeline events from an organization and its repositories using GitHub REST API
    Note: Currently limited to first repository to avoid rate limits
    """
    logger.info(f"Starting data collection for organization: {org_name}")
    
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {github_token}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    # Get organization repositories with pagination
    repos = []
    page = 1
    
    logger.debug(f"Fetching repositories for organization: {org_name}")
    
    while True:
        repos_url = f"https://api.github.com/orgs/{org_name}/repos"
        repos_params = {
            "per_page": cfg['per_page'],
            "page": page
        }
        
        repos_response = make_api_request_with_retry(repos_url, headers, repos_params)
        
        if repos_response.status_code != 200:
            logger.error(f"Error fetching repos (page {page}): {repos_response.status_code}")
            return
        
        page_repos = repos_response.json()
        
        # If no repos returned, we've reached the end
        if not page_repos:
            break
        
        repos.extend(page_repos)
        
        # Parse the Link header to determine if there are more pages
        link_header = repos_response.headers.get('Link', '')
        links = parse_link_header(link_header)
        has_more_pages = 'next' in links
        
        if not has_more_pages:
            break
        
        page += 1
    
    logger.info(f"Found {len(repos)} repositories in {org_name} across {page} pages")
    
    if not repos:
        logger.warning("No repositories found")
        return
    
    logger.info(f"Processing {len(repos)} repositories")

    for repo in repos:
        # Insert repository record first using db_manager
        if db_manager:
            db_manager.insert_repository(repo.get('name'), org_name)
        
        repo_name = repo.get('name')
        since = repo_last_issue_updated_at.get(repo_name) if repo_name in repo_last_issue_updated_at else None
        if since:
            logger.debug(f"Processing repository {repo_name} since {since}")
        else:
            logger.debug(f"Processing repository {repo_name} from the beginning")
        
        get_repo_issues_and_timeline(repo, org_name, headers, db, since)






def get_repo_issues_and_timeline(repo, org_name, headers, db, since=None):
    """
    Fetch issues and timeline events for a repository, processing page by page
    """
    repo_name = repo.get('name')
    logger.info(f"Processing repository: {repo_name}")


    # Process issues page by page
    page = 1
    total_issues = 0
    total_events = 0
    
    # Get DatabaseManager instance for cleaner database operations
    db_manager = DatabaseManager()
    db_manager.connection = db  # Use existing connection
    
    while True:
        logger.debug(f"Processing page {page} for repository {repo_name}")
        
        # Fetch one page of issues
        issues_page, has_more_pages = get_issues_page(org_name, repo_name, headers, page, since)
        
        # If no issues returned, we've reached the end
        if not issues_page:
            logger.debug(f"No more issues found for repository {repo_name}, stopping at page {page}")
            break
        
        logger.debug(f"Found {len(issues_page)} issues on page {page} for repository {repo_name}")
        
        # Process timeline events and prepare issues data in a single loop
        page_timeline_events = []
        issues_data = []
        
        for issue in issues_page:
            # Get timeline events for this issue
            timeline_events = get_issue_timeline(issue, org_name, repo_name, headers)
            page_timeline_events.extend(timeline_events)
            
            # Prepare issue data for bulk insert
            user_login = issue.get('user', {}).get('login') if issue.get('user') else None
            assignee_login = issue.get('assignee', {}).get('login') if issue.get('assignee') else None
            
            issues_data.append([
                issue.get('id'), issue.get('number'), issue.get('title'), issue.get('state'), 
                issue.get('created_at'), issue.get('updated_at'), issue.get('closed_at'), 
                repo_name, user_login, assignee_login, org_name
            ])
        
        logger.debug(f"Found {len(page_timeline_events)} timeline events for page {page} of repository {repo_name}")
        
        # Prepare events data for bulk insert
        events_data = []
        for event in page_timeline_events:
            # Extract event data correctly from GitHub API response
            event_type = event.get('event')  # GitHub uses 'event', not 'event_type'
            actor_login = event.get('actor', {}).get('login') if event.get('actor') else None

            # ################################################################ #
            # NOTE: For the purposes of this home assignment and considering
            #       the time constraint we are keeping extra data in the event
            #       table for events that need it. 
            #       This is not a good practice and should be avoided. 
            #       In a real-life scenario, we would normalize the database
            #       and would have extra tables for the extra data to avoid
            #       having empty values under event-type specific columns.
            # ################################################################ #

            # Extra data for labeled and unlabeled events
            label_name = None
            if event_type == 'labeled' or event_type == 'unlabeled':
                label = event.get('label')
                label_name = label.get('name') if label else None

            # Extra data for assigned and unassigned events
            assignee_name = None
            if event_type == 'assigned' or event_type == 'unassigned':
                assignee = event.get('assignee')
                assignee_name = assignee.get('login') if assignee else None

            # Extra data for commented events
            comment_author = None
            comment_body = None
            if event_type == 'commented':
                user = event.get('user')
                comment_author = user.get('login') if user else None
                comment_body = event.get('body')

            # Use GitHub's unique event ID - skip events without IDs
            event_id = event.get('id')
            if not event_id:
                logger.debug(f"Skipping {event_type} event without GitHub ID")
                continue
            
            events_data.append([
                str(event_id), event.get('issue_id'), event_type, event.get('created_at'), actor_login, label_name, assignee_name, comment_author, comment_body
            ])
        
        # Bulk upsert using DatabaseManager methods
        db_manager.bulk_upsert_issues(issues_data)
        db_manager.bulk_upsert_events(events_data)
        db_manager.commit()
        
        page_issues, page_events = len(issues_page), len(page_timeline_events)
        
        logger.debug(f"Processed {page_events} timeline events for page {page} of repository {repo_name}")
        
        total_issues += page_issues
        total_events += page_events
        
        # Check if we should continue to next page
        if not has_more_pages:
            logger.debug(f"Reached end of issues for repository {repo_name} (no more pages after page {page})")
            break
        
        page += 1
        


    logger.info(f"Repository {repo_name} processing complete: {total_issues} issues, {total_events} timeline events across {page} pages")

def parse_link_header(link_header):
    """
    Parse GitHub's Link header to extract pagination information
    Returns a dictionary with rel types as keys and URLs as values
    Example: {'next': 'https://api.github.com/...', 'last': 'https://api.github.com/...'}
    """
    if not link_header:
        return {}
    
    links = {}
    # Split by comma and parse each link
    for link_part in link_header.split(','):
        link_part = link_part.strip()
        # Extract URL and rel using regex (this is what github recommends)
        match = re.match(r'<([^>]+)>;\s*rel="([^"]+)"', link_part)
        if match:
            url, rel = match.groups()
            links[rel] = url
    
    return links

def get_issues_page(org_name, repo_name, headers, page, since=None):
    """
    Fetch a single page of issues for a repository. Only returns actual issues (not pull requests)
    Returns tuple: (actual_issues, has_more_pages)
    """
    issues_url = f"https://api.github.com/repos/{org_name}/{repo_name}/issues"
    issues_params = {
        "state": "all",  # Get both open and closed issues
        "per_page": cfg['per_page'],  # Maximum per page
        "sort": "updated",
        "direction": "asc",
        "since": since,  # ISO 8601 format
        "page": page
    }
    
    issues_response = make_api_request_with_retry(issues_url, headers, issues_params)
    
    if issues_response.status_code != 200:
        logger.error(f"Error fetching issues for {repo_name} (page {page}): {issues_response.status_code}")
        return [], False
    
    issues = issues_response.json()
    
    # If no issues returned, we've reached the end
    if not issues:
        return [], False
    
    # Parse the Link header to determine if there are more pages
    link_header = issues_response.headers.get('Link', '')
    links = parse_link_header(link_header)
    has_more_pages = 'next' in links
    
    # Filter out pull requests to get only actual issues
    actual_issues = [issue for issue in issues if 'pull_request' not in issue]
    
    logger.debug(f"Fetched {len(issues)} items from {repo_name} page {page}, {len(actual_issues)} are actual issues")
    
    return actual_issues, has_more_pages

    

def get_issue_timeline(issue, org_name, repo_name, headers):
    """
    Fetch timeline events for a specific issue
    """
    item_number = issue.get('number')
    logger.debug(f"Processing issue #{item_number} from {repo_name}")
    
    all_events = []
    page = 1
    
    while True:
        timeline_url = f"https://api.github.com/repos/{org_name}/{repo_name}/issues/{item_number}/timeline"
        timeline_params = {
            "per_page": 100,  # Maximum per page for timeline events
            "page": page
        }
        
        timeline_response = make_api_request_with_retry(timeline_url, headers, timeline_params)
        
        if timeline_response.status_code != 200:
            logger.error(f"Error fetching timeline for issue #{item_number} (page {page}): {timeline_response.status_code}")
            break
        
        events = timeline_response.json()
        
        # If no events returned, we've reached the end
        if not events:
            break
        
        # Add issue context to each event
        for event in events:
            event['issue_number'] = item_number
            event['issue_id'] = issue.get('id')
        
        all_events.extend(events)
        
        # Parse the Link header to determine if there are more pages
        link_header = timeline_response.headers.get('Link', '')
        links = parse_link_header(link_header)
        has_more_pages = 'next' in links
        
        if not has_more_pages:
            break
        
        page += 1
    
    if page > 1:
        logger.debug(f"Fetched {len(all_events)} timeline events across {page} pages for issue #{item_number}")
    
    return all_events
    
def get_repo_last_issue_updated_at(org_name, db):
    """
    Get the last issue updated at for each repository
    Returns:
        {
            "repo-1": "2024-01-01T00:00:00Z",
            "repo-2": "2025-01-01T00:00:00Z"
        }
    """
    result = db.execute(f"SELECT repository, MAX(updated_at) FROM issues WHERE organization = '{org_name}' GROUP BY repository").fetchall()
    return {repo_name: last_updated for repo_name, last_updated in result}


def main():
    """
    Main function to collect and model issue lifecycle data
    """
    # Set log level based on configuration
    if is_debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("Starting GitHub Issues Data Collection")
    
    try:
        # Use configuration from config.yaml
        org_name = cfg['org_name']
        github_token = cfg['github_token']
        
        logger.info(f"Configuration loaded - Organization: {org_name}, Log Level: {cfg['log_level']}")

        # Initialize database manager and use context manager for automatic cleanup
        db_manager = DatabaseManager()
        db_connection = db_manager.connect()

        # Setup organization
        db_manager.insert_organization(org_name)

        logger.info("Collecting GitHub issue lifecycle data...")

        # Show current database state
        logger.info("Current database state:")
        stats = db_manager.get_database_stats()
        for table, count in stats.items():
            logger.info(f"{table.capitalize()}: {count}")

        repo_last_issue_updated_at = get_repo_last_issue_updated_at(org_name, db_connection)
        logger.debug(f"Repository last update timestamps: {repo_last_issue_updated_at}")

        # Example of repo_last_issue_updated_at format:
        # repo_last_issue_updated_at = {
        #     "airflow": "2024-01-01T00:00:00Z",
        #     "spark": "2024-02-01T00:00:00Z", 
        #     "hadoop": "2024-03-01T00:00:00Z"
        # }
        # Fetch all data
        get_org_issues_and_timeline(org_name, github_token, db_connection, repo_last_issue_updated_at, db_manager)

        logger.info("Data collection completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Received Ctrl+C. Exiting...")
        
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}", exc_info=True)
        
    finally:
        # Ensure database connection is closed
        try:
            if 'db_manager' in locals():
                db_manager.close()
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")
        logger.info("Application shutdown complete.")


if __name__ == "__main__":
    main()
