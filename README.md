# GitHub Issue Lifecycle Analysis

A comprehensive tool for analyzing GitHub issue lifecycle data from organizations. This project extracts, models, and visualizes GitHub issues and their associated events to provide insights into issue management patterns, contributor behavior, and project health metrics.

## 🏗️ Data Model

### DuckDB

DuckDB is used to store the collected data. It is an analytical database well-suited to support downstream analysis. During development, the DuckDB UI provided a convenient interface for database inspection and query testing.

### Tables

#### `organizations`

- `name` (TEXT, PRIMARY KEY): Organization name

#### `repositories`

- `name` (TEXT): Repository name
- `organization` (TEXT): Organization name (foreign key)
- PRIMARY KEY: `(name, organization)`

#### `issues`

- `id` (TEXT, PRIMARY KEY): GitHub issue ID
- `number` (INTEGER): Issue number within repository
- `title` (TEXT): Issue title
- `state` (TEXT): Current issue state (open/closed)
- `created_at` (TIMESTAMP): Issue creation timestamp
- `updated_at` (TIMESTAMP): Last update timestamp
- `closed_at` (TIMESTAMP): Issue closure timestamp (nullable)
- `repository` (TEXT): Repository name
- `user` (TEXT): Issue author username
- `assignee` (TEXT): Current assignee username (nullable)
- `organization` (TEXT): Organization name

#### `events`

- `id` (TEXT, PRIMARY KEY): Event ID
- `issue_id` (TEXT): Associated issue ID (foreign key)
- `event_type` (TEXT): Type of event (labeled, assigned, closed, etc.)
- `created_at` (TIMESTAMP): Event timestamp
- `actor` (TEXT): User who performed the action
- `label_name` (TEXT): Label name for label events (nullable)
- `assignee_name` (TEXT): Assignee name for assignment events (nullable)
- `comment_author` (TEXT): Author for comment events (nullable)
- `comment_body` (TEXT): Comment content (nullable)

> **Note**: Some event types like `cross-referenced` are excluded because they don't have unique IDs required for the events table primary key.

### Design Note

> **⚠️ Database Design Consideration**
>
> The current `events` table uses a denormalized structure with nullable columns (`label_name`, `assignee_name`, `comment_author`, `comment_body`) that are only populated for specific event types. This design was chosen for rapid development and simplicity within the assignment's time constraints.
>
> **Production Recommendation**: In a production environment, consider normalizing the database with separate tables for event-specific data (e.g., `label_events`, `assignment_events`, `comment_events`) to eliminate sparse columns and improve data integrity. This would follow database normalization best practices and provide better query performance for large datasets.

## 🔄 Incremental Update Strategy

Our system keeps GitHub issues in sync by only fetching and applying changes since the last run:

1. **Runtime Checkpoint Calculation**

   - For each repository, at the start of each run we query the `issues` table for the maximum `updated_at` value.
   - That value becomes our “checkpoint,” ensuring we pick up only new or changed issues.

2. **API-Side Filtering**

   - We pass the checkpoint into GitHub’s `since` parameter.
   - GitHub returns only issues updated after that timestamp, avoiding reprocessing all historical data.

3. **Ordered, Batched Processing**

   - When fetching the data from GitHub, we tune the query parameters to sort by `updated` in ascending order so we process oldest-first, preserving chronological consistency.
   - Data is handled in batches (e.g. 100 per page) to comply with GitHub's rate limits and pagination.

4. **Idempotent Upserts & Deduplication**
   - Issues are upserted: update existing records or insert new ones without duplicates.
   - Events use GitHub’s unique event IDs as primary keys to prevent reprocessing.

---

### Why Ascending Sort Matters

Sorting by `updated` timestamp in **ascending** order ensures that:

- **Chronological Integrity**: We process in the exact order changes occurred.
- **Checkpoint Safety**: We always know exactly how far we've gotten by tracking the latest timestamp we've processed, and can safely resume from that point.
- **Race-Condition Defense**: If an issue is updated mid-run, we won't miss it as it'll appear in the next run.

---

### Safe to Rerun at Any Time

Because everything is idempotent and checkpointed, you can restart the update job whenever you like:

- **Upserts Prevent Duplicates**: Repeated runs only update or insert as needed.
- **Event Deduplication**: Unique IDs ensure we never insert the same event twice.
- **Checkpoint Resilience**: On failure, the next run picks up from the last successful timestamp. Only data that wasn't inserted will be processed again.

---

### Checkpoint Retrieval Example

```python
def get_repo_last_issue_updated_at(org_name, db_connection):
    query = """
    SELECT repository, MAX(updated_at) AS last_updated_at
    FROM issues
    WHERE organization = ?
    GROUP BY repository
    """
    return db_connection.execute(query, [org_name]).fetchall()
```

---

### Fetching Only New or Changed Issues

We rely on GitHub’s server-side filtering to minimize data transfer:

```python
issues_params = {
    "since": "2024-01-01T10:30:45.123456Z",  # last processed checkpoint
    "sort": "updated",
    "direction": "asc",
    "state": "all",
    "per_page": 100
}
response = requests.get(api_url, params=issues_params, headers=...)
```

1. **`since`**: Fetches only issues updated after our checkpoint.
2. **`sort=updated&direction=asc`**: Ensures full coverage in chronological order.
3. **`state=all`**: Captures open and closed issues alike.

## 🚀 Installation & Setup

### Prerequisites

- Python 3.13+
- GitHub Personal Access Token with `repo` permissions
- UV package manager (recommended) or pip
  - To install uv, follow the instructions at: https://docs.astral.sh/uv/getting-started/installation/

> **Why UV?**
>
> This project uses uv as the preferred package manager because it eliminates the need to manually set up virtual environments. With uv, you can run `uv sync` and `uv run` commands directly without creating and activating a venv first.

### Installation

#### Option 1: Using UV (Recommended)

1. Clone the repository:

```bash
git clone <repository-url>
cd nagomi-assignment
```

2. Install dependencies:

```bash
uv sync
```

3. Configure the application:

```bash
cp config.yaml.example config.yaml
# Edit config.yaml with your settings
```

#### Option 2: Using Standard Python Tools

1. Clone the repository:

```bash
git clone <repository-url>
cd nagomi-assignment
```

2. Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate
```

**Note:** If a `.venv` directory already exists, you can use it instead:

```bash
source .venv/bin/activate
```

3. Install dependencies:

```bash
pip install -e .
```

**Note:** If the editable install fails, you can install dependencies directly:

```bash
pip install duckdb polars pyyaml requests matplotlib seaborn numpy
```

Or use the newer pip approach that supports `pyproject.toml`:

```bash
pip install .
```

4. Configure the application:

```bash
cp config.yaml.example config.yaml
# Edit config.yaml with your settings
```

### Configuration

Edit `config.yaml`:

```yaml
# GitHub personal access token
github_token: your_token_here

# Organization to analyze
org_name: your-org-name

# API pagination size
per_page: 100

# Log level: debug or info
log_level: info
```

## 📊 Usage

### Data Collection

Run the main data collection script:

**With UV:**

```bash
uv run src/main.py
```

**With Standard Python:**

```bash
python src/main.py
```

### Data Visualization

Generate visualizations for collected data:

> **Note**: The data visualization code was developed with Cursor AI assistance. While functional, the visualization implementation could benefit from further refinement and structure.

**With UV:**

```bash
uv run src/visualize_data.py
```

**With Standard Python:**

```bash
python src/visualize_data.py
```

## 📈 Analysis

### Data Collection Scope

For this home assignment, the tool demonstrates the ability to collect and store all required GitHub issue metadata including:

- Core issue data (creation, updates, state changes)
- Timeline events (labels, assignments, comments)
- Actor/contributor information
- Timestamps for lifecycle analysis

While the current implementation focuses on the core metadata specified in the assignment requirements, the flexible schema design allows for collecting additional fields if needed. The database structure (particularly the events table) uses a denormalized approach for rapid development, though a more normalized design would be recommended for production use.

The following example analyses showcase how the collected data can be used to derive the insights requested in the assignment specification:

### Pre-built SQL Queries

- **`state_duration.sql`**: Time spent in different issue states
- **`label_analysis.sql`**: Duration of label assignments
- **`assignee_history.sql`**: Assignment change patterns
- **`comment_history.sql`**: Comment activity analysis
- **`final_resolution_time.sql`**: Time to resolution metrics
- **`reopened_count.sql`**: Issue reopening patterns
- **`actor_involvement.sql`**: Contributor activity analysis
- **`all_transitions.sql`**: Complete state transition history

### Generated Visualizations

- **Issue State Distribution**: Pie chart of open vs closed issues
- **Issues Over Time**: Timeline of issue creation and closure
- **Resolution Time Analysis**: Histogram of issue resolution times
- **Top Contributors**: Bar chart of most active contributors
- **Event Types Analysis**: Distribution of different event types
- **Repository Breakdown**: Issue distribution across repositories

## 🔍 Assumptions

### Assumptions

1. **GitHub API Access**: Valid GitHub token with appropriate permissions
2. **Organization Visibility**: Public repositories or private repos with access
3. **Data Consistency**: GitHub API provides consistent data across requests
4. **Rate Limits**: GitHub API rate limits are respected (5000 requests/hour for authenticated users)
5. **Issue Definition**: Only actual issues are processed (pull requests are filtered out)

## 🛠️ Technical Architecture

### Key Components

- **`main.py`**: Primary data collection orchestrator
- **`database.py`**: Database management and schema definition
- **`visualize_data.py`**: Data visualization and chart generation
- **`queries/`**: SQL analysis queries
- **`config.yaml`**: Application configuration

### GitHub REST API Endpoints

The project uses the following GitHub REST API endpoints to collect issue lifecycle data:

#### 1. **Organization Repositories**

```
GET /orgs/{org}/repos
```

- **Purpose**: Retrieve all repositories for a given organization
- **Parameters**:
  - `per_page`: Number of results per page (max 100)
  - `page`: Page number for pagination
- **Response**: Array of repository objects containing metadata
- **Usage**: Initial discovery of repositories to process

#### 2. **Repository Issues**

```
GET /repos/{owner}/{repo}/issues
```

- **Purpose**: Fetch all issues for a specific repository
- **Parameters**:
  - `state`: Filter by issue state (hardcoded to `all` to get both open and closed issues)
  - `per_page`: Number of results per page (max 100)
  - `page`: Page number for pagination
  - `sort`: Sort order (`created`, `updated`, `comments`)
  - `direction`: Sort direction (`asc`, `desc`)
  - `since`: Only return issues updated after this timestamp (ISO 8601)
- **Response**: Array of issue objects with core metadata
- **Usage**: Retrieve issue data with incremental update support

#### 3. **Issue Timeline Events**

```
GET /repos/{owner}/{repo}/issues/{issue_number}/timeline
```

- **Purpose**: Fetch timeline events for a specific issue
- **Parameters**:
  - `per_page`: Number of results per page (max 100)
  - `page`: Page number for pagination
- **Response**: Array of timeline event objects
- **Usage**: Collect detailed event history for issue lifecycle analysis

#### API Headers Used

All requests include the following headers:

- `Accept: application/vnd.github+json`
- `Authorization: Bearer {token}`
- `X-GitHub-Api-Version: 2022-11-28`

#### Rate Limiting & Pagination

- **Rate Limits**: 5,000 requests per hour for authenticated users
- **Pagination**: Uses GitHub's Link header for cursor-based pagination
- **Retry Logic**: Automatic retry with exponential backoff for rate limit and server errors
- **Incremental Updates**: Uses `since` parameter to only fetch updated issues
