import duckdb
import logging

# Get logger for this module
logger = logging.getLogger(__name__)

# Database schema constants
CREATE_ORGANIZATIONS_TABLE = """
    CREATE TABLE IF NOT EXISTS organizations (
        name TEXT PRIMARY KEY
    )
"""

CREATE_REPOSITORIES_TABLE = """
    CREATE TABLE IF NOT EXISTS repositories (
        name TEXT,
        organization TEXT REFERENCES organizations(name),
        PRIMARY KEY (name, organization)
    )
"""

CREATE_ISSUES_TABLE = """
    CREATE TABLE IF NOT EXISTS issues (
        id TEXT PRIMARY KEY,
        number INTEGER NOT NULL,
        title TEXT,
        state TEXT,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL,
        closed_at TIMESTAMP,
        repository TEXT NOT NULL,
        user TEXT,
        assignee TEXT,
        organization TEXT NOT NULL,
        FOREIGN KEY (repository, organization) REFERENCES repositories(name, organization)
    )
"""

CREATE_EVENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY,
        issue_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL,
        actor TEXT,
        label_name TEXT, -- Used only for events of type "labeled" or "unlabeled"
        assignee_name TEXT, -- Used only for events of type "assigned" or "unassigned"
        comment_author TEXT, -- Used only for events of type "commented"
        comment_body TEXT, -- Used only for events of type "commented"
        FOREIGN KEY (issue_id) REFERENCES issues(id)
    )
"""

# SQL query constants for better readability
UPSERT_ISSUES_SQL = """
    INSERT INTO issues (id, number, title, state, created_at, updated_at, closed_at, repository, user, assignee, organization) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (id) DO UPDATE SET
        title = EXCLUDED.title,
        state = EXCLUDED.state,
        updated_at = EXCLUDED.updated_at,
        closed_at = EXCLUDED.closed_at,
        user = EXCLUDED.user,
        assignee = EXCLUDED.assignee
"""

UPSERT_EVENTS_SQL = """
    INSERT INTO events (id, issue_id, event_type, created_at, actor, label_name, assignee_name, comment_author, comment_body) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (id) DO NOTHING
"""


class DatabaseManager:
    def __init__(self, db_path='github_issues.duckdb'):
        self.db_path = db_path
        self.connection = None
    
    def connect(self):
        """Initialize database connection and create tables"""
        logger.info(f"Initializing DuckDB connection to {self.db_path}")
        self.connection = duckdb.connect(self.db_path)
        self._create_tables()
        logger.info("DuckDB connection initialized successfully")
        return self.connection
    
    def _create_tables(self):
        """Create all required tables"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
            
        logger.debug("Creating database tables if they don't exist")
        
        tables = [
            ("organizations", CREATE_ORGANIZATIONS_TABLE),
            ("repositories", CREATE_REPOSITORIES_TABLE),
            ("issues", CREATE_ISSUES_TABLE),
            ("events", CREATE_EVENTS_TABLE)
        ]
        
        for table_name, table_sql in tables:
            logger.debug(f"Creating table: {table_name}")
            self.connection.execute(table_sql)
        
        logger.debug("Database tables created successfully")
    
    def insert_organization(self, org_name):
        """Insert organization record"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
            
        try:
            logger.debug(f"Inserting organization: {org_name}")
            self.connection.execute("INSERT OR IGNORE INTO organizations (name) VALUES (?)", [org_name])
            self.connection.commit()
            logger.debug("Organization inserted successfully")
        except Exception as e:
            logger.error(f"Error inserting organization {org_name}: {e}")
            raise
    
    def insert_repository(self, repo_name, org_name):
        """Insert repository record"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
            
        try:
            logger.debug(f"Inserting repository: {repo_name} for organization: {org_name}")
            self.connection.execute("INSERT OR IGNORE INTO repositories (name, organization) VALUES (?, ?)", [repo_name, org_name])
            self.connection.commit()
            logger.debug("Repository inserted successfully")
        except Exception as e:
            logger.error(f"Error inserting repository {repo_name} for organization {org_name}: {e}")
            raise
    
    def bulk_upsert_issues(self, issues_data):
        """Bulk upsert issues with proper error handling"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
            
        if not issues_data:
            logger.debug("No issues data to upsert")
            return
            
        try:
            logger.debug(f"Bulk upserting {len(issues_data)} issues")
            self.connection.executemany(UPSERT_ISSUES_SQL, issues_data)
            logger.info(f"Successfully upserted {len(issues_data)} issues")
        except Exception as e:
            logger.error(f"Error bulk upserting {len(issues_data)} issues: {e}")
            raise
    
    def bulk_upsert_events(self, events_data):
        """Bulk upsert events with proper error handling"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
            
        if not events_data:
            logger.debug("No events data to upsert")
            return
            
        try:
            logger.debug(f"Bulk upserting {len(events_data)} events")
            self.connection.executemany(UPSERT_EVENTS_SQL, events_data)
            logger.info(f"Successfully upserted {len(events_data)} events")
        except Exception as e:
            logger.error(f"Error bulk upserting {len(events_data)} events: {e}")
            raise
    
    def commit(self):
        """Commit current transaction"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        self.connection.commit()
    
    def get_database_stats(self):
        """Get counts of all records in database"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
            
        logger.debug("Retrieving database statistics")
        
        stats = {}
        tables = ['issues', 'events', 'organizations', 'repositories']
        
        for table in tables:
            try:
                result = self.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                count = result[0] if result else 0
                stats[table] = count
                logger.debug(f"Table {table}: {count} records")
            except Exception as e:
                logger.error(f"Error getting stats for table {table}: {e}")
                stats[table] = 0
        
        return stats
    
    def close(self):
        """Close database connection"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Database connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")
                raise 