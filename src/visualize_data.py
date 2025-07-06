"""
GitHub Issues Data Visualization Script

This script connects to the DuckDB database and creates comprehensive visualizations
of GitHub issues lifecycle data using matplotlib.
Creates separate visualizations for each organization.

Note: The visualization script was developed with AI assistance. While functional, 
the implementation has not been reviewed thoroughly.
"""

import duckdb
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pathlib import Path
import warnings
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Get logger for this module
logger = logging.getLogger(__name__)

# Set up matplotlib style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")
warnings.filterwarnings('ignore')

class GitHubIssuesVisualizer:
    def __init__(self, db_path='github_issues.duckdb'):
        """Initialize the visualizer with database connection"""
        self.db_path = db_path
        self.connection = None
        self.connect()
    
    def connect(self):
        """Connect to the database"""
        try:
            self.connection = duckdb.connect(self.db_path)
            logger.info(f"Successfully connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise
    
    def get_organizations(self):
        """Get list of all organizations in the database"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        org_df = self.connection.execute("""
            SELECT o.name, COUNT(DISTINCT r.name) as repo_count
            FROM organizations o
            LEFT JOIN repositories r ON o.name = r.organization
            GROUP BY o.name
            ORDER BY o.name
        """).df()
        
        return org_df
    
    def get_database_stats(self, org_name=None):
        """Get and display database statistics, optionally filtered by organization"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        if org_name:
            logger.info(f"Database Statistics for {org_name}:")
        else:
            logger.info("Overall Database Statistics:")
        
        stats = {}
        
        # Organizations
        if org_name:
            org_count = 1
        else:
            org_result = self.connection.execute("SELECT COUNT(*) FROM organizations").fetchone()
            org_count = org_result[0] if org_result else 0
        stats['organizations'] = org_count
        logger.info(f"Organizations: {org_count:,}")
        
        # Repositories
        if org_name:
            repo_result = self.connection.execute(
                "SELECT COUNT(*) FROM repositories WHERE organization = ?", [org_name]
            ).fetchone()
        else:
            repo_result = self.connection.execute("SELECT COUNT(*) FROM repositories").fetchone()
        repo_count = repo_result[0] if repo_result else 0
        stats['repositories'] = repo_count
        logger.info(f"Repositories: {repo_count:,}")
        
        # Issues
        if org_name:
            issues_result = self.connection.execute(
                "SELECT COUNT(*) FROM issues WHERE organization = ?", [org_name]
            ).fetchone()
        else:
            issues_result = self.connection.execute("SELECT COUNT(*) FROM issues").fetchone()
        issues_count = issues_result[0] if issues_result else 0
        stats['issues'] = issues_count
        logger.info(f"Issues: {issues_count:,}")
        
        # Events
        if org_name:
            events_result = self.connection.execute("""
                SELECT COUNT(*) FROM events e
                JOIN issues i ON e.issue_id = i.id
                WHERE i.organization = ?
            """, [org_name]).fetchone()
        else:
            events_result = self.connection.execute("SELECT COUNT(*) FROM events").fetchone()
        events_count = events_result[0] if events_result else 0
        stats['events'] = events_count
        logger.info(f"Events: {events_count:,}")
        
        return stats
    
    def preview_data(self, org_name=None):
        """Display preview of key data tables, optionally filtered by organization"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
            
        if org_name:
            logger.info(f"Data Preview for {org_name}:")
        else:
            logger.info("Overall Data Preview:")
        
        # Preview issues
        logger.info("Issues Sample:")

        if org_name:
            issues_df = self.connection.execute("""
                SELECT id, number, title, state, created_at, updated_at, closed_at, 
                       repository, user, assignee, organization
                FROM issues 
                WHERE organization = ?
                LIMIT 5
            """, [org_name]).df()
        else:
            issues_df = self.connection.execute("""
                SELECT id, number, title, state, created_at, updated_at, closed_at, 
                       repository, user, assignee, organization
                FROM issues 
                LIMIT 5
            """).df()
        logger.debug(f"Issues sample data:\n{issues_df.to_string(index=False)}")
        
        # Preview events
        logger.info("Events Sample:")
        if org_name:
            events_df = self.connection.execute("""
                SELECT e.id, e.issue_id, e.event_type, e.created_at, e.actor, e.label_name, e.assignee_name
                FROM events e
                JOIN issues i ON e.issue_id = i.id
                WHERE i.organization = ?
                LIMIT 5
            """, [org_name]).df()
        else:
            events_df = self.connection.execute("""
                SELECT id, issue_id, event_type, created_at, actor, label_name, assignee_name
                FROM events 
                LIMIT 5
            """).df()
        logger.debug(f"Events sample data:\n{events_df.to_string(index=False)}")
        
        # Summary statistics
        logger.info("Issue Statistics:")
        if org_name:
            issue_stats = self.connection.execute("""
                SELECT 
                    state,
                    COUNT(*) as count,
                    ROUND(AVG(CASE WHEN closed_at IS NOT NULL 
                        THEN EXTRACT(EPOCH FROM (closed_at - created_at)) / 3600 
                        ELSE NULL END), 2) as avg_resolution_hours
                FROM issues
                WHERE organization = ?
                GROUP BY state
                ORDER BY count DESC
            """, [org_name]).df()
        else:
            issue_stats = self.connection.execute("""
                SELECT 
                    state,
                    COUNT(*) as count,
                    ROUND(AVG(CASE WHEN closed_at IS NOT NULL 
                        THEN EXTRACT(EPOCH FROM (closed_at - created_at)) / 3600 
                        ELSE NULL END), 2) as avg_resolution_hours
                FROM issues
                GROUP BY state
                ORDER BY count DESC
            """).df()
        logger.info(f"Issue statistics:\n{issue_stats.to_string(index=False)}")
        
        return issues_df, events_df, issue_stats
    
    def create_issue_state_distribution(self, org_name):
        """Create pie chart showing distribution of issue states for a specific organization"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        fig, ax = plt.subplots(figsize=(10, 8))
        
        # Query issue states for the organization
        df = self.connection.execute("""
            SELECT state, COUNT(*) as count
            FROM issues
            WHERE organization = ?
            GROUP BY state
            ORDER BY count DESC
        """, [org_name]).df()
        
        if len(df) == 0:
            ax.text(0.5, 0.5, f'No issues found for {org_name}', ha='center', va='center', 
                   transform=ax.transAxes, fontsize=14)
            ax.set_title(f'Issue State Distribution - {org_name}', fontsize=16, fontweight='bold', pad=20)
            plt.tight_layout()
            return fig
        
        # Create pie chart
        colors = ['#2E8B57', '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']
        pie_result = ax.pie(df['count'].tolist(), labels=df['state'].tolist(), 
                           autopct='%1.1f%%', startangle=90, colors=colors)
        
        # Unpack result properly - pie() returns either 2 or 3 elements
        if len(pie_result) == 3:
            wedges, texts, autotexts = pie_result
            # Enhance appearance
            plt.setp(autotexts, size=12, weight="bold", color='white')
        else:
            wedges, texts = pie_result
            autotexts = []
        
        plt.setp(texts, size=12, weight="bold")
        
        ax.set_title(f'Issue State Distribution - {org_name}', fontsize=16, fontweight='bold', pad=20)
        
        # Add count annotations
        for i, (state, count) in enumerate(zip(df['state'], df['count'])):
            ax.annotate(f'{count:,} issues', 
                       xy=(0, 0), xytext=(0.8, 0.8-i*0.15),
                       textcoords='axes fraction',
                       fontsize=10, ha='left')
        
        plt.tight_layout()
        return fig
    
    def create_issues_over_time(self, org_name):
        """Create timeline showing issue creation and closure over time for a specific organization"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
        
        # Query issues created over time
        created_df = self.connection.execute("""
            SELECT 
                DATE_TRUNC('week', created_at) as week,
                COUNT(*) as issues_created
            FROM issues
            WHERE created_at IS NOT NULL AND organization = ?
            GROUP BY week
            ORDER BY week
        """, [org_name]).df()
        
        # Query issues closed over time
        closed_df = self.connection.execute("""
            SELECT 
                DATE_TRUNC('week', closed_at) as week,
                COUNT(*) as issues_closed
            FROM issues
            WHERE closed_at IS NOT NULL AND organization = ?
            GROUP BY week
            ORDER BY week
        """, [org_name]).df()
        
        # Plot issues created
        if len(created_df) > 0:
            ax1.plot(created_df['week'], created_df['issues_created'], 
                    marker='o', linewidth=2.5, markersize=6, color='#2E8B57', label='Created')
        ax1.set_title(f'Issues Created Over Time - {org_name}', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Issues Created', fontsize=12)
        ax1.grid(True, alpha=0.3)
        ax1.legend()
        
        # Plot issues closed
        if len(closed_df) > 0:
            ax2.plot(closed_df['week'], closed_df['issues_closed'], 
                    marker='s', linewidth=2.5, markersize=6, color='#FF6B6B', label='Closed')
        ax2.set_title(f'Issues Closed Over Time - {org_name}', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Date', fontsize=12)
        ax2.set_ylabel('Issues Closed', fontsize=12)
        ax2.grid(True, alpha=0.3)
        ax2.legend()
        
        # Rotate x-axis labels for better readability
        for ax in [ax1, ax2]:
            ax.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        return fig
    
    def create_resolution_time_analysis(self, org_name):
        """Create histogram showing distribution of issue resolution times for a specific organization"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Query resolution times
        df = self.connection.execute("""
            SELECT 
                EXTRACT(EPOCH FROM (closed_at - created_at)) / 3600 as resolution_hours,
                EXTRACT(EPOCH FROM (closed_at - created_at)) / (24*3600) as resolution_days
            FROM issues
            WHERE closed_at IS NOT NULL 
            AND created_at IS NOT NULL
            AND closed_at > created_at
            AND organization = ?
        """, [org_name]).df()
        
        if len(df) > 0:
            # Histogram of resolution times in hours (capped at 720 hours = 30 days)
            resolution_hours_capped = df['resolution_hours'].clip(upper=720)
            ax1.hist(resolution_hours_capped, bins=50, alpha=0.7, color='#4ECDC4', edgecolor='black')
            ax1.set_title(f'Resolution Time Distribution - {org_name}', fontsize=14, fontweight='bold')
            ax1.set_xlabel('Resolution Time (hours, capped at 720)', fontsize=12)
            ax1.set_ylabel('Number of Issues', fontsize=12)
            ax1.grid(True, alpha=0.3)
            
            # Box plot for resolution times in days (capped at 90 days)
            resolution_days_capped = df['resolution_days'].clip(upper=90)
            ax2.boxplot(resolution_days_capped, vert=True, patch_artist=True,
                       boxprops=dict(facecolor='#96CEB4', alpha=0.7),
                       medianprops=dict(color='red', linewidth=2))
            ax2.set_title(f'Resolution Time Box Plot - {org_name}', fontsize=14, fontweight='bold')
            ax2.set_ylabel('Resolution Time (days, capped at 90)', fontsize=12)
            ax2.grid(True, alpha=0.3)
            
            # Add statistics text
            stats_text = f"""
            Count: {len(df):,}
            Mean: {df['resolution_hours'].mean():.1f} hours
            Median: {df['resolution_hours'].median():.1f} hours
            Std: {df['resolution_hours'].std():.1f} hours
            """
            ax2.text(0.02, 0.98, stats_text, transform=ax2.transAxes, 
                    verticalalignment='top', fontsize=10, 
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
        else:
            ax1.text(0.5, 0.5, f'No closed issues found for {org_name}', ha='center', va='center', transform=ax1.transAxes)
            ax2.text(0.5, 0.5, f'No closed issues found for {org_name}', ha='center', va='center', transform=ax2.transAxes)
        
        plt.tight_layout()
        return fig
    
    def create_top_contributors_analysis(self, org_name):
        """Create bar chart showing most active contributors for a specific organization"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Top issue creators
        creators_df = self.connection.execute("""
            SELECT user, COUNT(*) as issues_created
            FROM issues
            WHERE user IS NOT NULL AND organization = ?
            GROUP BY user
            ORDER BY issues_created DESC
            LIMIT 10
        """, [org_name]).df()
        
        # Top event actors
        actors_df = self.connection.execute("""
            SELECT e.actor, COUNT(*) as events_count
            FROM events e
            JOIN issues i ON e.issue_id = i.id
            WHERE e.actor IS NOT NULL AND i.organization = ?
            GROUP BY e.actor
            ORDER BY events_count DESC
            LIMIT 10
        """, [org_name]).df()
        
        # Plot top issue creators
        if len(creators_df) > 0:
            bars1 = ax1.bar(range(len(creators_df)), creators_df['issues_created'], 
                           color='#2E8B57', alpha=0.8)
            ax1.set_title(f'Top Issue Creators - {org_name}', fontsize=14, fontweight='bold')
            ax1.set_xlabel('Users', fontsize=12)
            ax1.set_ylabel('Issues Created', fontsize=12)
            ax1.set_xticks(range(len(creators_df)))
            ax1.set_xticklabels(creators_df['user'], rotation=45, ha='right')
            ax1.grid(True, alpha=0.3, axis='y')
            
            # Add value labels on bars
            for bar, value in zip(bars1, creators_df['issues_created']):
                ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                        str(value), ha='center', va='bottom', fontsize=10)
        else:
            ax1.text(0.5, 0.5, f'No issue creators found for {org_name}', ha='center', va='center', transform=ax1.transAxes)
        
        # Plot top event actors
        if len(actors_df) > 0:
            bars2 = ax2.bar(range(len(actors_df)), actors_df['events_count'], 
                           color='#FF6B6B', alpha=0.8)
            ax2.set_title(f'Most Active Event Actors - {org_name}', fontsize=14, fontweight='bold')
            ax2.set_xlabel('Actors', fontsize=12)
            ax2.set_ylabel('Events Count', fontsize=12)
            ax2.set_xticks(range(len(actors_df)))
            ax2.set_xticklabels(actors_df['actor'], rotation=45, ha='right')
            ax2.grid(True, alpha=0.3, axis='y')
            
            # Add value labels on bars
            for bar, value in zip(bars2, actors_df['events_count']):
                ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 10,
                        str(value), ha='center', va='bottom', fontsize=10)
        else:
            ax2.text(0.5, 0.5, f'No event actors found for {org_name}', ha='center', va='center', transform=ax2.transAxes)
        
        plt.tight_layout()
        return fig
    
    def create_event_types_analysis(self, org_name):
        """Create analysis of different event types for a specific organization"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Event type distribution
        events_df = self.connection.execute("""
            SELECT e.event_type, COUNT(*) as count
            FROM events e
            JOIN issues i ON e.issue_id = i.id
            WHERE i.organization = ?
            GROUP BY e.event_type
            ORDER BY count DESC
        """, [org_name]).df()
        
        # Events over time
        events_time_df = self.connection.execute("""
            SELECT 
                DATE_TRUNC('week', e.created_at) as week,
                e.event_type,
                COUNT(*) as count
            FROM events e
            JOIN issues i ON e.issue_id = i.id
            WHERE e.created_at IS NOT NULL
            AND e.event_type IN ('opened', 'closed', 'reopened', 'commented')
            AND i.organization = ?
            GROUP BY week, e.event_type
            ORDER BY week, e.event_type
        """, [org_name]).df()
        
        # Plot event type distribution
        if len(events_df) > 0:
            colors = plt.get_cmap('tab10')(np.linspace(0, 1, len(events_df)))
            bars = ax1.bar(range(len(events_df)), events_df['count'], color=colors, alpha=0.8)
            ax1.set_title(f'Event Types Distribution - {org_name}', fontsize=14, fontweight='bold')
            ax1.set_xlabel('Event Type', fontsize=12)
            ax1.set_ylabel('Count', fontsize=12)
            ax1.set_xticks(range(len(events_df)))
            ax1.set_xticklabels(events_df['event_type'], rotation=45, ha='right')
            ax1.grid(True, alpha=0.3, axis='y')
            
            # Add value labels
            for bar, value in zip(bars, events_df['count']):
                ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 10,
                        f'{value:,}', ha='center', va='bottom', fontsize=9)
        else:
            ax1.text(0.5, 0.5, f'No events found for {org_name}', ha='center', va='center', transform=ax1.transAxes)
        
        # Plot events over time
        if len(events_time_df) > 0:
            for event_type in events_time_df['event_type'].unique():
                event_data = events_time_df[events_time_df['event_type'] == event_type]
                ax2.plot(event_data['week'], event_data['count'], 
                        marker='o', linewidth=2, markersize=4, label=event_type)
            
            ax2.set_title(f'Event Types Over Time - {org_name}', fontsize=14, fontweight='bold')
            ax2.set_xlabel('Date', fontsize=12)
            ax2.set_ylabel('Count', fontsize=12)
            ax2.legend()
            ax2.grid(True, alpha=0.3)
            ax2.tick_params(axis='x', rotation=45)
        else:
            ax2.text(0.5, 0.5, f'No time-series events found for {org_name}', ha='center', va='center', transform=ax2.transAxes)
        
        plt.tight_layout()
        return fig
    
    def create_repository_breakdown(self, org_name):
        """Create repository breakdown analysis for a specific organization"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Issues by repository
        repos_df = self.connection.execute("""
            SELECT repository, COUNT(*) as issue_count
            FROM issues
            WHERE organization = ?
            GROUP BY repository
            ORDER BY issue_count DESC
        """, [org_name]).df()
        
        # Repository activity over time
        repo_activity_df = self.connection.execute("""
            SELECT 
                DATE_TRUNC('month', created_at) as month,
                repository,
                COUNT(*) as issues_created
            FROM issues
            WHERE organization = ? AND created_at IS NOT NULL
            GROUP BY month, repository
            ORDER BY month, repository
        """, [org_name]).df()
        
        # Plot issues by repository
        if len(repos_df) > 0:
            bars1 = ax1.bar(range(len(repos_df)), repos_df['issue_count'], 
                           color='#4ECDC4', alpha=0.8)
            ax1.set_title(f'Issues by Repository - {org_name}', fontsize=14, fontweight='bold')
            ax1.set_xlabel('Repository', fontsize=12)
            ax1.set_ylabel('Issue Count', fontsize=12)
            ax1.set_xticks(range(len(repos_df)))
            ax1.set_xticklabels(repos_df['repository'], rotation=45, ha='right')
            ax1.grid(True, alpha=0.3, axis='y')
            
            # Add value labels
            for bar, value in zip(bars1, repos_df['issue_count']):
                ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                        str(value), ha='center', va='bottom', fontsize=10)
        else:
            ax1.text(0.5, 0.5, f'No repositories found for {org_name}', ha='center', va='center', transform=ax1.transAxes)
        
        # Plot repository activity over time
        if len(repo_activity_df) > 0:
            for repo in repo_activity_df['repository'].unique():
                repo_data = repo_activity_df[repo_activity_df['repository'] == repo]
                ax2.plot(repo_data['month'], repo_data['issues_created'], 
                        marker='o', linewidth=2, markersize=4, label=repo)
            
            ax2.set_title(f'Repository Activity Over Time - {org_name}', fontsize=14, fontweight='bold')
            ax2.set_xlabel('Date', fontsize=12)
            ax2.set_ylabel('Issues Created', fontsize=12)
            ax2.legend()
            ax2.grid(True, alpha=0.3)
            ax2.tick_params(axis='x', rotation=45)
        else:
            ax2.text(0.5, 0.5, f'No repository activity data for {org_name}', ha='center', va='center', transform=ax2.transAxes)
        
        plt.tight_layout()
        return fig
    
    def create_organization_dashboard(self, org_name):
        """Create a comprehensive dashboard for a specific organization"""
        logger.info(f"Creating visualization dashboard for {org_name}")
        
        # Create individual visualizations
        figs = []
        
        logger.debug(f"Creating issue state distribution for {org_name}")
        fig1 = self.create_issue_state_distribution(org_name)
        figs.append((f"Issue State Distribution - {org_name}", fig1))
        
        logger.debug(f"Creating issues over time analysis for {org_name}")
        fig2 = self.create_issues_over_time(org_name)
        figs.append((f"Issues Over Time - {org_name}", fig2))
        
        logger.debug(f"Creating resolution time analysis for {org_name}")
        fig3 = self.create_resolution_time_analysis(org_name)
        figs.append((f"Resolution Time Analysis - {org_name}", fig3))
        
        logger.debug(f"Creating top contributors analysis for {org_name}")
        fig4 = self.create_top_contributors_analysis(org_name)
        figs.append((f"Top Contributors - {org_name}", fig4))
        
        logger.debug(f"Creating event types analysis for {org_name}")
        fig5 = self.create_event_types_analysis(org_name)
        figs.append((f"Event Types Analysis - {org_name}", fig5))
        
        logger.debug(f"Creating repository breakdown for {org_name}")
        fig6 = self.create_repository_breakdown(org_name)
        figs.append((f"Repository Breakdown - {org_name}", fig6))
        
        return figs
    
    def save_visualizations(self, figs, output_dir='visualizations'):
        """Save all visualizations to files"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        logger.info(f"Saving {len(figs)} visualizations to {output_path}")
        
        for name, fig in figs:
            # Clean filename by replacing spaces and special characters
            filename = name.lower().replace(' ', '_').replace('-', '_') + '.png'
            filepath = output_path / filename
            fig.savefig(filepath, dpi=300, bbox_inches='tight', facecolor='white')
            logger.debug(f"Saved: {filepath}")
            plt.close(fig)  # Close figure to free memory
        
        logger.info(f"All visualizations saved to {output_path}/")
    
    def run_full_analysis(self):
        """Run complete analysis and visualization for all organizations"""
        logger.info("Starting GitHub Issues Data Visualization Analysis")
        
        # Get all organizations
        orgs_df = self.get_organizations()
        logger.info(f"Found {len(orgs_df)} organizations:")
        for _, org in orgs_df.iterrows():
            logger.info(f"   - {org['name']}: {org['repo_count']} repositories")
        
        # Overall database stats
        self.get_database_stats()
        
        # Create visualizations for each organization
        all_figs = []
        
        for _, org in orgs_df.iterrows():
            org_name = org['name']
            
            logger.info(f"Processing Organization: {org_name}")
            
            # Get organization-specific stats
            self.get_database_stats(org_name)
            
            # Preview organization data
            self.preview_data(org_name)
            
            # Create organization dashboard
            org_figs = self.create_organization_dashboard(org_name)
            all_figs.extend(org_figs)
        
        # Save all visualizations
        self.save_visualizations(all_figs)
        
        logger.info("Analysis complete! Check the visualizations/ directory for saved plots.")
        logger.info(f"Generated {len(all_figs)} visualizations across {len(orgs_df)} organizations.")
        
        return all_figs
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed successfully")


def main():
    """Main function to run the visualization analysis"""
    visualizer = None
    try:
        # Initialize visualizer
        visualizer = GitHubIssuesVisualizer()
        
        # Run full analysis
        visualizer.run_full_analysis()
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}", exc_info=True)
        raise
    finally:
        if visualizer:
            visualizer.close()


if __name__ == "__main__":
    main() 