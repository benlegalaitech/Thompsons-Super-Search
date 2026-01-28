"""Query logging for smart search audit and improvement."""

import os
import sqlite3
import json
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

# Database location
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')
DB_PATH = os.path.join(DATA_DIR, 'search_logs.db')

# Retention period (days)
RETENTION_DAYS = int(os.environ.get('QUERY_LOG_RETENTION_DAYS', '90'))


def _ensure_data_dir():
    """Ensure the data directory exists."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)


def _get_connection() -> sqlite3.Connection:
    """Get a database connection."""
    _ensure_data_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def _db_connection():
    """Context manager for database connections."""
    conn = _get_connection()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    """Initialize the database schema."""
    with _db_connection() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS search_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                project_id TEXT NOT NULL,
                query_text TEXT NOT NULL,
                search_mode TEXT NOT NULL,
                query_plan TEXT,
                interpretation TEXT,
                result_count INTEGER,
                llm_latency_ms INTEGER,
                cache_hit INTEGER DEFAULT 0,
                error_message TEXT
            )
        ''')

        # Create indexes for common queries
        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_search_logs_timestamp
            ON search_logs(timestamp)
        ''')
        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_search_logs_project
            ON search_logs(project_id)
        ''')


def log_search(
    project_id: str,
    query_text: str,
    search_mode: str,
    query_plan: Optional[Dict[str, Any]] = None,
    interpretation: Optional[str] = None,
    result_count: Optional[int] = None,
    llm_latency_ms: Optional[int] = None,
    cache_hit: bool = False,
    error_message: Optional[str] = None
):
    """
    Log a search query.

    Args:
        project_id: The project being searched
        query_text: The user's search query
        search_mode: 'smart' or 'keyword'
        query_plan: The parsed QueryPlan as dict (for smart search)
        interpretation: LLM's interpretation of the query
        result_count: Number of results returned
        llm_latency_ms: Time taken for LLM call in milliseconds
        cache_hit: Whether the query plan was served from cache
        error_message: Error message if search failed
    """
    try:
        init_db()  # Ensure schema exists

        with _db_connection() as conn:
            conn.execute('''
                INSERT INTO search_logs
                (timestamp, project_id, query_text, search_mode, query_plan,
                 interpretation, result_count, llm_latency_ms, cache_hit, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                datetime.utcnow().isoformat(),
                project_id,
                query_text,
                search_mode,
                json.dumps(query_plan) if query_plan else None,
                interpretation,
                result_count,
                llm_latency_ms,
                1 if cache_hit else 0,
                error_message
            ))
    except Exception as e:
        # Log errors silently - don't break search for logging failures
        print(f"Warning: Failed to log search: {e}")


def cleanup_old_logs():
    """Remove logs older than retention period."""
    try:
        cutoff = datetime.utcnow() - timedelta(days=RETENTION_DAYS)

        with _db_connection() as conn:
            result = conn.execute(
                'DELETE FROM search_logs WHERE timestamp < ?',
                (cutoff.isoformat(),)
            )
            return result.rowcount
    except Exception as e:
        print(f"Warning: Failed to cleanup logs: {e}")
        return 0


def get_search_stats(project_id: Optional[str] = None, days: int = 7) -> Dict[str, Any]:
    """
    Get search statistics for a project.

    Args:
        project_id: Optional project to filter by
        days: Number of days to look back

    Returns:
        Dict with statistics
    """
    try:
        init_db()
        cutoff = datetime.utcnow() - timedelta(days=days)

        with _db_connection() as conn:
            # Base query conditions
            where_clause = "WHERE timestamp > ?"
            params = [cutoff.isoformat()]

            if project_id:
                where_clause += " AND project_id = ?"
                params.append(project_id)

            # Total searches
            total = conn.execute(
                f'SELECT COUNT(*) as count FROM search_logs {where_clause}',
                params
            ).fetchone()['count']

            # By mode
            by_mode = conn.execute(
                f'''SELECT search_mode, COUNT(*) as count
                    FROM search_logs {where_clause}
                    GROUP BY search_mode''',
                params
            ).fetchall()

            # Error rate
            errors = conn.execute(
                f'''SELECT COUNT(*) as count FROM search_logs
                    {where_clause} AND error_message IS NOT NULL''',
                params
            ).fetchone()['count']

            # Cache hit rate (smart search only)
            cache_stats = conn.execute(
                f'''SELECT
                    SUM(CASE WHEN cache_hit = 1 THEN 1 ELSE 0 END) as hits,
                    COUNT(*) as total
                    FROM search_logs
                    {where_clause} AND search_mode = 'smart' ''',
                params
            ).fetchone()

            # Average latency
            latency = conn.execute(
                f'''SELECT AVG(llm_latency_ms) as avg_latency
                    FROM search_logs
                    {where_clause} AND llm_latency_ms IS NOT NULL''',
                params
            ).fetchone()['avg_latency']

            # Zero-result queries
            zero_results = conn.execute(
                f'''SELECT COUNT(*) as count FROM search_logs
                    {where_clause} AND result_count = 0''',
                params
            ).fetchone()['count']

            return {
                'total_searches': total,
                'by_mode': {row['search_mode']: row['count'] for row in by_mode},
                'error_count': errors,
                'error_rate': errors / total if total > 0 else 0,
                'cache_hit_rate': cache_stats['hits'] / cache_stats['total'] if cache_stats['total'] > 0 else 0,
                'avg_latency_ms': round(latency) if latency else None,
                'zero_result_count': zero_results,
                'days': days
            }
    except Exception as e:
        return {'error': str(e)}


def get_common_queries(project_id: Optional[str] = None, limit: int = 20, days: int = 30) -> List[Dict[str, Any]]:
    """
    Get most common search queries.

    Args:
        project_id: Optional project to filter by
        limit: Max number of queries to return
        days: Number of days to look back

    Returns:
        List of query info dicts
    """
    try:
        init_db()
        cutoff = datetime.utcnow() - timedelta(days=days)

        with _db_connection() as conn:
            where_clause = "WHERE timestamp > ?"
            params = [cutoff.isoformat()]

            if project_id:
                where_clause += " AND project_id = ?"
                params.append(project_id)

            rows = conn.execute(
                f'''SELECT query_text, COUNT(*) as count,
                    AVG(result_count) as avg_results
                    FROM search_logs
                    {where_clause}
                    GROUP BY LOWER(query_text)
                    ORDER BY count DESC
                    LIMIT ?''',
                params + [limit]
            ).fetchall()

            return [
                {
                    'query': row['query_text'],
                    'count': row['count'],
                    'avg_results': round(row['avg_results']) if row['avg_results'] else 0
                }
                for row in rows
            ]
    except Exception as e:
        return []


def get_zero_result_queries(project_id: Optional[str] = None, limit: int = 50, days: int = 30) -> List[Dict[str, Any]]:
    """
    Get queries that returned zero results.

    Args:
        project_id: Optional project to filter by
        limit: Max number of queries to return
        days: Number of days to look back

    Returns:
        List of query info dicts
    """
    try:
        init_db()
        cutoff = datetime.utcnow() - timedelta(days=days)

        with _db_connection() as conn:
            where_clause = "WHERE timestamp > ? AND result_count = 0"
            params = [cutoff.isoformat()]

            if project_id:
                where_clause += " AND project_id = ?"
                params.append(project_id)

            rows = conn.execute(
                f'''SELECT query_text, interpretation, COUNT(*) as count
                    FROM search_logs
                    {where_clause}
                    GROUP BY LOWER(query_text)
                    ORDER BY count DESC
                    LIMIT ?''',
                params + [limit]
            ).fetchall()

            return [
                {
                    'query': row['query_text'],
                    'interpretation': row['interpretation'],
                    'count': row['count']
                }
                for row in rows
            ]
    except Exception as e:
        return []
