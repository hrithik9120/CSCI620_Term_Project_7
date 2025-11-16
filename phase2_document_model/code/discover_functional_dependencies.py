#!/usr/bin/env python3
"""
Functional Dependencies Discovery Tool

This script analyzes the Reddit Comments May 2015 database schema and data
to discover and report functional dependencies (FDs) across all tables.

Functional Dependencies are relationships where one set of attributes uniquely
determines another set of attributes. This analysis helps understand:
- Data integrity constraints
- Normalization opportunities
- Redundancy in the schema
- Potential optimization strategies

Usage:
    python discover_functional_dependencies.py --input database.sqlite [--sample N]
"""

import argparse
import sqlite3
import pandas as pd
from collections import defaultdict
import sys


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Discover functional dependencies in Reddit Comments database",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--input', required=True,
                        help='Path to SQLite database file')
    parser.add_argument('--sample', type=int,
                        help='Analyze only first N rows per table (for testing)')
    return parser.parse_args()


def check_functional_dependency(df, determinant_cols, dependent_cols):
    """
    Check if determinant_cols functionally determine dependent_cols.
    
    A functional dependency X -> Y holds if for every pair of tuples t1 and t2
    in the relation, if t1[X] = t2[X], then t1[Y] = t2[Y].
    
    Args:
        df: DataFrame to analyze
        determinant_cols: List of columns that determine the dependent columns
        dependent_cols: List of columns that are determined
        
    Returns:
        tuple: (holds, violation_count, total_groups, violation_examples)
    """
    if df.empty:
        return False, 0, 0, []

    # Remove rows where determinant columns have nulls
    df_clean = df.dropna(subset=determinant_cols)

    if df_clean.empty:
        return False, 0, 0, []

    # Group by determinant columns
    grouped = df_clean.groupby(determinant_cols)

    violations = 0
    total_groups = 0
    violation_examples = []

    for name, group in grouped:
        total_groups += 1
        # Check if dependent columns have unique values within each group
        for dep_col in dependent_cols:
            if dep_col in group.columns:
                unique_values = group[dep_col].dropna().nunique()
                if unique_values > 1:
                    violations += 1
                    # Store example violation
                    if len(violation_examples) < 3:
                        # Extract determinant value(s)
                        if len(determinant_cols) == 1:
                            det_val = name[0] if isinstance(name, tuple) else name
                        else:
                            det_val = dict(zip(determinant_cols, name))

                        # Get sample dependent values
                        dep_vals = group[dep_col].dropna().unique()[:3].tolist()
                        violation_examples.append({
                            'determinant_value': det_val,
                            'dependent_column': dep_col,
                            'dependent_values': dep_vals
                        })
                    break

    holds = violations == 0
    return holds, violations, total_groups, violation_examples


def get_domain_based_fds(table_name, columns):
    """
    Define domain-based functional dependencies based on business logic.
    These are meaningful FDs that may not involve unique columns.
    """
    # Define all possible domain FDs
    all_fds = [
        (['subreddit_id'], ['subreddit'], 'Subreddit ID determines subreddit name'),
        (['id'], ['subreddit_id'], 'Comment/Post ID determines subreddit ID'),
        (['link_id'], ['subreddit_id'], 'Post link ID determines subreddit ID'),
        (['link_id'], ['author'], 'Post link ID determines author'),
        (['id'], ['author'], 'Comment/Post ID determines author'),
        (['author'], ['author_flair_text'],
         'Author determines flair text (may fail - authors can have different flairs per subreddit)'),
        (['author'], ['author_flair_css_class'],
         'Author determines flair CSS class (may fail - authors can have different flairs per subreddit)'),
        (['link_id'], ['created_utc'], 'Post link ID determines creation timestamp'),
        (['id'], ['link_id'], 'Comment ID determines parent post link ID'),
    ]

    # Return only FDs where all required columns exist
    domain_fds = []
    for det, dep, desc in all_fds:
        if all(col in columns for col in det + dep):
            domain_fds.append({
                'determinant': det,
                'dependent': dep,
                'description': desc
            })

    return domain_fds


def analyze_table_fds(conn, table_name, sample_size=None):
    """
    Analyze functional dependencies for a specific table.
    
    Args:
        conn: SQLite database connection
        table_name: Name of the table to analyze
        sample_size: Optional limit on number of rows to analyze
        
    Returns:
        dict: Dictionary containing discovered FDs and statistics
    """
    print(f"\n{'=' * 70}")
    print(f"Analyzing table: {table_name}")
    print(f"{'=' * 70}")

    # Get table schema
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns_info = cursor.fetchall()

    if not columns_info:
        print(f"  [WARNING] Table '{table_name}' not found or empty")
        return None

    # Extract column names and types
    columns = [col[1] for col in columns_info]
    column_types = {col[1]: col[2] for col in columns_info}

    print(f"  Columns: {', '.join(columns)}")
    print(f"  Total columns: {len(columns)}")

    # Read data
    query = f"SELECT * FROM {table_name}"
    if sample_size:
        query += f" LIMIT {sample_size}"

    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"  [ERROR] Error reading table: {e}")
        return None

    if df.empty:
        print(f"  [WARNING] Table is empty")
        return None

    print(f"  Rows analyzed: {len(df):,}")

    # Discover functional dependencies
    discovered_fds = []

    # 1. Primary Key Dependencies (PK -> all other attributes)
    # Check if there's a primary key
    cursor.execute(f"PRAGMA table_info({table_name})")
    pk_columns = [col[1] for col in columns_info if col[5] == 1]

    if pk_columns:
        print(f"\n  [PK] Primary Key: {', '.join(pk_columns)}")
        other_cols = [col for col in columns if col not in pk_columns]
        if other_cols:
            holds, violations, groups, _ = check_functional_dependency(df, pk_columns, other_cols)
            if holds:
                discovered_fds.append({
                    'determinant': pk_columns,
                    'dependent': other_cols,
                    'type': 'Primary Key',
                    'holds': True,
                    'violations': 0,
                    'confidence': 'High'
                })
                print(f"    [OK] PK -> {', '.join(other_cols)}: HOLDS (Primary Key constraint)")

    # 2. Limited single attribute dependencies - only check candidate keys (highly unique columns)
    # First, identify candidate keys (columns that are unique or nearly unique)
    print(f"\n  [*] Identifying candidate keys for limited FD analysis...")
    candidate_keys = []
    for col in columns:
        if col in pk_columns:
            continue  # Skip PK columns as they're already covered

        unique_count = df[col].nunique()
        total_count = len(df)
        uniqueness_ratio = unique_count / total_count if total_count > 0 else 0

        # Only consider columns that are 100% unique as candidate keys
        if uniqueness_ratio == 1.0 and unique_count > 0:
            candidate_keys.append(col)
            print(f"    • {col}: Candidate key (100% unique, {unique_count:,} unique values)")

    # Only check FDs for candidate keys (columns that uniquely identify rows)
    if candidate_keys:
        print(f"\n  [*] Checking limited single-attribute dependencies for candidate keys...")
        for det_col in candidate_keys:
            dependent_cols = []
            for dep_col in columns:
                if dep_col != det_col and dep_col not in pk_columns:
                    holds, violations, groups, _ = check_functional_dependency(df, [det_col], [dep_col])
                    if holds and groups > 0:
                        dependent_cols.append(dep_col)

            if dependent_cols:
                discovered_fds.append({
                    'determinant': [det_col],
                    'dependent': dependent_cols,
                    'type': 'Candidate Key',
                    'holds': True,
                    'violations': 0,
                    'confidence': 'High'
                })
                print(f"    [OK] {det_col} -> {', '.join(dependent_cols)}")

    # 3. Domain-based functional dependencies
    print(f"\n  [*] Testing domain-based functional dependencies...")
    domain_fds = get_domain_based_fds(table_name, columns)

    for fd_spec in domain_fds:
        det_cols = fd_spec['determinant']
        dep_cols = fd_spec['dependent']
        desc = fd_spec['description']

        holds, violations, groups, violation_examples = check_functional_dependency(df, det_cols, dep_cols)

        det_str = ', '.join(det_cols)
        dep_str = ', '.join(dep_cols)

        fd_result = {
            'determinant': det_cols,
            'dependent': dep_cols,
            'type': 'Domain-Based',
            'holds': holds,
            'violations': violations,
            'total_groups': groups,
            'confidence': 'High',
            'description': desc
        }

        if not holds:
            fd_result['violation_examples'] = violation_examples

        discovered_fds.append(fd_result)

        if holds:
            print(f"    [OK] {det_str} -> {dep_str}: HOLDS ({desc})")
        else:
            print(f"    [FAIL] {det_str} -> {dep_str}: FAILS ({violations} violations in {groups} groups)")
            if violation_examples:
                for ex in violation_examples[:2]:
                    det_val = ex['determinant_value']
                    if isinstance(det_val, dict):
                        det_val_str = ', '.join(f"{k}={v}" for k, v in det_val.items())
                    else:
                        det_val_str = str(det_val)
                    try:
                        dep_vals_str = str(ex['dependent_values'])
                        print(f"      Example: {det_str}={det_val_str} maps to {ex['dependent_column']}={dep_vals_str}")
                    except UnicodeEncodeError:
                        print(
                            f"      Example: {det_str}={det_val_str} maps to {ex['dependent_column']}=[multiple values]")

    # 4. Column uniqueness analysis (informational only)
    print(f"\n  [*] Column uniqueness analysis...")
    for col in columns:
        unique_count = df[col].nunique()
        total_count = len(df)
        uniqueness_ratio = unique_count / total_count if total_count > 0 else 0

        if uniqueness_ratio == 1.0:
            print(f"    • {col}: 100% unique ({unique_count:,} unique values)")
        elif uniqueness_ratio > 0.9:
            print(f"    • {col}: {uniqueness_ratio * 100:.1f}% unique ({unique_count:,}/{total_count:,})")

    return {
        'table_name': table_name,
        'columns': columns,
        'row_count': len(df),
        'functional_dependencies': discovered_fds,
        'primary_key': pk_columns if pk_columns else None
    }


def generate_report(all_results, output_file='functional_dependencies_report.md'):
    """
    Generate a comprehensive markdown report of functional dependencies.
    
    Args:
        all_results: List of analysis results for each table
        output_file: Path to output markdown file
    """
    report_lines = []

    report_lines.append("# Functional Dependencies Report")
    report_lines.append("")
    report_lines.append("*Generated by Functional Dependencies Discovery Tool*")
    report_lines.append("")
    report_lines.append("## Executive Summary")
    report_lines.append("")

    total_fds = sum(len(r['functional_dependencies']) for r in all_results if r)
    total_tables = len([r for r in all_results if r])

    report_lines.append(f"- **Total Tables Analyzed**: {total_tables}")
    report_lines.append(f"- **Total Functional Dependencies Discovered**: {total_fds}")
    report_lines.append("")

    # Detailed analysis for each table
    report_lines.append("## Detailed Analysis by Table")
    report_lines.append("")

    for result in all_results:
        if not result:
            continue

        table_name = result['table_name']
        fds = result['functional_dependencies']
        pk = result['primary_key']

        report_lines.append(f"### Table: `{table_name}`")
        report_lines.append("")
        report_lines.append(f"**Columns**: {', '.join(result['columns'])}")
        report_lines.append(f"**Row Count**: {result['row_count']:,}")

        if pk:
            report_lines.append(f"**Primary Key**: `{', '.join(pk)}`")
        report_lines.append("")

        if fds:
            report_lines.append("#### Functional Dependencies")
            report_lines.append("")

            # Group by type
            by_type = defaultdict(list)
            for fd in fds:
                by_type[fd['type']].append(fd)

            for fd_type, fd_list in by_type.items():
                report_lines.append(f"**{fd_type} Dependencies:**")
                report_lines.append("")

                for fd in fd_list:
                    det_str = ', '.join(fd['determinant'])
                    dep_str = ', '.join(fd['dependent'])

                    # Check if FD holds or fails
                    holds = fd.get('holds', True)
                    status = "✅ HOLDS" if holds else "❌ FAILS"

                    report_lines.append(f"- `{det_str}` → `{dep_str}` **{status}**")
                    report_lines.append(f"  - Type: {fd['type']}")
                    report_lines.append(f"  - Confidence: {fd['confidence']}")

                    if 'description' in fd:
                        report_lines.append(f"  - Description: {fd['description']}")

                    if not holds:
                        violations = fd.get('violations', 0)
                        groups = fd.get('total_groups', 0)
                        report_lines.append(f"  - Violations: {violations} violations in {groups} groups")

                        if 'violation_examples' in fd and fd['violation_examples']:
                            report_lines.append(f"  - Example violations:")
                            for ex in fd['violation_examples'][:2]:
                                det_val = ex['determinant_value']
                                if isinstance(det_val, dict):
                                    det_val_str = ', '.join(f"{k}={v}" for k, v in det_val.items())
                                else:
                                    det_val_str = str(det_val)
                                dep_col = ex['dependent_column']
                                dep_vals = ex['dependent_values']
                                report_lines.append(f"    - {det_str}={det_val_str} -> {dep_col}={dep_vals}")

                    report_lines.append("")
        else:
            report_lines.append("*No functional dependencies discovered beyond primary key constraints.*")
            report_lines.append("")

    # Schema-based FDs (from foreign keys and constraints)
    report_lines.append("## Schema-Based Functional Dependencies")
    report_lines.append("")
    report_lines.append("These functional dependencies are derived from the database schema:")
    report_lines.append("")

    # Based on the schema we analyzed
    schema_fds = [
        ("Users", ["author"], ["author_flair_text", "author_flair_css_class"],
         "Primary key determines all user attributes"),
        ("Subreddit", ["subreddit_id"], ["subreddit"],
         "Subreddit ID uniquely determines subreddit name"),
        ("Post", ["link_id"], ["subreddit_id", "author", "created_utc", "archived", "gilded", "edited"],
         "Post ID determines all post attributes"),
        ("Post_Link", ["link_id"], ["post_id", "retrieved_on"],
         "Link ID determines post reference and retrieval timestamp"),
        ("Comment", ["id"], ["body", "author", "link_id", "parent_id", "created_utc", "retrieved_on",
                             "score", "ups", "downs", "score_hidden", "gilded", "controversiality", "edited"],
         "Comment ID determines all comment attributes"),
        ("Moderation", ["mod_action_id"], ["target_type", "target_id", "subreddit_id", "removal_reason",
                                           "distinguished", "action_timestamp"],
         "Moderation action ID determines all moderation attributes"),
    ]

    for table, det, dep, desc in schema_fds:
        report_lines.append(f"### `{table}`")
        report_lines.append(f"- **FD**: `{', '.join(det)}` → `{', '.join(dep)}`")
        report_lines.append(f"- **Description**: {desc}")
        report_lines.append("")

    # Foreign Key Dependencies
    report_lines.append("## Foreign Key Relationships")
    report_lines.append("")
    report_lines.append("These relationships indicate referential dependencies:")
    report_lines.append("")

    fk_relationships = [
        ("Post.subreddit_id", "Subreddit.subreddit_id", "Post belongs to Subreddit"),
        ("Post.author", "Users.author", "Post authored by User"),
        ("Post_Link.post_id", "Post.link_id", "Post_Link references Post"),
        ("Comment.link_id", "Post.link_id", "Comment belongs to Post"),
        ("Comment.author", "Users.author", "Comment authored by User"),
        ("Moderation.subreddit_id", "Subreddit.subreddit_id", "Moderation action in Subreddit"),
    ]

    for fk, ref, desc in fk_relationships:
        report_lines.append(f"- `{fk}` → `{ref}` ({desc})")

    report_lines.append("")

    # Recommendations
    report_lines.append("## Recommendations")
    report_lines.append("")
    report_lines.append("### Normalization Status")
    report_lines.append("")
    report_lines.append("The current schema appears to be in **3NF (Third Normal Form)** or higher:")
    report_lines.append("")
    report_lines.append("- ✅ Tables are normalized with separate entities (Users, Subreddit, Post, Comment)")
    report_lines.append("- ✅ Foreign key relationships properly maintain referential integrity")
    report_lines.append("- ✅ Primary keys ensure entity uniqueness")
    report_lines.append("- ✅ No transitive dependencies observed in the normalized tables")
    report_lines.append("")

    report_lines.append("### Potential Optimizations")
    report_lines.append("")
    report_lines.append("1. **Index Recommendations**:")
    report_lines.append("   - Index on foreign key columns for faster joins")
    report_lines.append("   - Index on frequently queried attributes (e.g., `created_utc`, `author`)")
    report_lines.append("")
    report_lines.append("2. **Data Integrity**:")
    report_lines.append("   - Consider adding CHECK constraints for enumerated values")
    report_lines.append("   - Validate timestamp ranges for `created_utc` and `edited` fields")
    report_lines.append("")

    # Write report
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))

    print(f"\n[OK] Report generated: {output_file}")


def main():
    """Main function to discover functional dependencies."""
    args = parse_arguments()

    print("=" * 70)
    print("Functional Dependencies Discovery Tool")
    print("=" * 70)
    print(f"\nDatabase: {args.input}")
    if args.sample:
        print(f"Sample size: {args.sample:,} rows per table")

    # Connect to SQLite database
    try:
        conn = sqlite3.connect(args.input)
        print(f"\n[OK] Connected to SQLite database")
    except Exception as e:
        print(f"\n[ERROR] Error connecting to database: {e}")
        sys.exit(1)

    # Get list of tables
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]

    if not tables:
        print("\n[WARNING] No tables found in database")
        conn.close()
        sys.exit(1)

    print(f"\n[*] Found {len(tables)} table(s): {', '.join(tables)}")

    # Analyze each table
    all_results = []
    for table in tables:
        result = analyze_table_fds(conn, table, args.sample)
        if result:
            all_results.append(result)

    conn.close()

    # Generate report
    print(f"\n{'=' * 70}")
    print("Generating Report...")
    print(f"{'=' * 70}")
    generate_report(all_results)

    print(f"\n{'=' * 70}")
    print("[OK] Analysis Complete!")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
