"""Module for generating comparison reports"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import os

def generate_comparison_report(source_df: pd.DataFrame, target_df: pd.DataFrame,
                           column_mapping: Dict[str, str], join_keys: Optional[List[str]],
                           output_path: str) -> None:
    """Generate pandas-based comparison report"""
    try:
        # Get source and target columns
        source_cols = list(column_mapping.keys())
        target_cols = [column_mapping[col] for col in source_cols]
        
        # Select relevant columns and copy DataFrames
        source_compare_df = source_df[source_cols].copy()
        target_compare_df = target_df[target_cols].copy()
        
        # Rename target columns to match source
        target_compare_df.columns = source_cols
        
        # Convert all columns to string and handle nulls
        for col in source_cols:
            source_compare_df[col] = source_compare_df[col].fillna('').astype(str)
            target_compare_df[col] = target_compare_df[col].fillna('').astype(str)
        
        # Perform merge
        if isinstance(join_keys, (list, tuple)) and join_keys:
            # Use provided join keys if they exist in source columns
            valid_keys = [k for k in join_keys if k in source_cols]
            if valid_keys:
                merged_df = pd.merge(
                    source_compare_df,
                    target_compare_df,
                    on=valid_keys,
                    how='outer',
                    indicator=True
                )
            else:
                # Fall back to index-based comparison if no valid join keys
                merged_df = pd.merge(
                    source_compare_df,
                    target_compare_df,
                    left_index=True,
                    right_index=True,
                    how='outer',
                    indicator=True
                )
        else:
            # Use index-based comparison if no join keys provided
            merged_df = pd.merge(
                source_compare_df,
                target_compare_df,
                left_index=True,
                right_index=True,
                how='outer',
                indicator=True
            )
        
        # Calculate statistics
        stats = {
            'total_rows_source': len(source_compare_df),
            'total_rows_target': len(target_compare_df),
            'matching_rows': len(merged_df[merged_df['_merge'] == 'both']),
            'source_only_rows': len(merged_df[merged_df['_merge'] == 'left_only']),
            'target_only_rows': len(merged_df[merged_df['_merge'] == 'right_only'])
        }
        
        # Calculate match percentage
        total_rows = len(source_compare_df)
        match_percentage = (stats['matching_rows'] / total_rows * 100) if total_rows > 0 else 0
        
        # Generate summary
        summary = f"""
        Comparison Summary:
        - Total Rows in Source: {stats['total_rows_source']}
        - Total Rows in Target: {stats['total_rows_target']}
        - Matching Rows: {stats['matching_rows']}
        - Source Only Rows: {stats['source_only_rows']}
        - Target Only Rows: {stats['target_only_rows']}
        - Match Percentage: {match_percentage:.2f}%
        """
        
        # Generate column statistics
        col_stats = []
        for col in source_cols:
            col_stats.append({
                'Column': col,
                'Source_Count': len(source_compare_df[col].dropna()),
                'Source_Unique': source_compare_df[col].nunique(),
                'Target_Count': len(target_compare_df[col].dropna()),
                'Target_Unique': target_compare_df[col].nunique(),
                'Match_Status': 'Match' if source_compare_df[col].equals(target_compare_df[col]) else 'Mismatch'
            })
        
        col_stats_df = pd.DataFrame(col_stats)
        col_stats_html = col_stats_df.to_html(classes='table table-striped', index=False)
        
        # Generate HTML for mismatched records
        source_only = merged_df[merged_df['_merge'] == 'left_only']
        target_only = merged_df[merged_df['_merge'] == 'right_only']
        
        source_only_html = source_only.to_html(classes='table table-striped', index=False) if not source_only.empty else '<p>No records unique to source</p>'
        target_only_html = target_only.to_html(classes='table table-striped', index=False) if not target_only.empty else '<p>No records unique to target</p>'
        
        # Write HTML report
        with open(output_path, 'w') as f:
            f.write(f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <title>Data Comparison Report</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    .report {{ max-width: 1200px; margin: 0 auto; }}
                    .section {{ margin: 20px 0; padding: 20px; border: 1px solid #ddd; border-radius: 5px; }}
                    .match {{ color: green; }}
                    .mismatch {{ color: red; }}
                    table {{ border-collapse: collapse; width: 100%; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f5f5f5; }}
                </style>
            </head>
            <body>
                <div class="report">
                    <h1>Data Comparison Report</h1>
                    <div class="section">
                        <h2>Summary</h2>
                        <pre>{summary}</pre>
                    </div>
                    <div class="section">
                        <h2>Column Statistics</h2>
                        {col_stats_html}
                    </div>
                    <div class="section">
                        <h2>Mismatched Records</h2>
                        <h3>Records only in Source:</h3>
                        {source_only_html}
                        <h3>Records only in Target:</h3>
                        {target_only_html}
                    </div>
                </div>
            </body>
            </html>
            """)
            
    except Exception as e:
        # Generate error report
        error_html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head><title>Comparison Error Report</title></head>
        <body>
            <h1>Error Generating Comparison Report</h1>
            <p>An error occurred: {str(e)}</p>
            <p>Please check that:</p>
            <ul>
                <li>All required columns exist in both datasets</li>
                <li>Join keys are valid column names</li>
                <li>Data types are compatible for comparison</li>
            </ul>
        </body>
        </html>
        """
        with open(output_path, 'w') as f:
            f.write(error_html)
        raise Exception(f"Error generating comparison report: {str(e)}")
