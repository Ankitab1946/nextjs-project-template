"""Module for generating various comparison reports"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import os
from datetime import datetime
from ydata_profiling import ProfileReport
import datacompy
from openpyxl.styles import PatternFill, Font
from openpyxl.utils import get_column_letter
from config import EXCEL_STYLES, REPORTS_DIR

class ReportGenerator:
    @staticmethod
    def generate_regression_report(source_df: pd.DataFrame, target_df: pd.DataFrame, 
                                 column_mapping: Dict[str, str], output_path: str) -> None:
        """Generate regression report with multiple checks"""
        
        writer = pd.ExcelWriter(output_path, engine='openpyxl')
        
        # Generate Aggregation Check
        ReportGenerator._generate_aggregation_check(source_df, target_df, column_mapping, writer)
        
        # Generate Count Check
        ReportGenerator._generate_count_check(source_df, target_df, writer)
        
        # Generate Distinct Check
        ReportGenerator._generate_distinct_check(source_df, target_df, column_mapping, writer)
        
        writer.save()

    @staticmethod
    def _generate_aggregation_check(source_df: pd.DataFrame, target_df: pd.DataFrame, 
                                  column_mapping: Dict[str, str], writer: pd.ExcelWriter) -> None:
        """Generate aggregation check for numeric columns"""
        
        agg_data = []
        for source_col, target_col in column_mapping.items():
            # Check if columns are numeric
            if pd.api.types.is_numeric_dtype(source_df[source_col]) and pd.api.types.is_numeric_dtype(target_df[target_col]):
                source_sum = source_df[source_col].sum()
                target_sum = target_df[target_col].sum()
                result = 'PASS' if np.isclose(source_sum, target_sum, rtol=1e-05) else 'FAIL'
                
                agg_data.append({
                    'Source Column': source_col,
                    'Target Column': target_col,
                    'Source Sum': source_sum,
                    'Target Sum': target_sum,
                    'Result': result
                })
        
        if agg_data:
            df_agg = pd.DataFrame(agg_data)
            df_agg.to_excel(writer, sheet_name='AggregationCheck', index=False)
            
            # Apply conditional formatting
            worksheet = writer.sheets['AggregationCheck']
            for idx, row in enumerate(df_agg['Result'], start=2):  # start=2 to skip header
                cell = worksheet[f'E{idx}']
                style = EXCEL_STYLES['PASS'] if row == 'PASS' else EXCEL_STYLES['FAIL']
                cell.fill = PatternFill(start_color=style['fill']['fgColor'], end_color=style['fill']['fgColor'], fill_type='solid')
                cell.font = Font(color=style['font']['color'])

    @staticmethod
    def _generate_count_check(source_df: pd.DataFrame, target_df: pd.DataFrame, 
                            writer: pd.ExcelWriter) -> None:
        """Generate count check report"""
        
        count_data = {
            'Source Count': len(source_df),
            'Target Count': len(target_df),
            'Result': 'PASS' if len(source_df) == len(target_df) else 'FAIL'
        }
        
        df_count = pd.DataFrame([count_data])
        df_count.to_excel(writer, sheet_name='CountCheck', index=False)
        
        # Apply conditional formatting
        worksheet = writer.sheets['CountCheck']
        cell = worksheet['C2']  # Result column, first data row
        style = EXCEL_STYLES['PASS'] if count_data['Result'] == 'PASS' else EXCEL_STYLES['FAIL']
        cell.fill = PatternFill(start_color=style['fill']['fgColor'], end_color=style['fill']['fgColor'], fill_type='solid')
        cell.font = Font(color=style['font']['color'])

    @staticmethod
    def _generate_distinct_check(source_df: pd.DataFrame, target_df: pd.DataFrame, 
                               column_mapping: Dict[str, str], writer: pd.ExcelWriter) -> None:
        """Generate distinct value check for non-numeric columns"""
        
        distinct_data = []
        for source_col, target_col in column_mapping.items():
            # Check if columns are non-numeric
            if not pd.api.types.is_numeric_dtype(source_df[source_col]):
                source_distinct = source_df[source_col].nunique()
                target_distinct = target_df[target_col].nunique()
                
                source_values = set(source_df[source_col].dropna().unique())
                target_values = set(target_df[target_col].dropna().unique())
                
                values_match = source_values == target_values
                count_match = source_distinct == target_distinct
                result = 'PASS' if values_match and count_match else 'FAIL'
                
                distinct_data.append({
                    'Source Column': source_col,
                    'Target Column': target_col,
                    'Source Distinct Count': source_distinct,
                    'Target Distinct Count': target_distinct,
                    'Source Values': ', '.join(map(str, sorted(source_values))),
                    'Target Values': ', '.join(map(str, sorted(target_values))),
                    'Result': result
                })
        
        if distinct_data:
            df_distinct = pd.DataFrame(distinct_data)
            df_distinct.to_excel(writer, sheet_name='DistinctCheck', index=False)
            
            # Apply conditional formatting
            worksheet = writer.sheets['DistinctCheck']
            for idx, row in enumerate(df_distinct['Result'], start=2):  # start=2 to skip header
                cell = worksheet[f'G{idx}']
                style = EXCEL_STYLES['PASS'] if row == 'PASS' else EXCEL_STYLES['FAIL']
                cell.fill = PatternFill(start_color=style['fill']['fgColor'], end_color=style['fill']['fgColor'], fill_type='solid')
                cell.font = Font(color=style['font']['color'])

    @staticmethod
    def generate_side_by_side_report(source_df: pd.DataFrame, target_df: pd.DataFrame, 
                                   column_mapping: Dict[str, str], join_keys: List[Tuple[str, str]], 
                                   output_path: str) -> None:
        """Generate side by side comparison report"""
        
        # Prepare DataFrames for comparison
        source_cols = list(column_mapping.keys())
        target_cols = [column_mapping[col] for col in source_cols]
        
        # Merge DataFrames based on join keys
        if join_keys:
            source_join_cols = [key[0] for key in join_keys]
            target_join_cols = [key[1] for key in join_keys]
            
            merged_df = pd.merge(
                source_df[source_cols], 
                target_df[target_cols],
                left_on=source_join_cols,
                right_on=target_join_cols,
                how='outer',
                indicator=True
            )
            
            # Find differences
            diff_mask = merged_df['_merge'] != 'both'
            diff_df = merged_df[diff_mask].copy()
            
            if not diff_df.empty:
                # Rename columns to show source/target
                diff_df.columns = [f'Source_{col}' if col in source_cols else f'Target_{col}' 
                                 for col in diff_df.columns]
                
                # Save to Excel
                diff_df.to_excel(output_path, index=False)
                return True
            else:
                # Create empty DataFrame with message
                pd.DataFrame({'Message': ['No differences found']}).to_excel(output_path, index=False)
                return False
        else:
            # If no join keys, compare row by row
            comparison_df = pd.DataFrame()
            for s_col, t_col in column_mapping.items():
                comparison_df[f'Source_{s_col}'] = source_df[s_col]
                comparison_df[f'Target_{t_col}'] = target_df[t_col]
            
            comparison_df.to_excel(output_path, index=False)
            return True

    @staticmethod
    def generate_profile_reports(source_df: pd.DataFrame, target_df: pd.DataFrame, 
                               timestamp: str) -> Dict[str, str]:
        """Generate Y-Data profiling reports"""
        
        reports = {}
        
        # Create reports directory
        os.makedirs(REPORTS_DIR, exist_ok=True)
        
        # Generate source profile
        source_profile = ProfileReport(
            source_df,
            title="Source Data Profile Report",
            minimal=False,
            correlations={"cramers": True},
            vars={"num": {"low_categorical_threshold": 0}}
        )
        source_path = os.path.join(REPORTS_DIR, f"SourceProfile_{timestamp}.html")
        source_profile.to_file(source_path)
        reports['source'] = source_path
        
        # Generate target profile
        target_profile = ProfileReport(
            target_df,
            title="Target Data Profile Report",
            minimal=False,
            correlations={"cramers": True},
            vars={"num": {"low_categorical_threshold": 0}}
        )
        target_path = os.path.join(REPORTS_DIR, f"TargetProfile_{timestamp}.html")
        target_profile.to_file(target_path)
        reports['target'] = target_path
        
        # Generate comparison profile
        comparison_df = pd.DataFrame()
        for col in source_df.columns:
            comparison_df[f'Source_{col}'] = source_df[col]
        for col in target_df.columns:
            comparison_df[f'Target_{col}'] = target_df[col]
            
        comparison_profile = ProfileReport(
            comparison_df,
            title="Source vs Target Comparison Profile",
            minimal=False,
            correlations={"cramers": True},
            vars={"num": {"low_categorical_threshold": 0}}
        )
        comparison_path = os.path.join(REPORTS_DIR, f"ComparisonProfile_{timestamp}.html")
        comparison_profile.to_file(comparison_path)
        reports['comparison'] = comparison_path
        
        return reports

    @staticmethod
    def generate_datacompy_report(source_df: pd.DataFrame, target_df: pd.DataFrame,
                                column_mapping: Dict[str, str], join_keys: List[Tuple[str, str]],
                                output_path: str) -> None:
        """Generate DataCompy comparison report"""
        
        # Prepare DataFrames with mapped columns
        source_cols = list(column_mapping.keys())
        target_cols = [column_mapping[col] for col in source_cols]
        
        if join_keys:
            source_join_cols = [key[0] for key in join_keys]
            target_join_cols = [key[1] for key in join_keys]
        else:
            # If no join keys, use index
            source_df = source_df.copy()
            target_df = target_df.copy()
            source_df['_index'] = range(len(source_df))
            target_df['_index'] = range(len(target_df))
            source_join_cols = ['_index']
            target_join_cols = ['_index']
        
        # Create DataCompy comparison
        comparison = datacompy.Compare(
            source_df[source_cols],
            target_df[target_cols],
            join_columns=source_join_cols,  # Use source join columns
            df2_join_columns=target_join_cols,  # Use target join columns
            df1_name='Source',
            df2_name='Target'
        )
        
        # Generate HTML report
        with open(output_path, 'w') as f:
            f.write(f"""
            <html>
            <head>
                <title>DataCompy Comparison Report</title>
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
                    <h1>DataCompy Comparison Report</h1>
                    <div class="section">
                        <h2>Summary</h2>
                        <pre>{comparison.report()}</pre>
                    </div>
                    <div class="section">
                        <h2>Detailed Statistics</h2>
                        <h3>Matches</h3>
                        <div class="match">
                            <p>Number of rows match: {comparison.count_matching_rows()}</p>
                            <p>Number of columns match: {len(set(source_cols).intersection(target_cols))}</p>
                        </div>
                        <h3>Mismatches</h3>
                        <div class="mismatch">
                            <p>Rows only in Source: {len(comparison.df1_unq_rows)}</p>
                            <p>Rows only in Target: {len(comparison.df2_unq_rows)}</p>
                            <p>Source-only columns: {len(set(source_cols) - set(target_cols))}</p>
                            <p>Target-only columns: {len(set(target_cols) - set(source_cols))}</p>
                        </div>
                    </div>
                    <div class="section">
                        <h2>Column Statistics</h2>
                        {comparison.column_stats.to_html() if hasattr(comparison, 'column_stats') else '<p>No column statistics available.</p>'}
                    </div>
                </div>
            </body>
            </html>
            """)
