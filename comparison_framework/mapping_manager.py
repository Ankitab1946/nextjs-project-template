import pandas as pd
from typing import Dict, List, Tuple, Any, Optional
from utils import log_error

class MappingManager:
    """Class to handle column mappings and data type conversions"""
    
    # Define the type mapping dictionary as a class variable
    TYPE_MAPPING = {
        'int': 'int32',
        'int64': 'int64',
        'numeric': 'int64',
        'bigint': 'int64',
        'smallint': 'int64',
        'varchar': 'string',
        'nvarchar': 'string',
        'char': 'string',
        'date': 'datetime64[ns]',
        'datetime': 'datetime64[ns]',
        'decimal': 'float',
        'float': 'float',
        'bit': 'bool',
        'nchar': 'char',
        'boolean': 'bool'
    }

    @staticmethod
    def normalize_column_name(col: str) -> str:
        """
        Normalize column name by converting to lowercase and standardizing format.
        Handles SQL Server specific patterns.
        
        Args:
            col (str): Column name to normalize
            
        Returns:
            str: Normalized column name
        """
        if not isinstance(col, str):
            return str(col)
            
        # Convert to lowercase and strip whitespace
        normalized = col.lower().strip()
        
        # Handle SQL Server brackets if present
        if normalized.startswith('[') and normalized.endswith(']'):
            normalized = normalized[1:-1]
            
        # Replace common separators with underscore
        for char in [' ', '-', '.']:
            normalized = normalized.replace(char, '_')
            
        # Remove special characters but keep underscores
        normalized = ''.join(e for e in normalized if e.isalnum() or e == '_')
        
        # Remove duplicate underscores
        while '__' in normalized:
            normalized = normalized.replace('__', '_')
            
        # Remove leading/trailing underscores
        normalized = normalized.strip('_')
        
        return normalized

    @classmethod
    def auto_map_columns(cls, source_df: pd.DataFrame, target_df: pd.DataFrame) -> Dict[str, str]:
        """
        Automatically map columns between source and target DataFrames.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            
        Returns:
            dict: Mapping of source columns to target columns
        """
        try:
            mapping = {}
            
            # Get column names
            source_cols = source_df.columns
            target_cols = target_df.columns
            
            # Create normalized versions of column names
            normalized_source = {col: cls.normalize_column_name(col) for col in source_cols}
            normalized_target = {col: cls.normalize_column_name(col) for col in target_cols}
            
            # First pass: exact matches based on normalized names
            for source_col, norm_s in normalized_source.items():
                for target_col, norm_t in normalized_target.items():
                    if norm_s == norm_t:
                        mapping[source_col] = target_col
                        break
        
            # Second pass: fuzzy matches based on column names and data patterns
            unmapped_source = [col for col in source_cols if col not in mapping]
            unmapped_target = [col for col in target_cols if col not in mapping.values()]
            
            for source_col in unmapped_source:
                best_match = None
                best_score = 0
                
                # Get sample values and normalized name from source column
                source_sample = source_df[source_col].head(5).astype(str).tolist()
                source_dtype = str(source_df[source_col].dtype)
                norm_source = normalized_source[source_col]
                
                for target_col in unmapped_target:
                    try:
                        # Compare data types
                        target_dtype = str(target_df[target_col].dtype)
                        type_match = source_dtype == target_dtype
                        
                        # Compare normalized column names using fuzzy matching
                        norm_target = normalized_target[target_col]
                        name_score = cls._calculate_similarity(norm_source, norm_target)
                        
                        # Compare data patterns
                        target_sample = target_df[target_col].head(5).astype(str).tolist()
                        data_score = cls._calculate_data_similarity(source_sample, target_sample)
                        
                        # Calculate combined score with adjusted weights
                        # Increased weight for name similarity and type matching
                        combined_score = (name_score * 0.7) + (data_score * 0.3)
                        if type_match:
                            combined_score += 0.15  # Increased bonus for type match
                        
                        # Increased threshold for more accurate matching
                        if combined_score > best_score and combined_score > 0.75:  # 75% similarity threshold
                            best_score = combined_score
                            best_match = target_col
                    
                    except Exception as e:
                        log_error(f"Error comparing columns {source_col} and {target_col}: {str(e)}")
                        continue
                
                if best_match:
                    mapping[source_col] = best_match
                    unmapped_target.remove(best_match)
            
            # Log mapping results
            if not mapping:
                log_error("No column mappings found between source and target DataFrames")
            else:
                print(f"Successfully mapped {len(mapping)} columns")
            
            return mapping
            
        except Exception as e:
            log_error(f"Error in auto_map_columns: {str(e)}")
            return {}

    @staticmethod
    def _calculate_similarity(str1: str, str2: str) -> float:
        """Calculate similarity between two strings using Levenshtein distance."""
        if not str1 or not str2:
            return 0
            
        # Simple implementation of Levenshtein distance
        if len(str1) < len(str2):
            str1, str2 = str2, str1
        
        if not str2:
            return 0
            
        previous_row = range(len(str2) + 1)
        for i, c1 in enumerate(str1):
            current_row = [i + 1]
            for j, c2 in enumerate(str2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        # Convert distance to similarity score
        max_len = max(len(str1), len(str2))
        similarity = 1 - (previous_row[-1] / max_len)
        return similarity

    @staticmethod
    def _calculate_data_similarity(source_sample: List[str], target_sample: List[str]) -> float:
        """Calculate similarity between two lists of data samples."""
        if not source_sample or not target_sample:
            return 0
        
        # Convert all values to lowercase strings for comparison
        source_values = [str(val).lower() for val in source_sample]
        target_values = [str(val).lower() for val in target_sample]
        
        # Calculate pattern similarity
        total_score = 0
        comparisons = 0
        
        # Compare data patterns
        for s_val in source_values:
            best_match = 0
            for t_val in target_values:
                # Check for exact matches
                if s_val == t_val:
                    best_match = 1
                    break
                
                # Check for numeric patterns
                s_numeric = ''.join(c for c in s_val if c.isdigit())
                t_numeric = ''.join(c for c in t_val if c.isdigit())
                if s_numeric and t_numeric:
                    if s_numeric == t_numeric:
                        best_match = max(best_match, 0.9)
                        continue
                
                # Check for string patterns
                s_alpha = ''.join(c for c in s_val if c.isalpha())
                t_alpha = ''.join(c for c in t_val if c.isalpha())
                if s_alpha and t_alpha:
                    alpha_sim = MappingManager._calculate_similarity(s_alpha, t_alpha)
                    best_match = max(best_match, alpha_sim * 0.8)
            
            total_score += best_match
            comparisons += 1
        
        return total_score / comparisons if comparisons > 0 else 0

    @classmethod
    def generate_data_type_mapping(cls, df: pd.DataFrame) -> Dict[str, str]:
        """
        Generate data type mapping for DataFrame columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            dict: Mapping of column names to suggested data types
        """
        type_mapping = {}
        for column in df.columns:
            current_type = str(df[column].dtype)
            
            # Map pandas/numpy types to our type system
            if 'int' in current_type:
                type_mapping[column] = 'int64'
            elif 'float' in current_type:
                type_mapping[column] = 'float'
            elif 'datetime' in current_type:
                type_mapping[column] = 'datetime64[ns]'
            elif 'bool' in current_type:
                type_mapping[column] = 'bool'
            else:
                type_mapping[column] = 'string'
        
        return type_mapping

    @classmethod
    def apply_mapping(cls, df: pd.DataFrame, type_mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Apply data type mapping to DataFrame.
        
        Args:
            df: Input DataFrame
            type_mapping: Dictionary mapping column names to desired data types
            
        Returns:
            pd.DataFrame: DataFrame with applied type conversions
        """
        try:
            df_copy = df.copy()
            for column, dtype in type_mapping.items():
                if column in df_copy.columns:
                    try:
                        if dtype == 'datetime64[ns]':
                            df_copy[column] = pd.to_datetime(df_copy[column], errors='coerce')
                        else:
                            df_copy[column] = df_copy[column].astype(dtype)
                    except Exception as e:
                        log_error(f"Error converting column {column} to type {dtype}: {str(e)}")
            return df_copy
        except Exception as e:
            log_error(f"Error applying type mapping: {str(e)}")
            return df

    @staticmethod
    def validate_mapping(source_df: pd.DataFrame, target_df: pd.DataFrame, 
                        column_mapping: Dict[str, str]) -> Tuple[bool, List[str]]:
        """
        Validate column mapping between source and target DataFrames.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            column_mapping: Dictionary mapping source columns to target columns
            
        Returns:
            tuple: (is_valid, list of validation messages)
        """
        messages = []
        is_valid = True
        
        # Check if all mapped columns exist
        for source_col, target_col in column_mapping.items():
            if source_col not in source_df.columns:
                messages.append(f"Source column '{source_col}' not found")
                is_valid = False
            if target_col not in target_df.columns:
                messages.append(f"Target column '{target_col}' not found")
                is_valid = False
        
        # Check for duplicate mappings
        if len(set(column_mapping.values())) != len(column_mapping.values()):
            messages.append("Duplicate target columns in mapping")
            is_valid = False
        
        return is_valid, messages

    @staticmethod
    def get_unmapped_columns(source_df: pd.DataFrame, target_df: pd.DataFrame, 
                            column_mapping: Dict[str, str]) -> Tuple[List[str], List[str]]:
        """
        Get lists of unmapped columns from both source and target DataFrames.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            column_mapping: Dictionary mapping source columns to target columns
            
        Returns:
            tuple: (unmapped source columns, unmapped target columns)
        """
        unmapped_source = [col for col in source_df.columns if col not in column_mapping]
        unmapped_target = [col for col in target_df.columns if col not in column_mapping.values()]
        return unmapped_source, unmapped_target
