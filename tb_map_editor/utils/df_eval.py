"""
DataFrame evaluation and comparison utilities using Polars.
"""

import polars as pl
from typing import Union, Tuple
import numpy as np


def df_score(reference_df: pl.DataFrame, compare_df: pl.DataFrame) -> float:
    """
    Compute a similarity score between two Polars dataframes using statistical methods.
    
    The score aggregates multiple comparison metrics:
    - Schema compatibility (column names, types)
    - Row count similarity (normalized difference)
    - Data distribution similarity (statistical comparisons for numeric columns)
    - Null value patterns
    
    Args:
        reference_df: The reference (ground truth) DataFrame
        compare_df: The DataFrame to evaluate against the reference
        
    Returns:
        A similarity score between 0.0 (completely different) and 1.0 (identical)
        
    Raises:
        ValueError: If either dataframe is empty or has no columns
    """
    # Validation
    if reference_df.is_empty() or compare_df.is_empty():
        raise ValueError("DataFrames must not be empty")
    
    if len(reference_df.columns) == 0:
        raise ValueError("DataFrames must have at least one column")
    
    scores = []
    weights = []
    
    # 1. Schema compatibility score
    schema_score = _compute_schema_score(reference_df, compare_df)
    scores.append(schema_score)
    weights.append(0.2)
    
    # 2. Row count similarity
    row_count_score = _compute_row_count_score(reference_df, compare_df)
    scores.append(row_count_score)
    weights.append(0.15)
    
    # 3. Null pattern similarity
    null_score = _compute_null_similarity(reference_df, compare_df)
    scores.append(null_score)
    weights.append(0.15)
    
    # 4. Data distribution similarity (for numeric columns)
    distribution_score = _compute_distribution_score(reference_df, compare_df)
    scores.append(distribution_score)
    weights.append(0.3)
    
    # 5. Categorical/String value matching
    categorical_score = _compute_categorical_score(reference_df, compare_df)
    scores.append(categorical_score)
    weights.append(0.2)
    
    # Weighted average
    total_weight = sum(weights)
    weighted_score = sum(s * w for s, w in zip(scores, weights)) / total_weight
    
    # Clamp to [0, 1]
    return max(0.0, min(1.0, weighted_score))


def _compute_schema_score(ref_df: pl.DataFrame, cmp_df: pl.DataFrame) -> float:
    """Compare column names and data types."""
    ref_cols = set(ref_df.columns)
    cmp_cols = set(cmp_df.columns)
    
    if not ref_cols:
        return 0.0
    
    # Column name overlap
    common_cols = ref_cols & cmp_cols
    name_score = len(common_cols) / len(ref_cols)
    
    # Column type compatibility
    type_matches = 0
    for col in common_cols:
        ref_type = ref_df.schema[col]
        cmp_type = cmp_df.schema[col]
        if ref_type == cmp_type:
            type_matches += 1
    
    type_score = type_matches / len(common_cols) if common_cols else 0.0
    
    # Combined schema score
    return (name_score + type_score) / 2


def _compute_row_count_score(ref_df: pl.DataFrame, cmp_df: pl.DataFrame) -> float:
    """Compare number of rows (normalized difference)."""
    ref_rows = len(ref_df)
    cmp_rows = len(cmp_df)
    
    if ref_rows == 0:
        return 0.0
    
    # Use normalized absolute difference
    diff = abs(ref_rows - cmp_rows) / ref_rows
    score = max(0.0, 1.0 - diff)
    
    return score


def _compute_null_similarity(ref_df: pl.DataFrame, cmp_df: pl.DataFrame) -> float:
    """Compare null value patterns across common columns."""
    common_cols = set(ref_df.columns) & set(cmp_df.columns)
    
    if not common_cols:
        return 0.5  # Neutral score if no common columns
    
    null_scores = []
    for col in common_cols:
        ref_nulls = ref_df[col].null_count() / len(ref_df)
        cmp_nulls = cmp_df[col].null_count() / len(cmp_df)
        
        # Absolute difference in null proportions
        null_diff = abs(ref_nulls - cmp_nulls)
        null_scores.append(max(0.0, 1.0 - null_diff))
    
    return sum(null_scores) / len(null_scores) if null_scores else 0.5


def _compute_distribution_score(ref_df: pl.DataFrame, cmp_df: pl.DataFrame) -> float:
    """Compare statistical distributions for numeric columns."""
    common_cols = set(ref_df.columns) & set(cmp_df.columns)
    
    numeric_cols = []
    for col in common_cols:
        if ref_df.schema[col] in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, 
                                   pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
                                   pl.Float32, pl.Float64]:
            numeric_cols.append(col)
    
    if not numeric_cols:
        return 0.5  # Neutral if no numeric columns
    
    distribution_scores = []
    
    for col in numeric_cols:
        ref_col = ref_df[col].drop_nulls()
        cmp_col = cmp_df[col].drop_nulls()
        
        if len(ref_col) == 0 or len(cmp_col) == 0:
            distribution_scores.append(0.0)
            continue
        
        # Compare basic statistics
        ref_stats = {
            'mean': float(ref_col.mean() or 0),
            'std': float(ref_col.std() or 0),
            'min': float(ref_col.min() or 0),
            'max': float(ref_col.max() or 0),
            'median': float(ref_col.median() or 0),
        }
        
        cmp_stats = {
            'mean': float(cmp_col.mean() or 0),
            'std': float(cmp_col.std() or 0),
            'min': float(cmp_col.min() or 0),
            'max': float(cmp_col.max() or 0),
            'median': float(cmp_col.median() or 0),
        }
        
        # Compare using normalized difference
        col_score = _compare_statistics(ref_stats, cmp_stats)
        distribution_scores.append(col_score)
    
    return sum(distribution_scores) / len(distribution_scores) if distribution_scores else 0.5


def _compare_statistics(ref_stats: dict, cmp_stats: dict) -> float:
    """Compare two sets of statistics using normalized differences."""
    diffs = []
    
    # Mean comparison
    if abs(ref_stats['mean']) > 1e-10:
        mean_diff = abs(ref_stats['mean'] - cmp_stats['mean']) / (abs(ref_stats['mean']) + 1e-10)
        diffs.append(max(0.0, 1.0 - mean_diff))
    else:
        diffs.append(1.0 if abs(cmp_stats['mean']) < 1e-10 else 0.5)
    
    # Std comparison
    if abs(ref_stats['std']) > 1e-10:
        std_diff = abs(ref_stats['std'] - cmp_stats['std']) / (abs(ref_stats['std']) + 1e-10)
        diffs.append(max(0.0, 1.0 - std_diff))
    else:
        diffs.append(1.0 if abs(cmp_stats['std']) < 1e-10 else 0.5)
    
    # Min comparison
    if abs(ref_stats['min']) > 1e-10:
        min_diff = abs(ref_stats['min'] - cmp_stats['min']) / (abs(ref_stats['min']) + 1e-10)
        diffs.append(max(0.0, 1.0 - min_diff))
    else:
        diffs.append(1.0 if abs(cmp_stats['min']) < 1e-10 else 0.5)
    
    # Max comparison
    if abs(ref_stats['max']) > 1e-10:
        max_diff = abs(ref_stats['max'] - cmp_stats['max']) / (abs(ref_stats['max']) + 1e-10)
        diffs.append(max(0.0, 1.0 - max_diff))
    else:
        diffs.append(1.0 if abs(cmp_stats['max']) < 1e-10 else 0.5)
    
    # Median comparison
    if abs(ref_stats['median']) > 1e-10:
        median_diff = abs(ref_stats['median'] - cmp_stats['median']) / (abs(ref_stats['median']) + 1e-10)
        diffs.append(max(0.0, 1.0 - median_diff))
    else:
        diffs.append(1.0 if abs(cmp_stats['median']) < 1e-10 else 0.5)
    
    return sum(diffs) / len(diffs) if diffs else 0.5


def _compute_categorical_score(ref_df: pl.DataFrame, cmp_df: pl.DataFrame) -> float:
    """Compare categorical/string columns by value overlap."""
    common_cols = set(ref_df.columns) & set(cmp_df.columns)
    
    categorical_cols = []
    for col in common_cols:
        col_type = ref_df.schema[col]
        if col_type == pl.Utf8 or col_type in [pl.Categorical, pl.UInt16, pl.UInt32]:
            categorical_cols.append(col)
    
    if not categorical_cols:
        return 0.5  # Neutral if no categorical columns
    
    categorical_scores = []
    
    for col in categorical_cols:
        ref_vals = set(ref_df[col].drop_nulls().unique().to_list())
        cmp_vals = set(cmp_df[col].drop_nulls().unique().to_list())
        
        if not ref_vals and not cmp_vals:
            categorical_scores.append(1.0)
            continue
        
        if not ref_vals or not cmp_vals:
            categorical_scores.append(0.0)
            continue
        
        # Jaccard similarity
        intersection = len(ref_vals & cmp_vals)
        union = len(ref_vals | cmp_vals)
        jaccard = intersection / union if union > 0 else 0.0
        
        categorical_scores.append(jaccard)
    
    return sum(categorical_scores) / len(categorical_scores) if categorical_scores else 0.5
