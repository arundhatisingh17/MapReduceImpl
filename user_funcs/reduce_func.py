"""
Example Reduce Function for Aggregation

This function takes a key and all values associated with that key,
and produces aggregated results.

The reduce function should yield tuples of (key, value).
"""

def reduce_function(key, values):
    """
    Reduce function that aggregates values for each key.
    
    Args:
        key: The key to aggregate
        values: List of all values associated with this key
    
    Yields:
        Tuples of (key, aggregated_value) pairs
    """
    # Sum all numeric values
    total = sum(values)
    count = len(values)
    average = total / count if count > 0 else 0
    
    # Emit aggregated statistics
    yield (f"{key}_sum", total)
    yield (f"{key}_count", count)
    yield (f"{key}_avg", average)
    
    # For a simple word count task, you would just do:
    # yield (key, sum(values))

