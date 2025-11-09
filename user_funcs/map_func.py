"""
Example Map Function for Word Count

This function processes each record from the input dataset and emits (word, 1) pairs.
For demonstration, it processes numeric fields and emits them as keys.

The map function should yield tuples of (key, value).
"""

def map_function(key, record):
    """
    Map function that processes each record.
    
    Args:
        key: The index/key of the record
        record: Dictionary containing the record data
    
    Yields:
        Tuples of (key, value) pairs
    """
    # For the default dataset with columns: id, x, y, value
    # We'll create a simple aggregation: emit (x_bucket, value) pairs
    
    if 'x' in record and 'value' in record:
        # Bucket x values into groups of 10
        x_bucket = (record['x'] // 10) * 10
        yield (f"x_bucket_{x_bucket}", record['value'])
    
    if 'y' in record and 'value' in record:
        # Bucket y values into groups of 10
        y_bucket = (record['y'] // 10) * 10
        yield (f"y_bucket_{y_bucket}", record['value'])
    
    # You can also emit multiple key-value pairs per record
    # For example, for a word count task:
    # if 'text' in record:
    #     words = record['text'].split()
    #     for word in words:
    #         yield (word.lower(), 1)

