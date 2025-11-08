def map_func(record):
    """
    Map function for word count.
    :param record: A dictionary-like object representing a row from the input data.
                   It is expected to have a 'line' key with a string value.
    :return: A list of (key, value) pairs.
    """
    line = record.get('line', '')
    words = line.strip().split()
    return [(word.lower(), 1) for word in words]
