def reduce_func(key, values):
    """
    Reduce function for word count.
    :param key: The word.
    :param values: An iterable of counts (1s) for the word.
    :return: A tuple of (key, value) with the final count.
    """
    return (key, sum(values))
