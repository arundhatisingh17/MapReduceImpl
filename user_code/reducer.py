def reducer(key, values):
    yield (key, sum(values))