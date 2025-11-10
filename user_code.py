def mapper(line):
    for word in line.strip().split():
        yield (word, 1)

def reducer(key, values):
    yield (key, sum(values))