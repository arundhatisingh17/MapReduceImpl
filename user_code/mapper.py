def mapper(line):
    for word in line.strip().split():
        yield (word, 1)