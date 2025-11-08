def map_func(line):
    out = []
    for w in line.strip().split():
        word = ''.join(ch for ch in w if ch.isalnum()).lower()
        if word:
            out.append((word, 1))
    return out
