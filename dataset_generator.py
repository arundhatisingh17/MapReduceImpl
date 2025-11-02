import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

def generate_default_dataset():
    n = 100
    data = {
        "id": np.arange(1, n + 1),
        "x": np.random.randint(0, 100, n),
        "y": np.random.randint(0, 100, n),
        "value": np.random.random(n) * 10
    }

    df = pd.DataFrame(data)
    path = "hdfs://nn:9000/data/sample.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    fs = pa.fs.HadoopFileSystem("nn", 9000)
    pq.write_table(table, "/data/sample.parquet", filesystem=fs)
    print(f"[dataset_generator] Default dataset saved at {path}")
    return path


if __name__ == "__main__":
    generate_default_dataset()

