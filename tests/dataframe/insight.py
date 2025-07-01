from __future__ import annotations

import daft
from daft import col

# daft.context.set_runner_ray("ray://127.0.0.1:10001")
#
# daft.set_execution_config(
#     flotilla=True,
# )

df1 = (
    daft.read_csv(path="/opt/workspace/daft/data/images/csv")
    .select(col("name"), col("width"), col("height"), col("url"))
    .filter(col("width") >= 256)
)

df2 = daft.read_parquet(path="/opt/workspace/daft/data/images/parquet").select(col("name"), col("bytes"))

df = df1.join(df2, on="name", prefix="r_")
df = df.select(col("name"), col("width"), col("height"), col("bytes")).limit(10)

# df.explain(show_all=True)

df = df.collect()
print(df)
