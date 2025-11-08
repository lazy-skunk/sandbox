import numpy as np
import pandas as pd


def _round_specified_col(df: pd.DataFrame, col: str, digit: int) -> None:
    df[col] = df[col].round(digit)


SCORES = [0.1234567, 0.2345678, 0.3456789]
df_float32 = pd.DataFrame({"score": np.array(SCORES, dtype=np.float32)})
df_float64 = pd.DataFrame({"score": np.array(SCORES, dtype=np.float64)})
df_float32["original_dtype"] = "float32"
df_float64["original_dtype"] = "float64"
df_float32["score_length"] = df_float32["score"].astype(str).apply(len)
df_float64["score_length"] = df_float64["score"].astype(str).apply(len)
df_float32.to_parquet("df_float32.parquet", index=False)
df_float64.to_parquet("df_float64.parquet", index=False)

df_float32 = pd.read_parquet("df_float32.parquet")
df_float64 = pd.read_parquet("df_float64.parquet")

df_float32["score"] = df_float32["score"].astype("float64")
df_float64["score"] = df_float64["score"].astype("float64")

_round_specified_col(df_float32, "score", 3)
_round_specified_col(df_float64, "score", 3)

print("############################## 結合前 ##############################")
print(df_float32.head(10))
print(df_float64.head(10))
df_concat = pd.concat([df_float32, df_float64])
df_concat["score_length"] = df_concat["score"].astype(str).apply(len)
df_concat.to_csv("df_concat.csv", index=False)

print("############################## 結合後 ##############################")
print(df_concat.dtypes)
print(df_concat.head(10))
