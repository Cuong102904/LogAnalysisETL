from pyspark.sql import functions as F


def sha256_concat(*cols: str):
    return F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in cols]), 256)

