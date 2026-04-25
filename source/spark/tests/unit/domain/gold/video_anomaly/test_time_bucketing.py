from utils.time_buckets import bucket_second


def test_bucket_second() -> None:
    assert bucket_second(12.8, 5) == 10
