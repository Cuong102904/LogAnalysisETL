def bucket_second(value: float, bucket_size: int = 5) -> int:
    if value < 0:
        return 0
    return int(value // bucket_size) * bucket_size

