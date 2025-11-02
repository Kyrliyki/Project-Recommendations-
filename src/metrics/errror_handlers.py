def check_length_error(
        len_first: int,
        len_second: int,
) -> None:
    if len_first != len_second:
        raise ValueError("Массивы должны иметь одинаковую длину")