from decimal import Decimal
from typing import Any


def split_func(strategy: str, type: str, values: list[str]) -> list[tuple[Any, Any]]:
    splits = []
    match strategy:
        case "split-min-max":
            match type:
                case "str":
                    if "_" in values[0]:
                        for value in values:
                            min_val, max_val = value.split("_")
                            splits.append((min_val.strip(), max_val.strip()))
                    if "-" in values[0]:
                        for value in values:
                            min_val, max_val = value.split("-")
                            splits.append((min_val.strip(), max_val.strip()))
                    if "to" in values[0]:
                        for value in values:
                            min_val, max_val = value.split("to")
                            splits.append((min_val.strip(), max_val.strip()))
                case "int":
                    if "_" in values[0]:
                        for value in values:
                            min_val, max_val = value.split("_")
                            splits.append((int(min_val.strip()), int(max_val.strip())))
                    if "-" in values[0]:
                        for value in values:
                            min_val, max_val = value.split("-")
                            splits.append((int(min_val.strip()), int(max_val.strip())))
                    if "to" in values[0]:
                        for value in values:
                            min_val, max_val = value.split("to")
                            splits.append((int(min_val.strip()), int(max_val.strip())))
                case "float" | "double" | "decimal":
                    if "_" in values[0]:
                        for value in values:
                            min_val, max_val = value.split("_")
                            splits.append(
                                (Decimal(min_val.strip()), Decimal(max_val.strip()))
                            )
                    if "-" in values[0]:
                        for value in values:
                            min_val, max_val = value.split("-")
                            splits.append(
                                (Decimal(min_val.strip()), Decimal(max_val.strip()))
                            )
                    if "to" in values[0]:
                        for value in values:
                            min_val, max_val = value.split("to")
                            splits.append(
                                (Decimal(min_val.strip()), Decimal(max_val.strip()))
                            )
                case _:
                    raise ValueError("Unsupported type")
            return splits
        case "only-max":
            match type:
                case "str":
                    if "upto" in values[0]:
                        for value in values:
                            max_val = value.replace("upto", "")[1]
                            splits.append((None, max_val.strip()))
                case "int" | "integer":
                    if "upto" in values[0]:
                        for value in values:
                            max_val = value.replace("upto", "")[1]
                            splits.append((None, int(max_val.strip())))
                case "float" | "double" | "decimal":
                    if "upto" in values[0]:
                        for value in values:
                            max_val = value.replace("upto", "")[1]
                            splits.append((None, Decimal(max_val.strip())))
                case _:
                    raise ValueError("Unsupported type")
        case "year_max":
            if "upto" in values[0]:
                for value in values:
                    max_val = value.replace("upto", "")[1]
                    splits.append((None, int(max_val.strip())))
            elif "to" in values[0]:
                for value in values:
                    min_val, max_val = value.split("to")
                    splits.append((int(min_val.strip()), int(max_val.strip())))
            elif "_" in values[0]:
                for value in values:
                    min_val, max_val = value.split("_")
                    splits.append((int(min_val.strip()), int(max_val.strip())))
            elif "-" in values[0]:
                for value in values:
                    min_val, max_val = value.split("-")
                    splits.append((int(min_val.strip()), int(max_val.strip())))
            else:
                for value in values:
                    min_val, max_val = value.split(" ")
                    splits.append((int(min_val.strip()), int(max_val.strip())))
            return splits
        case "percentage-split":
            if "upto" in values[0]:
                for value in values:
                    max_val = value.replace("upto", "")[1]
                    splits.append((None, float(max_val.strip()) / 100))
            elif "to" in values[0]:
                for value in values:
                    min_val, max_val = value.split("to")
                    splits.append(
                        (float(min_val.strip()) / 100, float(max_val.strip()) / 100)
                    )
            elif "_" in values[0]:
                for value in values:
                    min_val, max_val = value.split("_")
                    splits.append(
                        (float(min_val.strip()) / 100, float(max_val.strip()) / 100)
                    )
            elif "-" in values[0]:
                for value in values:
                    min_val, max_val = value.split("-")
                    splits.append(
                        (float(min_val.strip()) / 100, float(max_val.strip()) / 100)
                    )
            else:
                for value in values:
                    min_val, max_val = value.split(" ")
                    splits.append(
                        (float(min_val.strip()) / 100, float(max_val.strip()) / 100)
                    )
            return splits
        case _:
            raise ValueError("Unsupported strategy")
    return splits
