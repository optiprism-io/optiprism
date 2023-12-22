import time

fields = ["i_8", "i_16", "i_32", "i_64", "decimal"]
aggs = ["min", "max", "avg", "sum", "count"]


def return_type(typ, agg):
    if agg == "min" or agg == "max":
        return typ
    elif agg == "count":
        return "i64"
    elif agg == "avg":
        return "f64"
    elif agg == "sum":
        if typ == "i8" or typ == "i16" or typ == "i32" or typ == "ts":
            return "i64"
        elif typ == "u8" or typ == "u16" or typ == "u32":
            return "u64"
        elif typ == "i64":
            return "i128"
        elif typ == "u64":
            return "u128"
        elif typ == "f32":
            return "f64"
        elif typ == "f64":
            return "f64"
        elif typ == "i128" or typ == "u128" or typ == "decimal":
            return "i128"


now = int(time.time())
