"""
The average of commodity values per year;
"""
from src.util.helpers import load_rdd, Indexes, save_rdd_to_file

rdd = load_rdd()


def build_value_year_tuple(row: str):
    values = row.split(";")
    year = values[Indexes.YEAR.value]
    value = values[Indexes.PRICE.value]
    return year, (int(value), 1)


tuple_rdd = rdd.map(build_value_year_tuple)
reduced_rdd = tuple_rdd.reduceByKey(lambda acc, actual: (acc[0] + actual[0], acc[1] + actual[1]))
avg_rdd = reduced_rdd.map(lambda t: (t[0], t[1][0] / t[1][1]))

print(reduced_rdd.take(5))
print(avg_rdd.take(5))

save_rdd_to_file(avg_rdd.coalesce(1), 'ex4')
