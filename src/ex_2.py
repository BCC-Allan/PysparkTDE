"""
The number of transactions per year
"""
from src.util.helpers import load_rdd, Indexes, save_rdd_to_file

rdd = load_rdd()


def build_transaction_year_tuple(row: str):
    year = row.split(';')[Indexes.YEAR.value]
    return year, 1


rdd_tuples = rdd.map(build_transaction_year_tuple)
rdd_transactions_per_year = rdd_tuples.reduceByKey(lambda a, b: a + b)

print(rdd_transactions_per_year.take(5))  # resultado
save_rdd_to_file(rdd_transactions_per_year.coalesce(1), 'ex2')
