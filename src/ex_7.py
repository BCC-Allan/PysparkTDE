"""
The number of transactions per flow type and year.
"""
from src.util.helpers import load_rdd, Indexes, save_rdd_to_file

rdd = load_rdd()


def transaction_flow_type_year(row):
    values = row.split(";")
    year = values[Indexes.YEAR.value]
    flow_type = values[Indexes.FLOW.value]
    key = f'{year}_{flow_type}'
    return key, 1


rdd_filtered = rdd.map(transaction_flow_type_year)
trans_flow_type_year = rdd_filtered.reduceByKey(lambda a, b: a + b)

print(trans_flow_type_year.take(5))

save_rdd_to_file(trans_flow_type_year.coalesce(1), 'ex7')
