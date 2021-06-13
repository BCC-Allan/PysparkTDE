"""
The most commercialized commodity (summing the quantities) in 2016, per flow type.
"""
from src.util.helpers import load_rdd, Indexes, save_rdd_to_file

rdd = load_rdd()


def year_qtd_filter(row):
    values = row.split(";")
    year = values[Indexes.YEAR.value]
    qty_name = values[Indexes.QUANTITY_NAME.value]
    qtd = values[Indexes.QUANTITY.value]
    return year == "2016" and qty_name != "no quantity" and qtd.isnumeric()


def build_flow_qtd_tuple(row):
    values = row.split(";")
    flow = values[Indexes.FLOW.value]
    qtd = values[Indexes.QUANTITY.value]
    return flow, int(qtd)


rdd_filtered = rdd.filter(year_qtd_filter)
pair_rdd = rdd_filtered.map(build_flow_qtd_tuple)
rdd_qtd_per_flow = pair_rdd.reduceByKey(lambda a, b: a + b)

print(rdd_qtd_per_flow.take(5))

save_rdd_to_file(rdd_qtd_per_flow.coalesce(1), 'ex3')
