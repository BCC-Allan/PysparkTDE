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
    comname = values[Indexes.COMNAME.value]
    return f"{comname}|{flow}", int(qtd)


def build_flow_com_max_tuple(pair_rdd):
    values = pair_rdd[0].split("|")
    comname = values[0]
    flow = values[1]
    sumed = pair_rdd[1]
    return flow, (comname, sumed)


def reduce_take_max(acc, actual):
    if acc[1] >= actual[1]:
        return acc
    return actual


rdd_filtered = rdd.filter(year_qtd_filter)
pair_rdd1 = rdd_filtered.map(build_flow_qtd_tuple)
rdd_qtd_per_flow = pair_rdd1.reduceByKey(lambda a, b: a + b)  # comodity com  soma
max_pair_rdd = rdd_qtd_per_flow.map(build_flow_com_max_tuple)
reduced_final_rdd = max_pair_rdd.reduceByKey(reduce_take_max)

print(reduced_final_rdd.take(5))

save_rdd_to_file(reduced_final_rdd.coalesce(1), 'ex3')
