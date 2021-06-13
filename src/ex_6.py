"""
The commodity with the highest price per unit type and year;
comoditi com maior preço por / unidade tipo e ano
"""

from src.util.helpers import load_rdd, Indexes, save_rdd_to_file

rdd = load_rdd()


def quantity_name_not_empty(row):
    values = row.split(";")
    qty_name = str(values[Indexes.QUANTITY_NAME.value])
    return qty_name.lower() != "no quantity"


def year_qty_name_per_comm_price_qty(row):
    values = row.split(";")
    year = values[Indexes.YEAR.value]
    commodity = values[Indexes.COMCODE.value]
    price = values[Indexes.PRICE.value]
    qty_name = values[Indexes.QUANTITY_NAME.value]
    key = f'{year}_{qty_name}'
    return key, (int(price), commodity)


"""
The commodity with the highest price per unit type and year;
comoditi com maior preço por / unidade tipo e ano
"""


def reduce_take_max(a, b):
    if a[0] >= b[0]:
        return a
    return b


first_filter = rdd.filter(quantity_name_not_empty)
rdd_filtered_2 = first_filter.map(year_qty_name_per_comm_price_qty)
rdd_qtd_per_flow = rdd_filtered_2.reduceByKey(reduce_take_max)
print(rdd_qtd_per_flow.take(5))
#
# print(rdd_filtered_2.take(5))
# 2012-'number of items'	commodity:620459-price:9649
# save_rdd_to_file(rdd_qtd_per_flow.coalesce(1), 'ex6')
