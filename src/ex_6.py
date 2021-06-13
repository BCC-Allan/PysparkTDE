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
    return key, (price, commodity)

"""
The commodity with the highest price per unit type and year;
comoditi com maior preço por / unidade tipo e ano
"""

first_filter = rdd.filter(quantity_name_not_empty)
rdd_filtered_2 = first_filter.map(year_qty_name_per_comm_price_qty)
rdd_qtd_per_flow = rdd_filtered_2.reduceByKey(lambda a, b: max(a, b))

print(rdd_qtd_per_flow.take(5))

save_rdd_to_file(rdd_qtd_per_flow.coalesce(1), 'ex6')
