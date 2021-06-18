"""
The commodity with the highest price per unit type and year;
comoditi com maior preço por / unidade tipo e ano
"""

from src.util.helpers import load_rdd, Indexes, save_rdd_to_file


def quantity_name_not_empty(row):
    values = row.split(";")
    qty_name = values[Indexes.QUANTITY_NAME.value]
    return qty_name.lower() != "no quantity"


def build_tuple(row):
    values = row.split(";")
    year = values[Indexes.YEAR.value]
    commodity = values[Indexes.COMNAME.value]
    price = values[Indexes.PRICE.value]
    qty_name = values[Indexes.QUANTITY_NAME.value]
    key = f'{qty_name}_{year}'
    return key, (commodity, int(price))


def reduce_take_max(acc, actual):
    if acc[1] >= actual[1]:
        return acc
    return actual


rdd = load_rdd()
first_filter = rdd.filter(quantity_name_not_empty)
tuple_rdd = first_filter.map(build_tuple)
max_rdd = tuple_rdd.reduceByKey(reduce_take_max)
print(max_rdd.take(5))

save_rdd_to_file(max_rdd.coalesce(1), 'ex6')
