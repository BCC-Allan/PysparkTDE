"""
The average price of commodities per unit type, year, and category in the export flow in Brazil;
preço medio de comodite por unidade, ano, categoria, e
exportados para o brasil
"""
from src.util.helpers import load_rdd, Indexes, save_rdd_to_file

rdd = load_rdd()


def mount_tuple(row: str):
    values = row.split(";")
    year = values[Indexes.YEAR.value]
    cat = values[Indexes.CATEGORY.value]
    unit_type = values[Indexes.QUANTITY_NAME.value]
    price = values[Indexes.PRICE.value]
    key = f"{year}|{cat}|{unit_type}"
    return key, (int(price), 1)


def filter_brazil_export(row: str):
    values = row.split(";")
    flow = values[Indexes.FLOW.value].lower()
    country = values[Indexes.COUNTRY.value].lower()
    return flow == 'export' and country == 'brazil'


rdd_filtered = rdd.filter(filter_brazil_export)
mapped_tuples = rdd_filtered.map(mount_tuple)
reduced = mapped_tuples.reduceByKey(lambda acc, actual: (acc[0] + actual[0], acc[1] + actual[1]))
avg_rdd = reduced.map(lambda t: (t[0], t[1][0] / t[1][1]))

print(avg_rdd.take(5))
save_rdd_to_file(avg_rdd.coalesce(1), 'ex5')
