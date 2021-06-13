"""
The average price of commodities per unit type, year, and category in the export flow in Brazil;
preço medio de comodite por unidade, ano, categoria, e
exportados para o brasil
"""
from src.util.helpers import load_rdd, Indexes, save_rdd_to_file

rdd = load_rdd()


def separete_per_key(row):
    values = row.split(";")
    year = values[Indexes.YEAR.value]
    cat = values[Indexes.CATEGORY.value]
    flow = values[Indexes.FLOW.value]
    unit_type = values[Indexes.QUANTITY_NAME.value]
    price = values[Indexes.PRICE.value]
    key = f"{year}|{flow}|{cat}|{unit_type}"
    return key, (int(price), 1)


rdd_filtered = rdd.filter(lambda l: l.split(';')[Indexes.COUNTRY.value] == 'Brazil')
mapped = rdd_filtered.map(separete_per_key)
rdd_result = mapped.map(lambda t: (t[0], t[1][0] / t[1][1]))

print(rdd_result.take(5))
save_rdd_to_file(rdd_qtd_per_flow.coalesce(1), 'ex5')
