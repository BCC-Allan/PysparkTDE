"""
The number of transactions involving Brazil
"""
from src.util.helpers import load_rdd, Indexes, save_rdd_to_file

rdd = load_rdd()

rdd = rdd.filter(lambda l: l.split(';')[Indexes.COUNTRY.value] == 'Brazil')

print(rdd.count()) # resultado
save_rdd_to_file(rdd.coalesce(1), 'ex1')
