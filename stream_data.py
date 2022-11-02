from os import getenv
import csv
from redis.client import Redis
from schema.retail_schema import *

MAX_ROWS_TO_STREAM = 2000

password = getenv('REDIS_PASSWORD')
host = getenv('REDIS_HOST')
port = getenv('REDIS_PORT')

r = Redis(password=password, host=host, port=port, decode_responses=True)

with open ('data/online_retail.csv', encoding='utf-8') as input_file:
    dict_reader = csv.DictReader(input_file)
    
    for row_num, line in enumerate(dict_reader):
        if row_num == MAX_ROWS_TO_STREAM:
            break
        print(f'Order num: {row_num} ---', line)
        r.xadd(name='Orders', fields=line)
        