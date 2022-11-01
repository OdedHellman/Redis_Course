from os import getenv
from redis import Redis
import datetime
import time
from pydantic import ValidationError
from schema.retail_schema import  *

password = getenv('REDIS_PASSWORD')
host = getenv('REDIS_HOST')
port = getenv('REDIS_PORT')

r = Redis(password=password, host=host, port=port, decode_responses=True)

while True:
	received = r.xread(streams={"Orders": '$'}, count=100, block=0)

	for result in received:
		data = result[1]
		for tuple in data:
			orderDict = tuple[1]
			print(orderDict)

			try:
				item = Product (
					StockCode=orderDict['StockCode'],
					Description=orderDict['Description'],
					UnitPrice=orderDict['UnitPrice']
				)

				order = Order (
					InvoiceNo=orderDict['InvoiceNo'],
					Item = item,
					Quantity=orderDict['Quantity'],
					InvoiceDate=datetime.datetime.strptime(orderDict['InvoiceDate'], '%m/%d/%Y %H:%M'),
					CustomerID=orderDict['CustomerID'],
					Country=orderDict['Country']
				)
            
			except (ValidationError, KeyError) as e:
				print(e)
				continue

			print(order.key())
			order.save()
