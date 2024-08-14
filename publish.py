import pika
import json
import uuid

# Connect to rabbit mq
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

#Declare an exchange name and type
channel.exchange_declare(
    exchange='order',
    exchange_type='direct'
)

# Information about the order by a customer
order = {
    'id': str(uuid.uuid4()),
    'user_email': 'john.doe@gmail.com',
    'product': 'Nike Shoes',
    'quantity': 3
}

# Send notification to customer
channel.basic_publish(
    exchange='order',
    routing_key='order.notify',
    body=json.dumps({'user_email': order['user_email']})
)

print(' [x] sent notify message')

# Send order details to database for storage
channel.basic_publish(
    exchange='order',
    routing_key='order.report',
    body=json.dumps(order)
)

print(' [x] sent report message')

connection.close()
