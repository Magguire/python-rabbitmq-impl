import pika
import json


# Connect to rabbitmq
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a queue name
queue = channel.queue_declare('order_report')
queue_name = queue.method.queue


# Create binding
channel.queue_bind(
    exchange='order',
    queue=queue_name,
    routing_key='order.report' # binding key
)

# define callback method
def callback(ch, method, properties, body):
    payload = json.loads(body)
    print(' [x] Generating report')
    print(f"""
    ID: {payload.get('id')},
    Email: {payload.get('user_email')},
    Product: {payload.get('product')},
    Quantity: {payload.get('quantity')}
    """)
    print(' [x] Done')
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(on_message_callback=callback, queue=queue_name)

print(' [x] Waiting for report messages. To exit, press CTRL+C')

channel.start_consuming()


