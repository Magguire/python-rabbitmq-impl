import pika
import json


# Connect to rabbitmq
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a new queue with a name
queue = channel.queue_declare('order_notify')
queue_name = queue.method.queue

print(f' [x] Queue Name: {queue_name}')

# Create a binding between our exchange and queue
channel.queue_bind(
    exchange='order',
    queue=queue_name,
    routing_key='order.notify' # binding key
)


# Define a consume method
def callback(ch, method, properties, body):
    payload = json.loads(body)
    print(' [x] Notifying {}'.format(payload['user_email']))
    print(' [x] Done')
    # Send an acknowledgement to rabbitmq that message was successfully received and processed, rabbitmq is free to delete this message
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Method called whenever the queue receives a new message
channel.basic_consume(on_message_callback=callback, queue=queue_name)

print(' [x] Waiting for notify messages. To exit, press CTRL+C')

channel.start_consuming()
