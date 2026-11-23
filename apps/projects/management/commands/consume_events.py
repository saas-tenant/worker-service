import json

import pika

from django.conf import settings
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Consume project events from RabbitMQ"

    def handle(self, *args, **options):
        rabbitmq = settings.RABBITMQ

        credentials = pika.PlainCredentials(
            rabbitmq["USERNAME"],
            rabbitmq["PASSWORD"],
        )

        parameters = pika.ConnectionParameters(
            host=rabbitmq["HOST"],
            port=rabbitmq["PORT"],
            virtual_host=rabbitmq["VHOST"],
            credentials=credentials,
            heartbeat=60,
            blocked_connection_timeout=30,
        )

        self.stdout.write("Connecting to RabbitMQ...")

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        queue_name = rabbitmq["QUEUE"]

        # Ensure the queue exists and is durable.
        channel.queue_declare(
            queue=queue_name,
            durable=True,
        )

        # Process one message at a time.
        channel.basic_qos(prefetch_count=1)

        def callback(ch, method, properties, body):
            try:
                event = json.loads(body)

                if event.get("event") != "project.created":
                    raise ValueError("Unsupported event type")

                self.stdout.write(
                    self.style.SUCCESS(
                        f"Received project.created: " f"{event['project_id']}"
                    )
                )

                self.stdout.write(f"Tenant: {event['tenant_id']}")
                self.stdout.write(f"Name: {event['name']}")

                # Acknowledge only after successful processing.
                ch.basic_ack(delivery_tag=method.delivery_tag)

            except Exception as error:
                self.stderr.write(self.style.ERROR(f"Failed to process event: {error}"))

                # Retry by returning the message to the queue.
                ch.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=True,
                )

        channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=False,
        )

        self.stdout.write(
            self.style.SUCCESS(f"Worker started. Listening on: {queue_name}")
        )

        try:
            channel.start_consuming()

        except KeyboardInterrupt:
            self.stdout.write("\nStopping worker...")

        finally:
            if channel.is_open:
                channel.close()

            if connection.is_open:
                connection.close()
