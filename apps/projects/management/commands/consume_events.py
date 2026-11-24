import json

import pika

from django.conf import settings
from django.core.management.base import BaseCommand

MAX_RETRIES = 3

RETRY_QUEUE = "projects.retry"
DEAD_LETTER_EXCHANGE = "saas.dlx"
DEAD_LETTER_ROUTING_KEY = "projects.dead-letter"


class Command(BaseCommand):
    help = "Consume project events from RabbitMQ"

    def get_retry_count(self, headers):
        """
        Get the number of previous failed processing attempts.

        RabbitMQ adds x-death metadata when a message is dead-lettered.
        We only count rejections from our main queue.
        """
        deaths = headers.get("x-death", [])

        for death in deaths:
            if (
                death.get("queue") == settings.RABBITMQ["QUEUE"]
                and death.get("reason") == "rejected"
            ):
                return death.get("count", 0)

        return 0

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

        channel.queue_declare(
            queue=queue_name,
            durable=True,
        )

        # Enable publisher confirms for messages we republish.
        channel.confirm_delivery()

        # Process one message at a time.
        channel.basic_qos(prefetch_count=1)

        def callback(ch, method, properties, body):
            try:
                event = json.loads(body)

                if event.get("event") != "project.created":
                    raise ValueError("Unsupported event type")

                self.stdout.write(
                    self.style.SUCCESS(
                        f"Received project.created: {event['project_id']}"
                    )
                )

                self.stdout.write(f"Tenant: {event['tenant_id']}")
                self.stdout.write(f"Name: {event['name']}")

                # TODO: Perform actual background processing here.
                # raise ValueError("Simulated processing failure")

                # Acknowledge after successful processing.
                ch.basic_ack(delivery_tag=method.delivery_tag)

            except Exception as error:
                self.stderr.write(self.style.ERROR(f"Failed to process event: {error}"))

                retry_count = self.get_retry_count(properties.headers or {})

                if retry_count < MAX_RETRIES:
                    # Reject without requeue.
                    # RabbitMQ policy routes it to projects.retry.
                    ch.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=False,
                    )

                    self.stdout.write(
                        self.style.WARNING(
                            f"Retry scheduled: {retry_count + 1}/{MAX_RETRIES}"
                        )
                    )

                else:
                    try:
                        # Publish to the dead-letter queue.
                        # Publisher confirms ensure RabbitMQ accepted it
                        # before we acknowledge the original message.
                        ch.basic_publish(
                            exchange=DEAD_LETTER_EXCHANGE,
                            routing_key=DEAD_LETTER_ROUTING_KEY,
                            body=body,
                            properties=pika.BasicProperties(
                                content_type=properties.content_type,
                                delivery_mode=2,
                                headers={
                                    **(properties.headers or {}),
                                    "x-final-error": str(error),
                                },
                            ),
                            mandatory=True,
                        )

                        ch.basic_ack(
                            delivery_tag=method.delivery_tag,
                        )

                        self.stdout.write(
                            self.style.ERROR("Message moved to projects.dead-letter")
                        )

                    except Exception as publish_error:
                        self.stderr.write(
                            self.style.ERROR(
                                f"Failed to publish dead-letter message: "
                                f"{publish_error}"
                            )
                        )

                        # Keep the original message if DLQ publishing fails.
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
