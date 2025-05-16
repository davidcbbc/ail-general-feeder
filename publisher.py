#!/usr/bin/env python3
import sys
from celery import Celery

# 1) Configure your publisher app exactly
#    to match the worker’s broker/backend/serializers:
app = Celery(
    'publisher',
    broker='pyamqp://guest@172.23.240.1//',   # ← point at your RabbitMQ
    backend='rpc://',                            # ← if you need .get() on results
    task_serializer='json',
    accept_content=['json'],
)

def main():
    if len(sys.argv) < 2:
        print("Usage: publisher.py <file_path> [optional_characters]")
        sys.exit(1)

    file_path = sys.argv[1]
    optional_chars = sys.argv[2] if len(sys.argv) >= 3 else ""

    # 2) Send by task name, not by importing the function
    #    Use the full name your worker registered, e.g. "worker.process_file"
    result = app.send_task(
        'worker.process_file',      # ← task name as the worker knows it
        args=[file_path, optional_chars],
        kwargs={},
        retry=True,                 # optional: auto-retry if broker is unreachable
    )

    print("Task submitted with ID:", result.id)

if __name__ == "__main__":
    main()
