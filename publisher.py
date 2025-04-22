#!/usr/bin/env python3
import sys
from worker import process_file  # Import the celery task

def main():
    if len(sys.argv) < 2:
        print("Usage: publisher.py <file_path> [optional_characters]")
        sys.exit(1)

    file_path = sys.argv[1]
    optional_chars = sys.argv[2] if len(sys.argv) >= 3 else ""

    # Submit the celery task; this sends a message via RabbitMQ.
    result = process_file.delay(file_path, optional_chars)
    print("Task submitted with ID:", result.id)

if __name__ == "__main__":
    main()

