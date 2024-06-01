import asyncio
import os
import time
import logging
from dotenv import load_dotenv
from telethon import TelegramClient, events

# Load environment variables from .env file
load_dotenv()

# Define the path to the config.yaml file
CONFIG_PATH = '/home/admin/Production/RickDir/config.yaml'

# Function to perform health check
async def health_check(start_time, bot_token, destination_group_id):
    try:
        while True:
            # Calculate the elapsed time since the script started
            elapsed_time = time.time() - start_time
            # Check if the elapsed time is greater than 8 hours (28800 seconds)
            if elapsed_time >= 8 * 60 * 60:
                message = "AlertV2: Telegram Forwards have been happening/running well for the past 8 hours."
            else:
                message = "AlertV2: Telegram Forwards have not been running well for the past 8 hours."

            # Send the message to the Telegram group
            await send_telegram_message(bot_token, destination_group_id, message)

            # Sleep for 8 hours before performing the next health check
            await asyncio.sleep(8 * 60 * 60)
    except Exception as e:
        logging.error(f"Error during health check: {e}")

# Function to send message to Telegram group
async def send_telegram_message(bot_token, destination_group_id, message):
    try:
        # Initialize Telegram client
        client = TelegramClient('health_check_client', bot_token.split(":")[0], bot_token.split(":")[1])
        await client.start(bot_token=bot_token)

        # Send the message to the destination channel
        await client.send_message(destination_group_id, message)

        # Disconnect the client
        await client.disconnect()
    except Exception as e:
        logging.error(f"Error sending Telegram message: {e}")

# Your existing message handler function
async def message_handler(event, client, destination_channel_id, sourcemessages_file, forwardedmessages_file, forwarded_message_ids):
    # Your existing message handling logic
    pass

# Your existing main function
async def main():
    try:
        config = load_config()

        api_id = os.getenv('TELEGRAM_API_ID')
        api_hash = os.getenv('TELEGRAM_API_HASH')
        destination_channel_id = int(os.getenv('DESTINATION_CHANNEL_ID'))

        source_channels = [int(channel_id) for channel_id in os.getenv('SOURCE_CHANNEL_IDS').split(',')]

        # Derive the absolute paths from the config file and script directory
        base_dir = os.path.dirname(CONFIG_PATH)
        sourcemessages_file = os.path.join(base_dir, config['files']['sourcemessages'])
        forwardedmessages_file = os.path.join(base_dir, config['files']['forwardedmessages'])
        source_queue_file = os.path.join(base_dir, config['queues']['source_queue'])
        session_file = os.path.join(base_dir, config['session_file'])

        log_file = os.path.join(base_dir, config['logs']['filename'])

        forwarded_message_ids = load_forwarded_message_ids(forwardedmessages_file)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

        client = TelegramClient(session_file, api_id, api_hash)

        # Rate Limiting Variables
        rate_limit_interval = 50  # seconds
        last_forwarded_time = 0

        # Queue for handling messages
        message_queue = asyncio.Queue()

        @client.on(events.NewMessage)
        async def handler(event):
            nonlocal last_forwarded_time

            chat_id = event.chat_id

            if chat_id in source_channels:
                await message_queue.put(event)
                logging.info(f'New message detected and added to queue: {event.id}')

        async def process_message_queue():
            nonlocal last_forwarded_time

            while True:
                # Get the next message from the queue
                event = await message_queue.get()

                # Process the message
                await message_handler(event, client, destination_channel_id, sourcemessages_file, forwardedmessages_file, forwarded_message_ids)

                # Check rate limit
                current_time = time.time()
                if current_time - last_forwarded_time < rate_limit_interval:
                    # Sleep to enforce rate limit
                    await asyncio.sleep(rate_limit_interval - (current_time - last_forwarded_time))

                # Update last forwarded time
                last_forwarded_time = time.time()

                # Mark the message as processed
                message_queue.task_done()

                # Save the queue state to a text file
                with open(source_queue_file, 'w') as f:
                    while not message_queue.empty():
                        item = await message_queue.get()
                        f.write(f'{item.id}\n')
                logging.info('Queue state saved.')

        try:
            await client.start()
            asyncio.create_task(process_message_queue())

            # Get bot token and destination group ID from environment variables
            bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
            destination_group_id = os.getenv('TELEGRAM_DESTINATION_GROUP_ID')

            # Track the start time of the script
            start_time = time.time()

            # Create a task to perform the health check every 8 hours
            health_check_task = asyncio.create_task(health_check(start_time, bot_token, destination_group_id))

            # Wait for the health check task to complete
            await health_check_task
        finally:
            # Graceful Shutdown: Close resources properly
            await client.disconnect()
            logging.info('Client disconnected.')

    except Exception as e:
        logging.error(f'Error in main function: {e}')

if __name__ == "__main__":
    asyncio.run(main())
