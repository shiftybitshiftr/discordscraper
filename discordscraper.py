import discord
import asyncio
import json
import os
import time

TOKEN = 'YOUR_TOKEN_HERE'
SERVER_NAME = 'YOUR_SERVER_NAME_HERE'
RATE_LIMIT_DELAY = 1.1  # Adding a slight delay to handle rate limits

intents = discord.Intents.default()
intents.messages = True
intents.guilds = True

client = discord.Client(intents=intents)

async def fetch_messages(channel, limit=None):
    messages = []
    try:
        async for message in channel.history(limit=limit):
            messages.append({
                'author': message.author.name,
                'content': message.content,
                'timestamp': message.created_at.isoformat()
            })
            await asyncio.sleep(RATE_LIMIT_DELAY)
    except discord.errors.HTTPException as e:
        print(f'\nError scraping channel {channel.name}: {e}')
    return messages

@client.event
async def on_ready():
    print(f'Logged in as {client.user}')
    guild = discord.utils.get(client.guilds, name=SERVER_NAME)
    if not guild:
        print('Server not found')
        return

    all_messages = []

    tasks = []

    for channel in guild.text_channels:
        print(f'Starting to scrape channel: {channel.name}')
        tasks.append(fetch_messages(channel))

    results = await asyncio.gather(*tasks)
    
    for i, channel_messages in enumerate(results):
        channel = guild.text_channels[i]
        print(f'Finished scraping channel: {channel.name}')
        all_messages.extend(channel_messages)

        # Save individual channel messages to a file
        channel_filename = f'{channel.name}_history.json'
        with open(channel_filename, 'w', encoding='utf-8') as f:
            json.dump(channel_messages, f, indent=4, ensure_ascii=False)

    # Save all messages to a single file
    full_history_filename = 'full_history.json'
    if os.path.exists(full_history_filename):
        with open(full_history_filename, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
        all_messages = existing_data + all_messages

    with open(full_history_filename, 'w', encoding='utf-8') as f:
        json.dump(all_messages, f, indent=4, ensure_ascii=False)

    await client.close()

client.run(TOKEN)
