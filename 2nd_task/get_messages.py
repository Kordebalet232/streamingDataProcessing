import asyncio
from telethon.sync import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch
import pyarrow.parquet as pq

api_id = YOUR_API_ID
api_hash = 'YOUR_API_HASH'
channel_username = 'CHANNEL_USERNAME'
output_folder = 'OUTPUT_FOLDER'

async def get_messages():
    # Создаем клиента с указанными api_id и api_hash
    client = TelegramClient('session', api_id, api_hash)

    try:
        # Авторизуемся
        await client.start()
    except SessionPasswordNeededError:
        # Если у аккаунта установлен пароль, введите его
        client.run(await client.get_password())

    # Ищем идентификатор канала по его юзернейму
    entity = await client.get_entity(channel_username)

    # Получаем последние сообщения за последнюю минуту
    messages = await client.get_messages(entity, limit=None, min_id=0, max_id=0, offset_date=None, offset_id=0, add_offset=0, filter=None, search=None, reply_to=None, ids=None, from_user=None, reply_markup=None, random_id=0, limit_date=None, min_date=(datetime.utcnow() - timedelta(minutes=1)), max_date=None)

    # Закрываем клиента
    await client.disconnect()

    return messages

async def save_messages_to_parquet(messages):
    # Создаем таблицу для сохранения сообщений
    table = pq.Table.from_pandas(messages)

    # Имя файла
    filename = f'{output_folder}/messages.parquet'

    # Сохраняем таблицу в Parquet
    pq.write_table(table, filename)

    print(f'Messages saved to {filename}')

if __name__ == '__main__':
    # Получаем все сообщения за последнюю минуту
    messages = asyncio.run(get_messages())

    # Сохраняем сообщения в Parquet
    asyncio.run(save_messages_to_parquet(messages))
