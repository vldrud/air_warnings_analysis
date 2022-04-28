import configparser
import json
import time
import sys
import os
from settings.channel_list import CHANNEL_LIST

from telethon.sync import TelegramClient
from telethon import connection

# для корректного переноса времени сообщений в json
from datetime import date, datetime

# классы для работы с каналами
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch

# класс для работы с сообщениями
from telethon.tl.functions.messages import GetHistoryRequest

# Считываем учетные данные
config = configparser.ConfigParser()
config.read("settings/config.ini")

# Присваиваем значения внутренним переменным
api_id   = config['Telegram']['api_id']
api_hash = config['Telegram']['api_hash']
username = config['Telegram']['username']

# time the running of program
program_time = time.time()
print(type(program_time))
print(program_time)

print(api_id)
print(api_hash)
print(username)

# Создадим объект клиента Telegram API:
client = TelegramClient(username, api_id, api_hash)
print(client)
client.start()

# directory path
AppPath = sys.path[0]
data_raw_folder = AppPath + "/data_raw"


async def dump_all_messages(channel, url, mode='rewrite'):
    # mode = rewrite or update
    """Записывает json-файл с информацией о всех сообщениях канала/чата"""
    offset_msg = 0    # номер записи, с которой начинается считывание
    limit_msg = 20   # максимальное число записей, передаваемых за один раз

    all_messages = []   # список всех сообщений
    total_messages = 0
    total_count_limit = 0  # поменяйте это значение, если вам нужны не все сообщения
    max_id_message_channel = 200
    i = 0

    class DateTimeEncoder(json.JSONEncoder):
        '''Класс для сериализации записи дат в JSON'''
        def default(self, o):
            if isinstance(o, datetime):
                return o.isoformat()
            if isinstance(o, bytes):
                return list(o)

            return json.JSONEncoder.default(self, o)

    while True:
        history = await client(GetHistoryRequest(
            peer=channel,
            offset_id=offset_msg,
            offset_date=None, add_offset=0,
            limit=limit_msg, max_id=0, min_id=0,
            hash=0))
        if not history.messages:
            break
        messages = history.messages

        for message in messages:
            print(message.id)
            time.sleep(0.01)
            all_messages.append(message.to_dict())
        if i == 0:
            max_id_message_channel = message.id
            i += 1
        offset_msg = messages[len(messages) - 1].id
        total_messages = len(all_messages)
        print(f'channel: {channel.title}, n:{total_messages}')
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break
    print(max_id_message_channel)
    os.chdir(data_raw_folder)
    with open(str(url)+'.json', 'w', encoding='utf8') as outfile:
        json.dump(all_messages, outfile, ensure_ascii=False, cls=DateTimeEncoder)
    os.chdir('..')


# пользователь передаст ссылку на интересующий источник:
async def main():
        list_url = CHANNEL_LIST.values() # collected
        mode = 'update'
        for url in list_url:
            channel = await client.get_entity(url)
            print(channel)
            print(channel.title)
            time.sleep(10)
            await dump_all_messages(channel, url, mode)

with client:
    client.loop.run_until_complete(main())
