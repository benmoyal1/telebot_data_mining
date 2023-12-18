from telethon.sync import TelegramClient
from telethon.sessions import MemorySession
import csv
from datetime import datetime, timezone
from datetime import datetime as dt
import time
import pymongo
from pymongo import MongoClient

BATCH_SIZE = 5000
collection = MongoClient("mongodb://localhost:27017/").Telegram_Test.chat_entries


def generate_entry(message, client, chat_to_scrape):
    # url of the post in the message
    url = 'no media'
    if message.media:
        url = f'https://t.me/{chat_to_scrape}/{message.id}'.replace('@', '')

    # reactions
    emoji_string = 0
    if message.reactions is not None:
        for reaction_count in message.reactions.results:
            # emoji = reaction_count.reaction.emoticon
            emoji_string += str(reaction_count.count)
            # emoji_string += emoji + " " + count + " "

    # channel name after extracted from the channel link
    channel_name = chat_to_scrape.split("/")[-1] if "/" in chat_to_scrape else chat_to_scrape[1:]

    # message date
    msg_data = message.date.strftime('%Y-%m-%d')
    msg_time = message.date.strftime('%H:%M:%S')

    # the returned entry
    message_entry = {"date": msg_data, "time": msg_time,
                     "channel name": channel_name, "channel url": chat_to_scrape,
                     "link": url,
                     "content": message.text,
                     "reactions": emoji_string,
                     "views": message.views
                     }

    # if there are comments # important to come after the content list with append following it, so as not to confuse
    # the 'message' and collect only the contents of the comments
    comments = []
    try:
        for reply_message in client.iter_messages(chat_to_scrape, reply_to=message.id):
            comments.append(reply_message.text)
    except:
        comments = ['possible adjustment']
    comments = ', '.join(comments).replace(', ', ';\n')

    # add of the content with the comments
    message_entry["comments"] = comments

    # updates the progress counter
    print(f'Id: {message.id:05}.\n')
    return message_entry


def authorize_client(client, phone_number):
    if not client.is_user_authorized():
        print("Autorizing client..")
        client.send_code_request(phone_number)
        # you will get a message to telegram ,insert it in the terminal
        client.sign_in(phone_number, input('Enter the code: '))
        print("Done.")


def load_messages(client, chat_to_scrape):
    print("Loading messages..")
    messages = client.iter_messages(chat_to_scrape)
    print("Done.")
    return messages


def scrape_to_csv(client, chat_to_scrape, phone_number, t):
    # verifies authorization
    authorize_client(client, phone_number)
    messages = load_messages(client, chat_to_scrape)
    counter = 0
    batch = list()
    # Todo handle the if statement of the dates and
    # Todo send daniel all info can be gathered from msg (just print)
    # 
    in_range = True
    for message in messages:
        # print(message.date <= datetime(t['t_yy'], t['t_mm'], t['t_dd']))
        from_date = datetime(t['f_yy'], t['f_mm'], t['f_dd'], tzinfo=timezone.utc)
        to_date = datetime(t['t_yy'], t['t_mm'], t['t_dd'], tzinfo=timezone.utc)
        if to_date > message.date > from_date:
            batch.append(generate_entry(message, client, chat_to_scrape))
        if counter >= BATCH_SIZE:
            collection.insert_many(batch)
            batch.clear()
            counter = 0
        counter += 1


def main():
    with open('credentials.txt', 'r') as file:
        hash_id_phone = [line for line in file]

    api_hash = hash_id_phone[0]  # insert your hash api
    api_id = int(hash_id_phone[1])  # insert your api id
    phone_number = int(hash_id_phone[2])  # insert internationally formatted phone number
    chat_to_scrape = 'https://t.me/admma_news'  # copy the link from the description

    # enter the date range of the information you want to scrape
    # f - from, t - to, dd - day, mm - month,yy - year
    t = {"f_dd": 17, "t_dd": 17,
         "f_mm": 12, "t_mm": 12,
         "f_yy": 2023, "t_yy": 2023,
         }
    client = TelegramClient(MemorySession(), api_id, api_hash)
    client.connect()
    # mine data from all the chats
    scrape_to_csv(client, chat_to_scrape, phone_number, t)
    client.disconnect()


if __name__ == '__main__':
    main()
