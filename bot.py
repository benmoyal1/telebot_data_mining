from telethon.sync import TelegramClient
from telethon.sessions import MemorySession
import csv
from datetime import datetime, timezone
from datetime import datetime as dt
import time
from pymongo import MongoClient
import pickle
from datetime import timedelta

# chose how many message you want to insert altogether
BATCH_SIZE = 20
MONGO_LOCAL = "mongodb://localhost:27017/"


def generate_entry(message, client, chat_to_scrape):
    # link to the  message
    url = f'https://t.me/{chat_to_scrape}/{message.id}'.replace('@', '')

    # reactions
    hearts_likes = 0
    total_reactions = 0
    if message.reactions is not None:
        for reaction_count in message.reactions.results:
            total_reactions += int(reaction_count.count)
            if reaction_count.reaction.emoticon in ['👍', '❤️']:
                hearts_likes += int(reaction_count.count)

    # channel name after extracted from the channel link
    channel_name = chat_to_scrape.split("/")[-1] if "/" in chat_to_scrape else chat_to_scrape[1:]

    # TODO make sure to correct the two hours gap
    # message date
    msg_data = message.date.strftime('%Y-%m-%d')
    msg_time = (message.date + timedelta(hours=2)).strftime('%H:%M:%S')

    # the returned entry
    message_entry = {"date": msg_data, "time": msg_time,
                     "channel name": channel_name,
                     "message_link": url,
                     "content": message.text,
                     "hearts_likes": hearts_likes,
                     "total_reactions": total_reactions,
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
    print(message_entry)
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


def scrape_to_db(client, chat_to_scrape, phone_number, t, collection):
    # verifies authorization
    authorize_client(client, phone_number)
    messages = load_messages(client, chat_to_scrape)
    counter = 0
    batch = []
    for message in messages:
        from_date = datetime(t['f_yy'], t['f_mm'], t['f_dd'], tzinfo=timezone.utc)
        to_date = datetime(t['t_yy'], t['t_mm'], t['t_dd'], tzinfo=timezone.utc)
        to_date = to_date.replace(hour=23, minute=59, second=59, microsecond=999999)

        # print(message.date <= to_date, message.date, "<=", to_date)
        # print(message.date >= from_date, message.date, ">=", from_date)

        if from_date <= message.date < to_date:
            batch.append(generate_entry(message, client, chat_to_scrape))
            counter += 1
            if counter >= BATCH_SIZE:
                collection.insert_many(batch)
                batch.clear()
                counter = 0
                break


def main():
    # insert your credentials into a pickle file for security reasons
    # use this 'with open' statement only when you use new api
    with open("credentials_pickel.pkl", 'wb') as file:
        pickle.dump({"api_hash": "2aa2e33a9asdv44sca4040c4e2c8asdc4",
                     "api_id": 11222222,
                     "phone": +11222222
                     }, file)

    # import credentials from pickle file
    with open("credentials_pickel.pkl", 'rb') as file:
        f = pickle.load(file)
        api_hash = f['api_hash']  # insert your hash api
        api_id = f["api_id"]  # insert your api id
        phone_number = f["phone"]  # insert internationally formatted phone number

    # the chat you want to scrape
    # copy the link from the chat or use @goupName *dont* copy the url from browser
    chat_to_scrape = 'https://t.me/admma_news'  # copy the link from the description

    # enter the date range of the information you want to scrape
    # from f_date <= scraped_message <= t_date
    # f - from, t - to, dd - day, mm - month,yy - year
    t = {"f_dd": 17, "t_dd": datetime.today().day,
         "f_mm": 12, "t_mm": datetime.today().month,
         "f_yy": 2023, "t_yy": datetime.today().year,
         }
    # connects to client
    client = TelegramClient(MemorySession(), api_id, api_hash)
    client.connect()
    # connects to db feel free to change
    collection = MongoClient(MONGO_LOCAL).Telegram_Test.chat_entries
    # collection.delete_many({})
    # mine data from all the chats
    scrape_to_db(client, chat_to_scrape, phone_number, t, collection)
    client.disconnect()


if __name__ == '__main__':
    main()
# attributes of message
# ['CONSTRUCTOR_ID', 'SUBCLASS_OF_ID', '__abstractmethods__', '__bytes__', '__class__',
# '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__',
# '__getattribute__', '__getstate__', '__gt__', '__hash__', '__init__', '__init_subclass__',
# '__le__', '__lt__', '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__',
# '__setattr__', '__sizeof__', '__slots__', '__str__', '__subclasshook__', '__weakref__', '_abc_impl',
# '_action_entities', '_broadcast', '_buttons', '_buttons_count', '_buttons_flat', '_bytes', '_chat',
# '_chat_peer', '_client', '_document_by_attribute', '_file', '_finish_init', '_forward', '_input_chat',
# '_input_sender', '_linked_chat', '_needed_markup_bot', '_refetch_chat', '_refetch_sender', '_reload_message',
# '_reply_message', '_sender', '_sender_id', '_set_buttons', '_text', '_via_bot', '_via_input_bot', 'action',
# 'action_entities', 'audio', 'button_count', 'buttons', 'chat', 'chat_id', 'click', 'client', 'contact', 'date',
# 'delete', 'dice', 'document', 'download_media', 'edit', 'edit_date', 'edit_hide', 'entities', 'file', 'forward',
# 'forward_to', 'forwards', 'from_id', 'from_reader', 'from_scheduled', 'fwd_from', 'game', 'geo',
# 'get_buttons', 'get_chat', 'get_entities_text', 'get_input_chat', 'get_input_sender'
# , 'get_reply_message', 'get_sender', 'gif', 'grouped_id', 'id', 'input_chat', 'input_sender',
# 'invert_media', 'invoice', 'is_channel', 'is_group', 'is_private', 'is_reply', 'legacy', 'mark_read',
# 'media', 'media_unread', 'mentioned', 'message', 'noforwards', 'out', 'peer_id', 'photo', 'pin',
# 'pinned', 'poll', 'post', 'post_author', 'pretty_format', 'raw_text', 'reactions', 'replies', 'reply',
# 'reply_markup', 'reply_to', 'reply_to_msg_id', 'respond', 'restriction_reason', 'sender', 'sender_id',
# 'serialize_bytes', 'serialize_datetime', 'silent', 'sticker', 'stringify', 'text', 'to_dict', 'to_id',
# 'to_json', 'ttl_period', 'unpin', 'venue', 'via_bot', 'via_bot_id', 'via_input_bot', 'video',
# 'video_note', 'views', 'voice', 'web_preview']
