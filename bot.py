from telethon.sync import TelegramClient
from telethon.sessions import MemorySession
import csv

def scrape_one_group(client,dialog, chat_groups):
    print(f"Group Name: {dialog.title}, Group ID: {dialog.id}")

    # Get all messages from the group
    messages = client.get_messages(dialog.id, limit=None)  # limit=None fetches all messages

    with open(f"{dialog.name}_data", 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['message id', 'message text'])
        for message in messages:
            print(f"Message ID: {message.id}, Message Text: {message.text}")
            csv_writer.writerow([message.id, message.text])


def get_all_groups(client, chat_groups, phone_number):
    # verifies authorization
    if not client.is_user_authorized():
        client.send_code_request(phone_number)
        # you will get a message to telegram ,insert it in the terminal
        client.sign_in(phone_number, input('Enter the code: '))

    # Get all dialogs (chats and channels)
    dialogs = client.get_dialogs()
    # Filter only groups from the dialogs
    for dialog in dialogs:
        if dialog.name in chat_groups:
            # dialog == group chat
            scrape_one_group(client, dialog, chat_groups)

def main():
    api_id =  # insert your api id
    api_hash = ""  # insert your hash api
    phone_number = +123456789  # insert internationally formatted phone number
    chats = ['Group123']  # choose the gourd you would like to scrape data from
    # create a connects as a client to telegram
    client = TelegramClient(MemorySession(), api_id, api_hash)
    client.connect()
    # mine data from all the chats
    get_all_groups(client, chats, phone_number)

    client.disconnect()

if __name__ == '__main__':
    main()
