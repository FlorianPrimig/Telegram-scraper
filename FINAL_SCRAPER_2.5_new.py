import json
from datetime import datetime
import csv
import time
import credentials
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.messages import (GetHistoryRequest)
from telethon.tl.types import (
    PeerChannel
)


# Hier den Pfad vom CSV mit Channel-Links einfügen und Seriennummer anpassen (entsprechend der Nummer des csv files).
# Würde die Seriennummer immer einem konkreten CSV file mit Channel-Link listen korrespondierend zuweisen
csv_inputfile_path = r"YourPath.csv"
serial_number = 1

# you can get telegram development credentials in telegram API Development Tools
api_id = credentials.api_id
api_hash = credentials.api_hash

# use full phone number including + and country code
phone = credentials.phone
username = credentials.username
api_hash = str(api_hash)

# Create the client and connect
client = TelegramClient(username, api_id, api_hash)


# some functions to parse json date
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        if isinstance(o, bytes):
            return list(o)

        return json.JSONEncoder.default(self, o)


def unpack_list_of_nested_dictionaries(input_list_of_dictionaries, number_of_messages=0, key_for_nested_dictionaries="",
    output_dictionary=None, current_message_index=0, recursion_depht=0):  
    if output_dictionary == None:
        output_dictionary = {}
    if number_of_messages == 0:
        number_of_messages = len(input_list_of_dictionaries)
    key_at_beginning_of_function_call = key_for_nested_dictionaries
    for message in input_list_of_dictionaries:
        for key in message.keys():
            current_value = message[key]
            # Check for nested dictionaries and recuresively call the function to unpack them; Keep track of recursion and appropiate key names
            if type(current_value) == dict:
                key_for_nested_dictionaries += f"{key}_"
                recursion_depht += 1
                unpack_list_of_nested_dictionaries([current_value], number_of_messages, key_for_nested_dictionaries, output_dictionary,
                    current_message_index, recursion_depht)
                # Reset of recursion depht and key to state before recursive call
                recursion_depht -= 1
                key_for_nested_dictionaries = key_at_beginning_of_function_call

            # Check for dictionaries within lists and unpack them recursively, if necessary    
            elif type(current_value) == list and list_contains_dict(current_value):
                for entry in current_value:
                    key_for_output = key_for_nested_dictionaries + key
                    if key_for_output not in output_dictionary.keys():
                        output_dictionary[key_for_output] = ["NA" for i in range(number_of_messages)]
                    if type(entry) == dict:
                        key_for_nested_dictionaries += f"{key}_"
                        recursion_depht += 1
                        unpack_list_of_nested_dictionaries([entry], number_of_messages, key_for_nested_dictionaries, output_dictionary,
                            current_message_index, recursion_depht)
                        recursion_depht -= 1
                        key_for_nested_dictionaries = key_at_beginning_of_function_call

                    else:
                        if entry == None:
                            entry = "NA"
                        if type(output_dictionary[key_for_output][current_message_index]) == list:
                            output_dictionary[key_for_output][current_message_index].append(entry)
                        else:
                            output_dictionary[key_for_output][current_message_index] = [entry]
                        if output_dictionary[key_for_output][current_message_index] == None:
                            output_dictionary[key_for_output][current_message_index] = "NA"

            # Create a list representing content of individual messages mapped to the according keys
            else:
                key_for_output = key_for_nested_dictionaries + key
                if key_for_output not in output_dictionary.keys():
                    output_dictionary[key_for_output] = ["NA" for i in range(number_of_messages)]
                output_dictionary[key_for_output][current_message_index] = current_value
                if output_dictionary[key_for_output][current_message_index] == None:
                    output_dictionary[key_for_output][current_message_index] = "NA"

        if recursion_depht == 0:
            current_message_index += 1
    
    if recursion_depht == 0:
        for return_key in output_dictionary.keys():
            output_dictionary[return_key] = tuple(output_dictionary[return_key])
    return output_dictionary


def list_contains_dict(list_to_check):
    for i in list_to_check:
        if type(i) == dict:
            return True
    return False


def windows_filename_contains_invalid_character(input_string):
    invalid_characters = ["\\", "/", ":", "*", "?", "\"", "<", ">", "|"]
    for character in input_string:
        if character in invalid_characters:
            return True


async def main(phone):
    await client.start()
    print("Client Created")
    # Ensure you're authorized
    if await client.is_user_authorized() == False:
        await client.send_code_request(phone)
        try:
            await client.sign_in(phone, input('Enter the code: '))
        except SessionPasswordNeededError:
            await client.sign_in(password=input('Password: '))

    me = await client.get_me()
    channel_counter = 0
    continue_excecution = True
    # So viele Chats werden vor der Pause analysiert und Pause ist X minuten lang (siehe auch am Ende des scripts)
    loop_limit = 15
    minutes_to_pause = 3

    try:
        with open(csv_inputfile_path, newline='') as csv_inputfile:
            list_of_channel_names = [element[0] for element in csv.reader(csv_inputfile)]
    except FileNotFoundError:
        print("Datei existiert nicht.")
        continue_excecution = False

    if continue_excecution:

        for user_input_channel in list_of_channel_names:
            channel_adress = str(user_input_channel)
            channel_name = channel_adress[13:]
            channel_counter += 1            
            output_file_name = f"channel_messages_{serial_number:d}_{channel_counter:>04d}_{channel_name:s}.csv"
            if windows_filename_contains_invalid_character(output_file_name):
                output_file_name = f"channel_messages_{serial_number:d}_{channel_counter:>04d}_invalidName.csv"
            
            if user_input_channel.isdigit():
                entity = PeerChannel(int(user_input_channel))
            else:
                entity = user_input_channel

            channel_exists = True
            try:
                my_channel = await client.get_entity(entity)
            except ValueError:
                channel_exists = False
            
            all_messages = []
            if channel_exists:
                offset_id = 0
                limit = 100

                total_messages = 0
                total_count_limit = 0

                while True:
                    print("Current Offset ID is:", offset_id, "; Total Messages:", total_messages)
                    history = await client(GetHistoryRequest(
                        peer=my_channel,
                        offset_id=offset_id,
                        offset_date=None,
                        add_offset=0,
                        limit=limit,
                        max_id=0,
                        min_id=0,
                        hash=0
                    ))
                    if not history.messages:
                        break
                    messages = history.messages
                    for message in messages:
                        all_messages.append(message.to_dict())
                    offset_id = messages[len(messages) - 1].id
                    total_messages = len(all_messages)
                    if total_count_limit != 0 and total_messages >= total_count_limit:
                        break

            else:
                print(f"Invalid channel adress in csv-file at position {channel_counter}: {channel_adress}")
                all_messages.append("NA")
                output_file_name = f"channel_messages_{serial_number:d}_{channel_counter:>04d}_erroneousLink.csv"
        
            number_of_messages = len(all_messages)
            # If the channel name was invalid, an empty file is written
            if number_of_messages == 1:
                with open(output_file_name, "w") as output_file:
                    writer = csv.writer(output_file)
                    writer.writerow(["NA"])
                error_message = f"Empty file: \"{output_file_name}\". Invalid channel adress."
                print(error_message + " Appending to Error logfile.")
                with open("Error_log.txt", "a") as error_log:
                    error_log.write(error_message)
            # Convert messages into a csv file
            else:
                output_dictionary = unpack_list_of_nested_dictionaries(all_messages)
                csv_header = [key for key in output_dictionary.keys()]
                try:
                    with open(output_file_name, "w", encoding='utf-8') as output_file:
                        writer = csv.writer(output_file)
                        writer.writerow(csv_header)
                        for index in range(number_of_messages):
                            writer.writerow([output_dictionary[key][index] for key in csv_header])
                except IOError:
                    csv_error_message = f"An error occured when writing the file \"{output_file_name}\""
                    print(csv_error_message + " Appending to error logfile.")
                    with open("Error_log.txt", "a") as error_log:
                        error_log.write(csv_error_message)

            print(f"Conversion for channel {channel_adress} finished. Check error logfile in case of errors.")

            if channel_counter == loop_limit:
                loop_limit += 15
                time.sleep(minutes_to_pause * 180)

# hier oben loop_limit und time.sleep nach Bedarf anpassen!
with client:
    client.loop.run_until_complete(main(phone))
