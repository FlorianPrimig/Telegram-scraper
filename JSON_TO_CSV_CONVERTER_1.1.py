import json
import csv


def main():
    # Insert path of file containing the names of files to be converted here:
    filepath_filenames = r"filenames_test.txt"

    line_in_list_of_filenames = 1
    continue_execution = True

    print("Starting conversion...")

    try:
        with open(filepath_filenames, "r") as input_file_filenames:
            list_of_filenames = [line.strip() for line in input_file_filenames]
    except FileNotFoundError:
        print("Invalid filepath for list of filenames, please double-check.\nTerminating program.")
        continue_execution = False

    print(list_of_filenames)

    if continue_execution:
        for filename in list_of_filenames:
            print(filename)
            output_filename = f"{filename[0:-5]:s}_converted.csv"
            file_exists = True

            try:
                with open(filename, "r") as input_json:
                    list_of_messages = json.load(input_json)
            except FileNotFoundError:
                error_message = f"File at position {line_in_list_of_filenames} in filename list does not exist."
                print(error_message + " Appending to Error logfile.")
                with open("Error_log.txt", "a") as error_log:
                    error_log.write(error_message)
                line_in_list_of_filenames += 1
                file_exists = False

            if file_exists:
                number_of_messages = len(list_of_messages)
                if number_of_messages == 1:
                    with open(output_filename, "w") as output_file:
                        writer = csv.writer(output_file)
                        writer.writerow(["NA"])
                    error_message = f"Empty file at position {line_in_list_of_filenames} in filename list."
                    print(error_message + " Appending to Error logfile.")
                    with open("Error_log.txt", "a") as error_log:
                        error_log.write(error_message)
                    line_in_list_of_filenames += 1

                else:
                    line_in_list_of_filenames += 1
                    output_dict = unpack_list_of_nested_dictionaries(list_of_messages, number_of_messages)

                    csv_header = [key for key in output_dict.keys()]

                    with open(output_filename, "w", encoding='utf-8') as output_file:
                        writer = csv.writer(output_file)
                        writer.writerow(csv_header)
                        for index in range(number_of_messages):
                            writer.writerow([output_dict[key][index] for key in csv_header])

        print("Conversion finished. Check error logfile in case of errors.")


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
                unpack_list_of_nested_dictionaries([current_value], number_of_messages, key_for_nested_dictionaries,
                    output_dictionary, current_message_index, recursion_depht)
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
                        unpack_list_of_nested_dictionaries([entry], number_of_messages, key_for_nested_dictionaries,
                            output_dictionary, current_message_index, recursion_depht)
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
        for output_dictionary_key in output_dictionary.keys():
            output_dictionary[output_dictionary_key] = tuple(output_dictionary[output_dictionary_key])
    return output_dictionary


def list_contains_dict(list_to_check):
    for i in list_to_check:
        if type(i) == dict:
            return True
    return False


if __name__ == "__main__":
    main()
