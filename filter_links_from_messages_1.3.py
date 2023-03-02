import csv


def main():
    filepath = r"test_file.txt" #put the path(s) to your seed channel(s) here as a txt file
    output_filename_all_links = "channel_links_snowballing_round_1_all_links.csv"
    continue_execution = True
    try:
        with open(filepath, "r") as file_with_filenames:
            list_of_filenames = [file.strip() for file in file_with_filenames]
    except IOError:
        print(f"An error occured when reading {filepath}. Terminating the program.")
        continue_execution = False
    if continue_execution:
        output_data = {"channel_name": [], "channel_ID": [], "link": []}
        for filename in list_of_filenames:
            current_channel_name = extract_channelname_from_filename(filename)
            if current_channel_name == "NA":
                continue
            list_of_messages, current_channel_id = list_all_messages_from_channel_and_channelID(filename)
            if list_of_messages == None:
                continue
            list_of_links = get_links_from_messages(list_of_messages, "t.me/")
            output_data["channel_ID"] += [current_channel_id for i in range(len(list_of_links))]
            output_data["channel_name"] += [current_channel_name for i in range(len(list_of_links))]
            output_data["link"] += list_of_links
        
        number_of_entries = len(output_data["link"])
        csv_header = [key for key in output_data.keys()]
        try:
            with open(output_filename_all_links, "w", encoding="utf-8", newline="") as output_file:
                writer = csv.writer(output_file)
                writer.writerow(csv_header)
                for index in range(number_of_entries):
                    writer.writerow([output_data[key][index] for key in csv_header])
        except IOError:
            print("An error occured when creating the output file.")

        print("Extraction successful. Terminating program.")


def extract_channelname_from_filename(filename):
    if filename[-4:] == ".csv":
        if filename[-15:-4] == "invalidName":
            write_error_log("Invalid filename detected. Please check channel ID for identification.")
            return "NA"
        elif filename[-17:-4] == "erroneousLink":
            write_error_log("Empty file detected. Referenced channel does not exist.")
            return "NA"
        else:
            index_of_third_underscore = 0
            underscore_count = 0
            for letter in filename:
                if letter == "_":
                    underscore_count += 1
                if underscore_count == 3:
                    break
                index_of_third_underscore += 1
            return filename[index_of_third_underscore + 1:-4]


def list_all_messages_from_channel_and_channelID(filepath):
    try:
        with open(filepath, encoding="utf-8") as infile:
            dict_reader = csv.DictReader(infile)
            channel_id = None
            output_list = []
            for row in dict_reader:
                if channel_id == None:
                    channel_id = row["peer_id_channel_id"]
                output_list.append(row["message"])
            return output_list, channel_id
    except IOError:
        error_message = f"A problem occured while reading the file {filepath}"
        write_error_log(error_message)
        print(error_message)
        return None, None


def get_links_from_messages(list_of_messages, pattern):
    output_list = []
    list_of_tokens = []
    for message in list_of_messages:
        list_of_tokens += [token for token in message.split() if pattern in token]
    for token in list_of_tokens:
        output_list += extract_links_from_token(token, pattern)
    return output_list


def extract_links_from_token(token:str, pattern:str, output_list=None):
    if output_list == None:
        output_list = []
    valid_characters_for_link = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q",
        "r", "s", "t", "u", "v", "w", "x", "y", "z", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
        "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "1", "2", "3", "4", "5", "6", "7", "8", "9",
        "0", "_", "/"}
    new_token = None
    pattern_start_index = token.find(pattern)

    # No link in token => Return an empty list and proceed with next token
    if pattern_start_index == -1:
        return []
    pattern_end_index = pattern_start_index + len(pattern) - 1

    # Check prefix of t.me\ and set link_start_index accordingly
    if token[pattern_start_index - 8:pattern_start_index] == "https://":
        link_start_index = pattern_start_index - 8
    elif token[pattern_start_index - 7:pattern_start_index] == "http://":
        link_start_index = pattern_start_index - 7
    else:
        link_start_index = pattern_start_index

    # Check whether several links exist in a row and split them
    if pattern in token[pattern_end_index + 1:]:
        second_pattern_start_index = token[pattern_end_index + 1:].find(pattern) + len(token[:pattern_end_index + 1])
        if token[second_pattern_start_index - 8:second_pattern_start_index] == "https://":
            second_link_start_index = second_pattern_start_index - 8
        elif token[second_pattern_start_index - 7:second_pattern_start_index] == "http://":
            second_link_start_index = second_pattern_start_index - 7
        else:
            second_link_start_index = second_pattern_start_index
        new_token = token[second_link_start_index:]
        token_to_test = token[:second_link_start_index]
    else:
        token_to_test = token

    # Find end of link + Create new token if token exceeds link and does not exists
    link_end_index = pattern_end_index
    for letter in token_to_test[link_end_index + 1:]:
        if letter in valid_characters_for_link:
            link_end_index += 1
        else:
            if new_token == None:
                new_token = token_to_test[link_end_index + 1:]
            break

    # Check whether the link contains a message reference and do not append to output it in this case
    link = token[link_start_index:link_end_index + 1]
    link_without_prefix_and_pattern = link[link.find(pattern) + 5:]
    if "s/" in link_without_prefix_and_pattern:
        link_without_prefix_and_pattern = link_without_prefix_and_pattern[2:]
    link_prefix_and_pattern = link[:-len(link_without_prefix_and_pattern)]
    start_index_of_message_reference = len(link_prefix_and_pattern) + link_without_prefix_and_pattern.find("/")

    
    if "/" in link_without_prefix_and_pattern:
        output_list.append(link[:start_index_of_message_reference])
    else:
        output_list.append(link)

    # If at least one additional link remains in the remainder, call function recursively
    if new_token != None and pattern in new_token:
        extract_links_from_token(new_token, pattern, output_list)

    return output_list


def write_error_log(message):
    try:
        with open("error_log_link_filter.txt", "a") as error_logfile:
            error_logfile.write(message + "\n")
    except IOError:
        print("A problem occured when writing to the error log.")


if __name__ == "__main__":
    main()
