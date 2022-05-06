from subprocess import check_output
import os
from datetime import datetime
import uuid
import csv
import pandas as pd


def get_system_address():
    if os.uname().sysname == "Linux":
        ips = check_output(['hostname', '--all-ip-addresses']).decode("utf-8").strip()
        return ips
    else:
        return "localhost"


def create_uuid(length=8):
    return str(uuid.uuid4())[:length]


def get_timestamp():
    return datetime.now()


def get_time_difference(start, end):
    difference = end - start
    # print(difference.total_seconds() * 1000)
    return difference.total_seconds()
    # return divmod(difference.days * seconds_in_day + difference.seconds, 60)


header = ['topic', 'pub_id', 'sub_id', 'time_difference', 'pub_timestamp', 'sub_timestamp']
data = []
header_averages = ["pubs/subs", "brokers", "registries", "average", "sub messages"]
average = []


# construct message to publish
def get_publish_message(topic, value, pub_ip, uuid):
    timestamp = get_timestamp()
    message = "%s %s %s-%s %s" % (topic, value, pub_ip, uuid, timestamp)
    # print(message)
    return message


# get message received by subscriber and save data in local
def get_subscribe_message(message, sub_ip, uuid):
    print(">>>>>>>>>>>>")
    print(message)
    # get current timestamp (when subscriber receives message)
    current_time = get_timestamp()

    topic, value, pub_id, *time = message.split()
    sent_time = " ".join(time)
    time_difference = get_time_difference(datetime.strptime(sent_time, '%Y-%m-%d %H:%M:%S.%f'), current_time)
    sub_id = "{}-{}".format(sub_ip, uuid)

    # print("{} {} {} {} {}".format(topic, value, pub_id, sub_id, time_difference))
    row = [topic, pub_id, sub_id, time_difference, sent_time, current_time.strftime('%Y-%m-%d %H:%M:%S.%f')]
    data.append(row)
    # print(row)
    return [topic, value, pub_id, sub_id, sent_time, current_time.strftime('%Y-%m-%d %H:%M:%S.%f')]


# results/2-1-2-broker-linear/2-1-2-broker-linear.csv
csv_file = "results/{}-{}-{}-{}-{}-{}/{}-{}-{}-{}-{}-{}.csv"


# get subscriber data from local and write to csv file
def write_to_csv(num_pubs, num_subs, num_brokers, num_registries, strategy, topo):
    file = csv_file.format(num_pubs, num_subs, num_brokers, num_registries, strategy, topo, num_pubs, num_subs, num_brokers, num_registries, strategy, topo)
    print("writing data to:", file)
    sorted_data = sorted(data, key=lambda row: (row[4]), reverse=False)  # sorted by time sent by pub
    for d in sorted_data:
        print(d[3])
    new_data_entry_length = len(sorted_data)
    print("number of data rows:", new_data_entry_length)

    # dropping highest and lowest delay time
    # _, *sorted_data, _ = sorted_data
    # print("number of data rows after dropping:", len(sorted_data))

    topo = topo if topo is not None else ""
    if csv_exists(file):
        append_data_row(file, sorted_data)
    else:
        os.makedirs("results/{}-{}-{}-{}-{}-{}".format(num_pubs, num_subs, num_brokers, num_registries, strategy, topo), exist_ok=True)
        with open(file, "w", encoding="UTF8", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(sorted_data)

    averages(new_data_entry_length, num_pubs, num_subs, num_brokers, num_registries, strategy, topo)
    total_average(file, num_pubs, num_subs, num_brokers, num_registries, strategy, topo)


def append_data_row(file, rows):
    # open file in append mode
    with open(file, 'a+', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)


def combine_averages(file, pubsub, new_av):
    print(file, pubsub, new_av)
    old_averages = pd.read_csv(file)
    print(old_averages)
    indexer = old_averages[old_averages["pubs/subs"] == pubsub].index
    print(indexer)
    print(old_averages.loc[indexer, "average"])

    with open(file, "w", encoding="UTF8", newline='') as f:
        writer = csv.writer(f)
        pub_sub = old_averages["pubs/subs"]

        # for row in average_data:
        #     print(row)
        #     if row[0] == pubsub:
        #         print(row[0], row[1])
        #         old_average = row[1]
        #         new_average = (old_average + new_av[0][1])/2
        #         writer.writerow([pubsub, new_average])
        #         break
        #     else:
        #         writer.writerow(row)
        #     writer.writerows(average_data)


def csv_exists(path):
    return os.path.exists(path)


def averages(new_data_count, num_pubs, num_subs, num_brokers, num_registries, strategy, topo):
    filepath_averages = "results/{}-{}-{}-{}-{}-{}/averages_{}_{}.csv".format(num_pubs, num_subs, num_brokers, num_registries, strategy, topo, strategy, topo)
    csv_data = pd.read_csv(csv_file.format(num_pubs, num_subs, num_brokers, num_registries, strategy, topo, num_pubs, num_subs, num_brokers, num_registries, strategy, topo))
    new_data = csv_data.tail(new_data_count)
    print("averaging {} rows".format(new_data_count))

    # data_min = csv_data["time_difference"].min()
    # data_max = csv_data["time_difference"].max()
    #
    # print("not including min {} and max {} time delay in average".format(data_min, data_max))

    average_time_delay = new_data["time_difference"].mean().round(8)

    average_row = ["{}/{}".format(num_pubs, num_subs), "{}".format(num_brokers), "{}".format(num_registries), average_time_delay, len(new_data)]
    average.append(average_row)
    print("average for {} pubs, {} subs, {} brokers, {} registries: {}".format(num_pubs, num_subs, num_brokers, num_registries, average_row))

    if csv_exists(filepath_averages):
        append_data_row(filepath_averages, average)
        # combine_averages(filepath_averages, "{}/{}".format(num_pubs, num_subs), average)
    else:
        with open(filepath_averages, "w", encoding="UTF8", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header_averages)
            writer.writerows(average)


def total_average(csv_data_file, num_pubs, num_subs,  num_brokers,num_registries, strategy, topo):
    filepath_total_average = "results/{}-{}-{}-{}-{}-{}/total_average_{}-{}-{}-{}_{}_{}.csv".format(num_pubs, num_subs, num_brokers, num_registries, strategy, topo, num_pubs, num_subs, num_brokers, num_registries, strategy, topo)
    csv_data = pd.read_csv(csv_data_file)
    average_time_delay = csv_data["time_difference"].mean().round(8)
    csv_length = len(csv_data)
    print("total average_time_delay {} for {} rows".format(average_time_delay, csv_length))
    # time = csv_data.iloc[0, 3]
    # print("first row time delay: {}".format(time))

    total_average_row = [["{}/{}".format(num_pubs, num_subs), "{}".format(num_brokers), "{}".format(num_registries), average_time_delay, csv_length]]

    if csv_exists(filepath_total_average):
        append_data_row(filepath_total_average, total_average_row)
    else:
        with open(filepath_total_average, "w", encoding="UTF8", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header_averages)
            writer.writerows(total_average_row)

    # with open(filepath_total_average, "w", encoding="UTF8", newline='') as f:
    #     writer = csv.writer(f)
    #     if not csv_exists(filepath_total_average):
    #         writer.writerow(header_averages)
    #         writer.writerows(total_average_row)
    #     else:
    #         csv_data.loc[0, "average"] = average_time_delay
