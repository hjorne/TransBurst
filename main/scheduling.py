import time
import urllib2

import predictor
from manager import hack_url


def time_until_deadline(deadline):
    # WARNING:  Time must be formatted the same as pattern
    #
    try:
        pattern = "%m/%d/%Y %H:%M:%S"
        epoch = int(time.mktime(time.strptime(deadline, pattern)))
        if epoch < time.time():
            raise Exception("Negative deadline")
        return epoch - time.time()
    except ValueError:
        print "Deadline required to be of form: MM/DD/YYYY HH:MM:SS"
    except Exception as e:
        print e
        return 1


def partition(remaining, swift, container_name='videos', file_list=None):
    if not file_list: 
        container_data = []
        for data in swift.get_container(container_name)[1]:
            container_data.append('{0}\t{1}'.format(data['name'], data['bytes']))
        container_data = [token.split('\t') for token in container_data]

        # use a list comprehension to create a list of all the filenames
        file_list = []
        try:
            file_list = [token[0] for token in container_data]
        except IndexError:
            print "IndexError: Container empty"
    
    # where we store the partitioned list of videos.
    # Internal lists separate what is possible to transcode in time on one VM
    partitioned_video_list = []

    # given a time-until-completion by joe's look up table, we keep
    # decrementing "time_until_deadline" by these times until it reaches
    # zero, then, create a new list (representing a new vm), and repeat.
    tmp_t_u_d = remaining
    print "Time Remaining:", predictor.prettify_time(remaining)
    single_vm_capacity = []
    for video in file_list:
        single_vm_capacity.append(video)
        prediction_time = predictor.predict(video)
        if prediction_time > remaining:
            print "WARNING:  File is too big to be transcoded by VM in time."
            partitioned_video_list.append(single_vm_capacity)
            single_vm_capacity = []
            tmp_t_u_d -= prediction_time
            continue

        if tmp_t_u_d - prediction_time > 0:
            tmp_t_u_d -= prediction_time
            if video == file_list[-1]:
                partitioned_video_list.append(single_vm_capacity)

        else:
            tmp_t_u_d = remaining
            partitioned_video_list.append(single_vm_capacity)
            single_vm_capacity = []

    return partitioned_video_list


def transcode_complete(nova_client, server_list, loc):
    for index, server in enumerate(server_list):
        addr_keys = nova_client.servers.ips(server).keys()[0]
        ip_address = nova_client.servers.ips(server)[addr_keys][0][
            'addr'].encode('ascii')
        url = "http://" + ip_address + ':5000/jobs/status'
        if loc == 'local':
            url = hack_url(url)
        website = urllib2.urlopen(url)
        if "False" == website.read().strip():
            return False
    return True
