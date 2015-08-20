""" Set of functions dealing with the scheduling and workload distribution
among the worker nodes
"""
import time
import urllib2
import predictor
from manager import hack_url


def time_until_deadline(deadline):
    """ Find the time until a deadline formatted as MM/DD/YYYY HH:MM:SS in
    seconds. If the time until deadline is negative (the deadline has already
    passed) the function will return 1 second until the deadline, and will keep
    trying to do the job anyway
    """
    try:
        # Tries to read the deadline in a specific format
        pattern = "%m/%d/%Y %H:%M:%S"
        epoch = int(time.mktime(time.strptime(deadline, pattern)))

        if epoch < time.time():
            return 1.0
        return epoch - time.time()

    except ValueError:
        # If the deadline isn't of this format, the program halts,
        # as a deadline is necessary to the program. You could return 1 here
        # as we did with negative times, but we chose not to
        print "Deadline required to be of form: MM/DD/YYYY HH:MM:SS"


def partition(remaining, swift, container_name='videos', file_list=None):
    """ Naive partitioning algorithm for figuring out which workloads can go
    on each VM. This is done using the predict machine learning algorithm on
    each of the files using the index file written earlier during the ingest
    portion of the program.

    Note: I (the person writing the docstrings) did not create this
    algorithm, so am unsure exactly of what's going on here. The lines and
    comments were left mostly as I found them with some minor formatting
    changes. Contact Ruben Madera (https://github.com/Roastmaster) for more
    info.
    """
    if not file_list: 
        container_data = []
        for data in swift.get_container(container_name)[1]:
            container_data.append('{0}\t{1}'.format(data['name'], data['bytes']))
        container_data = [token.split('\t') for token in container_data]

        # Use a list comprehension to create a list of all the file names
        file_list = []
        try:
            file_list = [token[0] for token in container_data]
        except IndexError:
            print "IndexError: Container empty"
    
    # Where we store the partitioned list of videos.
    # Internal lists separate what is possible to transcode in time on one VM
    partitioned_video_list = []

    # Given a time-until-completion by Joe's look up table, we keep
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
    """ The worker image has a function on /jobs/status that knows whether or
    not all of the jobs sent to it have been completed or not based off the
    number of files sent, and the number of files that have been placed back
    in swift. This function calls that for every server int he server_list,
    and if they are all done, it returns true, and otherwise returns false
    """
    for index, server in enumerate(server_list):
        # Finds the IP address of each of the server
        addr_keys = nova_client.servers.ips(server).keys()[0]
        ip_address = nova_client.servers.ips(server)[addr_keys][0]['addr']\
            .encode('ascii')

        # Creates the URL. /jobs/status is where the REST API listens for
        # requests about transcode jobs being complete
        url = "http://" + ip_address + ':5000/jobs/status'

        # Voodoo demo magic
        if loc == 'local':
            url = hack_url(url)

        # /jobs/status just returns 'True' or 'False' if the job is done or
        # not, so it's easy to check without any fancy HTML parsing
        website = urllib2.urlopen(url)
        if "False" == website.read().strip():
            return False
    return True
