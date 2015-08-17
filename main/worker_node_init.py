# This file contains functions that automate the process for uploading, starting, and creating worker nodes
# in glance.  Dependencies include python glance-client, our proprietary state of the art virtual machine image 
# (courtesy of joe), and python nova.  

from novaclient import client
from novaclient import exceptions
from keystoneclient import session
from keystoneclient.auth.identity import v2
from glanceclient import Client
from time import sleep
from threading import Thread
from requests import post, get, ConnectionError

from hackurl import hackurl


# after the image is uploaded, you will need to boot it with nova
#
def activate_image(nova_client, ImageID, ServerName, Flavor):
    server = nova_client.servers.create(ServerName, ImageID, Flavor)
    return server


# once you get a server object, that information is constant.
# in order to get the most up to date information on a nova server, you
# must ask for it again.  this is used primarily for finding the status
# of a machine (active, error, building...)
#
def update_status(nova_client, server):
    server = nova_client.servers.get(server.id)
    return server


# REST api magic
def post_workload(nova_client, server, workload, loc):
    # retrieve ip address of the server for the post request
    addr_keys = nova_client.servers.ips(server).keys()[0]
    ip_address = nova_client.servers.ips(server)[addr_keys][0]['addr'].encode(
        'ascii')
    url = "http://" + ip_address + ':5000/jobs'
    if loc == 'local':
        url = hackurl(url)

    # post request takes a dictionary as argument, {filename: file pointer}
    files_to_upload = {'file': open(workload, 'rb')}

    # make the request using the two variables created above
    post(url, files=files_to_upload)


# search for a flavor that has exactly 4GB of RAM and 2 VCPUS
# if such a flavor is not found, begin a recursive search for the closest matching flavor
def find_flavor(nova_client, RAM=4096, vCPUS=2):
    # upper bound of recursive search
    if RAM > 262144 or vCPUS > 64:
        return None

    # search for the flavor
    for flavor in nova_client.flavors.list():
        if flavor.ram == RAM:
            if flavor.vcpus == vCPUS:
                print "Flavor found!  Specs: RAM=%d vCPUS=%d" % (RAM, vCPUS)
                return flavor.id.encode("ascii")

    # if not found, look for something bigger in RAM or vCPUS
    return find_flavor(nova_client, RAM * 2, vCPUS) or find_flavor(nova_client,
                                                                   RAM,
                                                                   vCPUS * 2)


def spawn_helper(nova_client, ImageID, ServerName, loc, schedule, flavor, num,
                 server_list):
    # print "Spawning transburst server with flavor id", flavor, "..."
    try:
        # and put that vm's workload in that file.
        server = activate_image(nova_client, ImageID, "Transburst Server Group",
                                flavor)


        # keep checking to make sure the server has been booted.
        # if an error state is reached, fall back.
        while not is_done_booting(nova_client, server, loc):
            server = update_status(nova_client, server)
            if (server.status == "ERROR"):
                server.delete()
                return server_list
            sleep(2)


        # reminder: schedule stores a list of list of videos.  each internal list is a seperate workload 
        # for each vm.  
        # each time we go through this loop for each VM, the workload will be different.
        # files argument takes a dictionary where keys are destination path and value is the contents of the file
        # on the server, we can create a file called "workload.txt"
        workload = schedule.pop(0)
        print "Workload for VM #", num, ":", workload,

        f = open("workload.txt", 'w')
        for video in workload:
            f.write(video + '\n')

        f.close()

        # using the rest api, send the workload to the vm.
        post_workload(nova_client, server, "workload.txt", loc)
    except exceptions.Forbidden:
        print "Your credentials don't give access to build more servers here."
        print "This instance will be launched on the remote cloud #", num
        return

    except exceptions.RateLimit:
        print "Rate limit reached"
        print "3..."
        sleep(1)
        print "2..."
        sleep(1)
        print"1..."
        sleep(1)
        return

    except (exceptions.ClientException, exceptions.OverLimit) as e:
        print "Local cloud resource quota reached."
        return

    # it's probably a good idea to keep these servers stored somewhere easily accessible    
    server_list.append(server)
    print "Booted %s server #%i" % (loc, len(server_list))


# keep spamming servers until we run out of room
def spawn(nova_client, ImageID, ServerName, loc, schedule, flavor):
    server_list = []
    max_num_instances = len(schedule)
    thread_list = []
    for i in range(0, max_num_instances):
        print "Spawning %s transburst server #%d..." %(loc, i)
        server_init_thread = Thread(target=spawn_helper,
                                    args=(nova_client, ImageID, ServerName, loc,
                                          schedule, flavor, i, server_list))
        thread_list.append(server_init_thread)
        thread_list[-1].start()
        # check to see if we booted enough vms

    for thread in thread_list:
        thread.join()

    print "%s servers done booting. Listening on port 5000." %loc
    print "Total servers needed:", len(server_list)
    print "Total vCPUs needed:", len(server_list) * 2
    print "Total RAM consumed:", len(server_list) * 4096
    return server_list


# checks the server status to see if it's still building


def is_done_booting(nova_client, server, loc):
    if server.status == 'ACTIVE':
        addr_keys = nova_client.servers.ips(server).keys()[0]
        ip_address = nova_client.servers.ips(server)[addr_keys][0][
            'addr'].encode('ascii')
        url = 'http://' + ip_address + ':5000/boot'
        if loc == 'local':
            url = hackurl(url)
        try:
            get(url)
            print ip_address + ' is done booting. REST API listening.'
            return True
        except ConnectionError:
            pass
    return False


# clean up time
#
def kill_servers(server_list):
    for index, server in enumerate(server_list):
        print "Destroying server " + str(index + 1)
        server.delete()


# given the resources you have available and the resources a single vm consumes,
# calculate how many vms you can fit on your cloud.
#
def find_local_max_number_of_vms(nova_client, tenant_name, flavor):
    flavor = nova_client.flavors.get(flavor)
    ram_per_vm = flavor.ram
    cores_per_vm = flavor.vpus

    quota = nova_client.quotas.get(tenant_name)
    max_ram = quota.ram
    max_cores = quota.cores
    max_servers = quota.server_groups

    num_vms = min(max_ram / ram_per_vm, max_cores / cores_per_vm, max_servers)
    return num_vms


# main is used for testing
#
if __name__ == "__main__":
    auth = v2.Password(auth_url="http://10.0.2.15:35357/v2.0", username="admin",
                       password="light", tenant_name="admin")

    sess = session.Session(auth=auth)

    nova_client = client.Client("2.26.0", session=sess)

    # NOTE: try the swift auth token
    # can be found via command "swift stat -v"
    glance_client = Client('1', endpoint="http://10.131.69.112:9292",
                           token="8520212cd1a34b39b0e9d8d475144abb")

    image = upload_new_image(glance_client, "gather.raw", True)
    activate_image(nova_client, image.id, "test server", 2)
