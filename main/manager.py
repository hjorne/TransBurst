""" The manager.py file contains functions to automate uploading, starting,
creating, and otherwise managing worker nodes.
"""
from novaclient import exceptions
from time import sleep
from threading import Thread
from requests import post, get, ConnectionError
import json


def load_credentials(filename):
    """ Simple function to load and return a credentials json dictionary """
    with open(filename) as credentials:
        return json.load(credentials)


def activate_image(nova_client, image_id, flavor, server_name='TransBurst'):
    """ Boots up a VM with the specified image and flavor, and returns a
    client used for interacting with it.
    """
    server = nova_client.servers.create(server_name, image_id, flavor)
    return server


def update_status(nova_client, server):
    """ Once you get a server object, that information is constant. In order
    to get the most up to date information on a nova server, you must ask for
    it again.  this is used primarily for finding the status of a machine
    (active, error, building, etc.)
    """
    server = nova_client.servers.get(server.id)
    return server


def hack_url(url):
    """ This function is specific for our Cisco demo. We could not access our
    VM IP addresses directly for the local cloud which were needed to access
    the REST API for controlling the worker nodes, so using port forwarding and
    this hack_url function, we found a work around. Mostly ignore this unless
    you have similar problems
    """
    scheme, x, rest = url.partition('://')
    netloc, x, path = rest.partition('/')
    host, x, port = netloc.partition(':')
    x, y, octet = host.rpartition('.')
    newport = str(int(octet) + 20000)
    return scheme + '://172.29.74.183:' + newport + '/' + path


def post_workload(nova_client, server, workload, loc):
    """ Using the REST API on each of the worker nodes, this is function uses
    HTTP POST to send a workload to each image after all the workloads have
    been partitioned.
    """

    # Retrieve IP address of the server for the post request
    addr_keys = nova_client.servers.ips(server).keys()[0]
    ip_address = nova_client.servers.ips(server)[addr_keys][0]['addr']\
        .encode('ascii')

    # REST API is listening on port 5000, and the POST URL is /jobs
    url = "http://" + ip_address + ':5000/jobs'

    # Voodoo bit of coding to make it work on local and non-local clouds
    # using the hack_url function
    if loc == 'local':
        url = hack_url(url)

    # POST request takes a dictionary as argument, {filename: file pointer}
    files_to_upload = {'file': open(workload, 'rb')}
    post(url, files=files_to_upload)


def find_flavor(nova_client, RAM=4096, vCPUs=2):
    """ Search for a flavor that has exactly 4GB of RAM and 2 vCPUs.
    If such a flavor is not found, begin a recursive search for the closest
    matching flavor
    """

    # Upper bound of recursive search
    if RAM > 262144 or vCPUs > 64:
        return None

    # Search for the flavor
    for flavor in nova_client.flavors.list():
        if flavor.ram == RAM:
            if flavor.vcpus == vCPUs:
                print "Flavor found!  Specs: RAM=%d vCPUs=%d" % (RAM, vCPUs)
                return flavor.id.encode("ascii")

    # If not found, look for something bigger in RAM or vCPUs
    return (find_flavor(nova_client, RAM * 2, vCPUs) or
            find_flavor(nova_client, RAM, vCPUs * 2))


def spawn_thread(nova_client, image_id, loc, schedule, flavor, num,
                 server_list):
    """ Function that gets called when we use threading to simultaneously
    spawn all required VMs
    """

    # The try block will attempt to boot the server. There are a number of
    # different things can go wrong, and those are handled separately in the
    # multiple except blocks below.
    try:
        # Attempt to boot server
        server = activate_image(nova_client, image_id, flavor)

        # Keep checking to make sure the server has been booted.
        # If an error state is reached, fall back.
        while not done_booting(nova_client, server, loc):
            server = update_status(nova_client, server)
            if server.status == "ERROR":
                server.delete()
                return server_list
            sleep(2)

        # The schedule variable stores a list of list of videos. Each internal
        # list is a different workload for each VM. Each time we go through
        # this loop for each VM, the workload will be different.
        workload = schedule.pop(0)
        print "Workload for VM #", num, ":", workload,

        # Write the workload to the necessary file to be POST'd to the REST
        # API
        f = open("workload.txt", 'w')
        for video in workload:
            f.write(video + '\n')

        f.close()

        # Using the REST API, send the workload to the VM
        post_workload(nova_client, server, "workload.txt", loc)

    # Various exception blocks to handle problems booting VMs. Print
    # statements cover pretty well what each exception block handles
    except exceptions.Forbidden:
        print "Your credentials don't give access to build more servers here."
        print 'This instance will be launched on the remote cloud #', num
        return

    except exceptions.RateLimit:
        print 'Rate limit reached. Retrying in 5 seconds...'
        sleep(5)
        return

    except (exceptions.ClientException, exceptions.OverLimit):
        print 'Local cloud resource quota reached.'
        return

    # If the code reaches here, the server has been booted successfully
    server_list.append(server)
    print "Booted %s server #%i" % (loc, len(server_list))


def spawn(nova_client, flavor_id, image_id, location, schedule,
          server_name='TransBurst'):
    """ Spawns a number of VMs given by max_num_instances, which is based
    on the scheduling algorithm. Uses threading so that each VM can be
    spawned simultaneously
    """
    server_list = []
    max_num_instances = len(schedule)
    thread_list = []

    # max_num_instances places an upper bound on how many we might need based
    # off the scheduling algorithm
    for i in range(0, max_num_instances):
        print "Spawning %s TransBurst server #%d..." % (location, i)
        arg_list = (nova_client, image_id, server_name, location, schedule,
                    flavor_id, i, server_list)
        server_init_thread = Thread(target=spawn_thread,
                                    args=arg_list)
        thread_list.append(server_init_thread)
        thread_list[-1].start()

    # Waits until all thread are finished booting to exit the function
    for thread in thread_list:
        thread.join()

    print "%s servers done booting. Listening on port 5000." % location
    print "Total servers needed:", len(server_list)
    print "Total vCPUs needed:", len(server_list) * 2
    print "Total RAM consumed:", len(server_list) * 4096
    return server_list


def done_booting(nova_client, server, loc):
    """ Using the REST API of a worker node, check if the node is done
    booting by seeing if the REST API is listening. If it is, then the
    requests function 'get' will return a 200 OK, otherwise it will throw a
    ConnectionError, which we catch.
    """

    # If OpenStack doesn't think the server is done booting, then it
    # definitely isn't, and we can return false
    if server.status == 'ACTIVE':
        # If OpenStack does think it's done booting, it may be done booting,
        # but the REST API may not be listening. Here we ping it.

        # Generate the URL to ping based off the IP address from the
        # nova_client. The REST API is listening on port 5000 and the URL
        # we're looking for on the REST API is /boot
        addr_keys = nova_client.servers.ips(server).keys()[0]
        ip_address = nova_client.servers.ips(server)[addr_keys][0]['addr']\
            .encode('ascii')
        url = 'http://' + ip_address + ':5000/boot'

        # Voodoo magic for demo. Can probably remove these if your clouds
        # work fine.
        if loc == 'local':
            url = hack_url(url)

        # Try/Except block for checking if the REST API is up and listening
        try:
            get(url)
            print ip_address + ' is done booting. REST API listening.'
            return True
        except ConnectionError:
            # If a ConnectionError is thrown, the except block simply passes,
            # as the next line will return false anyway
            pass
    return False


def kill_servers(server_list):
    """ Kills a list of servers """
    for index, server in enumerate(server_list):
        print "Destroying server " + str(index + 1)
        server.delete()


def find_image(glance_client, image_name='worker'):
    """ Find and return an image ID for the image we want, since this is how
    OpenStack actually allows us to start up images of a specified type. For
    this project, the image we have is always called 'worker', so this id the
    default, but it can be easily changed.
    """
    image_list = list(glance_client.images.list())
    for image in image_list:
        if image.name == image_name:
            return image.id
    return None


def find_local_max(nova_client, tenant_name, flavor):
    """ Given the resources you have available and the resources a single VM
    consumes, calculate how many VMs you can fit on your cloud. To do this we
    use the quotas API, and while this isn't perfect, it gets the job done
    satisfactorily in this case
    """
    flavor = nova_client.flavors.get(flavor)
    ram_per_vm = flavor.ram
    cores_per_vm = flavor.vpus

    quota = nova_client.quotas.get(tenant_name)
    max_ram = quota.ram
    max_cores = quota.cores
    max_servers = quota.server_groups

    num_vms = min(max_ram / ram_per_vm, max_cores / cores_per_vm, max_servers)
    return num_vms
