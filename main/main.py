from scheduling import *
from clients import *
from init import *
from upload import *
from move import *

# The default state is to run only in the local cloud, using the remote on an
# as-needed basis
local_only = True

local_credentials = load_credentials('local.json')
local_keystone = create_keystone_client(local_credentials)
local_glance = create_glance_client(local_keystone)
local_swift = create_swift_client(local_credentials)
local_nova = create_nova_client(local_credentials)

deadline = local_credentials['DEADLINE']

# Determine what can be done in the allotted time
time_remaining = time_until_deadline(deadline)
schedule = partition(time_remaining, local_swift)
num_instances = len(schedule)
print 'Predicted number of instances needed: ', num_instances

# Start up image on our local cloud
local_servers = spawn(local_nova, find_flavor(local_nova),
                      find_image(local_glance), 'local', schedule)

# Determine if a remote cloud is needed
remote_workload = []
if len(local_servers) < num_instances:
    # If workload cannot fit on local cloud, send the remaining workload to the
    # remote cloud
    local_only = False
    remote_workload = schedule

print 'Number of instances required on local cloud:', len(local_servers)
print 'Number of instances required on remote cloud:', len(remote_workload)

if not local_only:
    # Given a deadline, workload, and a collection of data, determine
    # which cloud to outsource to

    remote_credentials = load_credentials('remote.json')
    remote_keystone = create_keystone_client(remote_credentials)
    remote_glance = create_glance_client(remote_keystone)
    remote_nova = create_nova_client(remote_credentials)
    remote_swift = create_swift_client(remote_credentials)

    print 'Moving data to remote cloud...'
    # Using that cloud's api, move the video files to that cloud

    # Find remote schedule
    remote_list = [video for sub_list in remote_workload for video in
                   sub_list]
    time_remaining = time_until_deadline(deadline)
    remote_schedule = partition(time_remaining, remote_swift,
                                file_list=remote_list)

    print 'Corrected number of remote instances required:', len(remote_schedule)

    # Start up the image on our remote cloud
    remote_servers = spawn(remote_nova, find_flavor(remote_nova),
                           find_image(remote_glance), 'remote', remote_schedule)

    print 'Waiting for completion signal from remote nodes...'
    while not transcode_complete(remote_nova, remote_servers, 'remote'):
        sleep(5)
    print 'Received completion signal from remote nodes'

    kill_servers(remote_servers)


print 'Waiting for completion signal from local nodes...'
while not transcode_complete(local_nova, local_servers, 'local'):
    sleep(5)
print 'Received completion signal from local nodes'

print 'Retrieving data...'
retrieve(local_swift)
kill_servers(local_servers)
print 'JOB COMPLETE!'
