from scheduling import *
from clients import *
from init import *
from upload import *
from move import *


local_credentials = load_credentials('local.json')
remote_credentials = load_credentials('remote.json')

local_keystone = create_keystone_client(local_credentials)
local_glance = create_glance_client(local_keystone)
local_swift = create_swift_client(local_credentials)
local_nova = create_nova_client(local_credentials)

local_only = True

deadline = local_credentials['DEADLINE']

# Determine what can be done in the allotted time
time_remaining = time_until_deadline(deadline)
schedule = partition(time_remaining, local_swift, 'videos')
num_instances = len(schedule)
print 'Predicted number of instances needed: ', num_instances

images = []

'''Check if our image is already on the cloud, if it isn't, upload it'''
image = find_image(local_glance)
if image is None:
    upload(local_glance, images)
else:
    images.append(image)

'''Start up image on our local cloud'''
flavor = find_flavor(local_nova, RAM=4096, vCPUS=2)
local_servers = spawn(local_nova, images[0],
                           'Local Transburt Server Group', 'local',
                           schedule, flavor)

'''Determine if a remote cloud is needed'''
remote_workload = []
if len(local_servers) < num_instances:
    # if we can't fit all the workload on the local cloud, send the remaining workload to the remote cloud
    local_only = False
    remote_workload = schedule

print 'Predicted number of instances needed on local cloud: ', len(
    local_servers)
print 'Predicted number of instances needed on remote cloud: ', len(
    remote_workload)
remote_servers = []
if not local_only:
    '''Given a deadline, workload, and a collection of data, determine
     which cloud to outsource to'''
    # remote_credentials = find_optimal_cloud(deadline, work_load_outsourced)

    print 'Logging in to ' + remote_credentials['OS_AUTH_URL'] + ' as ' + \
          remote_credentials['OS_USERNAME'] + '...'

    '''(ASSUMING THE OPTIMAL CLOUD RUNS OPENSTACK) Given credentials,
    spawn a new client keystone client so that we may have permission to move files around'''

    remote_keystone = create_keystone_client(remote_credentials)
    remote_glance = create_glance_client(remote_keystone)
    remote_nova = create_nova_client(remote_credentials)
    remote_swift = create_swift_client(remote_credentials)

    print 'Moving data to remote cloud...'
    '''Using that cloud's api, move the video files to that cloud'''
    # move_data.Move_data_to_remote_cloud_OPENSTACK(remote_workload, swclient, remote_swclient)


    '''Check if our image exists on the remote cloud, if not, upload it'''
    image = find_image(remote_glance)
    if image is None:
        upload(remote_glance, images)
    else:
        print 'Image found on remote cloud!'
        images.append(image)

    '''Find remote schedule'''
    remote_list = [video for sublist in remote_workload for video in
                   sublist]
    time_remaining = time_until_deadline(deadline)
    remote_schedule = partition(time_remaining, remote_swift,
                                           'videos', file_list=remote_list)

    print 'Number of remote instances needed (course corrected): ', len(
        remote_schedule)
    '''Start up the image on our remote cloud'''
    flavor = find_flavor(remote_nova, RAM=4096, vCPUS=2)
    remote_servers = spawn(remote_nova, images[1],
                                'Remote Transburst Server Group', 'remote',
                                remote_schedule, flavor)

    '''Wait for a signal from the workers saying that they are done'''
    print 'Waiting for completion signal...'
    while not transcode_complete(remote_nova, remote_servers,
                                            'remote'):
        sleep(5)

    '''Once the job is complete, kill the servers'''
    kill_servers(remote_servers)

print 'Waiting for completion signal from local nodes...'
while not transcode_complete(local_nova, local_servers,
                                        'local'):
    sleep(5)

print 'JOB COMPLETE!'
retrieve_data_from_local_cloud(local_swift)
kill_servers(local_servers)
