""" COPYRIGHT Cisco Systems, Inc. 2015
Small collection of functions about moving data to and from swift
"""
from threading import Thread


def move_thread(swift_client, clip, container):
    """ Thread that gets called so swift moves can be done in parallel """
    print 'Uploading %s' % clip
    with open(clip, 'rb') as f:
        swift_client.put_object(container, clip, contents=f,
                                content_type="video")
    print 'Done uploading %s' % clip


def move(swift_client, file_list, container="videos"):
    """ Moves a list of files in 'parallel' using threads into a swift
    container
    """
    swift_client.put_container(container)
    print '\"Videos\" container created'
    threads = []
    # Loops through all the video clips in the file_list to upload them to swift
    for clip in file_list:
        arg_list = (swift_client, clip, container)

        # Here we keep a list of the threads moving stuff to swift
        threads.append(Thread(target=move_thread, args=arg_list).start())
        threads[-1].start()

    # This ensures that the move function will not exit until all threads are
    # finished
    for thread in threads:
        thread.join()

    print 'Done uploading to LOCAL cloud...'


def retrieve(swift_client):
    """ Grabs all of the files from the 'completed' folder in the the local
    swift, which is where they are always put at the end of a transcode
    """
    container_data = []
    # Goes through to get all of the files in the container
    for data in swift_client.get_container('completed')[1]:
        container_data.append('{0}'.format(data['name']))

    # Goes through to download all of the files found in the container
    for f in container_data:
        print 'Downloading %s to local drive...' % f
        obj_tuple = swift_client.get_object('completed', f)
        with open(f, 'wb') as xcode_bytes:
            xcode_bytes.write(obj_tuple[1])
