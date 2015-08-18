from threading import Thread


# Move_data_to_local_cloud is used for testing, putting data on our cloud.
#
def MDTLC_helper(swift_client, clip, container):
    print clip
    with open(clip, 'rb') as f:
        swift_client.put_object(container, clip, contents=f,
                                content_type="video")
    print "Done uploading %s" % clip


def Move_data_to_local_cloud(swift_client, ListOfFiles, container="videos"):
    swift_client.put_container(container)
    print "\"Videos\" container created"
    threads = []
    for clip in ListOfFiles:
        threads.append(
            Thread(target=MDTLC_helper, args=(swift_client, clip, container)))
        threads[-1].start()
    for thread in threads:
        thread.join()
    print "Done uploading to LOCAL cloud..."


def retrieve_data_from_local_cloud(swift_client):
    container_data = []
    for data in swift_client.get_container("completed")[1]:
        container_data.append('{0}'.format(data['name']))
    for f in container_data:
        print "Downloading %s to local drive..." % f
        obj_tuple = swift_client.get_object('completed', f)
        with open(f, 'wb') as xcode_bytes:
            xcode_bytes.write(obj_tuple[1])
