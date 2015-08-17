import keystoneclient.v2_0.client as ksclient
import glanceclient.v2.client as glclient


# Note: be sure an accessuble glance public endpoint is available or this would time out
def upload(glance_client, images):
    image = glance_client.images.create(name="worker", disk_format='raw',
                                        container_format='bare')

    print 'Beginning upload of image'
    images.append(image)
    glance_client.images.upload(image.id,
                                open('/Users/rumadera/Documents/worker.raw',
                                     'rb'))
    print 'Finished uploading of image'

    return image


def find_image(glance_client):
    image_list = list(glance_client.images.list())
    for image in image_list:
        if image.name == "worker":
            return image.id
    return None
