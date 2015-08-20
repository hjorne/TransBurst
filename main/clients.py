""" A collection of function that spawn Python OpenStack clients """
import keystoneclient.v2_0.client as keystone_client
from keystoneclient import session
from keystoneclient.auth.identity import v2
import glanceclient.v2.client as glance_client
from novaclient import client as nova_client
from swiftclient import Connection


def create_keystone_client(credentials):
    """ Spawns keystone client based of a credentials dictionary passed to
    the function
    """
    keystone = keystone_client.Client(auth_url=credentials["OS_AUTH_URL"],
                                      username=credentials["OS_USERNAME"],
                                      password=credentials["OS_PASSWORD"],
                                      tenant_name=credentials["OS_TENANT_NAME"],
                                      region_name=credentials["OS_REGION_NAME"])
    return keystone


def create_nova_client(credentials):
    """ Spawns nova client based of a credentials dictionary passed to the
    function
    """
    auth = v2.Password(auth_url=credentials["OS_AUTH_URL"],
                       username=credentials["OS_USERNAME"],
                       password=credentials["OS_PASSWORD"],
                       tenant_name=credentials["OS_TENANT_NAME"])

    sess = session.Session(auth=auth)
    nova = nova_client.Client("2", session=sess)
    return nova


# Note: A swift endpoint is required for creating a swift client
def create_swift_client(credentials):
    """ Spawns swift client based of a credentials dictionary passed to the
    function
    """
    swift = Connection(user=credentials["OS_USERNAME"],
                       key=credentials["OS_PASSWORD"],
                       authurl=credentials["OS_AUTH_URL"],
                       tenant_name=credentials["OS_TENANT_NAME"],
                       auth_version="2.0")

    return swift


def create_glance_client(keystone_client):
    """ Spawns a glance client based from a keystone client passed to it """
    glance_endpoint = keystone_client.service_catalog.url_for(
        service_type='image')
    glance = glance_client.Client(endpoint=glance_endpoint,
                                  token=keystone_client.auth_token)
    return glance
