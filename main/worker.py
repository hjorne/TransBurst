""" COPYRIGHT Cisco Systems, Inc. 2015
This file is automatically run on each of the worker nodes when the image
boots up, contains all functions for transcoding, and contains the REST API.
There are 3 main threads in this program, the grab thread, the convert
thread, and the place thread.

The grab thread continuously grabs files from swift, based of the grab queue.
When it finishes grabbing it from swift, it places it in the convert queue.

The convert thread continuously listens to the convert queue, and whenever
something gets placed in it, it converts it. When it is finished converting
it, it tars all of the associated files, and puts them in the place queue.

The place thread continuously listens to the place queue, and whenever
something gets put in it, it places it back into swift. All of these threads
run simultaneously, such that time won't be wasted when one thread or another
is backed up.

Note: Many areas of this file don't have comments, as it was thought the
print statements sufficiently explain what each section of the code is doing.
Read those in lieu of comments.
"""
from threading import Thread
from Queue import Queue
import tarfile
import os
from flask import *
from converter import ffmpeg
from clients import create_swift_client


# Global variables required for the REST API
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = '/home/interns/eas/main'

# Global variables required for global multithreading
grabQ = Queue()
convertQ = Queue()
placeQ = Queue()

# Global variables for /jobs/status to see how many jobs have been processed
# out of how many total there are
num_total = 0
num_processed = 0


@app.route('/')
def index():
    """ If you just go to the index URL '/', you get redirected to 'jobs' """
    return redirect(url_for('jobs'))


@app.route('/boot', methods=['GET'])
def booted():
    """ Called to check if the REST API is up and listening. Doesn't really
    matter what it returns, as long as it returns something and a 200 OK
    """
    return 'True'


@app.route('/jobs', methods=['GET', 'POST'])
def jobs():
    """ The /jobs URL has 2 methods, GET and POST. the GET method just prints
    out the queues so you can see what it's currently doing, while the POST
    method takes a list of files from swift to grab, and then it starts up
    the 3 threads that do the work on the worker node
    """
    if request.method == 'POST':
        # Downloads the file POSTED to the /jobs method
        print'Accessed POST method on /jobs'
        swift_files = 'swift_list'
        f = request.files['file']
        f.save(os.path.join(app.config['UPLOAD_FOLDER'], swift_files))
        print 'Finished saving workload'

        # After it downloads the file, which *should* be a list of swift
        # files, it fills the grab queue
        fill_grabQ(swift_files)

        print 'Spawning grab thread'
        Thread(target=grab_thread).start()

        print 'Spawning convert thread'
        Thread(target=convert_thread).start()

        print 'Spawning place thread'
        Thread(target=place_thread).start()
        return ''

    else:

        print 'Accessed GET method on /jobs'
        return print_all_queues()


@app.route('/jobs/status')
def completed():
    """ Simple function that checks whether or not the number of total jobs
    matches the number of processed jobs. If so, then the worker is finished
    with all its work, and returns a completed status.
    """
    global num_processed, num_total
    print 'Accessed /jobs/status'
    print 'Jobs remaining: ' + str(num_total - num_processed)
    return str(num_processed == num_total)


def grab_thread():
    """ Defines the grab thread, which has an infinite loop in it that keeps
    listening and grabbing items from the grab queue
    """
    global grabQ, convertQ

    print 'GRAB THREAD: Loading credentials'
    credentials = json.load(open('config/local.json'))

    print 'GRAB THREAD: Spawning swift client'
    sw_client = create_swift_client(credentials)

    while True:
        print 'GRAB THREAD: Listening in grab queue...'
        filename = grabQ.get()

        print 'GRAB THREAD: Grabbing ' + filename + ' from swift'
        grab(sw_client, filename)

        print 'GRAB THREAD: Putting ' + filename + ' in convert queue'
        convertQ.put(filename)


def convert_thread():
    """ Defines the convert thread, which has an infinite loop in it that keeps
    listening and converting items from the convert queue
    """
    global convertQ, placeQ

    while True:
        print 'CONVERT THREAD: Listening on convert queue'
        filename = convertQ.get()

        print 'CONVERT THREAD: Converting ' + filename
        new_name = convert(filename)

        print 'CONVERT THREAD: Putting ' + new_name + ' in place queue'
        placeQ.put(new_name)


def place_thread():
    """ Defines the place thread, which has an infinite loop in it that keeps
    listening and placing items from the place queue
    """
    global placeQ, num_processed, num_total

    print 'PLACE THREAD: Loading credentials'
    credentials = json.load(open('config/local.json'))

    print 'PLACE THREAD: Spawning swift client'
    sw_client = create_swift_client(credentials)

    while True:
        print 'PLACE THREAD: Listening on place queue'
        filename = placeQ.get()

        print 'PLACE THREAD: Placing ' + filename + ' back in swift'
        place(sw_client, filename)
        num_processed += 1


def fill_grabQ(swift_urls):
    """ Fills the grab queue with items from the file POSTed to the /jobs URL
    """
    global grabQ, num_total

    print 'Filling grab queue...'
    with open(swift_urls, 'r+') as swift_url_list:
        for line in swift_url_list.readlines():
            print 'Adding ' + line.strip() + ' to grab queue'
            grabQ.put(line.strip())
    num_total = grabQ.qsize()


def read_config(config_file='config/transcode.json'):
    """ Reads the transcode.json config file, which has information about how
    the worker node should transcode the file
    """
    with open(config_file) as json_config:
        return json.load(json_config)


def grab(sw_client, filename):
    """ In order to interact with swift storage, we need credentials and we
    need to create an actual client with the swiftclient API this assumes
    several things:
    1) the remote credentials have been posted to the worker VM
    2) clients.py and transburst_utils.py are in the current directory
    """

    # Reminder: sw_client.get_object returns a tuple in the form of:
    # (filename, file content)
    vid_tuple = sw_client.get_object('videos', filename)

    # Finally, write a file to the local directory with the same name as the
    # file we are retrieving
    with open(filename, 'wb') as new_vid:
        new_vid.write(vid_tuple[1])


def place(sw_client, filename, container='completed', content_type='video'):
    """ Places a file into swift. Since this is only done on completed
    transcoded videos, the default container is 'completed', and the default
    content type is 'videos'. This is of course easily generalizable
    """
    sw_client.put_container(container)
    with open(filename, 'rb') as f:
        sw_client.put_object(container, filename, contents=f,
                             content_type=content_type)
    os.remove(filename)


def convert(filename, config=None):
    """ Using python-video-converter as an ffmpeg wrapper, convert a
    given file to match the given config. After this is done, all associated
    files with the transcode are tar'd, as a transcode can generate an
    arbitrary number of files for the HLS transcode format
    """

    # If no config is passed, read in the default
    if not config:
        config = read_config()

    # Create the new name based off the new format (found in the config
    # dictionary)
    name_parts = filename.split('.')
    base = name_parts[0]
    form_type = config['format']
    new_name = base + '.' + form_type

    # Although a dictionary is easiest to work with for entering
    # options from a human-readable point of view, the low-level ffmpeg
    # wrapper takes in a list of manual ffmpeg options. Those are
    # established here
    new_config = ['-codec:a', config['audio']['codec'],
                  '-codec:v', config['video']['codec']]

    if 'fps' in config['video']:
        new_config += ['-r', config['video']['fps']]
    if 'bitrate' in config['video']:
        new_config += ['-b:v', config['video']['bitrate']]
    if 'size' in config['video']:
        new_config += ['-s', config['video']['size']]

    f = ffmpeg.FFMpeg()

    # Creates the generator used to convert the file
    c_gen = f.convert(filename, new_name, new_config, timeout=0)

    # Not sure why, but this is the standard way to convert files using the
    # python-video-converter framework. Just the way it is
    for c in c_gen:
        pass

    # The old file is no longer needed, so it can be removed
    os.remove(filename)

    # The tar function will tar all data associated with the base name (that
    # is, the name with no extension such as '.mp4'), and then will return
    # the file name of the tar'd data
    return tar(base)


def tar(base):
    """ Tar's all files associated with a base name. Does not delete old files
    """
    print 'Writing tar archive as ' + base + '.tar'
    archive = tarfile.open(base + '.tar', 'w')
    for filename in os.listdir('.'):
        if base in filename:
            print 'Adding ' + filename + ' to ' + base + '.tar'
            archive.add(filename)
    archive.close()
    return base + '.tar'


def print_all_queues():
    """ Converts all queues to lists and prints them out
    """
    global grabQ, convertQ, placeQ
    grab_list = list(grabQ.queue)
    convert_list = list(convertQ.queue)
    place_list = list(placeQ.queue)

    return str(grab_list) + str(convert_list) + str(place_list)


# Since this function is run on its own, this is necessary. Listens on
# 0.0.0.0:5000
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
