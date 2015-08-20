""" COPYRIGHT Cisco Systems, Inc. 2015
This file will ingest all .mp4 and .mkv files in a given directory when
run, although it can also be imported into other files and run from there.
The default directory is the directory ingest.py is run from, but this can be
changed using a command line argument: './ingest.py ingest/directory/path'.
Files are also by default put into the local swift storage, although this can
be changed as well
"""
import subprocess
import json
import os
import sys
from converter import Converter
from clients import create_swift_client


def find_num_frames(frame_type, filename):
    """Find and return the number of frames of a given type
    Valid frame_types are 'I', 'P', and 'B'.
    """
    print 'Finding number of', frame_type, 'frames for', filename
    command = ('ffprobe -loglevel quiet -show_frames ' + filename + ' | ' +
               'grep pict_type=' + frame_type + ' | wc -l')
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    return int(process.stdout.read())


def ingest(credentials, directory):
    """ Ingest all .mp4 and .mkv files in a given directory. More details on
    ingesting found in the ingest function
    """
    print 'Beginning ingest'
    for filename in os.listdir(directory):
        if filename.endswith('.mp4') or filename.endswith('.mkv'):
            ingest_file(credentials, filename)
    print 'Finished ingesting'


def ingest_file(credentials, filename):
    """ Ingests a given file. This means that it generates an index for this
    file with all relevant information about the file (FPS, resolution,
    I/P/B frames, etc.) into an index file (index.json) and then moves it to
    a swift client specified by the credentials dictionary
    """
    print 'Ingesting file', filename
    index = generate_index(filename)
    write_index(filename, index)
    swift_move(filename, credentials)


def generate_index(filename):
    """ Generates an index dictionary, using python-video-convert to access
    ffprobe for simple attributes such as FPS or resolution, and subprocess
    to call a ffprobe directly for finding the I/B/P frames. See
    find_num_frames for more details on that.
    """
    print 'Generating index for file', filename
    c = Converter()
    info = c.probe(filename)
    index = dict()

    index['i frames'] = find_num_frames('I', filename)
    index['b frames'] = find_num_frames('B', filename)
    index['p frames'] = find_num_frames('P', filename)
    index['duration'] = info.format.duration
    index['width'] = info.video.video_width
    index['height'] = info.video.video_height
    index['format'] = info.format.format
    index['fps'] = info.video.video_fps
    index['v codec'] = info.video.codec
    index['a codec'] = info.audio.codec

    return index


def swift_move(filename, credentials, container='videos', content_type='video'):
    """ Moves a given file to a swift specified by the credentials
    dictionary. The container and container type are by default the ones we
    use across the entire project, but they can be easily changed.
    """
    print 'Moving', filename, 'to swift'
    swift = create_swift_client(credentials)

    # Creates the container we need to use if it doesn't already exist
    swift.put_container(container)
    with open(filename, 'rb') as f:
        swift.put_object(container, filename, contents=f,
                         content_type=content_type)


def read_index(index_filename='index.json'):
    """ Reads in the index file, which is specified in a json file """
    return json.load(open(index_filename))


def write_index(filename, index, index_filename='index.json'):
    """ Writes an index for a filename, using an index dictionary passed to
    it. If a previously existing index exists, it will attempt to write it
    there, otherwise, it will create a new index.

    The index is of the form: {
                                filename1: {*attributes*}
                                filename2: {*attributes*}
                                ...
                              }
    """
    print 'Writing index for', filename
    # The try block attempts to write it to a previously existing index
    try:
        with open(index_filename, 'r') as index_file:
            total_index = json.load(index_file)
        total_index[filename] = index
        with open(index_filename, 'w+') as index_file:
            json.dump(total_index, index_file, sort_keys=True, indent=4)
    # If there is an IOError, the index does not exist, and it must be created
    except IOError:
        total_index = dict()
        total_index[filename] = index
        with open(index_filename, 'w+') as index_file:
            json.dump(total_index, index_file, sort_keys=True, indent=4)


# Typically, ingest.py is run on its own rather than with main.py, so a main
# is required to be specified.
if __name__ == '__main__':
    with open('config/local.json', 'r') as cred_file:
        credentials = json.load(cred_file)
    if len(sys.argv) > 1:
        directory = sys.argv[1]
    else:
        directory = '.'
    ingest(credentials, directory)
