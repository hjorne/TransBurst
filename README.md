# TransBurst
Transcoding software for audio/video is processor-intensive, this project's
mission is to make the **transcoding process more cost-effective and
high-performance**. To complete this mission we have used **cloud-bursting
between private and public clouds to improve performance and cut costs**.
This is the purpose to which TransBurst was built.


# Example
If you are planning to use the default worker image with this project, as
well as the default machine learning predictor modules found in
main/predictor and main/scaler, then using it is simple.

First, you run ingest.py on a directory with whatever videos you want
transcode: `./ingest.py relative/path/to/directory`

While this is running, it will print out a variety of statements elaborating
on what the ingest is currently working on. Once the ingest is completed, you
 will have an index json file with details needed for the transcode time
 prediction algorithm, as well as all of your files moved to the local cloud
 swift. After that, simply run `./main.py` in order start the program. Once
 `main.py` is run, simply look at the terminal in order to see what the
 program is doing.


# Installation
At least 11GB of storage available in your local cloud object storage for image
storage, Python 2.7, and the following modules:

*   Flask
*   sklearn
*   python-video-converter
*   numpy
*   youtube-dl
*   python-swiftclient
*   python-novaclient
*   python-keystoneclient
*   python-glancelclient

In addition to each of these modules, you need a worker image with this
repository on it that runs `worker.py` on image boot.