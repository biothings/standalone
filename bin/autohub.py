#!/usr/bin/env python

# because we'll generate dynamic nested class, 
# which are un-pickleable by default, we need to 
# override multiprocessing with one using "dill",
# which allows pickling nested classes (and many other things)
import concurrent.futures
import multiprocessing_on_dill
concurrent.futures.process.multiprocessing = multiprocessing_on_dill

import sys, os, logging

# shut some mouths...
logging.getLogger("elasticsearch").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)
import botocore
logging.getLogger("botocore").setLevel(logging.ERROR)

try:
    # workking dir in api folder
    apipath = sys.argv[1]
    sys.path.insert(0,apipath)
    # also add folder containoing standalone path so we can
    # "import standalone.*"
    rootdir,_ = os.path.split(os.path.split(os.path.dirname(os.path.realpath(__file__)))[0])
    sys.path.insert(0,rootdir)
except IndexError:
    sys.exit("Provide folder path to API (containing config file)")

import config, biothings
from biothings.utils.version import set_versions

# fill app & autohub versions
standalone_folder,_bin = os.path.split(os.path.dirname(os.path.realpath(__file__)))
assert _bin == "bin", "Expecting 'bin' to be part of launch script path"
app_folder,_src = os.path.split(os.path.abspath(apipath))
assert _src == "src", "Expecting 'src' to be part of app path"
set_versions(config,app_folder)

biothings.config_for_app(config)

logging.info("Hub DB backend: %s" % biothings.config.HUB_DB_BACKEND)
logging.info("Hub database: %s" % biothings.config.DATA_HUB_DB_DATABASE)

from standalone.hub import AutoHubServer

s3_folders = config.BIOTHINGS_S3_FOLDER.split(",")
server = AutoHubServer(s3_folders,source_list=None,name="Auto-hub",
                       api_config=None,dataupload_config=False,websocket_config=False)


if __name__ == "__main__":
        server.start()

