#!/usr/bin/env python

# because we'll generate dynamic nested class, 
# which are un-pickleable by default, we need to 
# override multiprocessing with one using "dill",
# which allows pickling nested classes (and many other things)
import concurrent.futures
import multiprocessing_on_dill
concurrent.futures.process.multiprocessing = multiprocessing_on_dill


import sys, os
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


import asyncio, asyncssh, sys, os
from functools import partial
from collections import OrderedDict
import logging

import config, biothings
from biothings.utils.version import set_versions

# fill app & autohub versions
standalone_folder,_bin = os.path.split(os.path.dirname(os.path.realpath(__file__)))
assert _bin == "bin", "Expecting 'bin' to be part of launch script path"
app_folder,_src = os.path.split(os.path.abspath(apipath))
assert _src == "src", "Expecting 'src' to be part of app path"
set_versions(config,app_folder)

biothings.config_for_app(config)

# shut some mouths...
logging.getLogger("elasticsearch").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)
import botocore
logging.getLogger("botocore").setLevel(logging.ERROR)

logging.info("Hub DB backend: %s" % biothings.config.HUB_DB_BACKEND)
logging.info("Hub database: %s" % biothings.config.DATA_HUB_DB_DATABASE)

from biothings.utils.manager import JobManager
loop = asyncio.get_event_loop()
job_manager = JobManager(loop,num_workers=biothings.config.HUB_MAX_WORKERS,
                      num_threads=biothings.config.HUB_MAX_THREADS,
                      max_memory_usage=biothings.config.HUB_MAX_MEM_USAGE)

import biothings.hub.dataload.uploader as uploader
import biothings.hub.dataload.dumper as dumper
import biothings.hub.databuild.syncer as syncer
import biothings.hub.dataindex.indexer as indexer

syncer_manager = syncer.SyncerManager(job_manager=job_manager)
if hasattr(biothings.config,"SYNCER_CLASSES"):
    syncer_classes = biothings.config.SYNCER_CLASSES
    logging.info("Using custom syncer: %s" % syncer_classes)
else:
    syncer_classes = None
syncer_manager.configure(klasses=syncer_classes)

dmanager = dumper.DumperManager(job_manager=job_manager)
dmanager.schedule_all()

# will check every 10 seconds for sources to upload
umanager = uploader.UploaderManager(poll_schedule = '* * * * * */10', job_manager=job_manager)
umanager.poll('upload',lambda doc: upload_manager.upload_src(doc["_id"]))


# shell shared between SSH console and web API
from biothings.utils.hub import HubShell
shell = HubShell(job_manager)

# assemble resources that need to be propagated to REST API
# so API can query those objects (which are shared between the 
# hub console and the REST API).
#from biothings.hub.api import get_api_app
managers = {
        "job_manager" : job_manager,
        "dump_manager" : dmanager,
        "upload_manager" : umanager,
        "syncer_manager" : syncer_manager,
        }
settings = {'debug': True}
#app = get_api_app(managers=managers,shell=shell,settings=settings)


from biothings.hub.autoupdate import BiothingsDumper, BiothingsUploader, update
from biothings.utils.es import ESIndexer
from biothings.utils.backend import DocESBackend
# back-compat
from biothings.utils.hub import pending, CompositeCommand
try:
    # biothings 0.2.1
    from biothings.utils.hub import schedule, done
    from biothings.utils.hub import start_server
except ImportError:
    # biothings 0.2.2, 0.2.3
    from biothings.hub import schedule
    done = "done"

    from biothings.hub import HubSSHServer, HUB_REFRESH_COMMANDS
    import aiocron
    @asyncio.coroutine
    def start_server(loop,name,passwords,keys=['bin/ssh_host_key'],shell=None,
                     host='',port=8022):
        for key in keys:
            assert os.path.exists(key),"Missing key '%s' (use: 'ssh-keygen -f %s' to generate it" % (key,key)
        HubSSHServer.PASSWORDS = passwords
        HubSSHServer.NAME = name
        HubSSHServer.SHELL = shell
        cron = aiocron.crontab(HUB_REFRESH_COMMANDS,func=shell.__class__.refresh_commands,
                               start=True, loop=loop)
        yield from asyncssh.create_server(HubSSHServer, host, port, loop=loop,
                                     server_host_keys=keys)


# Generate dumper, uploader classes dynamically according
# to the number of "BIOTHINGS_S3_FOLDER" we need to deal with.
# Also generate specific hub commands to deal with those dumpers/uploaders

COMMANDS = OrderedDict()

s3_folders = biothings.config.BIOTHINGS_S3_FOLDER
if type(s3_folders) is str:
    s3_folders = [s3_folders]
for s3_folder in s3_folders:

    BiothingsDumper.BIOTHINGS_S3_FOLDER = s3_folder
    index_name = biothings.config.ES_INDEX_NAME
    src_name = s3_folder
    if "-" in s3_folder:
        # it's biothings API with more than 1 index, meaning they are suffixed.
        # as a convention, use the s3_folder's suffix to complete index name
        whatever, suffix = s3_folder.split("-")
        assert not "-" in whatever, "More than one '-' found, invalid folder name"
        index_name += "_" + suffix
        src_name = suffix
    pidxr = partial(ESIndexer,index=index_name,
                    doc_type=biothings.config.ES_DOC_TYPE,
                    es_host=biothings.config.ES_HOST)
    partial_backend = partial(DocESBackend,pidxr)

    # dumper
    class dumper_klass(BiothingsDumper):
        TARGET_BACKEND = partial_backend
        #SRC_NAME = BiothingsDumper.SRC_NAME + suffix
        SRC_NAME = src_name
        SRC_ROOT_FOLDER = os.path.join(biothings.config.DATA_ARCHIVE_ROOT, SRC_NAME)
        BIOTHINGS_S3_FOLDER = s3_folder
        AWS_ACCESS_KEY_ID = biothings.config.STANDALONE_AWS_CREDENTIALS.get("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = biothings.config.STANDALONE_AWS_CREDENTIALS.get("AWS_SECRET_ACCESS_KEY")
    dmanager.register_classes([dumper_klass])
    # dump commands
    COMMANDS["versions"] = partial(dmanager.call,method_name="versions")
    COMMANDS["check"] = partial(dmanager.dump_src,check_only=True)
    COMMANDS["info"] = partial(dmanager.call,method_name="info")
    COMMANDS["download"] = partial(dmanager.dump_src)

    # uploader
    # syncer will work on index used in web part
    esb = (biothings.config.ES_HOST, index_name, biothings.config.ES_DOC_TYPE)
    partial_syncer = partial(syncer_manager.sync,"es",target_backend=esb)
    # manually register biothings source uploader
    # this uploader will use dumped data to update an ES index
    class uploader_klass(BiothingsUploader):
        TARGET_BACKEND = partial_backend
        SYNCER_FUNC = partial_syncer
        AUTO_PURGE_INDEX = True # because we believe
        name = src_name
    umanager.register_classes([uploader_klass])
    # upload commands
    COMMANDS["apply"] = partial(umanager.upload_src)
    COMMANDS["update"] = partial(update,dmanager,umanager)

# admin/advanced
EXTRA_NS = {
    "dm" : dmanager,
    "um" : umanager,
    "jm" : job_manager,
    "q" : job_manager.process_queue,
    "t": job_manager.thread_queue,
    "g" : globals(),
    "l" : loop,
    "sch" : partial(schedule,loop),
    "top" : job_manager.top,
    "pending" : pending,
    "done" : done
    }

passwords = hasattr(biothings.config,"HUB_PASSWD") and biothings.config.HUB_PASSWD or {
        'guest': '9RKfd8gDuNf0Q', # guest account with no password
        }

shell.set_commands(COMMANDS,EXTRA_NS)
server = start_server(loop, "Auto-hub",passwords=passwords,
                      shell=shell, port=biothings.config.HUB_SSH_PORT)

try:
    loop.run_until_complete(server)
except (OSError, asyncssh.Error) as exc:
    sys.exit('Error starting server: ' + str(exc))

loop.run_forever()

