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
    # also add standalone path (= bin/..)
    autohubdir,_ = os.path.split(os.path.dirname(os.path.realpath(__file__)))
    sys.path.insert(0,autohubdir)
except IndexError:
    sys.exit("Provide folder path to API (containing config file)")


import asyncio, asyncssh, sys, os
from functools import partial
from collections import OrderedDict
import logging


import config, biothings
from utils.versions import set_versions

# fill app & autohub versions
standalone_folder,_bin = os.path.split(os.path.dirname(os.path.realpath(__file__)))
assert _bin == "bin", "Expecting 'bin' to be part of launch script path"
app_folder,_src = os.path.split(os.path.abspath(apipath))
assert _src == "src", "Expecting 'src' to be part of app path"
set_versions(config,standalone_folder,app_folder)

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
job_manager = JobManager(loop,num_workers=config.HUB_MAX_WORKERS,
                      num_threads=config.HUB_MAX_THREADS,
                      max_memory_usage=config.HUB_MAX_MEM_USAGE)

import biothings.hub.dataload.uploader as uploader
import biothings.hub.dataload.dumper as dumper
import biothings.hub.databuild.syncer as syncer
import biothings.hub.dataindex.indexer as indexer

syncer_manager = syncer.SyncerManager(job_manager=job_manager)
syncer_manager.configure()

dmanager = dumper.DumperManager(job_manager=job_manager)
dmanager.schedule_all()

# will check every 10 seconds for sources to upload
umanager = uploader.UploaderManager(poll_schedule = '* * * * * */10', job_manager=job_manager)
umanager.poll('upload',lambda doc: upload_manager.upload_src(doc["_id"]))


from biothings.hub.autoupdate.dumper import LATEST
def cycle_update(src_name, version=LATEST, max_cycles=10):
    """
    Update hub's data up to the given version (default is latest available),
    using full and incremental updates to get up to that given version (if possible).
    To prevent any infinite loop that could occur (eg. network issues), a max of
    max_cycles will be considered to bring the hub up-to-date.
    """
    @asyncio.coroutine
    def do(version):
        cycle = True
        count = 0
        while cycle:
            jobs = dmanager.dump_src(src_name,version=version,check_only=True)
            check = asyncio.gather(*jobs)
            res = yield from check
            assert len(res) == 1
            if res[0] == "Nothing to dump":
                cycle = False
            else:
                remote_version = res[0]
                jobs = dmanager.dump_src(src_name,version=remote_version)
                download = asyncio.gather(*jobs)
                res = yield from download
                assert len(res) == 1
                if res[0] == None:
                    # download ready, now update
                    jobs = umanager.upload_src(src_name)
                    upload = asyncio.gather(*jobs)
                    res = yield from upload
                else:
                    assert res[0] == "Nothing to dump"
                    cycle = False
            count += 1
            if count >= max_cycles:
                logging.warning("Reach max updating cycle (%s), now aborting process. " % count + \
                                "You may want to run another cycle to make sure biothings data is up-to-date")
                cycle = False

    return asyncio.ensure_future(do(version))

# shell shared between SSH console and web API
from biothings.utils.hub import start_server, HubShell
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


from biothings.hub.autoupdate import BiothingsDumper, BiothingsUploader 
from biothings.utils.es import ESIndexer
from biothings.utils.backend import DocESBackend
from biothings.utils.hub import schedule, pending, done, CompositeCommand


# Generate dumper, uploader classes dynamically according
# to the number of "BIOTHINGS_S3_FOLDER" we need to deal with.
# Also generate specific hub commands to deal with those dumpers/uploaders

COMMANDS = OrderedDict()

s3_folders = config.BIOTHINGS_S3_FOLDER.split(",")
for s3_folder in s3_folders:

    BiothingsDumper.BIOTHINGS_S3_FOLDER = s3_folder
    suffix = ""
    if len(s3_folders) > 1:
        # it's biothings API with more than 1 index, meaning they are suffixed.
        # as a convention, use the s3_folder's suffix to complete index name
        # TODO: really ? maybe be more explicit ??
        suffix = "_%s" % s3_folder.split("-")[-1]
    pidxr = partial(ESIndexer,index=config.ES_INDEX_NAME + suffix,
                    doc_type=config.ES_DOC_TYPE,es_host=config.ES_HOST)
    partial_backend = partial(DocESBackend,pidxr)

    # dumper
    class dumper_klass(BiothingsDumper):
        TARGET_BACKEND = partial_backend
        SRC_NAME = BiothingsDumper.SRC_NAME + suffix
        SRC_ROOT_FOLDER = os.path.join(config.DATA_ARCHIVE_ROOT, SRC_NAME)
        BIOTHINGS_S3_FOLDER = s3_folder
        AWS_ACCESS_KEY_ID = config.STANDALONE_AWS_CREDENTIALS.get("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = config.STANDALONE_AWS_CREDENTIALS.get("AWS_SECRET_ACCESS_KEY")
    dmanager.register_classes([dumper_klass])
    # dump commands
    cmdsuffix = suffix.replace("demo_","")
    COMMANDS["versions%s" % cmdsuffix] = partial(dmanager.call,"biothings%s" % suffix,"versions")
    COMMANDS["check%s" % cmdsuffix] = partial(dmanager.dump_src,"biothings%s" % suffix,check_only=True)
    COMMANDS["info%s" % cmdsuffix] = partial(dmanager.call,"biothings%s" % suffix,"info")
    COMMANDS["download%s" % cmdsuffix] = partial(dmanager.dump_src,"biothings%s" % suffix)

    # uploader
    # syncer will work on index used in web part
    esb = (config.ES_HOST, config.ES_INDEX_NAME + suffix, config.ES_DOC_TYPE)
    partial_syncer = partial(syncer_manager.sync,"es",target_backend=esb)
    # manually register biothings source uploader
    # this uploader will use dumped data to update an ES index
    class uploader_klass(BiothingsUploader):
        TARGET_BACKEND = partial_backend
        SYNCER_FUNC = partial_syncer
        AUTO_PURGE_INDEX = True # because we believe
        name = BiothingsUploader.name + suffix
    umanager.register_classes([uploader_klass])
    # upload commands
    COMMANDS["apply%s" % cmdsuffix] = partial(umanager.upload_src,"biothings%s" % suffix)
    COMMANDS["step_update%s" % cmdsuffix] = CompositeCommand("download%s() && apply%s()" % (cmdsuffix,cmdsuffix))
    COMMANDS["update%s" % cmdsuffix] = partial(cycle_update,"biothings%s" % suffix)

# Expose cycle_update function as a service
#class CycleUpdateHandler(HubHandler):
#    @asyncio.coroutine
#    def post(self, name):
#        cycle_update(name)
#        self.write({"updating":name})
#app.add_handlers(r".*",[(r"/update/(\w+)", CycleUpdateHandler, {"managers":managers, "shell":shell})])

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

passwords = hasattr(config,"HUB_PASSWD") and config.HUB_PASSWD or {
        'guest': '9RKfd8gDuNf0Q', # guest account with no password
        }

#EXTRA_NS["app"] = app

# register app into current event loop
#import tornado.platform.asyncio
#tornado.platform.asyncio.AsyncIOMainLoop().install()
#app_server = tornado.httpserver.HTTPServer(app)
#app_server.listen(config.HUB_API_PORT)
#app_server.start()

shell.set_commands(COMMANDS,EXTRA_NS)
server = start_server(loop, "Auto-hub",passwords=passwords,
                      shell=shell, port=config.HUB_SSH_PORT)

try:
    loop.run_until_complete(server)
except (OSError, asyncssh.Error) as exc:
    sys.exit('Error starting server: ' + str(exc))

loop.run_forever()

