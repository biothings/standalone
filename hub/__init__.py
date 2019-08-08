import os, logging, sys
from functools import partial
from collections import OrderedDict
import asyncio

from biothings import config as btconfig

from biothings.hub.autoupdate.dumper import LATEST
from biothings.hub.autoupdate import BiothingsDumper, BiothingsUploader
from biothings.utils.es import ESIndexer
from biothings.utils.backend import DocESBackend
from biothings.utils.hub import CompositeCommand
from biothings.hub import HubServer

class AutoHubServer(HubServer):

    DEFAULT_FEATURES = ["job","dump","upload","sync","terminal","ws"]

    def __init__(self, s3_folders, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_folders = s3_folders

    def cycle_update(self, src_name, version=LATEST, max_cycles=10):
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
                jobs = self.managers["dump_manager"].dump_src(src_name,version=version,check_only=True)
                check = asyncio.gather(*jobs)
                res = yield from check
                assert len(res) == 1
                if res[0] == "Nothing to dump":
                    cycle = False
                else:
                    remote_version = res[0]
                    jobs = self.managers["dump_manager"].dump_src(src_name,version=remote_version)
                    download = asyncio.gather(*jobs)
                    res = yield from download
                    assert len(res) == 1
                    if res[0] == None:
                        # download ready, now update
                        jobs = self.managers["upload_manager"].upload_src(src_name)
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


    def configure_commands(self):
        assert self.managers, "No managers configured"
        self.commands = OrderedDict()
        for s3_folder in self.s3_folders:
        
            BiothingsDumper.BIOTHINGS_S3_FOLDER = s3_folder
            suffix = ""
            if len(self.s3_folders) > 1:
                # it's biothings API with more than 1 index, meaning they are suffixed.
                # as a convention, use the s3_folder's suffix to complete index name
                # TODO: really ? maybe be more explicit ??
                suffix = "_%s" % s3_folder.split("-")[-1]
            pidxr = partial(ESIndexer,index=btconfig.ES_INDEX_NAME + suffix,
                            doc_type=btconfig.ES_DOC_TYPE,es_host=btconfig.ES_HOST)
            partial_backend = partial(DocESBackend,pidxr)
        
            SRC_NAME = BiothingsDumper.SRC_NAME + suffix
            dumper_klass = type("%sDumper" % suffix,(BiothingsDumper,),
                    {"TARGET_BACKEND" : partial_backend,
                     "SRC_NAME" : BiothingsDumper.SRC_NAME + suffix,
                     "SRC_ROOT_FOLDER" : os.path.join(btconfig.DATA_ARCHIVE_ROOT, SRC_NAME),
                     "BIOTHINGS_S3_FOLDER" : s3_folder,
                     "AWS_ACCESS_KEY_ID" : btconfig.STANDALONE_AWS_CREDENTIALS.get("AWS_ACCESS_KEY_ID"),
                     "AWS_SECRET_ACCESS_KEY" : btconfig.STANDALONE_AWS_CREDENTIALS.get("AWS_SECRET_ACCESS_KEY")
                     })
            sys.modules["standalone.hub"].__dict__["%sDumper" % suffix] = dumper_klass
            # dumper
            self.managers["dump_manager"].register_classes([dumper_klass])
            # dump commands
            cmdsuffix = suffix.replace("demo_","")
            self.commands["versions%s" % cmdsuffix] = partial(self.managers["dump_manager"].call,"biothings%s" % suffix,"versions")
            self.commands["check%s" % cmdsuffix] = partial(self.managers["dump_manager"].dump_src,"biothings%s" % suffix,check_only=True)
            self.commands["info%s" % cmdsuffix] = partial(self.managers["dump_manager"].call,"biothings%s" % suffix,"info")
            self.commands["download%s" % cmdsuffix] = partial(self.managers["dump_manager"].dump_src,"biothings%s" % suffix)
        
            # uploader
            # syncer will work on index used in web part
            esb = (btconfig.ES_HOST, btconfig.ES_INDEX_NAME + suffix, btconfig.ES_DOC_TYPE)
            partial_syncer = partial(self.managers["sync_manager"].sync,"es",target_backend=esb)
            # manually register biothings source uploader
            # this uploader will use dumped data to update an ES index
            uploader_klass = type("%sUploader" % suffix,(BiothingsUploader,),
                    {"TARGET_BACKEND" : partial_backend,
                     "SYNCER_FUNC" : partial_syncer,
                     "AUTO_PURGE_INDEX" : True, # because we believe
                     "name" : BiothingsUploader.name + suffix,
                    })
            sys.modules["standalone.hub"].__dict__["%sUploader" % suffix] = uploader_klass
            self.managers["upload_manager"].register_classes([uploader_klass])
            # upload commands
            self.commands["apply%s" % cmdsuffix] = partial(self.managers["upload_manager"].upload_src,"biothings%s" % suffix)
            self.commands["step_update%s" % cmdsuffix] = CompositeCommand("download%s() && apply%s()" % (cmdsuffix,cmdsuffix))
            self.commands["update%s" % cmdsuffix] = partial(self.cycle_update,"biothings%s" % suffix)

