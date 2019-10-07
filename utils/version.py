import os, logging, sys
from git import Repo

def set_standalone_version(config, standalone_folder):
    if not hasattr(config,"STANDALONE_VERSION"):
        repo = Repo(standalone_folder)
        try:
            commit = repo.head.object.hexsha[:6]
            commitdate = repo.head.object.committed_datetime.isoformat()
        except Exception as e:
            logging.warning("Can't determine app commit hash: %s" % e)
            commit = "unknown"
            commitdate = "unknown"
        try:
            config.STANDALONE_VERSION = {"branch" : repo.active_branch.name,
                                  "commit" : commit,
                                  "date" : commitdate}
        except Exception as e:
            logging.warning("Can't determine app version, defaulting to 'master': %s" % e)
            config.STANDALONE_VERSION = {"branch" : "master",
                                  "commit" : commit,
                                  "date" : commitdate}
    else:
        logging.info("standalone_version '%s' forced in configuration file" % config.STANDALONE_VERSION)

    logging.info("Running standalone_version=%s" % repr(config.STANDALONE_VERSION))
