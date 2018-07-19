import os, logging, sys
from git import Repo

def set_versions(config, autohub_folder, app_folder):
    # autohub_version: version of the standalone launcher (this script/repo)
    if not hasattr(config,"AUTOHUB_VERSION"):
        repo = Repo(autohub_folder)
        try:
            config.AUTOHUB_VERSION = repo.active_branch.name
        except Exception as e:
            logging.warning("Can't determine autohub version, defaulting to 'master': %s" % e)
            config.AUTOHUB_VERSION = "master"
    else:
        logging.info("autohub_version '%s' forced in configuration file" % config.AUTOHUB_VERSION)

    # app_version: version of the API application
    if not hasattr(config,"APP_VERSION"):
        repo = Repo(app_folder) # app dir (mygene, myvariant, ...)
        try:
            config.APP_VERSION =  repo.active_branch.name
        except Exception as e:
            logging.warning("Can't determine app version, defaulting to 'master': %s" % e)
            config.APP_VERSION = "master"
    else:
        logging.info("app_version '%s' forced in configuration file" % config.APP_VERSION)
    logging.info("Using autohub version='%s' to launch standalone app version='%s'" % (config.AUTOHUB_VERSION,config.APP_VERSION))

