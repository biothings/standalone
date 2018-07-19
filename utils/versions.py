import os, logging, sys
from git import Repo

def set_versions(config, standalone_folder, app_folder):
    # autohub_version: version of the standalone launcher (this script/repo)
    if not hasattr(config,"STANDALONE_VERSION"):
        repo = Repo(standalone_folder)
        try:
            config.STANDALONE_VERSION = repo.active_branch.name
        except Exception as e:
            logging.warning("Can't determine autohub version, defaulting to 'master': %s" % e)
            config.STANDALONE_VERSION = "master"
    else:
        logging.info("autohub_version '%s' forced in configuration file" % config.STANDALONE_VERSION)

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

    # biothings_version: version of BioThings SDK
    if not hasattr(config,"BIOTHINGS_VERSION"):
        import biothings
        # .../biothings.api/biothings/__init__.py
        bt_folder,_bt = os.path.split(os.path.split(os.path.realpath(biothings.__file__))[0])
        assert _bt == "biothings", "Expectig 'biothings' dir in biothings lib path"
        repo = Repo(bt_folder) # app dir (mygene, myvariant, ...)
        try:
            config.BIOTHINGS_VERSION =  repo.active_branch.name
        except Exception as e:
            logging.warning("Can't determine biothings version, defaulting to 'master': %s" % e)
            config.BIOTHINGS_VERSION = "master"
    else:
        logging.info("biothings_version '%s' forced in configuration file" % config.BIOTHINGS_VERSION)

    logging.info("Using standalone version='%s' " % config.STANDALONE_VERSION + \
                 "to launch app version='%s' " % config.APP_VERSION + \
                 "with biothings version='%s'" % config.BIOTHINGS_VERSION)

