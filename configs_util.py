from log_utils import get_logger
import ssh_utils
from threading import Thread
import subprocess
import platform

logger = get_logger(__name__)



def setup_ranger_policy_url(policy_host,blueprint_file):
    logger.info("Updating Ranger Policy Manager URL in blueprint {0}".format(blueprint_file))
    logger.info("Policy Manager URL is {0}".format(policy_host))
    ssh_utils.run_shell_command("sed -i 's/POLICY_URL_PLACEHOLDER/{0}/g' {1}".format(policy_host, blueprint_file))

def update_db_hosts_in_blueprint(db_host,blueprint_file):
    logger.info("Updating DB HOSTS in blueprint {0}".format(blueprint_file))
    logger.info("DB HOSTS is {0}".format(db_host))
    ssh_utils.run_shell_command("sed -i 's/DB_HOST_PLACEHOLDER/{0}/g' {1}".format(db_host, blueprint_file))

