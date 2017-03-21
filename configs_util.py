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
