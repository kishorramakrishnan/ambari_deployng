import logging
import ssh_utils
import sys
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
import os
import time
from threading import  Thread


logger.info("YES IT WORKED")


def prepare_configs(agent_hosts):
    logger.info("Preparing Configs")
    ssh_utils.run_shell_command("cp conf/cluster_deploy.json conf/cluster_deploy_1.json")
    host_number = 1
    for host_name in agent_hosts:
        command = "sed -i 's/host_group_ph_{0}/{1}/g' conf/cluster_deploy_1.json".format(host_number,host_name)
        print command
        resp = ssh_utils.run_shell_command(command)
        logger.info(resp[0])
        print resp[0]
        host_number = host_number+1


def print_args(thread_name):

    print "This is Called from thread {0} \n".format(thread_name)
    if thread_name in ["os6","rhel6","centos6","r6"]:
        print "Yes Rhel6"

if __name__ == "__main__":
    print_args("r6")

    print "Hell yeah it worked"
    #print_args()