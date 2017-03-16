from log_utils import get_logger
import subprocess
import ssh_utils

logger = get_logger(__name__)

def install_and_setup_mysql_connector():
    print "Install and setup MySQL Connector"
    subprocess.Popen("chmod 777 setup_mysql_connector.sh",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = ssh_utils.run_shell_command("ls -lrt")[0]
    print out
    setup_mysql = subprocess.Popen("./setup_mysql_connector.sh",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logger.info("Command executed Install And Setup Mysql Connector : {0}".format(setup_mysql.communicate()[1]))
