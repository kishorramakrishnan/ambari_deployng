from log_utils import get_logger
import subprocess
import ssh_utils

logger = get_logger(__name__)

def install_and_setup_mysql_connector():
    logger.info("Install and setup MySQL Connector")
    subprocess.Popen("chmod 777 setup_mysql_connector.sh",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = ssh_utils.run_shell_command("ls -lrt")[0]
    logger.debug(out)
    setup_mysql = subprocess.Popen("./setup_mysql_connector.sh",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logger.info("Command executed Install And Setup Mysql Connector : {0}".format(setup_mysql.communicate()[1]))

def setup_oozie_db(database_host):
    logger.info("Setting up oozie DB")
    db_name = "ooziedb"
    db_user = "oozieuser"
    db_password = "password"
    if copy_db_script_to_db_host(database_host)[2] == 0:
        ssh_utils.run_ssh_cmd("root",database_host,"./mysql_db_util.sh drop_user {0}".format(db_user))
        ssh_utils.run_ssh_cmd("root", database_host, "./mysql_db_util.sh drop_db {0}".format(db_name))
        ssh_utils.run_ssh_cmd("root", database_host, "./mysql_db_util.sh create_db {0}".format(db_name))
        ssh_utils.run_ssh_cmd("root", database_host, "./mysql_db_util.sh create_user {0} {1}".format(db_user,db_password))



def setup_hive_db(database_host):
    logger.info("Setting up HIVE DB")
    db_name = "hivedb"
    db_user = "hiveuser"
    db_password = "password"
    if copy_db_script_to_db_host(database_host)[2] == 0:
        ssh_utils.run_ssh_cmd("root", database_host, "./mysql_db_util.sh drop_user {0}".format(db_user))
        ssh_utils.run_ssh_cmd("root", database_host, "./mysql_db_util.sh drop_db {0}".format(db_name))
        ssh_utils.run_ssh_cmd("root", database_host, "./mysql_db_util.sh create_db {0}".format(db_name))
        ssh_utils.run_ssh_cmd("root", database_host,
                              "./mysql_db_util.sh create_user {0} {1}".format(db_user, db_password))


def setup_ranger_db(database_host):
    if copy_db_script_to_db_host(database_host)[2] == 0:
        logger.info("Setting up RANGER DB")
        db_password = "mysql"
        ssh_utils.run_ssh_cmd("root", database_host, "./mysql_db_util.sh setup_ranger_db {0}".format(db_password))

def copy_db_script_to_db_host(database_host):
    ssh_utils.run_shell_command("scp -i /root/ec2-keypair mysql_db_util.sh root@{0}:/root/".format(database_host))
    return ssh_utils.run_ssh_cmd("root",database_host,"chmod 777 /root/mysql_db_util.sh")

