import ssh_utils
from log_utils import get_logger
from threading import Thread
import subprocess
import random
import string

logger = get_logger(__name__)

def install_kerberos_client_on_multiple_hosts(hostnames):
    print "Installing Kerberos client on multiple hosts"
    logger.info("Setting up ambari repo on multiple hosts : {0}".format(hostnames))
    try:
        for hostname in hostnames:
            logger.info("Setting up repo on : {0}".format(hostname))
            setup_thread = Thread(target=install_kerberos_client_on_single_host, args=(hostname,))
            setup_thread.daemon = True
            setup_thread.start()
            setup_thread.join(timeout=30)
    except:
        logger.info("Error: unable to start thread")

def install_kerberos_client_on_single_host(host):
    logger.info("Installing Kerberos clients on host : {0}".format(host))
    ssh_utils.run_ssh_cmd("root",host,"yum install krb5-workstation -y")
    ssh_utils.run_ssh_cmd("root", host, "yum install unzip -y")

def distribute_JCE_on_multiple_hosts(hostnames):
    print "Installing JCE on multiple hosts"
    logger.info("Installing JCE  on multiple hosts : {0}".format(hostnames))
    unzip_command = "unzip -o -j -q /var/lib/ambari-server/resources/UnlimitedJCEPolicyJDK7.zip -d"
    try:
        for hostname in hostnames:
            logger.info("Setting up JCE on : ", hostname)
            copy_command = ""
            ssh_utils.run_shell_command("scp -i /root/ec2-keypair root@{0} {1}".format(hostname,copy_command))

    except:
        logger.info("Error: unable to start thread")


def install_and_setup_kerberos(kdc_type,kdc_host):
    logger.info("Install and setup Kerberos")
    ssh_utils.run_shell_command("chmod 777 setup_kerberos.sh")
    ssh_utils.run_shell_command("ls -lrt")
    if "mit" in kdc_type:
        setup_kerberos = subprocess.Popen("./setup_kerberos.sh {0} {1} {2} {3}".format("mit",kdc_host,"admin","admin/admin@EXAMPLE.COM"),shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info("Command executed Setup KDC : {0}".format(setup_kerberos.communicate()[1]))
    logger.info("Install and setup Kerberos : COMPLETED")

def update_kdc_params_in_blueprint(blueprint_file,kdc_host,ambari_server_host,kdc_type,cluster_name):
    logger.info("Updating KDC properties in blueprint {0}".format(blueprint_file))
    if "mit" in kdc_type:
        logger.info("KDC is {0}".format(kdc_type))
        ssh_utils.run_shell_command("sed -i 's/KDC_TYPE_PLACEHOLDER/{0}/g' {1}".format(kdc_type,blueprint_file))
        ssh_utils.run_shell_command("sed -i 's/KDC_HOST_PLACEHOLDER/{0}/g' {1}".format(kdc_host,blueprint_file))
        ssh_utils.run_shell_command("sed -i 's/KDC_AMBARI_SERVER_PLACEHOLDER/{0}/g' {1}".format(ambari_server_host,blueprint_file))
        ssh_utils.run_shell_command("sed -i 's/KDC_ENCRYPTION_TYPE_PLACEHOLDER/{0}/g' {1}".format("aes des3-cbc-sha1 rc4 des-cbc-md5",blueprint_file))
        serv_check_principal = cluster_name+"-"+"".join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(10))
        ssh_utils.run_shell_command("sed -i 's/KDC_SERVICE_CHECK_PRINICPAL/{0}/g' {1}".format(serv_check_principal,blueprint_file))


