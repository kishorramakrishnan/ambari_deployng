from log_utils import get_logger
import ssh_utils
from threading import Thread
import subprocess
import platform

logger = get_logger(__name__)



def setup_ranger_policy_url(policy_host,blueprint_file):
    logger.info("Updating Ranger Policy Manager URL in blueprint {0}".format(blueprint_file))
    logger.info("Policy Manager URL is {0}".format(policy_host))
    ssh_utils.run_shell_command("sed -i 's#POLICY_URL_PLACEHOLDER#{0}#g' {1}".format(policy_host, blueprint_file))

def update_db_hosts_in_blueprint(db_host,blueprint_file):
    logger.info("Updating DB HOSTS in blueprint {0}".format(blueprint_file))
    logger.info("DB HOSTS is {0}".format(db_host))
    ssh_utils.run_shell_command("sed -i 's/DB_HOST_PLACEHOLDER/{0}/g' {1}".format(db_host, blueprint_file))

def setup_ranger_ha(cluster_name,ranger_admin_hosts,load_balacer_host,ranger_admin_port):
    logger.info("Setting up load balancer on {0}".format(load_balacer_host))
    logger.info("Setting up load balancer for {0}".format(ranger_admin_hosts))
    update_http_conf(cluster_name,ranger_admin_hosts,ranger_admin_port,load_balacer_host)


def update_http_conf(cluster_name,balancer_hosts,ranger_admin_port,load_balancer_host):
    ssh_utils.run_ssh_cmd("root",load_balancer_host,"echo -e \"LoadModule ssl_module modules/mod_ssl.so\" >> /etc/httpd/conf/httpd.conf")
    final_conf_file = "conf/ranger_loadbalancer_final.conf"
    ssh_utils.copy_file("conf/ranger_loadbalancer.conf",final_conf_file)
    replace_string_in_file(final_conf_file,"RANGER_ADMIN_PORT_PH",ranger_admin_port)
    replace_string_in_file(final_conf_file, "CLUSTER_NAME_PH", cluster_name)
    balancer_host_string = ""
    for balancer_host in balancer_hosts:
        balancer_host_string = balancer_host_string + "BalancerMember http://{0}:{1} loadfactor=1 route=1 \n".format(balancer_host,ranger_admin_port)
    logger.info("Balance host String is {0}".format(balancer_host_string))
    replace_string_in_file(final_conf_file, "BALANCER_HOST_PLACEHOLDER", balancer_host_string)
    ssh_utils.copy_file_to_host(load_balancer_host,final_conf_file,"/root/")
    ssh_utils.run_ssh_cmd("root",load_balancer_host,"cat /root/ranger_loadbalancer_final.conf >> /etc/httpd/conf/httpd.conf")
    ssh_utils.run_ssh_cmd("root", load_balancer_host, "semanage port -a -t http_port_t -p tcp 11000")
    ssh_utils.run_ssh_cmd("root", load_balancer_host, "semanage port -a -t http_port_t -p tcp 6080")
    ssh_utils.run_ssh_cmd("root", load_balancer_host, "a2enmod headers")
    ssh_utils.run_ssh_cmd("root", load_balancer_host, "service httpd restart")

def replace_string_in_file(file_name,original_string,replacement_string):
    replace_cmd = "sed -i 's#{0}#{1}#g' {2}".format(original_string,replacement_string,file_name)
    logger.info("Executing command {0}".format(replace_cmd))
    response = ssh_utils.run_shell_command(replace_cmd)
    logger.info("Executing command {0} COMPLETED {1}".format(replace_cmd,response[0]))



