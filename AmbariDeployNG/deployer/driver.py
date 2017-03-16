import subprocess
import time
import sys
import mysql_utils
import kerberos_utils
import requests_util
import ambari_utils
import json

#Setup Ambari Server on the gateway host. Exit if something fails

import ssh_utils
from log_utils import get_logger

logger = get_logger(__name__)

def prepare_host_mapping(agent_hosts):
    logger.info("Assigning hosts to Groups {0}".format(agent_hosts))
    ssh_utils.run_shell_command("cp conf/cluster_host_groups.json conf/cluster_host_groups_runtime.json")

    total_hosts = len(agent_hosts)
    #TODO : Change the number to 5 once we arrive at solution to include DB host( one host is dedicated for DB)
    if total_hosts < 4 :
        logger.error("Number of nodes in the cluster should be atleast 5")
        exit()
    #Static 
    
    client_slave_fqdns = "\"fqdn\":\""+agent_hosts[0]+"\""
    master_slave_dep_fdqns = "\"fqdn\":\""+agent_hosts[1]+"\""
    master_only_fqdns = "\"fqdn\":\""+agent_hosts[2]+"\""
    master_slave_fqdns = "\"fqdn\":\""+agent_hosts[3]+"\""
    
    total_hosts_consumed = 4
    available_host_cnt = total_hosts - total_hosts_consumed
    
    while available_host_cnt > 2:
        master_only_fqdns.append(",\"fqdn\":\""+agent_hosts[total_hosts_consumed]+"\"")
        master_slave_fqdns.append(",\"fqdn\":\""+agent_hosts[total_hosts_consumed+1]+"\"")
        available_host_cnt-2
        total_hosts_consumed+2
    
    if available_host_cnt == 1:
        master_only_fqdns.append(",\"fqdn\":\""+agent_hosts[total_hosts_consumed]+"\"")
    
    logger.info("client_slave_fqdns".format(client_slave_fqdns))
    logger.info("master_slave_dep_fdqns".format(master_slave_dep_fdqns))
    logger.info("master_only_fqdns".format(master_only_fqdns))
    logger.info("master_slave_fqdns".format(master_slave_fqdns))
    
    
def prepare_configs(agent_hosts, is_secure):
    logger.info("Preparing Configs")
    if is_secure:
        logger.info("Secure Cluster")
        ssh_utils.run_shell_command("cp conf/cluster_deploy_secure.json conf/cluster_deploy_1.json")
    else:
        logger.info("Unsecure Cluster")
        ssh_utils.run_shell_command("cp conf/cluster_deploy.json conf/cluster_deploy_1.json")
    host_number = 1
    for host_name in agent_hosts:
        command = "sed -i 's/host_group_ph_{0}/{1}/g' conf/cluster_deploy_1.json".format(host_number, host_name)
        print command
        resp = ssh_utils.run_shell_command(command)
        logger.info(resp[0])
        print resp[0]
        host_number = host_number + 1


def register_blueprint(blueprint_json,ambari_server_host,blueprint_name):
    logger.info("Registering Blueprint using REST API")
    register_bp = requests_util.post_api_call("http://{0}:8080/api/v1/blueprints/{1}?validate_topology=false".format(ambari_server_host,blueprint_name),blueprint_json)
    logger.info("Register BP response code : {0}".format(register_bp.status_code))
    logger.debug("Register BP response JSON : \n {0}".format(register_bp.json()))
    if register_bp.status_code !=201:
        logger.error("BP registration failed : {0}. Stopping Deploy Now See More logs at {1}".format(register_bp.status_code,"logs/deploy.log"))
        exit()

def deploy_cluster(cluster_name,ambari_server_host,cluster_json):
    logger.info("Deploy cluster using REST API")
    create_cluster = requests_util.post_api_call("http://{0}:8080/api/v1/clusters/{1}".format(ambari_server_host,cluster_name),cluster_json)
    logger.info("Command executed : {0} ".format(str(create_cluster.returncode)))
    if create_cluster.returncode !=200:
        logger.error("Cluster Creation failed {0} Stopping Deploy Now. See more logs at {1}".format(create_cluster.returncode,"logs/deploy.log"))
        exit()
def wait_for_cluster_status(cluster_name,ambari_server_host):
    logger.info("Waiting for Cluster Deploys status REST API")
    try:
        cluster_requests = requests_util.get_api_call("http://{0}:8080/api/v1/clusters/{1}/requests/1".format(ambari_server_host,cluster_name))
        if cluster_requests.status_code == 200:
            total_wait_time_in_seconds = 3600
            elapsed_time = 0
            while elapsed_time < total_wait_time_in_seconds:
                deploy_status = requests_util.get_api_call("http://{0}:8080/api/v1/clusters/{1}/requests/1".format(ambari_server_host,cluster_name))
                deploy_status_value = deploy_status.json()['Requests']['request_status']
                logger.debug("Status : {0}".format(deploy_status_value))
                if "IN_PROGRESS" in deploy_status_value:
                    logger.info("Deploy in progress : Time elapsed in seconds: {0}".format(elapsed_time))
                    time.sleep(60)
                elif "FAILED" in deploy_status_value:
                    logger.info("Deploy Failed")
                    logger.error("Cluster Creation failed {0} Stopping Deploy Now. See more logs at {1}".format(deploy_status.returncode, "logs/deploy.log"))
                    exit()
                    break
                elif "COMPLETED" in deploy_status_value:
                    logger.info("DEPLOY COMPLETED!!! Took {0} seconds to finish".format(elapsed_time))
                    break
                else:
                    logger.info("Something wrong {0}".format(deploy_status.json()))
                    break
                elapsed_time = elapsed_time + 60
        else:
            logger.error("Something wrong : Cluster Deploy failed {0} {1}".format(cluster_requests.status_code, cluster_requests.json()))
    except Exception,e:
        logger.error("BP creation API failed {0}".format(e))
    logger.info("Command executed : {0} ".format(deploy_status.returncode))

#setupAmbariServer("oracle","XE","admin","admin","localhost","1521")
#setup_ambari_repo("172.27.24.196","http://dev.hortonworks.com.s3.amazonaws.com/ambari/centos6/2.x/updates/2.5.0.1/ambariqe.repo")
#install_ambari_agent_on_single_host("172.27.24.196","172.27.14.130")
#setup_ambari_repo_on_multiple_hosts(["172.27.24.196","172.27.28.136","172.27.14.131"],"http://dev.hortonworks.com.s3.amazonaws.com/ambari/centos6/2.x/updates/2.5.0.1/ambariqe.repo")
#install_ambari_server("localhost")
#setup_ambari_server("mysql","ambaricustom","ambaricustomuser","bigdatacustom","172.27.14.131","3306")
#TODO: MysqlGTID setup to be done


def deploy():
    cluster_type = sys.argv[1]
    secure = sys.argv[2]
    print "Cluster Type is : "+ cluster_type
    set_prop = subprocess.Popen("set -euf -o pipefail",shell=True)
    set_prop.communicate()
    hosts_file = open("/root/hosts","r")
    all_hosts = hosts_file.read().splitlines()
    agent_hosts = all_hosts[0:len(all_hosts)-1]
    if "yes" in secure:
        prepare_configs(agent_hosts, True)
        kerberos_utils.install_and_setup_kerberos()
        kerberos_utils.install_kerberos_client_on_multiple_hosts(agent_hosts)
    else:
        prepare_configs(agent_hosts, False)
    ambari_host = agent_hosts[0]
    mysql_utils.install_and_setup_mysql_connector()
    ambari_utils.restart_ambari_server(ambari_host)
    ambari_utils.setup_ambari_repo_on_multiple_hosts(agent_hosts,"http://dev.hortonworks.com.s3.amazonaws.com/ambari/centos6/2.x/updates/2.5.0.1/ambariqe.repo")
    ambari_utils.install_ambari_agent_on_multiple_hosts(agent_hosts)
    ambari_utils.register_and_start_ambari_agent_on_multiple_hosts(agent_hosts,ambari_host)
    prepare_host_mapping(agent_hosts)
    register_blueprint("conf/blueprint_{0}.json".format(str(cluster_type).strip()),ambari_host,"blueprint-def")
    deploy_cluster("cl1",ambari_host,"conf/cluster_deploy_1.json")
    wait_for_cluster_status("cl1",ambari_host)

if __name__ == "__main__":
    deploy()
