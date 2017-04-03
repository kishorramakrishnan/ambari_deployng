import subprocess
import time
import sys
import db_utils
import kerberos_utils
import requests_util
import ambari_utils
import json
import configs_util
import os
#Setup Ambari Server on the gateway host. Exit if something fails

import ssh_utils
from log_utils import get_logger

logger = get_logger(__name__)

def prepare_host_mapping(agent_hosts, is_secure):
    logger.info("Assigning hosts to Groups {0}".format(agent_hosts))
    if is_secure:
        logger.info("Secure Cluster")
    	ssh_utils.run_shell_command("cp conf/cluster_template_sec_mit.json conf/cluster_template.json")
    else:
        logger.info("Unsecure Cluster")
        ssh_utils.run_shell_command("cp conf/cluster_template_unsec.json conf/cluster_template.json")
    total_hosts = len(agent_hosts)
    #TODO : Change the number to 5 once we arrive at solution to include DB host( one host is dedicated for DB)
    if total_hosts < 4 :
        logger.error("Number of nodes in the cluster should be atleast 5")
        exit()
    #Static 
    
    client_slave_fqdns = "\"fqdn\":\""+agent_hosts[0]+"\""
    master_slave_dep_fqdns = "\"fqdn\":\""+agent_hosts[1]+"\""
    master_only_fqdns = "\"fqdn\":\""+agent_hosts[2]+"\""
    master_slave_fqdns = "\"fqdn\":\""+agent_hosts[3]+"\""
    
    total_hosts_consumed = 4
    available_host_cnt = total_hosts - total_hosts_consumed
    
    while available_host_cnt >= 2:
        master_only_fqdns+=("},{\"fqdn\":\""+agent_hosts[total_hosts_consumed]+"\"")
        master_slave_fqdns+=("},{\"fqdn\":\""+agent_hosts[total_hosts_consumed+1]+"\"")
        available_host_cnt=available_host_cnt-2
        total_hosts_consumed=total_hosts_consumed+2
    
    if available_host_cnt == 1:
        master_only_fqdns+=("},{\"fqdn\":\""+agent_hosts[total_hosts_consumed]+"\"")
    
    logger.info("client_slave_fqdns {0}".format(client_slave_fqdns))
    logger.info("master_slave_dep_fqdns {0}".format(master_slave_dep_fqdns))
    logger.info("master_only_fqdns {0}".format(master_only_fqdns))
    logger.info("master_slave_fqdns {0}".format(master_slave_fqdns))
    
    #Replace the json content
    with open('conf/cluster_template.json', 'r+') as file:
    	hosts_json_content = file.read()
   	file.seek(0)
    	hosts_json_content=hosts_json_content.replace('\"client_slave_fqdns\":\"\"', client_slave_fqdns)
        hosts_json_content=hosts_json_content.replace('\"master_slave_dep_fqdns\":\"\"', master_slave_dep_fqdns)
        hosts_json_content=hosts_json_content.replace('\"master_only_fqdns\":\"\"', master_only_fqdns)
        hosts_json_content=hosts_json_content.replace('\"master_slave_fqdns\":\"\"', master_slave_fqdns)
    	file.write(hosts_json_content)
        logger.info("after replacing hosts {0}".format(hosts_json_content))
    
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
    if register_bp.status_code !=201:
        logger.error("BP registration failed : {0}. Stopping Deploy Now See More logs at {1}".format(register_bp.status_code,"logs/deploy.log"))
        logger.error(register_bp.json())
        exit()

def deploy_cluster(cluster_name,ambari_server_host,cluster_json):
    logger.info("Deploy cluster using REST API")
    create_cluster = requests_util.post_api_call("http://{0}:8080/api/v1/clusters/{1}".format(ambari_server_host,cluster_name),cluster_json)
    logger.info("Command executed : {0} ".format(str(create_cluster.status_code)))
    if create_cluster.status_code !=202:
        logger.error("Cluster Creation failed {0} Stopping Deploy Now. See more logs at {1}".format(create_cluster.status_code,"logs/deploy.log"))
        logger.error(create_cluster.json())
        exit()
def wait_for_cluster_status(cluster_name,ambari_server_host):
    logger.info("Waiting for Cluster Deploys status REST API")
    deploy_status="NOT STARTED"
    try:
        time.sleep(60)
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
                    logger.error("Cluster Creation failed {0} Stopping Deploy Now. See more logs at {1}".format(deploy_status.status_code, "logs/deploy.log"))
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
    logger.info("Command executed : {0} ".format(deploy_status.status_code))

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
    os_type = sys.argv[3]
    is_ha_test = sys.argv[4]
    hdp_base_url = sys.argv[5]
    cluster_name ="cl1"
    print "Cluster Type is : "+ cluster_type
    print "HDP BASE URL : " +hdp_base_url
    print "HA TEST : "+is_ha_test
    if not os.path.isfile("blueprints/blueprint_{0}.json".format(cluster_type)):
        logger.error("Invalid configuration Cluster Type : {0} Blueprint Does Not Exists. Recheck configuration or create a new blueprint under blueprints/ folder with format blueprint_{cluster_type} Exiting now!".format(cluster_type))
        exit()
    set_prop = subprocess.Popen("set -euf -o pipefail",shell=True)
    set_prop.communicate()
    hosts_file = open("/root/hosts","r")
    all_hosts = hosts_file.read().splitlines()
    agent_hosts = all_hosts[0:len(all_hosts)-1]
    ambari_host = agent_hosts[0]
    db_host = all_hosts[len(all_hosts)-1]
    final_blueprint = "blueprints/blueprint_final.json"
    ssh_utils.run_shell_command("cp blueprints/blueprint_{0}.json {1}".format(str(cluster_type).strip(),final_blueprint))
    if "yes" in secure:
        prepare_host_mapping(agent_hosts, True)
        kerberos_utils.install_and_setup_kerberos("mit",ambari_host)
        kerberos_utils.install_kerberos_client_on_multiple_hosts(agent_hosts)
        kerberos_utils.update_kdc_params_in_blueprint(final_blueprint, ambari_host, ambari_host, "mit-kdc", "cl1")
    else:
        prepare_host_mapping(agent_hosts, False)
    db_utils.install_and_setup_mysql_connector()
    db_utils.setup_hive_db(db_host)
    db_utils.setup_oozie_db(db_host)
    db_utils.setup_ranger_db(db_host)
    if "yes" in is_ha_test:
        configs_util.setup_ranger_ha(cluster_name,agent_hosts,db_host,"6040")
    configs_util.setup_ranger_policy_url(ambari_host,final_blueprint)
    configs_util.update_db_hosts_in_blueprint(db_host,final_blueprint)
    ambari_utils.setup_ambari_server_after_ranger_setup(ambari_host,"mysql")
    ambari_utils.restart_ambari_server(ambari_host)
    repo_url = ambari_utils.get_ambari_repo_url(os_type)
    if "INVALID" in repo_url:
        logger.error("Invalid OS provided in the command :{0}".format(os_type))
        exit()
    ambari_utils.setup_ambari_repo_on_multiple_hosts(agent_hosts,repo_url)
    ambari_utils.install_ambari_agent_on_multiple_hosts(agent_hosts)
    ambari_utils.register_and_start_ambari_agent_on_multiple_hosts(agent_hosts,ambari_host)
    os_version = ambari_utils.get_os_version_string(os_type)
    ambari_utils.register_stack_version(ambari_host, "2.6", os_version, hdp_base_url)
    ambari_utils.provide_log_directory_permissions(agent_hosts)
    register_blueprint(final_blueprint,ambari_host,"blueprint-def")
    deploy_cluster(cluster_name,ambari_host,"conf/cluster_template.json")
    wait_for_cluster_status(cluster_name,ambari_host)

if __name__ == "__main__":
    deploy()
