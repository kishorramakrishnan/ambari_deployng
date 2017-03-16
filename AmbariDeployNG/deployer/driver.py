import os,collections
import json
import logging
import re
import platform
import subprocess
import time
from threading import Thread
import sys


#Setup Ambari Server on the gateway host. Exit if something fails

import ssh_utils
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('logs/deploy.log')
handler.setLevel(logging.DEBUG)
# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(handler)

def prepare_host_mapping(agent_hosts):
    logger.info("Assigning hosts to Groups ",agent_hosts)
    ssh_utils.run_shell_command("cp conf/cluster_host_groups.json conf/cluster_host_groups_runtime.json")


    total_hosts = len(agent_hosts)
    #TODO : Change the number to 5 once we arrive at solution to include DB host( one host is dedicated for DB)
    if total_hosts < 4 :
        logger.error("Number of nodes in the cluster should be atleast 5")
        exit
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
    
    logger.info("client_slave_fqdns" , client_slave_fqdns)
    logger.info("master_slave_dep_fdqns" , master_slave_dep_fdqns)
    logger.info("master_only_fqdns" , master_only_fqdns)
    logger.info("master_slave_fqdns" , master_slave_fqdns)
    
    


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


def setup_ambari_server(db_type, db_name, db_username,db_password,db_host,db_port):
    setup_command = "ambari-server setup --database={0} --databasehost={1} --databaseport={2} --databasename={3} --databaseusername={4} --databasepassword={5} -s"
    setup_command = setup_command.format(db_type,db_host,db_port,db_name,db_username,db_password)
    logger.info("DEPLOYNG : Setting up Ambari Server :",setup_command)
    process = ssh_utils.run_shell_command(setup_command)
    logger.info("Command executed : {0}".format(process[0]))
    logger.info("Command Exit code : {0}", format(process[1]))

#Setup ambari repo on multiple hosts
def setup_ambari_repo_on_multiple_hosts(hostnames,repo_url):
    logger.info("Setting up ambari repo on multiple hosts : ",hostnames)
    try:
        for hostname in hostnames:
            logger.info("Setting up repo on : ",hostname)
            setup_thread = Thread(target=setup_ambari_repo, args=(hostname, repo_url,))
            setup_thread.daemon = True
            setup_thread.start()
            setup_thread.join(timeout=30)
    except:
        logger.info("Error: unable to start thread")


#Installing ambari-agent on multiple hosts
def install_ambari_agent_on_multiple_hosts(hostnames):
    logger.info("Installing ambari agent on multiple hosts")
    try:
        for hostname in hostnames:
            install_thread = Thread(target=install_ambari_agent_on_single_host,args=(hostname,))
            install_thread.daemon=True
            install_thread.start()
            install_thread.join()
    except:
        logger.info("Error: unable to start thread")

#Starting ambari-agent on multiple hosts
def start_ambari_agent_on_multiple_hosts(hostnames):
    logger.info("Installing ambari agent on multiple hosts")
    try:
        for hostname in hostnames:
            start_thread = Thread(target=start_ambari_agent_on_single_host, args=(hostname))
            start_thread.daemon=True
            start_thread.start()
            start_thread.join()
    except:
        logger.info("Error: unable to start thread")
        raise "Ambari-agents not started properly"

# Starting ambari-agent on multiple hosts
def register_and_start_ambari_agent_on_multiple_hosts(hostnames,server_host):
    logger.info("Resgistering and starting ambari agent on multiple hosts")
    try:
        for hostname in hostnames:
            start_thread = Thread(target=register_ambari_agent_on_single_host, args=(hostname,server_host))
            start_thread.daemon=True
            start_thread.start()
            start_thread.join()
    except:
        logger.info("Error: unable to start thread")
        raise "Ambari-agents not started properly"


#install on ambari agents
def install_ambari_agent_on_single_host(hostname):
    logger.info("Installing ambari agent on single host", hostname)
    setup_repo = subprocess.Popen("ssh -t -i /root/ec2-keypair root@{0} yum install ambari-agent -y".format(hostname),shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,error = setup_repo.communicate()
    print out + " : " + error
    logger.info("Command executed : {0}".format(setup_repo.returncode))

#Check Amabri-agent status on host
def is_ambari_agent_running(hostname):
    logger.info("Checking ambari agent on single host", hostname)
    agent_running_command = "ssh -t -i /root/ec2-keypair root@{0} ambari-agent status".format(hostname)
    setup_repo = subprocess.Popen(agent_running_command,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, error = setup_repo.communicate()
    if "Ambari Server running" in out:
        return True
    else:
        return False


def register_ambari_agent_on_single_host(hostname,ambari_server_host):
    register_host_command = "ssh -t -i /root/ec2-keypair root@{0} sed -i 's/hostname=localhost/hostname={1}/g' /etc/ambari-agent/conf/ambari-agent.ini"
    register_host_command = register_host_command.format(hostname,ambari_server_host)
    print register_host_command
    register_host = subprocess.Popen(register_host_command,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, error = register_host.communicate()
    print out + " : " + error
    start_ambari_agent_on_single_host(hostname)
    logger.info("Command executed : {0} ".format(register_host.returncode))


#install on ambari agents
def start_ambari_agent_on_single_host(hostname):
    logger.info("Starting ambari agent on single host", hostname)
    if is_ambari_agent_running(hostname):
        start_agent = subprocess.Popen("ssh -t -i /root/ec2-keypair root@{0} ambari-agent restart".format(hostname),shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        start_agent = subprocess.Popen("ssh -t -i /root/ec2-keypair root@{0} ambari-agent start".format(hostname),shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    logger.info("Command executed :", start_agent.communicate()[0])


#install on ambari agents
def install_ambari_server(hostname):
    logger.info("Installing ambari server on  host ", hostname)
    setup_repo = subprocess.Popen("yum install ambari-server -y",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    setup_repo.communicate()[0]
    logger.info("Command executed : {0} ".format(setup_repo.returncode))

#install on ambari agents
def start_ambari_server(hostname):
    logger.info("Starting ambari server on  host ", hostname)
    setup_repo = subprocess.Popen("ambari-server start",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    setup_repo.communicate()[0]
    logger.info("Command executed :", setup_repo.returncode)


#Restart ambari server
def restart_ambari_server(hostname):
    logger.info("Starting ambari server on  host ", hostname)
    setup_repo = subprocess.Popen("ambari-server restart",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    setup_repo.communicate()[0]
    logger.info("Command executed : {0} ".format(setup_repo.returncode))

#Get Ambari server status
def get_ambari_server_status(hostname):
    logger.info("Checking ambari server status on  host ", hostname)
    check_server_status = subprocess.Popen("ambari-server status",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,err = check_server_status.communicate()
    logger.info("Command executed : {0} ".format(check_server_status.returncode))
    if "Ambari Server running" not in out:
        return True
    else:
        return False


def register_blueprint(blueprint_json,ambari_server_host,blueprint_name):
    logger.info("Registering Blueprint using REST API")
    register_bp_command = "curl -i -u admin:admin -H 'X-Requested-By: ambari'  -X POST --data @{0} {1}"
    register_bp_command = register_bp_command.format(blueprint_json,"http://{0}:8080/api/v1/blueprints/{1}?validate_topology=false".format(ambari_server_host,blueprint_name))
    register_bp = subprocess.Popen(register_bp_command,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,error = register_bp.communicate()
    print "Out put : {0} {1}".format(out,error)
    logger.info("Command executed : {0} ".format(register_bp.returncode))


def deploy_cluster(cluster_name,ambari_server_host,cluster_json):
    logger.info("Deploy cluster using REST API")
    cluster_create_command = "curl -H 'X-Requested-By: ambari' -X POST -u admin:admin http://{0}:8080/api/v1/clusters/{1} -d @{2}"
    cluster_create_command = cluster_create_command.format(ambari_server_host,cluster_name,cluster_json)
    total_wait_time_in_seconds = 3600
    create_cluster = subprocess.Popen(cluster_create_command,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,error = create_cluster.communicate()
    print "Out put : {0} {1}".format(out, error)
    logger.info("Command executed : {0} ".format(create_cluster.returncode))

def wait_for_cluster_status(cluster_name,ambari_server_host):
    logger.info("Waiting for Cluster Deploys status REST API")
    cluster_deploy_command = "curl -H 'X-Requested-By: ambari' -X GET -u admin:admin http://{0}:8080/api/v1/clusters/{1}/requests/1"
    cluster_deploy_command = cluster_deploy_command.format(ambari_server_host,cluster_name)
    total_wait_time_in_seconds = 3600
    elapsed_time = 0
    while elapsed_time < total_wait_time_in_seconds:
        deploy_status = subprocess.Popen(cluster_deploy_command, shell=True, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        out, error = deploy_status.communicate()
        logger.debug("Out put : {0} {1}".format(out, error))
        if "IN_PROGRESS" in out:
            logger.info("Deploy in progress : Time elapsed in seconds: ", elapsed_time)
            time.sleep(60)
        elif "FAILED" in out:
            logger.info("Deploy Failed")
            break
        elif "COMPLETED" in out:
            logger.info("DEPLOY COMPLETED!!! Tooke {0} seconds to finish".format(elapsed_time))
            break
        else:
            logger.info("Something wrong {0} : {1}".format(out,error))
        elapsed_time = elapsed_time + 60
    logger.info("Command executed : {0} ".format(deploy_status.returncode))

def install_kerberos_client_on_multiple_hosts(hostnames):
    print "Installing Kerberos client on multiple hosts"
    logger.info("Setting up ambari repo on multiple hosts : ", hostnames)
    try:
        for hostname in hostnames:
            logger.info("Setting up repo on : ", hostname)
            setup_thread = Thread(target=install_kerberos_client_on_single_host, args=(hostname))
            setup_thread.daemon = True
            setup_thread.start()
            setup_thread.join(timeout=30)
    except:
        logger.info("Error: unable to start thread")

def install_kerberos_client_on_single_host(host):
    logger.info("Installing Kerberos clients on host : ",host)
    ssh_utils.run_ssh_cmd("user",host,"yum install krb5-workstation -y")
    ssh_utils.run_ssh_cmd("user", host, "yum install unzip -y")

def distribute_JCE_on_multiple_hosts(hostnames):
    print "Installing JCE on multiple hosts"
    logger.info("Installing JCE  on multiple hosts : ", hostnames)
    unzip_command = "unzip -o -j -q /var/lib/ambari-server/resources/UnlimitedJCEPolicyJDK7.zip -d"
    try:
        for hostname in hostnames:
            logger.info("Setting up JCE on : ", hostname)
            copy_command = ""
            ssh_utils.run_shell_command("scp -i /root/ec2-keypair root@{} {}".format(hostname,copy_command))

    except:
        logger.info("Error: unable to start thread")


def install_and_setup_kerberos(kdc_host):
    logger.info("Install and setup Kerberos")
    ssh_utils.run_shell_command("chmod 777 setup_kerberos.sh")
    ssh_utils.run_shell_command("ls -lrt")
    setup_kerberos = subprocess.Popen("./setup_kerberos.sh {0} {1} {2}".format(kdc_host,"admin","admin/admin@EXAMPLE.COM"),shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logger.info("Command executed Setup KDC : {0}".format(setup_kerberos.communicate()[1]))

def install_and_setup_mysql_connector():
    print "Install and setup MySQL Connector"
    subprocess.Popen("chmod 777 setup_mysql_connector.sh",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = ssh_utils.run_shell_command("ls -lrt")[0]
    print out
    setup_mysql = subprocess.Popen("./setup_mysql_connector.sh",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logger.info("Command executed Install And Setup Mysql Connector : {0}".format(setup_mysql.communicate()[1]))


#setup ambari repo
def setup_ambari_repo(hostname, ambari_repo_url):
    logger.info("Repo setup on single host", hostname)
    logger.info("Platform : "+platform.platform())
    if "centos-6" in platform.platform() or "Darwin" in platform.platform():
        setup_repo_command = "ssh -t -i /root/ec2-keypair root@{0} wget -O /etc/yum.repos.d/ambari.repo {1}".format(hostname,ambari_repo_url)
        logger.info("REPO COMMAND :: >",setup_repo_command)
        try:
            command_out = subprocess.Popen(setup_repo_command,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE,bufsize=-1)
            out,error = command_out.communicate()
            print out + " : " + error
            logger.info("Setup Command executed :{0} ".format(command_out.returncode))
        except Exception,e:
            logger.info("exception in Setup ",e)
            command_out.kill()





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
    else:
        prepare_configs(agent_hosts, False)
    ambari_host = agent_hosts[0]
    install_and_setup_mysql_connector()
    restart_ambari_server(ambari_host)
    setup_ambari_repo_on_multiple_hosts(agent_hosts,"http://dev.hortonworks.com.s3.amazonaws.com/ambari/centos6/2.x/updates/2.5.0.1/ambariqe.repo")
    install_ambari_agent_on_multiple_hosts(agent_hosts)
    register_and_start_ambari_agent_on_multiple_hosts(agent_hosts,ambari_host)
    install_and_setup_kerberos(ambari_host)
    register_blueprint("conf/blueprint_{0}.json".format(str(cluster_type).strip()),ambari_host,"blueprint-def")
    deploy_cluster("cl1",ambari_host,"conf/cluster_deploy_1.json")
    wait_for_cluster_status("cl1",ambari_host)

if __name__ == "__main__":
    deploy()