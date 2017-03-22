from log_utils import get_logger
import ssh_utils
from threading import Thread
import subprocess
import platform
import requests_util

logger = get_logger(__name__)
final_hdp_repo_file = "conf/hdp_repo_final.json"
final_hdp_util_repo_file = "conf/hdp_util_repo_final.json"

def setup_ambari_server(db_type, db_name, db_username,db_password,db_host,db_port):
    setup_command = "ambari-server setup --database={0} --databasehost={1} --databaseport={2} --databasename={3} --databaseusername={4} --databasepassword={5} -s"
    setup_command = setup_command.format(db_type,db_host,db_port,db_name,db_username,db_password)
    logger.info("DEPLOYNG : Setting up Ambari Server :",setup_command)
    process = ssh_utils.run_shell_command(setup_command)
    logger.info("Command executed : {0}".format(process[0]))
    logger.info("Command Exit code : {0}".format(process[1]))

#Setup ambari repo on multiple hosts
def setup_ambari_repo_on_multiple_hosts(hostnames,repo_url):
    logger.info("Setting up ambari repo on multiple hosts : {0}".format(hostnames))
    thread_list = []
    try:
        for hostname in hostnames:
            logger.info("Setting up repo on : {0}".format(hostname))
            setup_thread = Thread(target=setup_ambari_repo, args=(hostname, repo_url,))
            setup_thread.daemon = True
            setup_thread.start()
            thread_list.append(setup_thread)
        for thread in thread_list:
            thread.join(timeout=30)
        logger.info("Setting up ambari repo on multiple hosts : COMPLETED")
    except:
        logger.info("Error: unable to start thread")


#Installing ambari-agent on multiple hosts
def install_ambari_agent_on_multiple_hosts(hostnames):
    logger.info("Installing ambari agent on multiple hosts")
    thread_list = []
    try:
        for hostname in hostnames:
            install_thread = Thread(target=install_ambari_agent_on_single_host,args=(hostname,))
            install_thread.daemon=True
            install_thread.start()
            thread_list.append(install_thread)
        for thread in thread_list:
            thread.join()
        logger.info("Installing Ambari Agents on multiple hosts : COMPLETED")
    except:
        logger.info("Error: unable to start thread")

#Starting ambari-agent on multiple hosts
def start_ambari_agent_on_multiple_hosts(hostnames):
    logger.info("Installing ambari agent on multiple hosts")
    thread_list = []
    try:
        for hostname in hostnames:
            start_thread = Thread(target=start_ambari_agent_on_single_host, args=(hostname))
            start_thread.daemon=True
            start_thread.start()
            thread_list.append(start_thread)
        for thread in thread_list:
            thread.join()
        logger.info("Starting Ambari Agents on multiple hosts : COMPLETED")
    except:
        logger.info("Error: unable to start thread")
        raise "Ambari-agents not started properly"

# Starting ambari-agent on multiple hosts
def register_and_start_ambari_agent_on_multiple_hosts(hostnames,server_host):
    logger.info("Resgistering and starting ambari agent on multiple hosts")
    thread_list = []
    try:
        for hostname in hostnames:
            start_thread = Thread(target=register_ambari_agent_on_single_host, args=(hostname,server_host))
            start_thread.daemon=True
            start_thread.start()
            thread_list.append(start_thread)
        for thread in thread_list:
            thread.join()
        logger.info("Registering Ambari Agents on multiple hosts : COMPLETED")
    except:
        logger.info("Error: unable to start thread")
        raise "Ambari-agents not started properly"


#install on ambari agents
def install_ambari_agent_on_single_host(hostname):
    logger.info("Installing ambari agent on single host {0}".format(hostname))
    setup_repo = subprocess.Popen("ssh -t -i /root/ec2-keypair root@{0} yum install ambari-agent -y".format(hostname),shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,error = setup_repo.communicate()
    print out + " : " + error
    logger.info("Command executed : {0}".format(setup_repo.returncode))

#Check Amabri-agent status on host
def is_ambari_agent_running(hostname):
    logger.info("Checking ambari agent on single host {0}".format(hostname))
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
    logger.info("Starting ambari agent on single host {0}".format(hostname))
    if is_ambari_agent_running(hostname):
        start_agent = subprocess.Popen("ssh -t -i /root/ec2-keypair root@{0} ambari-agent restart".format(hostname),shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        start_agent = subprocess.Popen("ssh -t -i /root/ec2-keypair root@{0} ambari-agent start".format(hostname),shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    logger.info("Command executed :".format(start_agent.communicate()[0]))


#install on ambari agents
def install_ambari_server(hostname):
    logger.info("Installing ambari server on  host {0}".format(hostname))
    setup_repo = subprocess.Popen("yum install ambari-server -y",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    setup_repo.communicate()[0]
    logger.info("Command executed : {0} ".format(setup_repo.returncode))

#install on ambari agents
def start_ambari_server(hostname):
    logger.info("Starting ambari server on  host host {0}".format(hostname))
    setup_repo = subprocess.Popen("ambari-server start",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    setup_repo.communicate()[0]
    logger.info("Command executed : {0}".format(setup_repo.returncode))


#Restart ambari server
def restart_ambari_server(hostname):
    logger.info("Restarting ambari server on  host {0}".format(hostname))
    setup_repo = subprocess.Popen("ambari-server restart",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    setup_repo.communicate()[0]
    logger.info("Command executed : {0} ".format(setup_repo.returncode))

#Get Ambari server status
def get_ambari_server_status(hostname):
    logger.info("Checking ambari server status on  host {0}".format(hostname))
    check_server_status = subprocess.Popen("ambari-server status",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,err = check_server_status.communicate()
    logger.info("Command executed : {0} ".format(check_server_status.returncode))
    if "Ambari Server running" not in out:
        return True
    else:
        return False
#setup ambari repo
def setup_ambari_repo(hostname, ambari_repo_url):
    logger.info("Repo setup on single host {0}".format(hostname))
    logger.info("Platform : "+platform.platform())
    if "centos-6" in platform.platform() or "Darwin" in platform.platform():
        setup_repo_command = "ssh -t -i /root/ec2-keypair root@{0} wget -O /etc/yum.repos.d/ambari.repo {1}".format(hostname,ambari_repo_url)
        logger.info("REPO COMMAND :: > {0}".format(setup_repo_command))
        try:
            command_out = subprocess.Popen(setup_repo_command,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE,bufsize=-1)
            out,error = command_out.communicate()
            print out + " : " + error
            logger.info("Setup Command executed :{0} ".format(command_out.returncode))
        except Exception,e:
            logger.info("exception in Setup {0}".format(e))
            command_out.kill()

def provide_log_directory_permissions(hostnames):
    logger.info("Providing 777 access to /var/log directory")
    thread_list = []
    try:
        for hostname in hostnames:
            logger.info("Providing log access on host : {0}".format(hostname))
            setup_thread = Thread(target=run_command_on_single_host, args=("root",hostname, "chmod -R 777 /var/log/",))
            setup_thread.daemon = True
            setup_thread.start()
            thread_list.append(setup_thread)
        for thread in thread_list:
            thread.join()
        logger.info("Updating /var/log permissions : COMPLETED")
    except:
        logger.info("Error: unable to start thread")

def setup_ambari_server_after_ranger_setup(hostname,db_type):
    logger.info("Ambari setup after ranger setup onb host {0}".format(hostname))
    if "mysql" in db_type:
        jdbc_connector_path = "/usr/share/java/mysql-connector-java.jar"
        ssh_utils.run_shell_command("ambari-server setup --jdbc-db=mysql --jdbc-driver={0}".format(jdbc_connector_path))

def run_command_on_single_host(user,hostname,command):
    ssh_utils.run_ssh_cmd(user,hostname,command)

def update_hdp_repo(base_url):
    ssh_utils.run_shell_command("cp conf/repo.json {1}".format(final_hdp_repo_file))
    return ssh_utils.run_shell_command("sed -i 's/BASE_URL_PLACEHOLDER/{0}/g' {1}".format(base_url,final_hdp_repo_file))

def update_hdp_util_repo(base_url):
    ssh_utils.run_shell_command("cp conf/repo.json {1}".format(final_hdp_repo_file))
    return ssh_utils.run_shell_command("sed -i 's/BASE_URL_PLACEHOLDER/{0}/g' {1}".format(base_url,final_hdp_util_repo_file))


def register_stack_version(ambari_host,hdp_version,os_string,hdp_base_url):
    logger.info("Registering HDP and HDP util stack version")
    update_hdp_repo(hdp_base_url)
    register_stack_version_url = "http://{0}:8080/api/v1/stacks/HDP/versions/{1}/operating_systems/{2}/repositories/{3}"
    register_hdp_stack_version_url = register_stack_version_url.format(ambari_host,hdp_version,os_string,"HDP-"+hdp_version)
    requests_util.put_api_call(register_hdp_stack_version_url,final_hdp_repo_file)






