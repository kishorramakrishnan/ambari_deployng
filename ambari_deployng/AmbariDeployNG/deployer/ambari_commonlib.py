#
# Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between your
# company and Hortonworks, Inc, any use, reproduction, modification,
# redistribution, sharing, lending or other exploitation of all or
# any part of the contents of this file is strictly prohibited.
#
#
""" This Class contains all the common libraries for getting the cluster configuration and permorming ssh and scp into remote machines"""

import os
import socket
from stat import S_ISDIR
import re
import warnings
import sys
import shutil
import paramiko

import logging

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)

class CommonLib:

    MasterComponents = set(["NameNode", "SNameNode", "ResourceManager", "App Timeline Server", "HistoryServer", "HBase Master","Hive Metastore","HiveServer2","WebHCat Server","Oozie Server","Kafka Broker","Knox Gateway","ZooKeeper Server","MySQL Server","Nimbus","DRPC Server","Storm UI Server","Storm REST API Server","Falcon Server","Metrics Collector","Ranger Admin","Ranger Usersync","Spark History Server","Accumulo GC","Accumulo Master","Accumulo Monitor","Accumulo Tracer","Atlas Server"])
    HOST_URL = None
    ambari_server_properties = '/etc/ambari-server/conf/ambari.properties'

    def __init__(self, ambari_property_dir):
        self.HOST_URL = CommonLib.get_host_uri(CommonLib.initialize_properties(ambari_property_dir))
        self.username = 'root'
        self.KEY_ON_SERVER = '/root/ec2-keypair'


    # This function is used to perform a passwordless SSH Login to a remote machine
    def ssh_passwordless_login(self, hostip, username):
        logger.info("Connecting to the host "+ username + "@" +hostip)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        privatekeyfile = os.path.expanduser(self.KEY_ON_SERVER)
        key = paramiko.RSAKey.from_private_key_file(privatekeyfile)
        ssh.connect(hostname=hostip, username=username, pkey=key)
        logger.info("Connected to the host "+ username + "@" +hostip)
        return ssh

    # This function is used to execute commands on the remote machine
    def execute_command(self, hostip, username, commands):
        ssh = self.ssh_passwordless_login(hostip, username)
        content = []
        err_content = []
        for command in commands:
            logger.info("Executing command: " + command)
            try:
                (stdin, stdout, stderr) = ssh.exec_command(command=command)

            except socket.error:
                logger.info("Socket Timeout")
            except Exception, e:
                logger.info("Error executing the command: "+ command + "\n Reason : "+str(e))
                sys.exit()

            logger.info("Command Output: ")
            for line in stdout:
                content.append(line)
                #print '... ' + line.strip('\n')
            for line1 in stderr:
                err_content.append(line1)
                #print '... ' + line1.strip('\n')
        logger.info("Disconnecting from the host "+ username + "@" +hostip)
        ssh.close()
        if len(err_content) > 0:
            return err_content
        else:
            return content

    # This function is used to execute interactive commands on the remote machine
    def execute_interactive_command(self, hostip, username, commands):
        ssh = self.ssh_passwordless_login(hostip, username)
        logger.info("Executing command: " + commands[0])
        stdin, stdout, stderr = ssh.exec_command(commands[0])
        for i in range(1,len(commands)):
            stdin.write(commands[i])
            stdin.write('\n')
            stdin.flush()
        #logger.info("Command Output: ")
        for line in stdout:
            pass
            #print '... ' + line.strip('\n')
        logger.info("Disconnecting from the host "+ username + "@" +hostip)
        ssh.close()

    # This function is used to execute interactive commands on the remote machine
    def execute_interactive_command1(self, hostip, username, commands):
        ssh = self.ssh_passwordless_login(hostip, username)
        logger.info("Executing command: " + commands[0])
        stdin, stdout, stderr = ssh.exec_command(commands[0])
        for i in range(1,len(commands)):
            stdin.write(commands[i])
            stdin.write('\n')
            stdin.flush()
        logger.info("Disconnecting from the host "+ username + "@" +hostip)
        ssh.close()

    # This function is used to copy file to a remote machine
    def secure_copy_file_to_remote_server(self, hostip, username, localpath, remotepath):
        ssh = self.ssh_passwordless_login(hostip, username)
        try:
            transfer = ssh.open_sftp()
            transfer.put(localpath, remotepath)
            transfer.close()
        except OSError:
            logger.info("OSError")
        ssh.close()

    # This function is used to copy file from a remote machine
    def secure_copy_file_from_remote_server(self, hostip, username, remotepath, localpath):
        ssh = self.ssh_passwordless_login(hostip, username)
        try:
            transfer = ssh.open_sftp()
            transfer.get(remotepath, localpath)
            transfer.close()
        except OSError:
            logger.info("OSError")
        ssh.close()

    # This function is used to copy directory to a remote machine
    def secure_copy_directory_to_remote_server(self, hostip, username, localpath, remotepath):
        ssh = self.ssh_passwordless_login(hostip, username)
        sftp = ssh.open_sftp()
        os.chdir(os.path.split(localpath)[0])
        parent=os.path.split(localpath)[1]
        for walker in os.walk(parent):
            try:
                sftp.mkdir(os.path.join(remotepath,walker[0]))
            except:
                pass
            for file in walker[2]:
                self.secure_copy_file_to_remote_server(os.path.join(walker[0],file),os.path.join(remotepath,walker[0],file))
            ssh.close()

    # This function is a helper function which performs a walk into a directory to get all the files in it
    def sftp_walk(self,hostip, username, remotepath):
        ssh = self.ssh_passwordless_login(hostip, username)
        sftp = ssh.open_sftp()
        path=remotepath
        files=[]
        folders=[]
        for f in sftp.listdir_attr(remotepath):
            if S_ISDIR(f.st_mode):
                folders.append(f.filename)
            else:
                files.append(f.filename)
        # print (path,folders,files)
        yield path,folders,files
        for folder in folders:
            new_path=os.path.join(remotepath,folder)
            for x in self.sftp_walk(new_path):
                yield x
        ssh.close()

    # This function is used to copy directory from a remote machine
    def secure_copy_directory_from_remote_server(self,hostip, username, localpath, remotepath):
        ssh = self.ssh_passwordless_login(hostip, username)
        sftp = ssh.open_sftp()
        sftp.chdir(os.path.split(remotepath)[0])
        parent=os.path.split(remotepath)[1]
        try:
            os.mkdir(localpath)
        except:
            pass
        for walker in self.sftp_walk(parent):
            try:
                os.mkdir(os.path.join(localpath,walker[0]))
            except:
                pass
            for file in walker[2]:
                self.secure_copy_file_from_remote_server(os.path.join(walker[0],file),os.path.join(localpath,walker[0],file))
        ssh.close()

    # This function is used to copy a file from the server to the host
    def copy_file_from_server_to_host(self, internal_host_to, pathFrom, pathTo, filename):
        logger.info("Copying file "+ filename + " from "+pathFrom + " to "+internal_host_to+":"+pathTo)
        ssh_host = paramiko.SSHClient()
        ssh_host.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        privatekeyfile = os.path.expanduser(self.KEY_ON_SERVER)
        key = paramiko.RSAKey.from_private_key_file(privatekeyfile)
        ssh_host.connect(hostname=internal_host_to, username=self.username, pkey=key)
        transfer = ssh_host.open_sftp()
        transfer.put(pathFrom+filename, pathTo+filename)
        transfer.close()
        ssh_host.close()

    # This function is used to copy a file to the server from the host
    def copy_file_to_server_from_host(self, internal_host_from, pathFrom, pathTo, filename):
        logger.info("Copying file "+ filename + " from "+internal_host_from+":"+pathFrom + " to "+pathTo)
        ssh_host = paramiko.SSHClient()
        ssh_host.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        privatekeyfile = os.path.expanduser(self.KEY_ON_SERVER)
        key = paramiko.RSAKey.from_private_key_file(privatekeyfile)
        ssh_host.connect(hostname=internal_host_from, username=self.username, pkey=key)
        transfer = ssh_host.open_sftp()
        transfer.get(pathTo+filename, pathFrom+filename)
        transfer.close()
        ssh_host.close()

    # This function is used to get the external hostname for a given internal host name
    # This function is not complete yet
    def get_external_name_by_internal(self, hostInternal):
        logger.info("Getting External name by internal for : " + hostInternal)
        if hostInternal is None:
            raise Exception("Invalid internal hostname - " + hostInternal)

        if hostInternal.startswith('dev'):
            return hostInternal

        if '.amazonaws' in hostInternal:
            return hostInternal

        # If this method was executed earlier during this session, it will get external name from static list of hostnames.
        for hostname in self.get_internal_hosts():
            if hostname is hostInternal:
                return self.get_external_host(hostname)

        # For humboldt
        if ".cloudapp.net" in hostInternal:
            externalHost = self.get_external_hosts[0]
            logger.info("humboldt external VM " + externalHost)
            return externalHost.trim()

        ec2host = False
        if hostInternal.endswith('ec2.internal'):
            ec2host = True
            key = self.KEY_ON_SERVER
            command = "ssh -o StrictHostKeyChecking=no -i " + key + " root@" + hostInternal + " 'wget -O - -q icanhazip.com'"
        elif hostInternal.contains(".novalocal"):
            command = "grep -oiP '^# [1-9\\.]+.*(?=\\s+" + hostInternal + ")' /etc/hosts| tr -d ' ' | tr -d '#'"
        else:
            command = "grep -oiP '^[1-9\\.]+.*(?=\\s+" + hostInternal + ")' /etc/hosts"

        output = self.execute_command(self.get_external_hosts()[0], self.username, list(command))
        pattern = re.compile('\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b')
        splittedResult = output.split("\n")
        for line in splittedResult:
            if pattern.match(line) is not None:
                if ec2host is True:
                    externalName = "ec2-" + line.replace(".", "-") + ".compute-1.amazonaws.com"
                else:
                    externalName = line;

                internalHosts = self.get_internal_hosts()
                internalHosts.append(hostInternal)
                externalHosts = self.get_external_hosts()
                externalHosts.append(externalName)
                logger.info(externalName)
                return externalName
        raise Exception("Appropriate external name was not found for " + hostInternal)

    @classmethod
    def initialize_properties(cls, ambari_property_dir):
        AmbariProperties = dict()
        for line in open(ambari_property_dir + '/ambari.properties'):
            if line.__contains__('='):
                AmbariProperties[line.strip().split('=')[0]] = line.strip().split('=')[1]

        return AmbariProperties

    # This function constructs the host uri for API calls
    @classmethod
    def get_host_uri(cls, ambari_properties):
        if CommonLib.is_ambari_security_enabled(ambari_properties.get('AMBARI_SERVER_HTTPS')) or CommonLib.is_ambari_security_enabled(ambari_properties.get('WIRE_ENCRYPTION')):
            port = 8443
            protocol = 'https'
        else:
            port = 8080
            protocol = 'http'

        HOST_URL = protocol + '://' + ambari_properties.get('HOST').split(':')[0] + ":" + str(port)
        print "HOST_URL = ", HOST_URL
        return HOST_URL

    @classmethod
    def read_job_properties(cls, local_work_dir):
        propChanges = {}

        COMPONENT = Config.get('ambari', 'COMPONENT')

        if 'deploytest' in COMPONENT:
            propChanges['INSTALL_HDP'] = "true"
            propChanges['MR_FRAMEWORK'] = Config.get('ambari', 'MR_FRAMEWORK')

        propChanges['HOST'] = Config.get('ambari', 'HOST') + ":8080"
        propChanges['CLUSTER_NAME'] = Config.get('ambari', 'CLUSTER_NAME')
        propChanges['STACK'] = Config.get('ambari', 'STACK')
        propChanges['UMASK'] = Config.get('ambari', 'UMASK')
        propChanges['AMBARI_CUSTOM_USER'] = Config.get('ambari', 'AMBARI_CUSTOM_USER')
        propChanges['AMBARI_AGENT_USER'] = Config.get('ambari', 'AMBARI_AGENT_USER')
        propChanges['PWD_ENCRYPT'] = Config.get('ambari', 'PWD_ENCRYPT')
        propChanges['CUSTOMIZED_SERVICES_USERS'] = Config.get('ambari', 'CUSTOMIZED_SERVICES_USERS')
        propChanges['CUSTOM_PIDS'] = Config.get('ambari', 'CUSTOM_PIDS')
        propChanges['AMBARI_2WAY_SSL'] = Config.get('ambari', 'AMBARI_2WAY_SSL')
        propChanges['AMBARI_SERVER_HTTPS'] = Config.get('ambari', 'AMBARI_SERVER_HTTPS')
        propChanges['IS_TMP_NOEXEC'] = Config.get('ambari', 'IS_TMP_NOEXEC')
        propChanges['NUMBER_OF_BASE_HOSTS'] = Config.get('ambari', 'NUMBER_OF_BASE_HOSTS')
        propChanges['DN_NONROOT'] = Config.get('ambari', 'DN_NONROOT')
        propChanges['MOTD_ENABLE'] = Config.get('ambari', 'MOTD_ENABLE')
        propChanges['HDP_REPO_BASEURL'] = Config.get('ambari', 'HDP_REPO_BASEURL')
        propChanges['HDP_UTILS_REPO_BASEURL'] = Config.get('ambari', 'HDP_UTILS_REPO_BASEURL')
        propChanges['AMBARI_DB'] = Config.get('ambari', 'AMBARI_DB')
        propChanges['JDK_VERSION'] = Config.get('ambari', 'JDK_VERSION')
        propChanges['IS_SECURE'] = Config.get('machine', 'IS_SECURE')
        propChanges['CLIENT'] = Config.get('ambari', 'CLIENT')
        propChanges['CLIENT_OS'] = Config.get('ambari', 'CLIENT_OS')
        propChanges['CLIENT_PORT'] = Config.get('ambari', 'CLIENT_PORT')
        propChanges['UPGRADE_TO'] = Config.get('ambari', 'UPGRADE_TO')
        propChanges['STACK_UPGRADE_TO'] = Config.get('ambari', 'STACK_UPGRADE_TO')
        propChanges['VIDEO_RECORDING'] = Config.get('ambari', 'VIDEO_RECORDING')
        propChanges['BROWSER'] = Config.get('ambari', 'BROWSER')
        propChanges['AMBARI_RPM_URL'] = Config.get('ambari', 'AMBARI_VERSION')
        propChanges['HIVE_DB'] = Config.get('ambari', 'DATABASE_FLAVOR')
        propChanges['DRUID_DB'] = Config.get('ambari', 'DATABASE_FLAVOR')
        propChanges['OOZIE_DB'] = Config.get('ambari', 'DATABASE_FLAVOR')
        propChanges['XA_DATABASE_FLAVOR'] = Config.get('ambari', 'XA_DATABASE_FLAVOR')
        propChanges['IS_HA_TEST'] = Config.get('ambari', 'IS_HA_TEST')
        propChanges['ENABLE_HA_COMPONENTS'] = Config.get('ambari', 'ENABLE_HA_COMPONENTS')
        propChanges['USE_BLUEPRINT'] = Config.get('ambari', 'USE_BLUEPRINT')
        propChanges['USER_KERBEROS_SERVER_TYPE'] = Config.get('ambari', 'USER_KERBEROS_SERVER_TYPE')
        propChanges['KERBEROS_SERVER_TYPE'] = Config.get('ambari', 'KERBEROS_SERVER_TYPE')
        propChanges['REALM'] = Config.get('ambari', 'REALM')
        propChanges['USER_REALM'] = Config.get('ambari', 'USER_REALM')
        propChanges['AD_SERVER_HOST'] = Config.get('ambari', 'AD_SERVER_HOST')
        propChanges['WIRE_ENCRYPTION'] = Config.get('ambari', 'WIRE_ENCRYPTION')
        propChanges['SPLIT_NUM'] = Config.get('ambari', 'SPLIT_NUM')
        propChanges['AMBARI_TESTSNAMES'] = Config.get('ambari', 'AMBARI_TESTSNAMES')
        propChanges['RUN_MARKER_LIST'] = Config.get('ambari', 'RUN_MARKER_LIST')
        propChanges['RUN_MARKER_VERSION'] = Config.get('ambari', 'RUN_MARKER_VERSION')

        ADDITIONAL_AMBARI_PROPS = Config.get('ambari','ADDITIONAL_AMBARI_PROPS')
        if ADDITIONAL_AMBARI_PROPS:
            parameter_map = ADDITIONAL_AMBARI_PROPS.split(",")
            for parameter in parameter_map:
                key_value = parameter.split("=")
                key = key_value[0]
                value = key_value[1]
                print "Reading key :%s = value :%s" %(key, value)
                propChanges[key] = value

        if 'sanity' in COMPONENT and not 'sanity-preupgrade' in COMPONENT or 'postupg-sec-enable' in COMPONENT:
            stack_upgrade_to =  Config.get('ambari', 'STACK_UPGRADE_TO')
            if stack_upgrade_to is not None and len(stack_upgrade_to) > 0:
                propChanges['STACK'] = Config.get('ambari', 'STACK_UPGRADE_TO')

        if not propChanges['UPGRADE_TO'] is None:
            logger.info("Check value " + propChanges['UPGRADE_TO'])

        # If the gateway is not deployed, change /root/hosts to have only those hosts that are to be part of gateway cluster
        if CommonLib.update_hosts_for_deployed_clusters(propChanges):
            propChanges['INSTALL_HDP'] = "true"

        util.writePropertiesToFile(os.path.join(local_work_dir, 'ambari.properties'),
                                   os.path.join(local_work_dir, 'ambari.properties'), propChanges)

        log_prop_changes = {}
        log_prop_changes['log4j.appender.UIFRM.File'] = os.path.join(
            local_work_dir, "uifrm.log")
        util.writePropertiesToFile(os.path.join(local_work_dir, 'log4j.properties'),
                                   os.path.join(local_work_dir, 'log4j.properties'), log_prop_changes)

        return propChanges

    @classmethod
    def update_hosts_for_deployed_clusters(cls, propChanges):
        number_of_clusters = Config.get('ambari','NUM_OF_CLUSTERS')

        if 'NUM_OF_DEPLOYED_CLUSTERS' in propChanges:
            number_of_deployed_clusters= propChanges['NUM_OF_DEPLOYED_CLUSTERS']
        else:
            number_of_deployed_clusters = number_of_clusters

        print "Number of deployed cluster: %s and number of clusters : %s" %(number_of_deployed_clusters, number_of_clusters)

        if number_of_deployed_clusters < number_of_clusters :
            print "Copying old root hosts to /root/hosts"
            Machine.runas('root', 'cp /root/old_hosts /root/hosts')
            return True
        else:
            return False

    # @classmethod
    # def copy_files(cls):
    #     print "Component = ", CommonLib.get_component()
    #     print "TESTSUITE_FILE = ", TESTSUITE_FILE
    #     print "SRC_DIR = ", SRC_DIR
    #
    #     #Set permissions on ambari testcode location
    #     Machine.runas('root', 'chmod -R 755 ' + DEPLOY_CODE_DIR)
    #     #copy the ambari ui test code to artifacts dir
    #     shutil.copytree(SRC_DIR, LOCAL_WORK_DIR)
    #     #copy the ambari pytest file to artifacts dir
    #     shutil.copy(PYTEST_FILE, LOCAL_WORK_DIR)

    @classmethod
    def get_component(cls):
        return Config.get('ambari', 'COMPONENT')

    @classmethod
    def delete_test_time_duration_file(cls):
        jsonFile = os.path.join('/tmp', 'testTimeDurations.json')
        if os.path.exists(jsonFile):
            Machine.runas('root', 'rm -rf /tmp/testTimeDurations.json')

    @classmethod
    def common_startup_operations(cls, local_work_dir):
        from tools.artifact_server_helper import artifact_server_start
        artifact_server_start(Config.getEnv('ARTIFACTS_DIR'))
        Machine.runas('root', 'chmod -R 755 ' + os.path.join(local_work_dir, 'target'))

    @classmethod
    def log_test_properties(cls, propChanges):
        splitNumStr = str(propChanges['SPLIT_NUM'])
        ambariTestClass = str(propChanges['AMBARI_TESTSNAMES'])

        print "splitNumStr is: ", splitNumStr
        print "Ambari tests to run: ",ambariTestClass
        print "Additional properties for run :", Config.get('ambari','ADDITIONAL_AMBARI_PROPS')

    @classmethod
    def Maven2runas(cls, cmd, local_work_dir, cwd=None, env=None, mavenOpts=None, logoutput=True, user=None):
        from beaver.component.ambariHelper import Maven2
        # make sure maven is setup before its run
        Maven2.setupMaven()

        # initialize env
        if not env:
            env = {}

        # determine if MAVEN_OPTS need to be set
        if mavenOpts:
            opts = os.environ.get('MAVEN_OPTS')
            if not opts:
                opts = mavenOpts
            else:
                opts = ' '.join([opts, mavenOpts])
            env['MAVEN_OPTS'] = opts

        env['JAVA_HOME'] = Maven2._java_home
        env['M2_HOME'] = Maven2._maven_home

        # print the env so we can see what we are setting
        logger.info('Env for mvn cmd')
        logger.info(env)
        print "Maven2runas:LOCAL_WORK_DIR = ", local_work_dir
        Machine.runas('root', 'chmod -R 777 ' + local_work_dir)
        maven_cmd = "cd %s; %s %s" % (local_work_dir,Maven2._maven_cmd, cmd)
        exit_code, stdout = Machine.runas(user,maven_cmd, cwd=cwd, env=env,logoutput=logoutput)
        return exit_code, stdout

    # This function reads the entry of 'api.ssl' in '/etc/ambari-server/conf/ambari.properties' file and also value from EC2 input job
    # Retrunss true if https or wire encrpytion are enabled in Ambari server
    @classmethod
    def is_ambari_security_enabled(cls, property):
        ambari_server_ssl = util.getPropertyValueFromFile(cls.ambari_server_properties, "api.ssl")
        if ambari_server_ssl and ambari_server_ssl.lower() == 'true' and property.lower() == 'yes':
            return True
        else:
            return False