import subprocess
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def run_ssh_cmd(user, host,  command):
    full_command = "ssh -T -i /root/ec2-keypair {0}@{1} {2}"
    full_command = full_command.format(user,host,command)
    logger.info("Executing : {0}".format(full_command))
    return run_shell_command(full_command)


def run_shell_command(command):
    process = subprocess.Popen([command], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,err = process.communicate()
    return out,err,process.returncode

def copy_file_to_host(hostname,file_source,file_destination):
    logger.info("Copying files to remote host {0}".format(hostname))
    copy_command = "scp -i /root/ec2-keypair {0} {1}@{2}:{3}".format(file_source,"root",hostname,file_destination)
    logger.info("Executing : {0} ".format(copy_command))
    response = run_shell_command(copy_command)
    logger.debug("Execution completed with {0} return code. Output :{1}".format(response[2],response[0]))

def copy_file(file_source,file_destination):
    copy_command = "cp {0} {1}".format(file_source,file_destination)
    logger.info("Executing : {0} ".format(copy_command))
    response = run_shell_command(copy_command)
    logger.debug("Execution completed with {0} return code. Output :{1}".format(response[2],response[0]))