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
