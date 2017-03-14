import subprocess


def run_ssh_cmd(user, host,  command):
    full_command = "ssh -T -i /root/ec2-keypair root@{0}"
    full_command = full_command.format(host,command)
    print "Executing : {0}".format(full_command)
    return run_shell_command(full_command,command)


def run_shell_command(command,args):
    process = subprocess.Popen([command,args,"StrictHostKeyChecking=no"], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,err = process.communicate()
    return out,err,process.returncode
