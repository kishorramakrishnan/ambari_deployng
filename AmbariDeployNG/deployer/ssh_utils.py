import subprocess


def run_ssh_cmd(user, host,  command):
    command = "ssh -i /root/ec2-keypair {0}@{1} {2}"
    command = command.format(user,host,command)
    print "Executing : ",command
    return run_shell_command()


def run_shell_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return process