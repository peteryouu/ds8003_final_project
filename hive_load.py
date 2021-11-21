# pip install paramiko

import paramiko

# Let's just ssh into the sandbox to execute hive commands. For a prod setup we'd probably need something more robust,
#  but for a demo this works just fine
ssh = paramiko.SSHClient()

# This is super not cool, but again, dev env so we'd do it properly in prod
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

ssh.connect('sandbox-hdp.hortonworks.com', username='root', password='hdpsandbox', port=2222)

hive_load = """hive -e "LOAD DATA INPATH '/tmp/yahoo_chart_staging/' INTO TABLE yahoo_finance.chart" """
_, ssh_stdout, ssh_stderr = ssh.exec_command(hive_load)
print(ssh_stdout.readlines())
print(ssh_stderr.readlines())
