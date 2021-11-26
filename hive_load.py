# pip install paramiko

import paramiko


def load_staging_to_hive(file_name, trading_date, staging_folder='yahoo_chart_staging'):
    # Let's just ssh into the sandbox to execute hive commands. For a prod setup we'd probably need something more robust,
    #  but for a demo this works just fine
    ssh = paramiko.SSHClient()

    # This is super not cool, but again, dev env so we'd do it properly in prod
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect('sandbox-hdp.hortonworks.com', username='root', password='hdpsandbox', port=2222)

    hive_load = f"""hive -e "LOAD DATA INPATH '/tmp/{staging_folder}/{file_name}' OVERWRITE INTO TABLE yahoo_finance.chart partition (trading_date='{trading_date}')" """
    _, ssh_stdout, ssh_stderr = ssh.exec_command(hive_load)
    print(f"Running hive load: {hive_load}")
    # print(ssh_stdout.readlines())
    # print(ssh_stderr.readlines())
