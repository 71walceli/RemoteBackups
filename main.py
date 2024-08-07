import json
import yaml
import os, os.path, shutil
import subprocess
from threading import Thread, Semaphore
from datetime import datetime
from argparse import ArgumentParser
import paramiko

from FtpClient import FtpClient


def download_database(db_credentials, local_file):
  print("Database: INIT")
  command = [
    'mysqldump',
    '--user',     db_credentials["user"],
    '--host',     db_credentials["host"],
    f'--password={db_credentials["password"]}',
    '--databases',db_credentials["dbName"],
    '--column-statistics=0'
  ]
  # Use subprocess to run the command and capture the stdout
  with open(local_file, 'wb') as download_file:
    try:
      subprocess.run(
        command,
        stdout=download_file,
        check=True,  # Raise an error if the command fails
        text=False  # Ensure binary mode for stdout
      )
    except subprocess.CalledProcessError as e:
      os.remove(local_file)
      # Handle any errors, if necessary
      pass
  print("Database: END")

def download_database_ssh(credential, local_file):
  print("Database: INIT")
  
  ssh_conn = ssh_connect(credential)
  command = [
    'mysqldump',
    '--user',     credential["dbUser"],
    '--host',     credential.get("host", "localhost"),
    f'--password={credential["dbPass"]}',
    '--databases',credential["dbName"],
  ]
  command = " ".join([command[0], *("'"+r"\'".join(arg.split("'"))+"'" for arg in command[1:])])
  result = ssh_conn.exec_command(command)
  _,stdout,_ = result
  with stdout:
    with open(local_file, "wb") as download:
      chunk_size = 4096  # Adjust the chunk size as needed
      while True:
        chunk = stdout.read(chunk_size)
        if not chunk:
          break  # Break the loop when there's no more data
        download.write(chunk)
  
  # Use subprocess to run the command and capture the stdout
  print("Database: END")

def download_files(credential, local_dir):
  print("File Folder: INIT")
  
  try_make_dirs(local_dir)
  
  # TODO Move SSH logic to other module file
  if credential["type"] == "sshFolder":
    backup_file = f"{datetime.now().isoformat().replace(':', '-')}.tar"

    ssh_conn = ssh_connect(credential)
    remote_file = f"{credential['directory']}/{backup_file}"
    
    command = f"tar c $( find {credential['directory']} -mindepth 1 -maxdepth 1 ) | gzip -1"
    result = ssh_conn.exec_command(command)
    _,stdout,_ = result
    with stdout:
      with open(f"{local_dir}/{backup_file}", "wb") as download:
        chunk_size = 4096  # Adjust the chunk size as needed
        while True:
          chunk = stdout.read(chunk_size)
          if not chunk:
            break  # Break the loop when there's no more data
          download.write(chunk)
    ssh_conn.exec_command(f"rm {remote_file}")
    print(f"rm {remote_file}")
    
    command = f"find {credential['directory']} -type f -print0 | xargs -0 md5sum"
    result = ssh_conn.exec_command(command)
    _,stdout,_ = result
    backup_file_md5 = f"{backup_file}.md5"
    with stdout:
      with open(f"{local_dir}/{backup_file_md5}", "wb") as download:
        chunk_size = 4096  # Adjust the chunk size as needed
        while True:
          chunk = stdout.read(chunk_size)
          if not chunk:
            break  # Break the loop when there's no more data
          download.write(chunk)
    ssh_conn.close()
  elif credential["type"] == "ftpFolder":
    with FtpClient(credential["host"], credential["user"], credential["password"]) \
    as ftp:
      ftp.cwd(credential["directory"])
      ftp.cloneFolder(credential["directory"], local_dir)

  print("File Folder: END")

def ssh_connect(credential):
  ssh_conn = paramiko.SSHClient() 
  ssh_conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  ssh_conn.connect(
    hostname=credential["host"], 
    username=credential["user"],
    password=credential.get("password"),
  )
  
  return ssh_conn

def archivar(directorio, archivo_zip):
  print("Archiving: INIT")
  command = ["7z", "a", "-v3996m", "-mx=9", "-mmt=2", archivo_zip, directorio,]
  # Use subprocess to run the command and capture the stdout
  try:
    subprocess.run(
      command,
      check=True,  # Raise an error if the command fails
      text=False  # Ensure binary mode for stdout
    )
    delete_local_folder(directorio)
  except subprocess.CalledProcessError as e:
    print("Couldn't create archive.")
  print("Archiving: END")

def run_in_semaphore(semaphore: Semaphore, thread: Thread):
  with semaphore:
    thread.start()
    thread.join()

def parse_arguments():
  parser = ArgumentParser(
    description="Backup script for remote to local backups, including remote databases"
  )
  parser.add_argument("--backup_folder", help="Backup folder path", required=True)
  parser.add_argument("--hosting_creds", help="Hosting credentials file path", required=True)
  return parser.parse_args()

def delete_local_folder(backup_folder_domain):
  try:
    shutil.rmtree(backup_folder_domain)
  except FileNotFoundError:
    pass

def backup_website(credentials, backup_folder, dominio, _, backup_archive):
  threads = []
  for credential in credentials:
    # TODO Identify why we need intermediate values
    match credential["type"]:
      # TODO Split FTP and SSH code.
      case "ftpFolder" | "sshFolder":
        fileCredentials = credential
        threads.append(Thread(
          target=lambda: download_files(fileCredentials, 
            os.path.join(backup_folder, fileCredentials["directory"].replace("/", "@"))
          )
        ))
        #break
      case "mysqlDbOverSsh":
        mysqlDbOverSshCredential = credential
        threads.append(Thread(
          target=lambda: download_database_ssh(
            mysqlDbOverSshCredential, 
            os.path.join(backup_folder, f"{mysqlDbOverSshCredential['dbName']}.sql")
          )
        ))
        #break
      case "mysqlDb":
        mysqlDbCredential = credential
        threads.append(Thread(
          target=lambda: download_database(
            mysqlDbCredential, 
            os.path.join(backup_folder, f"{mysqlDbCredential['dbName']}.sql")
          )
        ))
        #break
      case _:
        raise RuntimeError("Unknown type of resource")

  print(f"DOMAIN {dominio}")
  try_make_dirs(backup_folder)

  for thread in threads:
    thread.start()
    thread.join()
  
  hilo_Archiving = Thread( target=lambda: archivar(backup_folder, backup_archive) )
  #run_in_semaphore(archiving_thread_pool, Thread( target=lambda: hilo_Archiving.start() ))

def try_make_dirs(backup_folder):
  try:
    os.makedirs(backup_folder)
  except FileExistsError:
    return
  except:
    raise
    
def batch_backup(credentials, backup_folder):
  # TODO For every spec, create separate thread.
  for dominio, credential in credentials.items():
    if dominio.startswith("__"):
      continue
    if dominio == "define":
      continue
    fecha_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_folder_domain = os.path.join(backup_folder, f"{fecha_hora} {dominio}")
    backup_archive_path = os.path.join(backup_folder, f"{fecha_hora} {dominio}.7z")
    
    delete_local_folder(backup_folder_domain)
    backup_website(credential, backup_folder_domain, dominio, fecha_hora, backup_archive_path)


if __name__ == "__main__":
  args = parse_arguments()
  backup_folder = args.backup_folder
  hosting_creds_path = args.hosting_creds

  archiving_thread_pool = Semaphore( os.environ.get("MAX_ARCHIVE_THREADS", 2) )

  with open(hosting_creds_path) as jsonData:
    file_extension = hosting_creds_path.split(".")[-1].lower()
    if file_extension == "json":
      website_backend_credentials = json.loads(jsonData.read())
    elif file_extension in ("yaml", "yml"):
      website_backend_credentials = yaml.safe_load(jsonData.read())
  batch_backup(website_backend_credentials, backup_folder)
