import json
import yaml
import os, os.path, shutil
import subprocess
from threading import Thread
from datetime import datetime
from argparse import ArgumentParser
import paramiko


from FtpClient import FtpClient


NOMBRE_CARPETA_ARCHIVOS = "Archivos"
NOMBRE_ARCHIVO_BASE_DATOS = "BaseDatos.sql"


def download_database(db_credentials, local_file):
  print("Base de Datos: Iniciando")
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
  print("Base de Datos: Terminando")

def download_files(credentials, local_dir):
  print("Descarga Archivos: Iniciando")
  
  # TODO Move SSH logic to other module file
  if credentials["connectionType"] == "ssh":
    backup_file = f"{datetime.now().isoformat().replace(':', '-')}.tar"

    ssh_conn = paramiko.SSHClient() 
    ssh_conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_conn.connect(
      hostname=credentials["host"], 
      username=credentials["user"],
      password=credentials["password"],
    )
    remote_file = f"{credentials['directory']}/{backup_file}"
    
    command = f"tar c $( find {credentials['directory']} -mindepth 1 -maxdepth 1 )"
    result = ssh_conn.exec_command(command)
    print(command)
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
    
    command = f"find {credentials['directory']} -type f -print0 | xargs -0 md5sum"
    result = ssh_conn.exec_command(command)
    print(command)
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
  elif credentials["connectionType"] == "ftp":
    with FtpClient(credentials["host"], credentials["user"], credentials["password"]) \
    as ftp:
      ftp.cwd(credentials["directory"])
      ftp.cloneFolder(credentials["directory"], local_dir)

  print("Descarga Archivos: Terminando")

def archivar(directorio, archivo_zip):
  print("Archivado: Iniciando")
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
  print("Archivado: Terminando")

def parse_arguments():
  parser = ArgumentParser(
    description="Backup script for remote to local backups, including remote databases"
  )
  parser.add_argument("--backup_folder", default="./.backups", help="Backup folder path")
  parser.add_argument("--hosting_creds", default="./websitesData.yaml", 
    help="Hosting credentials file path"
  )
  return parser.parse_args()

def delete_local_folder(backup_folder_domain):
  try:
    shutil.rmtree(backup_folder_domain)
  except FileNotFoundError:
    pass

if __name__ == "__main__":
  args = parse_arguments()
  backup_folder = args.backup_folder
  hosting_creds_path = args.hosting_creds

  # TODO Load YAML as well
  with open(hosting_creds_path) as jsonData:
    file_extension = hosting_creds_path.split(".")[-1].lower()
    if file_extension == "json":
      website_backend_credentials = json.loads(jsonData.read())
    elif file_extension in ("yaml", "yml"):
      website_backend_credentials = yaml.safe_load(jsonData.read())
  # TODO For every spec, create separate thread.
  for dominio, credentials in website_backend_credentials.items():
    fecha_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_folder_domain = os.path.join(backup_folder, f"{fecha_hora} {dominio}")
    delete_local_folder(backup_folder_domain)
    hilo_archivos = Thread(
      target=lambda: download_files(credentials["credentials"], 
        backup_folder_domain
      ))
    hilo_base_datos = Thread(
      target=lambda: download_database(credentials["dbCredentials"], 
        os.path.join(backup_folder_domain, f"{credentials['dbCredentials']['dbName']}.sql")
      ))
    
    print(f"DOMINIO {dominio}")
    while True:
      try:
        os.makedirs(backup_folder_domain)
        break
      except FileExistsError:
        break
      except:
        raise

    hilo_archivos.start()
    if "dbCredentials" in credentials:
      hilo_base_datos.start()

    hilo_archivos.join()
    if "dbCredentials" in credentials:
      hilo_base_datos.join()
    
    hilo_archivado = Thread(
      target=lambda: archivar(
        backup_folder_domain,
        os.path.join(backup_folder, f"{fecha_hora} {dominio}.7z"),
      )
    )
    hilo_archivado.start()
    hilo_archivado.join()
    
    print(f"DOMINIO {dominio} copiado correctamente")

