import json
import os, os.path, shutil
import subprocess
from threading import Thread
from datetime import datetime
from argparse import ArgumentParser
from paramiko import SSHClient

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
        #stderr=subprocess.PIPE,  # Redirect stderr if needed
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
    ssh_conn = SSHClient()
    ssh_conn.load_system_host_keys()
    ssh_conn.connect(
      hostname=credentials["host"], 
      username=credentials["user"],
      password=credentials["password"],
      auth_timeout=10,
    )
    #ssh_conn.exec_command(f"cd {credentials['directory']}; pwd")
    #_,stdout,_ = ssh_conn.exec_command(f"cd {credentials['directory']}; pwd")
    #_,stdout,_ = ssh_conn.exec_command(f"cd {credentials['directory']}; pwd")
    #print(stdout.readline())
    #ssh_conn.exec_command(f"tar cvf {backup_file} .")
    #_,stdout,_ = ssh_conn.exec_command(f"cat {backup_file} .")
    remote_file = f"{credentials['directory']}/{backup_file}"
    remote_fifo = f"{credentials['directory']}/{backup_file}.fifo"
    
    ssh_conn.exec_command(f"mkfifo {remote_fifo}")
    ssh_conn.exec_command(f"tar cvf - {credentials['directory']}/* {credentials['directory']}/.* > {remote_file}")
    #ssh_conn.exec_command(f"tar cvf - $(ls {credentials['directory']}) > {remote_file}")
    ssh_conn.exec_command(f"cat {remote_file} > {remote_fifo}")
    _,stdout,_ = ssh_conn.exec_command(f"cat {remote_fifo}")
    with stdout:
      with open(f"{local_dir}/{backup_file}", "wb") as download:
        chunk_size = 4096  # Adjust the chunk size as needed
        while True:
          chunk = stdout.read(chunk_size)
          if not chunk:
            break  # Break the loop when there's no more data
          download.write(chunk)
    ssh_conn.exec_command(f"rm {remote_file}")
    ssh_conn.exec_command(f"rm {remote_fifo}")
    ssh_conn.close()
  elif credentials["connectionType"] == "ftp":
    with FtpClient(credentials["host"], credentials["user"], credentials["password"]) \
    as ftp:
      ftp.cwd(credentials["directory"])
      ftp.cloneFolder(credentials["directory"], local_dir)

  print("Descarga Archivos: Terminando")

def archivar(directorio, archivo_zip):
  print("Archivado: Iniciando")
  resultado_comando = os.system(f"7z a -mx=9 {archivo_zip} {directorio}")
  if resultado_comando == 0:
    shutil.rmtree(directorio)
  print("Archivado: Terminando")

def parse_arguments():
  parser = ArgumentParser(
    description="Backup script for remote to local backups, including remote databases"
  )
  parser.add_argument("--backup_folder", default="./.backups", help="Backup folder path")
  parser.add_argument("--hosting_creds", default="./websitesData.json", help="Hosting credentials file path")
  return parser.parse_args()


def delete_local_folder(backup_folder_domain):
    try:
      shutil.rmtree(backup_folder_domain)
    except FileNotFoundError:
      pass

if __name__ == "__main__":
  args = parse_arguments()
  backup_folder = args.backup_folder
  hosting_creds = args.hosting_creds

  # TODO Load YAML as well
  with open(hosting_creds) as jsonData:
    website_backend_credentials = json.loads(jsonData.read())
  # TODO For every spec, create separate thread.
  for dominio, credentials in website_backend_credentials.items():
    backup_folder_domain = os.path.join(backup_folder, dominio)
    delete_local_folder(backup_folder_domain)
    hilo_archivos = Thread(
      target=lambda: download_files(credentials["credentials"], 
        backup_folder_domain
      ))
    hilo_base_datos = Thread(
      target=lambda: download_database(credentials["dbCredentials"], 
        os.path.join(backup_folder_domain, NOMBRE_ARCHIVO_BASE_DATOS)
      ))
    
    print(f"DOMINIO {dominio}")
    while True:
      try:
        os.makedirs(os.path.join(backup_folder, dominio))
        break
      except FileExistsError:
        break
      except:
        raise

    hilo_archivos.start()
    hilo_base_datos.start()

    hilo_archivos.join()
    hilo_base_datos.join()
    
    fecha_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
    hilo_archivado = Thread(
      target=lambda: archivar(
        os.path.join(backup_folder, dominio),
        os.path.join(backup_folder, f"{fecha_hora} {dominio}.zip"),
      )
    )
    hilo_archivado.start()
    hilo_archivado.join()
    
    delete_local_folder(backup_folder_domain)
    print(f"DOMINIO {dominio} copiado correctamente")

