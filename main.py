import ftplib
import json
from functools import reduce
import os, os.path, shutil, operator
import sys
from threading import Thread
from datetime import datetime


NOMBRE_CARPETA_ARCHIVOS = "Archivos"
NOMBRE_ARCHIVO_BASE_DATOS = "BaseDatos.sql"


class FtpClient(ftplib.FTP):
  """Just a wrapper for `ftplib.FTP` in order to add functionality"""
  def __init__(self, host='', user='', passwd='', acct='', timeout=None, source_address=None, 
    *, encoding='utf-8'
  ) -> None:
    super().__init__(host, user, passwd, acct, timeout, source_address, encoding=encoding)
    self.host = host
    self.user = user
    self.password = passwd
    self.path = self.pwd()

  def cwd(self, dirname: str) -> str:
    self.path = dirname
    return super().cwd(dirname)

  def reconnect(self):
    previous_path = self.path
    self.sendcmd("QUIT")
    self.connect()
    self.login(self.user, self.password)
    self.cwd(previous_path)

  def list_files_folders(self, directory: str) -> tuple[list, list]:
    """List all files and folders in specified directory."""
    files, dirs = [], []
    def classify(listing: str):
      """Clasifies each of the directory entries either as file or as folder."""
      listing = listing.split(maxsplit=8)
      listing_type = listing[0][0]
      node = listing[8]
      if node in [".", ".."]:
        return
      if listing_type == "d":
        dirs.append(node)
      else:
        files.append(node)
    self.dir(directory, classify)
    return files, dirs

  # Adapted from https://stackoverflow.com/a/55127679
  def cloneFolder(self, remote_dir, local_dir):
    # TODO Add counters to count identified files vs downloaded ones count.
    try: 
      shutil.rmtree(local_dir)
      pass
    except: pass
    try: 
      os.makedirs(local_dir)
      pass
    except: pass
    files, dirs = self.list_files_folders(remote_dir)
    for file in files:
      local_file_path = os.path.join(local_dir, file)
      remote_file_path = f"{remote_dir}/{file}"
      while True: 
        try:
          self.retrbinary('RETR '+ remote_file_path, open(local_file_path, 'wb').write)
          break
        except TimeoutError as exception:
          continue # Might happen on slow or unreliable connections
        except KeyboardInterrupt as exception:
          raise # Respond to CTRK+C here, given that caller won't be able to respond
        except BaseException as exception:
          print(f"Error [{exception}]: {remote_file_path}")
          continue
    #ftp_connection.reconnect()
    def recurse(dir): 
      #try:
      #  ftp.cwd(remote_path)
      #except: 
      #  print(f"[CWD Error] {remote_path}")
      remote_path = f"{remote_dir}/{dir}"
      local_path = os.path.join(local_dir, dir)
      return self.cloneFolder(remote_path, local_path)
    res = map(recurse, dirs)
    return reduce(operator.iand, res, True)


def download_database(db_credentials, local_file):
  db_user = db_credentials["user"]
  db_host = db_credentials["host"]
  db_password = db_credentials["password"]
  db_name = db_credentials["dbName"]
  print("Base de Datos: Iniciando")
  os.system(f"mysqldump --user={db_user} --host={db_host} --password={db_password} --databases {db_name} "
    +f"--column-statistics=0 > {local_file}")
  print("Base de Datos: Terminando")


def download_files(ftp_credentials, local_dir):
  print("Descarga Archivos: Iniciando")
  with FtpClient(ftp_credentials["host"], ftp_credentials["user"], ftp_credentials["password"]) \
  as ftp:
    ftp.cwd(ftp_credentials["directory"])
    ftp.cloneFolder(ftp_credentials["directory"], local_dir)
  print("Descarga Archivos: Terminando")

def archivar(directorio, archivo_zip):
  print("Archivado: Iniciando")
  resultado_comando = os.system(f"7z a -mx=9 {archivo_zip} {directorio}")
  if resultado_comando == 0:
    shutil.rmtree(directorio)
  print("Archivado: Terminando")


if __name__ == "__main__":
  carpeta_copias_seguridad = sys.argv[1] if sys.argv[1] else "./.backups"
  credenciales_hosting = sys.argv[2] if sys.argv[2] else "./websitesData.json"

  with open(credenciales_hosting) as jsonData:
    website_backend_credentials = json.loads(jsonData.read())
  for dominio, credentials in website_backend_credentials.items():
    hilo_archivos = Thread(
      target=lambda: download_files(credentials["ftpCredentials"], 
        os.path.join(carpeta_copias_seguridad, dominio, NOMBRE_CARPETA_ARCHIVOS)
      ))
    hilo_base_datos = Thread(
      target=lambda: download_database(credentials["dbCredentials"], 
        os.path.join(carpeta_copias_seguridad, dominio, NOMBRE_ARCHIVO_BASE_DATOS)
      ))
    
    print(f"DOMINIO {dominio}")
    while True:
      try:
        os.makedirs(os.path.join(carpeta_copias_seguridad, dominio))
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
    archivar(
      os.path.join(carpeta_copias_seguridad, dominio),
      os.path.join(carpeta_copias_seguridad, f"{fecha_hora} {dominio}.zip"),
    )
    print(f"DOMINIO {dominio} copiado correctamente")

