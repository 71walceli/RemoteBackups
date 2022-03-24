import ftplib
import json
from functools import reduce
import os, sys, os.path, shutil, operator
import utils


LOCAL_FTP_DIRECTORY = "./ftp"


class FtpClient(ftplib.FTP):
  def __init__(self, host='', user='', passwd='', acct='', timeout=None, source_address=None, 
    *, encoding='utf-8'
  ) -> None:
    super().__init__(host, user, passwd, acct, timeout, source_address, encoding=encoding)
    self.host = host
    self.user = user
    self.password = passwd
    self.path = self.pwd()

  def cwd(self, dirname: str) -> str:
    return super().cwd(dirname)

  def reconnect(self):
    previous_path = self.path
    self.sendcmd("QUIT")
    self.connect()
    self.login(self.user, self.password)
    self.cwd(previous_path)

# Adapted from https://stackoverflow.com/a/55127679
def cloneFtp(ftp_connection, remote_dir, local_dir):
  # TODO Add counters to count identified files vs downloaded ones count.
  try: 
    shutil.rmtree(local_dir)
    pass
  except: pass
  try: 
    os.makedirs(local_dir)
    pass
  except: pass
  files, dirs = utils.list_files_folders(ftp_connection, remote_dir)
  for file in files:
    local_file_path = os.path.join(local_dir, file)
    remote_file_path = f"{remote_dir}/{file}"
    while True: 
      try:
        ftp_connection.retrbinary('RETR '+ remote_file_path, open(local_file_path, 'wb').write)
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
    return cloneFtp(ftp_connection, remote_path, local_path)
  res = map(recurse, dirs)
  return reduce(operator.iand, res, True)


def download_database(ftp_credentials, local_dir):
  pass


def download_site_files(ftp_credentials, local_dir):
  with FtpClient(ftp_credentials["host"], ftp_credentials["user"], ftp_credentials["password"]) \
  as ftp:
    ftp.cwd(ftp_credentials["directory"])
    print(f"Downloading directory for {domain} into {local_dir}")
    cloneFtp(ftp, ftp_credentials["directory"], local_dir)


with open("./websitesData.json") as jsonData:
  website_backend_credentials = json.loads(jsonData.read())

if __name__ == "__main__":
  #print(websiteBackendCredentials)
  #print(type(websiteBackendCredentials))
  for domain, credentials in website_backend_credentials.items():
    ftp_credentials = credentials["ftpCredentials"]
    mysql_credentials = credentials["mysqlDbCredentials"]
    #download_database(mysql_credentials, os.path.join(LOCAL_FTP_DIRECTORY, domain))
    download_site_files(ftp_credentials, os.path.join(LOCAL_FTP_DIRECTORY, domain))


