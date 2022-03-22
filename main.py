import ftplib
from functools import reduce
import os, sys, os.path, shutil, operator
import json


LOCAL_FTP_DIRECTORY = "./ftp"

# Adapted from https://stackoverflow.com/a/55127679
def cloneFTP(ftp_connection, remote_dir, local_dir):
  try: 
    shutil.rmtree(local_dir)
    pass
  except: pass
  try: 
    os.makedirs(local_dir)
    pass
  except: pass
  dirs = []
  for filename in ftp.nlst():
    if filename in [".", ".."]:
      continue
    local_file_path = os.path.join(local_dir, filename)
    #print(f"  {local_file_path}")
    try:
      ftp.size(filename)
      ftp.retrbinary('RETR '+ filename, open(local_file_path, 'wb').write)
    except ftplib.error_perm as exception:
      #print(f"{filename} - {exception}")
      dirs.append(filename)
  def recurse(dir): 
    remote_path = f"{remote_dir}/{dir}"
    ftp.cwd(remote_path)
    #print(f"Recursing into {remote_path}:")
    local_path = os.path.join(local_dir, dir)
    return cloneFTP(ftp_connection, remote_path, local_path)
  res = map(recurse, dirs)
  return reduce(operator.iand, res, True)


with open("./websitesData.json") as jsonData:
  website_backend_credentials = json.loads(jsonData.read())

if __name__ == "__main__":
  #print(websiteBackendCredentials)
  #print(type(websiteBackendCredentials))
  for domain, credentials in website_backend_credentials.items():
    ftp_credentials = credentials["ftpCredentials"]
    with ftplib.FTP(ftp_credentials["host"], ftp_credentials["user"], 
      ftp_credentials["password"]
    ) as ftp:
      ftp.cwd(ftp_credentials["directory"])
      local_directory = os.path.join(LOCAL_FTP_DIRECTORY, domain)
      print(f"Downloading directory for {domain} into {local_directory}")
      cloneFTP(ftp, ftp_credentials["directory"], local_directory)


