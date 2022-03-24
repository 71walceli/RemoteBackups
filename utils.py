import ftplib
import json


def list_files_folders(ftp: ftplib.FTP, directory: str) -> tuple[list, list]:
  """List all files and folders in specified directory."""
  files, dirs = [], []
  def classify(listing: str):
    listing = listing.split(maxsplit=8)
    listing_type = listing[0][0]
    node = listing[8]
    if node in [".", ".."]:
      return
    if listing_type == "d":
      dirs.append(node)
    else:
      files.append(node)
  ftp.dir(directory, classify)
  return files, dirs


if __name__ == "__main__":
  with open("./websitesData.json") as jsonData:
    website_backend_credentials = json.loads(jsonData.read())

  for domain, credentials in website_backend_credentials.items():
    ftp_credentials = credentials["ftpCredentials"]
    with ftplib.FTP(ftp_credentials["host"], ftp_credentials["user"], 
      ftp_credentials["password"]
    ) as ftp:
      #ftp.cwd(ftp_credentials["directory"])
      #local_dir = os.path.join(LOCAL_FTP_DIRECTORY, domain)
      directory = "/public_html"
      files, folders = list_files_folders(ftp, directory)
      print(f"Files: {files}")
      print(f"Folders: {folders}")



