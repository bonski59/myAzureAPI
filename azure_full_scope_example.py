import os, ntpath
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from azure.storage.blob.blockblobservice import BlockBlobService
from configparser import ConfigParser
import pandas as pd
from io import StringIO


# make config file variables
config_file = r".\config.ini"
config = ConfigParser()
config.read(config_file)

# initialize temp folders
if __name__ == "__main__":
    download = r".\download"
    upload = r".\upload"

    def mk_dir(dir):
        if not os.path.isdir(dir):
            os.mkdir(dir)
        return

    mk_dir(download)
    mk_dir(upload)

# build class for dev tools
class azureDevTools:

    # pull credentials from config file
    connect_str = config['azure']['connect_str']
    container_name = config['azure']['container']

    # Create the BlobServiceClient object which will be used to create a container client
    block_blob_service = BlockBlobService(connection_string=connect_str)
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # get existing container
    try:
        container_client = blob_service_client.get_container_client(container_name)
    except Exception as ex:
        print('Exception: container client issue')
        print(ex)

    @classmethod
    def dev_mk_txt_file(cls, filename):  # add file to dev-testing container on Azure
        try:
            filename = filename + ".txt"
            # Create a local directory to hold blob data
            uploads_path = "{}/upload".format(os.getcwd())
            if not os.path.exists(uploads_path):
                os.mkdir(uploads_path)

            # Create a file in the local data directory to upload and download

            upload_file_path = os.path.join(uploads_path, filename)

            # Write text to the file
            file = open(upload_file_path, 'w')
            file.write("Hello, World!")
            file.close()

            # Create a blob client using the local file name as the name for the blob

            blob_client = cls.blob_service_client.get_blob_client(container=cls.container_name, blob=filename)

            print("\nUploading to Azure Storage as blob:\n\t" + filename)

            # Upload the created file
            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data)

            os.remove(upload_file_path)

        except Exception as xx:
            print('Exception: dev_mk_txt_file()')
            print(xx)

        return

    @classmethod
    def upload_file(cls, filepath):
        # confirm fp exist
        # if exist send
        head, tail = ntpath.split(filepath)
        if not os.path.exists(filepath):
            print("filepath does not exist: {}".format(tail))
            return
        else:
            print("\n Uploading File: {}".format(tail))
            try:
                blob_client = cls.blob_service_client.get_blob_client(container=cls.container_name, blob=tail)
                blob_client.upload_blob(filepath)
                print("\t Upload Successful: {}".format(tail))
            except Exception as xx:
                print('Upload Aborted for the Following')
                print('\t Exception: upload_file()')
                print("\t " + str(xx))

        return

    @classmethod
    def list_blobs(cls):
        print("\nListing blobs...")
        blob_arr = []
        # List the blobs in the container
        blob_list = cls.container_client.list_blobs()
        print("Container: {}".format(cls.container_name))
        for blob in blob_list:
            blob_arr.append(blob.name)
            print("\t" + blob.name)
        return blob_arr

    @classmethod
    def delete_container(cls):
            cls.container_client.delete_container()

    @classmethod
    def delete_blobs(cls, blob_names):
        if not type(blob_names) == list:
            blob_names = [blob_names]
        for i in blob_names:
            cls.container_client.delete_blobs(i)

    @classmethod
    def dev_download(cls, cloud_filename):
        if not type(cloud_filename) == list:
            cloud_filename = [cloud_filename]
        downloads_path = r"{}\download".format(os.getcwd())

        print("\nDownloading blob to \n\t" + downloads_path)
        try:
            for i in cloud_filename:
                download_file_path = r"{}\{}".format(downloads_path, i)
                blob_client = cls.blob_service_client.get_blob_client(container=cls.container_name, blob=i)
                with open(download_file_path, "wb") as download_file:
                    download_file.write(blob_client.download_blob().readall())
                print("\t Download Successful: {}".format(i))

        except Exception as xx:
            print('Download Aborted for the Following')
            print('\t Exception: dev_download()')
            print("\t " + str(xx))

    # TODO: build read functions for xl, csv, and txt

    @classmethod
    def read_txt(cls, cloud_txt):

        # read the content of the blob(assume it's a .txt file)
        str1 = cls.block_blob_service.get_blob_to_text(cls.container_name, cloud_txt)
        # split the string str1 with newline.
        arr1 = str1.content.splitlines()
        # read the one line each time.
        for a1 in arr1:
            print(a1)

    @classmethod
    def read_csv(cls, cloud_csv):
        blob_string = cls.block_blob_service.get_blob_to_text(cls.container_name, cloud_csv).content
        df = pd.read_csv(StringIO(blob_string))
        return df


