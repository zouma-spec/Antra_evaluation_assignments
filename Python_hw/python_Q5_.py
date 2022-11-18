from azure.storage.blob import BlobServiceClient

connection_string = '<my_connection_string>'
container_name = '<my_container_name>'

# Names, types and sizes of blobs in a certain container
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
blob_list = blob_service_client.get_container_client(container_name).list_blobs()
for blob in blob_list:
    print('{:<30}{:<15}{:<10}'.format(blob.name, blob.blob_type, blob.size), end='\n')
    # print(blob)

# Names and sizes of “folders” in a certain container
container_client = blob_service_client.get_container_client(container_name)
for file in container_client.walk_blobs():
    print(file.name) # haven't figured out how to do sizes
