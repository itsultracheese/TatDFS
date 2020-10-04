import requests, os

NAMENODE = "http://0.0.0.0:8080"


def init():
    '''
    Initialize empty DFS
    '''
    # ping name node
    response = requests.get(NAMENODE + "/ping")
    # check response
    if response.status_code // 100 == 2:
        print("connection to namenode was established")

        # request namenode to initialize the DFS
        response = requests.get(NAMENODE + "/init")
        # check response
        if response.status_code // 100 == 2:
            data = response.json()
            print(f"filesystem initialized")
            print(f"available space: {data['free_space'] // (2**20)} MB")
        else:
            print("an error has occurred while initializing filesystem :(")
    else:
        print("an error has occurred while connecting to namenode :(")


def create_file(filename):
    '''
    Create an empty file in DFS
    :param filename: name of the file to create
    '''

    # obtaining the base name
    filename = os.path.basename(filename)

    # request namenode to create file
    response = requests.post(NAMENODE + "/create", json={"filename": filename, 'filesize': 0})

    print(response)
    if response.status_code // 100 == 2:
        # receive file info from namenode
        file = response.json()["file"]
        print(file)
        # request each datanode to create a file
        for datanode in file['datanodes']:
            resp = requests.post(datanode + "/create", json={"file_id": file['id']})
            if resp.status_code // 100 != 2:
                print(f"failed to create file in {datanode}")

        print(f"FILE {filename} was created")
    else:
        print(f"FILE {filename} already exists :(")

def put_file(local_filename, dfs_filename):
    '''
    Put local file to DFS
    :param local_filename: name of the local file
    :param dfs_filename: name of the file in dfs
    '''

    # obtaining the base names
    # local_filename = os.path.basename(local_filename)
    dfs_filename = os.path.basename(dfs_filename)

    # obtaining file size in bytes
    filesize = os.stat(local_filename).st_size

    # request namenode to put file
    response = requests.post(NAMENODE + "/create", json={"filename": dfs_filename, "filesize": filesize})

    if response.status_code // 100 == 2:
        # receive file info from namenode
        file = response.json()["file"]
        print(file)
        # request each datanode to create a file
        for datanode in file['datanodes']:

            resp = requests.post(datanode + "/put",
                                 files={f"{file['id']}": open(local_filename, 'rb')})
            if resp.status_code // 100 != 2:
                print(f"failed to upload file to {datanode}")

        print(f"FILE {local_filename} was moved as {dfs_filename}")
    elif response.status_code == 409:
        print(f"FILE {dfs_filename} already exists :(")
    else:
        print(f"out of memory")

#init()
#create_file("zhopa_1")
put_file('test.txt', 'test.txt')