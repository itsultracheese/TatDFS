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

def get_file(filename):
    '''
    Downloads file from dfs to client's local host
    :param filename: name of the file in dfs
    '''

    # obtaining the base name
    filename = os.path.basename(filename)

    response = requests.get(NAMENODE + "/get", json={"filename": filename})

    if response.status_code // 100 == 2:
        # acquiring the file from the namenode
        file = response.json()['file']
        datanodes = file['datanodes']
        received = False
        for datanode in datanodes:
            print(f"requesting the file from datanode {datanode}")
            response = requests.get(datanode + "/get", json={"file_id": file['id']})
            if response.status_code // 100 == 2:
                print(f"file was acquired from {datanode}")
                received = True
                open(filename, 'wb').write(response.content)
                break
            else:
                print(f"couldn't acquire the file from {datanode}")

        if received:
            print("yay, the file was received")
        else:
            print("file wasn't received")

    else:
        print(f"FILE {filename} doesn't exist in current dir")


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


def make_directory(dirname):
    '''
    Create an empty directory
    :param dirname: name for directory to create
    '''
    # request namenode to create directory
    response = requests.post(NAMENODE + '/mkdir', json={'dirname': dirname})
    # check response
    if response.status_code // 100 == 2:
        print(f"directory {dirname} successfully created")
    else:
        print(f"directory {dirname} already exists")


def read_directory():
    '''
    Display the contents of the current directory
    '''
    # request namenode for the file list
    response = requests.get(NAMENODE + '/ls')
    # check response
    if response.status_code // 100 == 2:
        # print contents
        dirs = response.json()['dirs']
        files = response.json()['files']
        print('---------------DIRECTORIES---------------')
        for dir in dirs:
            print(dir)
        print('---------------FILES---------------')
        for file in files:
            print(file)
    else:
        print("failed to list contents of directory")


def change_directory(dirname):
    response = requests.post(NAMENODE + '/cd', json={'dirname': dirname})
    # check response
    if response.status_code // 100 == 2:
        # obtain the new working directory
        new_dirname = response.json()['dirname']
        print(f"current directory is {new_dirname}")


#init()
#create_file("zhopa_1")
#put_file('test.txt', 'test.txt')
# make_directory("dir1")
# make_directory("dir2")
# make_directory("dir3")
# make_directory("dir4")
# create_file('file1')
# create_file('file2')
# put_file('some_file.txt', 'file3.txt')
# read_directory()
# change_directory('dir3')
# create_file('file4')
# put_file('some_file.txt', 'file5')
# read_directory()
# change_directory('..')