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

    if response.status_code // 100 == 2:
        # receive file info from namenode
        file = response.json()["file"]
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
            print(f"requesting the file")
            response = requests.get(datanode + "/get", json={"file_id": file['id']})
            if response.status_code // 100 == 2:
                print(f"file was acquired")
                received = True
                open(filename, 'wb').write(response.content)
                break
            else:
                print(f"couldn't acquire the file from {datanode}")

        if received:
            print(f"the file {filename} was received")
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

def delete_file(filename):
    '''
    Deletes file from dfs

    :param filename: name of the file
    '''
    print(f"starting deleting file {filename}")
    # obtaining the base names
    filename = os.path.basename(filename)

    # request namenode to delete file
    response = requests.delete(NAMENODE + "/delete", json={"filename": filename})

    if response.status_code // 100 == 2:
        print("file was found in namenode")

        # acquiring info about file
        file = response.json()['file']

        print(f"file = {file}")

        # removing file from datanodes
        for datanode in file['datanodes']:
            print(f"sending delete request to datanode {datanode}")
            response = requests.delete(datanode + "/delete", json={"file_id": file['id']})

            if response.status_code // 100 == 2:
                print(f"file was deleted from datanode {datanode}")
            else:
                print(f"file couldn't be deleted from datanode {datanode}")

        print("file was deleted from dfs")
    else:
        print(f"FILE {filename} doesn't exist")


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
    elif response.status_code == 418:
        print("you cannot change directory to a file")
    else:
        print(f"failed to change directory to {dirname}")


def file_info(filename):
    response = requests.post(NAMENODE + '/info', json={'filename': filename})
    # check response
    if response.status_code // 100 == 2:
        file = response.json()['info']
        print(f"FILE {filename}\nSIZE {file['size'] / 2**10} KB\nCREATED {file['created_date']}\nDATANODES {file['datanodes']}")
    else:
        print(f"file {filename} does not exist")


def move_file(filename, path):

    if path[-1] == '/' and len(path) != 1:
        path = path[:-1]
    response = requests.post(NAMENODE + '/move', json={'filename': filename, 'path': path})
    # check response
    if response.status_code // 100 == 2:
        print(f"file {filename} was successfully moved to {path}")
    elif response.status_code == 418:
        print("you cannot move file into file")
    else:
        print(f"file {filename} cannot be moved to {path}")


init()
create_file("zhopa_1")
put_file('test2.txt', 'test.txt')
make_directory("dir1")
make_directory("dir2")
change_directory('dir2')
create_file('file4')
read_directory()
move_file('file4', '/dir1')
read_directory()
change_directory('..')
change_directory("dir1")
read_directory()
file_info('file4')
read_directory()
delete_file('file4')
change_directory("..")
move_file("test.txt", "dir2")
change_directory("dir2")
get_file("test.txt")
delete_file("test.txt")
read_directory()