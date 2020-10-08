import requests, os

NAMENODE = "http://0.0.0.0:8080"


def show_help(*_):
    print("""COMMANDS USAGE\n
        NOTE: paths are resolved only with respect to current directory\n
        ---------------------------------------------------------------\n
        init                : initialize the distributed filesystem\n
        touch <file>        : create an empty file\n
        get <file>          : download file from the dfs\n
        put <file> <dir>    : upload a file from the host to the dfs\n
        rm <file>           : delete a file from the dfs\n
        cp <file> <dir>     : copy a file to the other directory\n
        mkdir <dir>         : initialize an empty directory\n
        ls                  : list the contents of the current directory\n
        cd <dir>            : change directory\n
        info <file>         : display information about the file\n
        mv <file> <dir>     : move the file in the dfs to another location\n
        exit                : exit the DFS client
        """)


def mistake():
    print("cannot execute command, please verify that you are using it correctly")


def init(*_):
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


def create_file(*arguments):
    '''
    Create an empty file in DFS
    :param filename: name of the file to create
    '''

    if len(arguments) == 2:
        filename = arguments[1]

        # obtaining the base name
        #filename = os.path.basename(filename)

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
    else:
        mistake()


def get_file(*arguments):
    '''
    Downloads file from dfs to client's local host
    :param filename: name of the file in dfs
    '''

    if len(arguments) == 2:
        filename = arguments[1]
        # obtaining the base name
        #filename = os.path.basename(filename)

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
    else:
        mistake()


def put_file(*arguments):
    '''
    Put local file to DFS
    :param local_filename: name of the local file
    :param dfs_filename: name of the file in dfs
    '''

    if len(arguments) == 3:
        local_filename = arguments[1]
        dfs_filename = arguments[2]

        try:
            # obtaining the base names
            #dfs_filename = os.path.basename(dfs_filename)

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
        except Exception as e:
            print(f"file {local_filename} does not exist")
    else:
        mistake()


def delete_file(*arguments):
    '''
    Deletes file from dfs

    :param filename: name of the file
    '''
    if len(arguments) == 2:
        filename = arguments[1]
        print(f"starting deleting file {filename}")
        # obtaining the base names
        #filename = os.path.basename(filename)

        # request namenode to delete file
        response = requests.delete(NAMENODE + "/delete", json={"filename": filename})

        if response.status_code // 100 == 2:
            print("file was found in namenode")

            # acquiring info about file
            file = response.json()['file']

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


def copy_file(*arguments):
    '''
    Copies the file from the current directory to somewhere else
    Name resolution: new filename is filename_copy<num of copy>
    :param filename: file in the current directory to copy
    :param dirname: path where to put the file
    '''
    if len(arguments) == 3:
        filename = arguments[1]
        dirname = arguments[2]

    else:
        mistake()


def make_directory(*arguments):
    '''
    Create an empty directory
    :param dirname: name for directory to create
    '''
    if len(arguments) == 2:
        dirname = arguments[1]
        # request namenode to create directory
        response = requests.post(NAMENODE + '/mkdir', json={'dirname': dirname})
        # check response
        if response.status_code // 100 == 2:
            print(f"directory {dirname} successfully created")
        else:
            print(f"directory {dirname} already exists")
    else:
        mistake()


def read_directory(*_):
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


def change_directory(*arguments):
    '''
    Change current working directory
    :param dirname: the directory that we want to make current working
    '''
    if len(arguments) == 2:
        dirname = arguments[1]
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
    else:
        mistake()


def file_info(*arguments):
    '''
    Displays file size, creation date and time, datanodes on which the file resides
    :param filename: name of file to display info about
    '''
    if len(arguments) == 2:
        filename = arguments[1]
        response = requests.post(NAMENODE + '/info', json={'filename': filename})
        # check response
        if response.status_code // 100 == 2:
            file = response.json()['info']
            print(f"FILE {filename}\nSIZE {file['size'] / 2**10} KB\nCREATED {file['created_date']}\nDATANODES {file['datanodes']}")
        else:
            print(f"file {filename} does not exist")
    else:
        mistake()


def move_file(*arguments):
    '''
    Moves file to a new location
    :param filename: file name in the current dir
    :param path: destination path
    '''
    if len(arguments) == 3:
        filename = arguments[1]
        path = arguments[2]
        if path[-1] == '/' and len(path) != 1:
            path = path[:-1]
        response = requests.post(NAMENODE + '/move', json={'filename': filename, 'path': path})
        # check response
        if response.status_code // 100 == 2:
            print(f"file {filename} was successfully moved to {path}")
        elif response.status_code == 418:
            print("you cannot move file into file")
        elif response.status_code == 419:
            print(f"file {filename} already exists in {path}")
        else:
            print(f"file {filename} cannot be moved to {path}")
    else:
        mistake()

# command to functions mapping
commands = {
    "help": show_help,
    "init": init,
    "touch": create_file,
    "get": get_file,
    "put": put_file,
    "rm": delete_file,
    "mkdir": make_directory,
    "ls": read_directory,
    "cd": change_directory,
    "info": file_info,
    "mv": move_file,
    "cp": copy_file
}

# init()
# create_file("zhopa_1")
# put_file('test2.txt', 'test.txt')
# make_directory("dir1")
# make_directory("dir2")
# change_directory('dir2')
# create_file('file4')
# read_directory()
# move_file('file4', '/dir1')
# read_directory()
# change_directory('..')
# change_directory("dir1")
# read_directory()
# file_info('file4')
# read_directory()
# delete_file('file4')
# change_directory("..")
# move_file("test.txt", "dir2")
# change_directory("dir2")
# get_file("test.txt")
# delete_file("test.txt")
# read_directory()

if __name__ == "__main__":
    print("TatDFS client successfully started\nHere is a small help on how to use it")
    show_help()
    while True:
        args = input("TatDFS𓆏 ").split()
        if len(args) == 0:
            continue
        command = args[0]
        if command == "exit":
            break
        try:
            commands[args[0]](*args)
        except Exception as e:
            print("cannot execute command, please verify that you are using it correctly")