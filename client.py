import requests, os

NAMENODE = "http://0.0.0.0:8080"
CURRENT_DIR = "/"


def mistake():
    print("cannot execute command, please verify that you are using it correctly")


def show_help(*arguments):
    if len(arguments) == 1:
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
            ls [dir]            : list the contents of the directory (or current directory if no arguments)\n
            cd <dir>            : change directory\n
            info <file>         : display information about the file\n
            mv <file> <dir>     : move the file in the dfs to another location\n
            rmdir <dir>         : delete directory (and its contents)\n
            exit                : exit the DFS client
            """)
    else:
        mistake()


def init(*arguments):
    '''
    Initialize empty DFS
    '''

    if len(arguments) == 1:
        try:
            # ping name node
            response = requests.get(NAMENODE + "/ping")
        except Exception as e:
            print(f"couldn't establish connection to NAMENODE because of\n{e}")
            return

        # check response
        if response.status_code // 100 == 2:
            print("connection to NAMENODE was established")
            try:
                # request namenode to initialize the DFS
                response = requests.get(NAMENODE + "/init")
            except Exception as e:
                print(f"FAILED NAMENODE while initializing filesystem\n{e}")
                return
            # check response
            if response.status_code // 100 == 2:
                data = response.json()
                print(f"filesystem initialized")
                print(f"available space: {data['free_space'] // (2**20)} MB")
            else:
                print(response.content.decode())
        else:
            print("couldn't ping NAMENODE")
    else:
        mistake()


def create_file(*arguments):
    '''
    Create an empty file in DFS
    :param filename: name of the file to create
    '''

    if len(arguments) == 2:
        filename = arguments[1]

        # request NAMENODE to create file
        try:
            response = requests.post(NAMENODE + "/create", json={"filename": filename, 'filesize': 0})
        except Exception as e:
            print(f"FAILED to CREATE FILE because of\n{e}")
            return

        if response.status_code // 100 == 2:
            # receive file info from namenode
            file = response.json()["file"]
            # request each datanode to create a file
            for datanode in file['datanodes']:
                try:
                    # requesting
                    resp = requests.post(datanode + "/create", json={"file_id": file['id']})
                except Exception as e:
                    print(f"FAILED to CREATE file in {datanode} due to\n{e}")
                    continue
                if resp.status_code // 100 != 2:
                    print(response.content.decode())
                else:
                    print(f"FILE {filename} was created in {datanode}")
        else:
            print(response.content.decode())
    else:
        mistake()


def get_file(*arguments):
    '''
    Downloads file from dfs to client's local host
    :param filename: name of the file in dfs
    '''

    if len(arguments) == 2:
        filename = arguments[1]

        try:
            response = requests.get(NAMENODE + "/get", json={"filename": filename})
        except Exception as e:
            print(f"FAILED to GET file from NAMENODE due to\n{e}")
            return

        if response.status_code // 100 == 2:
            # acquiring the file from the namenode
            file = response.json()['file']
            datanodes = file['datanodes']
            received = False
            for datanode in datanodes:
                print(f"requesting the file from DATANODE {datanode}")
                # requesting
                try:
                    response = requests.get(datanode + "/get", json={"file_id": file['id']})
                except Exception as e:
                    print(f"FAILED to GET file from {datanode} due to\n{e}")
                    continue

                if response.status_code // 100 == 2:
                    print(f"file was acquired")
                    received = True
                    filename = os.path.basename(filename)
                    open(filename, 'wb').write(response.content)
                    break
                else:
                    print(response.content.decode())

            if received:
                print(f"the file {filename} was received")
            else:
                print("file wasn't received")

        else:
            print(response.content.decode())
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

        # obtaining file size in bytes
        try:
            filesize = os.stat(local_filename).st_size
        except Exception as e:
            print(f"FAILED to calculate the size due to\n{e}")
            return

        # request namenode to put file
        try:
            response = requests.post(NAMENODE + "/create", json={"filename": dfs_filename, "filesize": filesize})
        except Exception as e:
            print(f"FAILED to put file in NAMENODE due to\n{e}")
            return

        if response.status_code // 100 == 2:
            # receive file info from namenode
            file = response.json()["file"]
            # request each datanode to create a file
            for datanode in file['datanodes']:
                # requesting
                try:
                    resp = requests.post(datanode + "/put",
                                         files={f"{file['id']}": open(local_filename, 'rb')})
                except Exception as e:
                    print(f"FAILED to put file in {datanode} due to\n{e}")
                    continue

                if resp.status_code // 100 != 2:
                    print(resp.content.decode())
                else:
                    print(f"FILE {local_filename} was put as {dfs_filename} in {datanode}")
        else:
            print(response.content.decode())

    else:
        mistake()


def delete_file(*arguments):
    '''
    Deletes file from dfs

    :param filename: name of the file
    '''
    if len(arguments) == 2:
        filename = arguments[1]
        # request namenode to delete file
        try:
            response = requests.delete(NAMENODE + "/delete", json={"filename": filename})
        except Exception as e:
            print(f"FAILED to REMOVE file from NAMENODE due to\n{e}")
            return

        if response.status_code // 100 == 2:
            print("file was found in namenode")

            # acquiring info about file
            file = response.json()['file']

            # removing file from datanodes
            for datanode in file['datanodes']:
                print(f"sending delete request to datanode {datanode}")
                try:
                    response = requests.delete(datanode + "/delete", json={"file_id": file['id']})
                except Exception as e:
                    print(f"FAILED to REMOVE file from {datanode} due to\n{e}")
                    continue

                if response.status_code // 100 == 2:
                    print(f"file was deleted from datanode {datanode}")
                else:
                    print(f"FAILED file wasn't found in datanode {datanode}")
        else:
            print(response.content.decode())
    else:
        mistake()


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
        # request namenode to copy file
        try:
            response = requests.post(NAMENODE + '/copy', json={'filename': filename, 'dirname': dirname})
        except Exception as e:
            print(f"FAILED to COPY in NAMENODE due to\n{e}")
            return

        # check response
        if response.status_code // 100 == 2:
            print("namenode was updated")
            original = response.json()['original']
            copy = response.json()['copy']
            print(f"original file: {original}")
            print(f"copy file: {copy}")

            for datanode_cp in copy['datanodes']:
                print(f"started copying in {datanode_cp}")
                if datanode_cp in original['datanodes']:
                    print("it already contains the file, started copying")
                    try:
                        response = requests.post(datanode_cp + "/copy/existing",
                                                 json={"original_id": original['id'], "copy_id": copy['id']})
                    except Exception as e:
                        print(f"FAILED to COPY file in {datanode_cp} due to \n{e}")
                        continue

                    if response.status_code // 100 == 2:
                        print(f"file was successfully copied to {datanode_cp}")
                    else:
                        print(f"file couldn't be copied to {datanode_cp}")

                else:
                    print("it doesn't contain the file, started copying")
                    for datanode_orig in original['datanodes']:
                        try:
                            response = requests.post(datanode_cp + "/copy/non-existing",
                                                     json={"original_id": original['id'], "copy_id": copy['id'],
                                                           "datanode": datanode_orig})
                        except Exception as e:
                            print(f"FAILED to COPY file in {datanode_cp} with {datanode_orig} due to \n{e}")
                            continue

                        if response.status_code // 100 == 2:
                            print(f"file was copied from {datanode_orig}")
                            break
                        else:
                            print(response.content.decode())

            print("finished copying")

        else:
            print(response.content.decode())

    else:
        mistake()


def delete_directory(*arguments):
    '''
    Removes the directory
    '''

    print("started deleting directory")

    if len(arguments) == 2:
        dirname = arguments[1]

        try:
            response = requests.delete(NAMENODE + "/delete/dir-notsure", json={'dirname': dirname})
        except Exception as e:
            print(f"FAILED to rmdir in NAMENODE due to\n{e}")
            return

        if response.status_code // 100 == 2:
            empty = response.json()['empty']
            if empty:
                print("empty dir was deleted")
            else:
                print("dir is not empty")
                while True:
                    delete = input("are you sure that you want do delete all contents? [y/n]")
                    if delete == 'y' or delete == 'Y' or delete == 'yes' or delete == 'Yes':
                        try:
                            response = requests.delete(NAMENODE + "/delete/dir-sure", json={'dirname': dirname})
                        except Exception as e:
                            print(f"FAILED to rmdir in NAMENODE due to\n{e}")
                            return

                        # delete from datanodes
                        if response.status_code // 100 == 2:
                            files = response.json()['files']
                            for file in files:
                                datanodes = file['datanodes']
                                id = file['id']
                                for datanode in datanodes:
                                    try:
                                        response = requests.delete(datanode + '/delete', json={'file_id': id})
                                    except Exception as e:
                                        print(f"FAILED to REMOVE file {file} from {datanode} due to\n{e}")
                                        continue

                                    if response.status_code // 100 == 2:
                                        print(f"{file} was deleted from {datanode}")
                                    else:
                                        print(response.content.decode())
                        else:
                            print(response.content.decode())

                        break
                    elif delete == 'n' or delete == 'N' or delete == 'no' or delete == 'No':
                        print("not deleting")
                        break
        else:
            print(response.content.decode())

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
        try:
            response = requests.post(NAMENODE + '/mkdir', json={'dirname': dirname})
        except Exception as e:
            print(f"FAILED to mkdir in NAMENODE due to\n{e}")
            return

        # check response
        if response.status_code // 100 == 2:
            print(f"directory {dirname} successfully created")
        else:
            print(response.content.decode())
    else:
        mistake()


def read_directory(*arguments):
    '''
    Display the contents of the current directory
    '''

    dirname = ''
    if len(arguments) <= 2:
        if len(arguments) == 2:
            dirname = arguments[1]
        # request namenode for the file list
        try:
            response = requests.get(NAMENODE + '/ls', json={'dirname': dirname})
        except Exception as e:
            print(f"FAILED to READ dir from NAMENODE due to\n{e}")
            return

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
            print(response.content.decode())
    else:
        mistake()


def change_directory(*arguments):
    '''
    Change current working directory
    :param dirname: the directory that we want to make current working
    '''
    global CURRENT_DIR

    if len(arguments) == 2:
        dirname = arguments[1]
        try:
            response = requests.post(NAMENODE + '/cd', json={'dirname': dirname})
        except Exception as e:
            print(f"FAILED to ls in NAMENODE due to\n{e}")
        # check response
        if response.status_code // 100 == 2:
            # obtain the new working directory
            new_dirname = response.json()['dirname']
            cur_dir = response.json()['cur_dir']
            CURRENT_DIR = cur_dir
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
        try:
            response = requests.post(NAMENODE + '/info', json={'filename': filename})
        except Exception as e:
            print(f"FAILED to get info from NAMENODE due to\n{e}")
            return

        # check response
        if response.status_code // 100 == 2:
            file = response.json()['info']
            print(f"FILE {filename}\nSIZE {file['size'] / 2**10} KB\nCREATED {file['created_date']}\nDATANODES {file['datanodes']}")
        else:
            print(response.content.decode())
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
        try:
            response = requests.post(NAMENODE + '/move', json={'filename': filename, 'path': path})
        except Exception as e:
            print(f"FAILED to MOVE file in NAMENODE due to\n{e}")
            return

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
    "cp": copy_file,
    "rmdir": delete_directory
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
    show_help([1])
    while True:
        args = input("TatDFS𓆏 " + CURRENT_DIR + " $ ").split()
        if len(args) == 0:
            continue
        command = args[0]
        if command == "exit":
            break
        try:
            commands[args[0]](*args)
        except Exception as e:
            print("cannot execute command, please verify that you are using it correctly")