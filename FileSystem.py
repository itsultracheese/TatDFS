from anytree import Node, RenderTree, Resolver
import random, requests
from datetime import datetime
import os

ROOT_DIR = "root"
HOST = '0.0.0.0'
PORT = 8080
HEARTBEAT_RATE = 30


class FileSystem:
    def __init__(self):
        self.root = Node(ROOT_DIR, is_file=False)
        self.free_space = 1000000000000000000000
        self.cur_dir = ROOT_DIR
        # current directory
        self.cur_node = self.root
        self.live_datanodes = []  # store alive datanodes
        self.dead_datanodes = []  # store dead datanodes
        self.needs_replica = {}  # store files that need replicas in dic (node with file: # of needed replicas)
        # id of the next file created
        self.id = 0
        # replication factor
        self.replication = 2
        # info about files stored in datanodes
        # datanode: array of file ids
        self.datanodes_files = {}

    def get_current_dirname(self):
        dirname = '/'
        count = 0
        for node in self.cur_node.path:
            if count == 0:
                count += 1
                continue
            else:
                dirname += node.name + '/'
                count += 1
        return dirname

    def update_needs_replica(self, node, remove):
        '''
        recalculated needs_replica for the given node
        :param node: node
        :param remove: if true, the node is removed from needs_replica
        :return:
        '''
        if remove:
            if node in self.needs_replica.keys():
                self.needs_replica.pop(node)
            return

        cur_replica = len(node.file['datanodes'])  # obtaining current replica
        # updating needs_replica
        if cur_replica < self.replication:
            self.needs_replica[node] = self.replication - cur_replica
        elif cur_replica == self.replication:
            if node in self.needs_replica.keys():
                self.needs_replica.pop(node)

    def choose_datanodes(self, n=None, exclude=None):
        '''
        Choose datanodes to store file
        :param n: number of datanodes to return, equals to self.replication if not stated
        :param exclude: listed datanodes won't present in the returned list
        :return: a list of datanodes of size min(n, len(live_datanodes))
        '''
        # choose random datanodes to store the file
        amount = self.replication
        if n:
            amount = n

        if exclude:
            subset = [node for node in self.live_datanodes if node not in exclude]
            return random.sample(subset, min(amount, len(subset)))
        else:
            return random.sample(self.live_datanodes, min(amount, len(self.live_datanodes)))

    def create_file(self, filename, parent_node, filesize=0):
        '''
        Create a new file in a given parent node
        :param filename: name of the file to create
        :param parent_node: node of the parent directory of the file
        :param filesize: size of the file to store, default 0
        :return: file information
        '''
        # choose datanodes for storing and replicating
        datanodes = self.choose_datanodes()
        # create file with info
        file = {"id": self.id, "datanodes": datanodes, "size": filesize, "created_date": datetime.now()}
        # add info about file to info about files in datanodes
        for datanode in datanodes:
            if datanode in self.datanodes_files.keys():
                self.datanodes_files[datanode].append(self.id)
            else:
                self.datanodes_files[datanode] = [self.id]
        # increment id
        self.id += 1
        # decrement free space
        self.free_space -= filesize
        # create file in FS tree
        node = Node(filename, parent=parent_node, file=file, is_file=True)
        self.update_needs_replica(node, remove=False)
        return file

    def delete_file(self, node):
        '''
        Delete file from the filesystem
        :param node: node which to delete
        :return: info about deleted file
        '''
        # obtain the file info
        file = node.file
        # update free space
        self.free_space += file['size']
        id = file['id']
        datanodes = file['datanodes']
        # update info for each datanode
        for datanode in datanodes:
            try:
                self.datanodes_files[datanode].remove(id)
            except Exception as e:
                print(f"file with id {id} not found in {datanode}")
        # remove from fs
        node.parent = None
        self.update_needs_replica(node, remove=True)
        return file

    def create_directory(self, dirname, parent_dir):
        '''
        Create a child directory
        :param dirname: directory name to create
        :param parent_dir: parent directory node
        '''
        node = Node(dirname, parent=parent_dir, is_file=False)

    def get_all_files_rec(self, node):
        '''
        Obtain all files stored under a directory
        :param node: node of the directory
        :return: array of file infos
        '''
        result = []
        for child in node.children:
            if child.is_file:
                self.update_needs_replica(child, remove=True)
                result.append(child.file)
            else:
                result += self.get_all_files_rec(child)
        return result

    def get_file(self, filename):
        '''
        Get the node with the given file
        :param filename: path to file relative to the current node
        :return: node with the given file or None
        '''
        # resolve the name
        if filename[0] == '/':
            filename = '/root' + filename
        r = Resolver("name")
        try:
            node = r.get(self.cur_node, filename)
            if node:
                if node.is_file:
                    return node
                else:
                    return None
            else:
                return None
        except Exception as e:
            return None

    def get_dir(self, dirname):
        '''
        Get the node with the given directory
        :param dirname: path to direcroty relative to the current node
        :return: node with the given directory or None
        '''
        # resolve the name
        if len(dirname) >= 1:
            if dirname[0] == '/':
                dirname = '/root' + dirname
        r = Resolver("name")
        try:
            node = r.get(self.cur_node, dirname)
            if node:
                if node.is_file:
                    return None
                else:
                    return node
            else:
                return None
        except Exception as e:
            return None

    def get_filenode_by_id(self, node, id):
        '''
        Traverse the tree and return the node storing the file with corresponding id
        :param id: int
        :param node: node from where start traversal
        :return: node
        '''

        # base case
        if node.is_file:
            if node.file['id'] == id:
                return node
            else:
                return None
        # iterate through all children
        else:
            for child in node.children:
                result = self.get_filenode_by_id(child, id)
                if result:
                    return result
                    break
        return None

    def replicate_on_dead(self, dead_datanode):
        '''
        Replicating files from dead datanode to alive
        :param index: index of the dead datanode in self.live_datanodes
        '''

        print(f"started replication on dead {dead_datanode}")
        # ids of file stored in the dead node
        print(f"datanodes_files: {self.datanodes_files}")
        file_ids = self.datanodes_files[dead_datanode]

        for id in file_ids:
            print(f"\tprocessing file with id: {id}")
            cur_node = self.get_filenode_by_id(self.root, id)  # node storing the file
            file = cur_node.file  # file itself
            print(f"\tfile: {file}")
            file['datanodes'].remove(dead_datanode)  # removing the dead node from datanodes list of the file
            new_datanodes = self.choose_datanodes(n=1, exclude=file['datanodes'])  # acquiring new datanode
            print(f"\tnew_datanodes: {new_datanodes}")
            if len(new_datanodes) > 0:
                # replicate the file
                print("\tavailable datanode was found")
                new_datanode = new_datanodes[0]
                for datanode in file['datanodes']:
                    print(f"\t\tstarted replicating from {datanode}")
                    try:
                        response = requests.post(new_datanode + '/get-replica', json={'file_id': id, 'datanode': datanode})
                    except Exception as e:
                        print(f"Exception while replicating\n{e}")
                        continue

                    if response.status_code // 100 == 2:
                        if new_datanode in self.datanodes_files.keys():
                            self.datanodes_files[new_datanode].append(id)
                        else:
                            self.datanodes_files[new_datanode] = [id]
                        print(f"\t\tfile was replicated")
                        break
                    else:
                        print(f"\t\tfile was NOT replicated")

            file['datanodes'] += new_datanodes  # updating the list of datanodes of the file
            cur_node.file = file
            print(f"\tupdated file datanodes: {file}")
            self.update_needs_replica(cur_node, remove=False)  # updating needs_replica
            print(f"\tupdated needs_replica: {self.needs_replica}")

        self.datanodes_files[dead_datanode] = []


fs = FileSystem()