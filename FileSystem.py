from anytree import Node, RenderTree, Resolver
import random
from datetime import  datetime

ROOT_DIR = "root"
HOST = '0.0.0.0'
PORT = 8080
HEARTBEAT_RATE = 60

class FileSystem:
    def __init__(self):
        self.root = Node(ROOT_DIR)
        self.free_space = 1000000000000000000000
        self.cur_dir = ROOT_DIR
        # current directory
        self.cur_node = self.root
        self.live_datanodes = []
        # id of the next file created
        self.id = 0
        # replication factor
        self.replication = 1
        # info about files stored in datanodes
        # datanode: array of file ids
        self.datanodes_files = {}

    def choose_datanodes(self):
        # choose random datanodes to store the file
        return random.sample(self.live_datanodes, self.replication)

    def create_file(self, filename, filesize=0):
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
        node = Node(filename, parent=self.cur_node, file=file)
        return file

    def delete_file(self, filename):
        node = [x for x in self.cur_node.children if x.name == filename][0]
        file = node.file
        self.free_space += file['size']
        node.parent = None
        return file

    def create_directory(self, dirname):
        node = Node(dirname, parent=self.cur_node)

    def replicate_on_dead(self, datanode):
        pass


fs = FileSystem()