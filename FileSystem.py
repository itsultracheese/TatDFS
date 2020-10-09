from anytree import Node, RenderTree, Resolver
import random
from datetime import  datetime
import os

ROOT_DIR = "root"
HOST = '0.0.0.0'
PORT = 8080
HEARTBEAT_RATE = 60

class FileSystem:
    def __init__(self):
        self.root = Node(ROOT_DIR, is_file=False)
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

    def choose_datanodes(self):
        # choose random datanodes to store the file
        return random.sample(self.live_datanodes, self.replication)

    def create_file(self, filename, parent_node, filesize=0):
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
        return file

    def delete_file(self, node):
        file = node.file
        self.free_space += file['size']
        id = file['id']
        datanodes = file['datanodes']
        for datanode in datanodes:
            try:
                self.datanodes_files[datanode].remove(id)
            except Exception as e:
                print(f"file with id {id} not found in {datanode}")
        node.parent = None
        return file

    def create_directory(self, dirname, parent_dir):
        node = Node(dirname, parent=parent_dir, is_file=False)

    def get_all_files_rec(self, node):
        result = []
        for child in node.children:
            if child.is_file:
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

    def replicate_on_dead(self, datanode):
        pass


fs = FileSystem()