from flask import Flask, Response, request, jsonify
import logging
from anytree import Node, RenderTree, Resolver
from datetime import datetime
from threading import Thread
import os, requests, random, time
from FileSystem import fs

ROOT_DIR = "root"
HOST = '0.0.0.0'
PORT = 8080
DATANODES = ["http://0.0.0.0:8085"]
#DATANODES = ["http://0.0.0.0:8085", "http://0.0.0.0:8086", "http://0.0.0.0:8087"]
HEARTBEAT_RATE = 60

app = Flask(__name__)
logging.basicConfig(filename='namenode.log', level=logging.DEBUG)


def heartbeat():
    while True:
        for i in range(len(fs.live_datanodes)):
            response = requests.get(fs.live_datanodes[i] + '/ping')
            if response.status_code // 100 != 2:
                app.logger.info(f"datanode {fs.live_datanodes[i]} is dead")
                fs.replicate_on_dead(fs.live_datanodes[i])
                fs.live_datanodes.pop(i)
        time.sleep(HEARTBEAT_RATE)


@app.route('/ping')
def ping():
    return Response("ping from namenode", 200)


@app.route('/init')
def init():
    print("starting init in namenode")

    # initialize FS
    fs.__init__()
    live_datanodes = []

    # check whether nodes are alive
    # if yes format them
    for datanode in DATANODES:
        response = requests.get(datanode + '/ping')
        if response.status_code // 100 == 2:
            live_datanodes.append(datanode)

            # formatting datanodes
            response = requests.get(datanode + '/format')

            if response.status_code // 100 != 2:
                app.logger.info(f"couldn't format datanode: {datanode}")

            else:
                spaces = response.json()
                app.logger.info(f"{spaces}")
                free = spaces['free']
                fs.free_space = min(free, fs.free_space)

        else:
            print("couldn't ping that boi")
            app.logger.info(f"couldn't ping datanode: {datanode}")

    # check whether the FS initialized successfully
    app.logger.info("checking len of live_datanodes")
    if len(live_datanodes) > 0:
        print(live_datanodes)
        app.logger.info(f"live datanodes: {live_datanodes}")
        fs.live_datanodes = live_datanodes
        return jsonify({"free_space": fs.free_space})
    else:
        return Response("couldn't initialize", 418)


@app.route('/delete', methods=['DELETE'])
def delete():
    # delete file from FS
    print("starting deleting file")
    filename = request.json['filename']

    print(f"filename = {filename}")
    node = fs.get_file(filename)
    if node:
        print("file exists")
        file = fs.delete_file(node)
        print(f"file = {file}")
        return jsonify({"file": file})
    else:
        print("file doesn't exist")
        return Response("file doesn't exist", 404)


@app.route('/delete/dir-notsure', methods=['DELETE'])
def delete_dir_notsure():
    print("starting deleting dir")
    dirname = request.json['dirname']

    print(f"dirname: {dirname}")

    dir_node = fs.get_dir(dirname)
    if dir_node:
        children = [x for x in dir_node.children]
        if len(children) == 0:
            dir_node.parent = None
            print("directory empty, was removed successfully")
            return jsonify({"empty": True})
        else:
            print("directory not empty")
            return jsonify({"empty": False})
    else:
        print("dir doesn't exist")
        return Response("dir doesn't exist", 404)


@app.route('/delete/dir-sure', methods=['DELETE'])
def delete_dir_sure():
    print("starting deleting dir")
    dirname = request.json['dirname']

    print(f"dirname: {dirname}")

    dir_node = fs.get_dir(dirname)
    if dir_node:
        files = fs.get_all_files_rec(dir_node)
        dir_node.parent = None
        return jsonify({"files": files})

    else:
        print("dir doesn't exist")
        return Response("dir doesn't exist", 404)


@app.route('/copy', methods=['POST'])
def copy():
    print("started copying file in namenode")
    filename = request.json['filename']
    dirname = request.json['dirname']

    original_node = fs.get_file(filename)
    if original_node:
        print(f"file {filename} found")
        new_node_par = fs.get_dir(dirname)
        if new_node_par:
            new_name = os.path.basename(filename) + '_copy'
            count = 1
            file = fs.get_file(new_name)
            while file:
                new_name = new_name + str(count)
                count += 1
                file = fs.get_file(new_name)
            file = fs.create_file(new_name, new_node_par, original_node.file['size'])
            print(f"file was copied under the filename {filename}")
            return jsonify({'original': original_node.file, 'copy': file})
        else:
            print("specified directory does not exist")
            return Response("specified directory does not exist", 404)


@app.route('/get', methods=['GET'])
def get():
    print("started getting the file in namenode")
    filename = request.json['filename']
    print(f"filename = {filename}")

    file = fs.get_file(filename)
    if file:
        print("file exists")
        print(f"file = {file.file}")
        return jsonify({"file": file.file})

    else:
        print("file doesn't exist")
        Response("file doesn't exist", 404)


@app.route('/create', methods=['POST'])
def create():
    # obtain filename
    filename = request.json['filename']
    filesize = 0
    if request.json['filesize']:
        filesize = request.json['filesize']

    # check whether file already exists
    if fs.get_file(filename) or fs.get_dir(filename):
        app.logger.info(f"file already exists {filename}")
        return Response("", 409)
    # create file, return info about datanodes and id
    else:
        app.logger.info(f"filesize: {filesize}   free_space:{fs.free_space}")
        if filesize > fs.free_space:  # check if there's available space
            return Response("not enough space", 413)
        file_dir = os.path.dirname(filename)
        file_name = os.path.basename(filename)
        file_parent = fs.get_dir(file_dir)
        if file_parent:
            file = fs.create_file(file_name, file_parent, filesize)
            return jsonify({"file": file})
        else:
            return Response('', 404)


@app.route('/mkdir', methods=['POST'])
def mkdir():
    # get directory name
    dirname = request.json['dirname']

    if fs.get_file(dirname) or fs.get_dir(dirname):
        return Response("", 409)
    else:
        # add directory to fs tree
        dir_parent = os.path.dirname(dirname)
        dir_name = os.path.basename(dirname)
        parent_node = fs.get_dir(dir_parent)
        if parent_node:
            fs.create_directory(dir_name, parent_node)
            return Response("", 200)
        else:
            return Response('', 404)


@app.route('/ls')
def ls():
    # get directory name
    dirname = request.json['dirname']
    dirs = []
    files = []

    dir_node = fs.get_dir(dirname)
    for node in dir_node.children:
        # check whether file or directory
        if node.is_file:
            files.append(node.name)
        else:
            dirs.append(node.name)
    return jsonify({'dirs': dirs, 'files': files})


@app.route('/cd', methods=['POST'])
def cd():
    # get directory name
    dirname = request.json['dirname']

    node = fs.get_dir(dirname)
    if node:
        fs.cur_node = node
        return jsonify({'dirname': fs.cur_node.name, 'cur_dir': fs.get_current_dirname()})
    else:
        return Response('', 404)


@app.route('/info', methods=['POST'])
def info():
    # get file name
    filename = request.json['filename']
    node = fs.get_file(filename)
    if node:
        return jsonify({'info': node.file})
    else:
        return Response('', 404)


@app.route('/move', methods=['POST'])
def move():
    # get file name
    filename = request.json['filename']
    # get path
    path = request.json['path']

    file_node = fs.get_file(filename)
    if file_node:
        node = fs.get_dir(path)
        if node:
            if filename in [x.name for x in node.children]:
                return Response('', 419)
            else:
                file_node.parent = node
                print(RenderTree(fs.root))
                return Response('', 200)
        else:
            return Response('', 404)
    else:
        return Response('', 404)


if __name__ == '__main__':
    # heartbeat_thread = Thread(target=heartbeat)
    # heartbeat_thread.start()
    app.run(debug=True, host=HOST, port=PORT)
