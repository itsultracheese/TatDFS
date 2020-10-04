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
HEARTBEAT_RATE = 60

app = Flask(__name__)
logging.basicConfig(filename='namenode.log', level=logging.DEBUG)


def check_if_file_exists(filename):
    filenames = [x.name for x in fs.cur_node.children]
    return filename in filenames


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
            print("could'nt ping that boi")
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
    filename = request.args['filename']
    if check_if_file_exists(filename):
        file = fs.delete_file(filename)
        return jsonify({"file": file})


@app.route('/create', methods=['POST'])
def create():

    # obtain filename
    filename = request.json['filename']
    filesize = 0
    if request.json['filesize']:
        filesize = request.json['filesize']

    # check whether file already exists
    if check_if_file_exists(filename):
        app.logger.info(f"file already exists {filename}")
        return Response("", 409)
    # create file, return info about datanodes and id
    else:
        app.logger.info(f"filesize: {filesize}   free_space:{fs.free_space}")
        if filesize > fs.free_space: # check if there's available space
            return Response("not enough space", 413)
        file = fs.create_file(filename, filesize)
        return jsonify({"file": file})


@app.route('/mkdir', methods=['POST'])
def mkdir():
    # get directory name
    dirname = request.json['dirname']
    if check_if_file_exists(dirname):
        return Response("", 409)
    else:
        # add directory to fs tree
        fs.create_directory(dirname)
        return Response("", 200)

@app.route('/ls')
def ls():
    dirs = []
    files = []
    for node in fs.cur_node.children:
        # check whether file or directory
        try:
            _ = node.file
            files.append(node.name)
        except Exception as e:
            dirs.append(node.name)
    return jsonify({'dirs': dirs, 'files': files})


@app.route('/cd', methods=['POST'])
def cd():
    # get directory name
    dirname = request.json['dirname']
    if dirname == '..':
        if fs.cur_node.parent:
            fs.cur_node = fs.cur_node.parent
            return jsonify({'dirname': fs.cur_node.name})
        else:
            return Response('', 404)
    else:
        try:
            r = Resolver('name')
            node = r.get(fs.cur_node, dirname)
            fs.cur_node = node
            return jsonify({'dirname': fs.cur_node.name})
        except Exception as e:
            return Response('', 404)


if __name__ == '__main__':
    #heartbeat_thread = Thread(target=heartbeat)
    #heartbeat_thread.start()
    app.run(debug=True, host=HOST, port=PORT)
