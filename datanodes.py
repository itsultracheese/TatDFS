from flask import Flask, Response, jsonify, request
import os, shutil, logging

HOST = '0.0.0.0'
PORT = 8085
CURRENT_DIR = os.path.join(os.getcwd(), "data")
app = Flask(__name__)
logging.basicConfig(filename='datanode.log', level=logging.DEBUG)


@app.route("/ping")
def ping():
    return Response("ping from datanode", 200)


@app.route("/format")
def format():
    '''
    Formats the contents of the datanode
    '''
    global CURRENT_DIR

    # create root folder if it does not exist
    if not os.path.exists(CURRENT_DIR):
        os.mkdir(CURRENT_DIR)

    # iterate through all files and dirs and delete
    for filename in os.listdir(CURRENT_DIR):
        path = os.path.join(CURRENT_DIR, filename)
        try:
            if os.path.isfile(path) or os.path.islink(path):
                os.unlink(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)
        except Exception as e:
            # if file/dir was not deleted write to log
            app.logger.info(f'failed to delete {path}, reason: {e}')

    # obtain info about free space
    _, _, free = shutil.disk_usage(CURRENT_DIR)

    return jsonify({"free": free})
    

@app.route("/get", methods=['GET'])
def get_file():
    '''
    Get file from datanode
    '''

    print("started transmitting file for get_file")
    file_id = request.json['file_id']

    if os.path.isfile(os.path.join(CURRENT_DIR, str(file_id))):
        print("file found, sending")
        return send_file(os.path.join(CURRENT_DIR, str(file_id)))
    else:
        print("file is not found")
        return Response("file doesn't exist in this node", 404)


@app.route("/put", methods=['POST'])
def put_file():
    '''
    Put file to datanode
    '''

    print("started uploading file")
    # obtain file from client
    file_id = [k for k in request.files.keys()][0]
    file = request.files[f'{file_id}']
    try:
        # create file
        print(f"file: {file}")
        print(f"file id: {file_id}")
        file.save(os.path.join(CURRENT_DIR, str(file_id)))
        return Response("", 200)
    except Exception as e:
        # if not created append to log, response 400
        app.logger.info(f"failed to upload file because of {e}")
        return Response("", 400)

@app.route("/create", methods=["POST"])
def create_file():
    '''
    Creates an empty file in the current directory
    '''
    # obtain file id from client
    print("started creating file")
    print(request.json)
    file_id = request.json['file_id']
    try:
        # create file
        print(file_id)
        open(CURRENT_DIR + '/' + str(file_id), 'a').close()
        return Response("", 200)
    except Exception as e:
        # if not created append to log, response 400
        app.logger.info(f"failed to create file because of {e}")
        return Response("", 400)



if __name__ == '__main__':
    app.run(debug=True, host=HOST, port=PORT)
