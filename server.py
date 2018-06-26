from flask import Flask, request, render_template
from node import Node
from cluster import Cluster
import json

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello World!"

@app.route("/newfile", methods=['GET', 'POST'])
def newfile():
    res = None
    if ('POST' == request.method):
        pass
    elif('GET' == request.method):
        res = Node().get_free_size()
    return res
#    return render_template('cmds.html', res=res)

@app.route("/status", methods=['GET', 'POST'])
def status():
    res = None
    if('GET' == request.method):
        res = json.dumps(Node().get_status())
    return res

@app.route("/all_status", methods=['GET', 'POST'])
def allstatus():
    res = None
    if('GET' == request.method):
        res = json.dumps(Cluster().get_status())
    return res



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8888)