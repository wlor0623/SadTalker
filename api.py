from flask import Flask, request, jsonify
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello World!'
@app.route('/testApi',methods=["GET"])
def test_api():
    print(request)
    id = request.args.get("id");
    return{"id":id}
if __name__ == '__main__':
   app.run(host='127.0.0.1',port=7860,debug=True)
