from flask import Flask, request, jsonify

import subprocess


app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello World!'
@app.route('/create',methods=["GET"])
def createVideo():
    print(request)
    id = request.args.get("id");
    subprocess.call(["python", "/content/SadTalker/inference.py",'--driven_audio', '/content/SadTalker/examples/driven_audio/RD_Radio31_000.wav', '--source_image', '/content/SadTalker/examples/source_image/my_img.jpg', '--result_dir', './my_results'])
    # subprocess.call(["python", "./video.py",'--driven_audio /content/SadTalker/examples/driven_audio/RD_Radio31_000.wav', '--source_image /content/SadTalker/examples/source_image/my_img.jpg', '--result_dir ./my_results'])
#     python inference.py \
#   --driven_audio ./examples/driven_audio/RD_Radio31_000.wav \
#   --source_image ./examples/source_image/my_img.jpg \
#   --result_dir ./my_results 
    return{"id":id}

if __name__ == '__main__':
   app.run(host='127.0.0.1',port=7860,debug=True)
