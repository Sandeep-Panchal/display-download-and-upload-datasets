from flask import Flask, request, redirect, send_file, render_template
from werkzeug.utils import secure_filename
import pandas as pd
import boto3

# import the data that has secret access key and security key for s3 bucket
df_keys = pd.read_csv('new_user_credentials.csv')

# getting access key id and secret access key
access_key_id = df_keys['Access key ID'][0]
secret_access_key = df_keys['Secret access key'][0]

# create an instance for flask
app = Flask(__name__)

# end point for the home page
# it displays the list of dataset
@app.route('/')
def home():

    # connect to the s3 bucket
    s3_boto = boto3.resource(service_name='s3',
                             region_name='ap-south-1',
                             aws_access_key_id=access_key_id,
                             aws_secret_access_key=secret_access_key)

    # select the bucket
    select_bucket = s3_boto.Bucket('dataset-house')

    # get the objects from the bucket so as to display in the home page
    datasets = []
    for d in select_bucket.objects.all():
        datasets.append(d.key)

    return render_template('data_house.html', datasets=datasets)

# end point to upload file to s3 bucket
@app.route('/upload_file', methods=["POST"])
def upload_file():
    if request.method == "POST":
        file_to_upload = request.files["file"]
        content_type = request.mimetype
        file_name = secure_filename(file_to_upload.filename)

        # connect to the s3 bucket
        s3_boto = boto3.resource(service_name='s3',
                                 region_name='ap-south-1',
                                 aws_access_key_id=access_key_id,
                                 aws_secret_access_key=secret_access_key)

        # select the bucket
        select_bucket = s3_boto.Bucket('dataset-house')

        # uploading to s3 bucket
        upload_file_response = select_bucket.put_object(Body=file_to_upload, Key=file_name, ContentType=content_type)

        return redirect("/")

# end point to download files
@app.route('/download_file/<filename>', methods=["GET"])
def download_file(filename):
    if request.method == "GET":

        # connect to the s3 bucket
        s3_boto = boto3.resource(service_name='s3',
                                 region_name='ap-south-1',
                                 aws_access_key_id=access_key_id,
                                 aws_secret_access_key=secret_access_key)

        # select the bucket
        select_bucket = s3_boto.Bucket('dataset-house')

        # downloading
        select_bucket.download_file(Filename=filename, Key=filename)
        
        return send_file(filename, as_attachment=True)

if __name__ == '__main__':
   app.run(debug = True)