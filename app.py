import os
import io
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, Response
from werkzeug.utils import secure_filename
from azure.storage.blob import BlobServiceClient

app = Flask(__name__)
app.secret_key = "dev-secret"  # for flash messages

# Get connection info from environment variables
CONN_STR = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER")

if not CONN_STR or not CONTAINER:
    raise RuntimeError("Please set AZURE_STORAGE_CONNECTION_STRING and AZURE_STORAGE_CONTAINER")

# Clients
blob_service_client = BlobServiceClient.from_connection_string(CONN_STR)
container_client = blob_service_client.get_container_client(CONTAINER)

@app.route("/", methods=["GET"])
def index():
    # List blobs in container
    blobs = container_client.list_blobs()
    blob_list = [b.name for b in blobs]
    return render_template("index.html", blobs=blob_list)

@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        flash("No file part")
        return redirect(url_for("index"))

    file = request.files["file"]
    if file.filename == "":
        flash("No selected file")
        return redirect(url_for("index"))

    filename = secure_filename(file.filename)
    blob_client = container_client.get_blob_client(filename)

    # Upload file stream (overwrite if exists)
    blob_client.upload_blob(file.stream, overwrite=True)

    flash(f"Uploaded {filename}")
    return redirect(url_for("index"))

@app.route("/download/<path:blob_name>", methods=["GET"])
def download(blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    downloader = blob_client.download_blob()
    data = downloader.readall()  # read blob into memory
    return send_file(
        io.BytesIO(data),
        as_attachment=True,
        download_name=blob_name
    )

# Optional: streaming download (better for big files)
def stream_blob_generator(blob_client):
    stream = blob_client.download_blob()
    for chunk in stream.chunks():
        yield chunk

@app.route("/stream-download/<path:blob_name>", methods=["GET"])
def stream_download(blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    headers = {"Content-Disposition": f"attachment; filename={blob_name}"}
    return Response(stream_blob_generator(blob_client), headers=headers, mimetype="application/octet-stream")

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
