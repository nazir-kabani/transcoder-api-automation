import argparse
import os 
import base64
import json
import re
import datetime

from google.cloud import pubsub_v1
from google.cloud.video import transcoder_v1
from google.cloud.video.transcoder_v1.services.transcoder_service import (
TranscoderServiceClient,
)   

def create_job_from_template(project_id, location, input_uri, output_uri, template_id):
    """Creates a job based on a job template.   

    Args:
        project_id: The GCP project ID.
        location: The location to start the job in.
        input_uri: Uri of the video in the Cloud Storage bucket.
        output_uri: Uri of the video output folder in the Cloud Storage bucket.
        template_id: The user-defined template ID."""   
    

def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    name = (event["name"])
    ext = os.path.splitext(name)
    fname=ext[0]
    ext=ext[1] 
    publisher = pubsub_v1.PublisherClient() 

    supported=[".AVI", ".avi", ".GXF", ".gfx", ".MKV", ".mkv", ".MOV", ".mov", ".MPEG2-TS", ".ts", ".MP4", ".mp4", ".MXF", ".mxf", ".WebM", ".webm", ".WMV", ".wmv"]    

    if ext in supported:
        print("File type is supported for Transcoding API")

        input_uri = "gs://{source-bucket-name}/%s" %name  #Source bucket name
        output_uri = "gs://{output-bucket-name}/%s/" %fname #Output bucket name
        template_id = "hd-h264-hls-dash" #Template ID      

        client = TranscoderServiceClient()  

        parent = f"projects/{project-id}/locations/{location}" #project id and preferred location
        job = transcoder_v1.types.Job()
        job.input_uri = input_uri
        job.output_uri = output_uri
        job.template_id = template_id

        response = client.create_job(parent=parent, job=job)
        print(f"Job: {response.name}") 


        try:
            data=response
            job=str(data.name)
            ljob_id=re.findall("/jobs/(.+)",job)
            job_id=''.join(map(str, ljob_id))
            print(f"Job ID:", job_id)
            print(f"Job state: {str(data.state)}")
            job_state=str(data.state)

            topic_path = 'projects/{project-id}/topics/transcoding-start-status' #pub/sub1 topic name (please refer architecture diagram in blog to understand pub/sub 1)
            
            message_json = json.dumps(
                {
                    'name': name,
                    'jobId': job_id,
                    'jobStartState': job_state
                }
            )
            message_bytes = message_json.encode('utf-8')


            publish_future = publisher.publish(topic_path, data=message_bytes)
            publish_future.result()  # Verify the publish succeeded
            print('Message published.') 
        except Exception as e:
            print(e)
            print (e, 500)

        return response

    else:
        print("File type is not supported for Transcoding API")
