import requests
import boto3
import json

s3_client = boto3.client("s3")
S3_BUCKET_NAME = "finalproject-aiwen-connie"
S3_FOLDER_NAME = "satellite_image"

# Mapbox API credentials
ACCESS_TOKEN = "pk.eyJ1IjoiYWl3ZW4wODAxIiwiYSI6ImNsaTN6NjFkYTB1bW8zZmxreXI3cHJ0a3oifQ.bPAGO-obyK_Oo1U2sxGDlQ"

# API endpoint URL
url = "https://api.mapbox.com/styles/v1/mapbox/satellite-v9/static"

# Centroid coordinates
#centroid_latitude = 41.791391
#centroid_longitude = -87.598076

def collect_image(centroid_latitude, centroid_longitude, image_num):
    # Calculate bounding box coordinates
    side_length = 0.05  # in kilometers
    degrees_per_km = 0.009  # approximate value
    delta = side_length * degrees_per_km
    bbox = (
        centroid_longitude - delta,
        centroid_latitude - delta,
        centroid_longitude + delta,
        centroid_latitude + delta
    )
    # Construct the complete request URL
    # api doc reference: https://docs.mapbox.com/api/maps/static-images/#style-parameters
    request_url = f"{url}/[{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}]/300x300?access_token={ACCESS_TOKEN}"
    
    # Send the GET request
    response = requests.get(request_url)

    # Check if the request was successful
    if response.status_code == 200:
        # Save the image
        file_name = str(image_num).zfill(11) + '.jpg'
        with open(file_name, "wb") as image_file:
            image_file.write(response.content)
        
        # Upload the image to S3
        s3_key = f"{S3_FOLDER_NAME}/{file_name}"
        s3_client.upload_file(file_name, S3_BUCKET_NAME, s3_key)
        
        return(file_name)


def lambda_handler(event, context):
    point_map = event['point_map']
    image_num = 1
    mapping_dict = {}
    for pair in point_map:
        tract_id = pair[0]
        points = pair[1]
        mapping_dict[tract_id] = []
        for point in points:
            point_lat = point[0]
            point_lon = point[1]
            file_name = collect_image(point_lat, point_lon, image_num)
            image_num += 1
            mapping_dict[tract_id].append(file_name)
            
    # Save the mapping_dict as a JSON file
    with open('mapping_dict.json', 'w') as json_file:
        json.dump(mapping_dict, json_file)

    return {'StatusCode': 200} 
