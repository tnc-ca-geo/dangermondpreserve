def UploadWellData(well, well_data, out_path, dendra_user, dendra_pass, sensor_depth):
    # Params
    url = 'https://api.dendra.science/v2/'  
    headers = {"Content-Type":"application/json"}

    #Authenticate Dendra User:
    # If you have a login and the data is not public, you must authenticate using your Dendra login
    creds = {
        'email': dendra_user,
        'strategy': 'local',
        'password': dendra_pass
    }
    r = requests.post(url+'authentication', json=creds)
    if r.status_code != 201:
        arcpy.AddMessage("Dendra authentication failed, check your email/password")
        raise AssertionError
    
    token = r.json()['accessToken']
    headers['Authorization'] = token

    #Find the well to upload to
    with arcpy.da.SearchCursor(well, ['Name_of_Well', 'well_url']) as well_cur:
        well_arr = well_cur.next()
    del well_cur

    #Fetch station
    d_well_id = well_arr[1].split("/")[-1]
    r = requests.get(url + "stations/" + d_well_id, headers=headers)
    d_well_meta = r.json()
    d_well_name = d_well_meta['slug'].replace("dangermond-", "")
    arcpy.AddMessage(f"Processing upload request to {d_well_name}")

    #Process LEV to CSV
    write = well_data_dendra_helpers.lev_to_csv(d_well_meta, well_data, out_path) #Returns true if successfully converted

    if write:
        arcpy.AddMessage(f"LEV successfully written to CSV at {out_path}")
    else:
        arcpy.AddError(f"LEV to CSV conversion unsuccessful")
    
    #Validate CSV
    well_data_csv = pd.read_csv(out_path, index_col="TIMESTAMP")
    well_data_csv.index = pd.to_datetime(well_data_csv.index, format="%Y-%m-%d %H:%M:%S")
    well_data_csv.index = well_data_csv.index + pd.Timedelta(hours = 8) #Convert TIMESTAMPS from PST to UTC for Dendra

    min_dt = well_data_csv.index.min()
    max_dt = well_data_csv.index.max()
    
    #Get existing datastream
    r = requests.get(url + "datastreams", headers=headers, params = {
        "station_id": d_well_id,
    }) 
    assert r.status_code == 200

    datastream_id = [ds['_id'] for ds in r.json()['data'] if "Well Water Level xle/lev" in ds['name']][0]

    #Get datapoints from datastream
    params = {
        "datastream_id": datastream_id,
        "time[$gte]": str(min_dt).replace(" ","T").replace(".000000000", "") + ".000Z",
        "time[$lt]": str(max_dt).replace(" ","T").replace(".000000000", "") + ".000Z",
        "$sort[time]": "1",
        "$limit": "2000",
        "time_local": False
    }
    r = requests.get(url + "datapoints", headers=headers, params = params)
    assert r.status_code == 200

    if len(r.json()['data']) >= 1800: #Data for these dates already included
        raise AssertionError("This dataset may have already been uploaded to Dendra, please double check.")

    #Copy and modify manifest
    with open(f".\\Utils\\upload_template.json", encoding="utf-8") as upload_template:
        upload_template_json = json.load(upload_template)
        upload_template_json = deepcopy(upload_template_json)
        upload_template_json['station_id'] = d_well_meta['_id']
        upload_template_json['spec']['options']['context']['source'] = f"/tnc/station_jldp_wells/source_{d_well_name}_xle_lev"

    #Post manifest (currently using subprocess for CLI calls)
    r = requests.post(url + "uploads", headers=headers, data=json.dumps(upload_template_json))
    assert r.status_code == 201
    upload_id = r.json()['_id']
    arcpy.AddMessage(f"Upload ID: {upload_id}")

    #Sleep for some time
    time.sleep(3)

    #Fetch upload URL
    r = requests.get(url + "uploads/" + upload_id, headers=headers)
    assert r.status_code == 200
    upload_url = r.json()['result_pre']['presigned_put_info']['url']

    #Curl to upload URL
    data_stream = io.StringIO()
    well_data_csv.to_csv(data_stream, sep=",", encoding="utf-8")
    data_stream.seek(0)
    r = requests.put(upload_url, headers=headers, data=data_stream)
    assert r.status_code == 200

    #Patch and activate upload
    patch_payload = json.dumps({"$set": {"is_active": True}})
    r = requests.patch(url + "uploads/" + upload_id, headers=headers, data=patch_payload)
    assert r.status_code == 200

    #Check status of upload up for up to one minute
    for _ in range(20):
        time.sleep(3)
        r_upload = requests.get(url + "uploads/" + upload_id, headers=headers)
        r_upload_json = r_upload.json()
        if (r_upload.status_code == 200) and (r_upload_json['state'] == "completed"): #remove dry run if complete
            upload_details = r_upload_json['result']['items'][0]

            #Validate record count
            if (upload_details['record_count'] != well_data_csv.shape[0]):
                raise AssertionError("Failed to upload all of the records in the .lev")
            
            #Validate time max and time min
            if (pd.to_datetime(upload_details['time_max'], unit="ms") != max_dt or pd.to_datetime(upload_details['time_min'], unit="ms") != min_dt):
                raise AssertionError("Failed to correctly parse dates on the upload")
            
            #Patch the upload to kick it off for real!
            active_spec = r_upload_json['spec']
            active_spec['options']['dry_run'] = False
            r_patch = requests.patch(url + "uploads/" + upload_id, headers=headers, data=json.dumps({"$set": {"spec" : active_spec, "is_active": True}}))
            assert r_patch.status_code == 200
            arcpy.AddMessage(f"Successfully completed upload (id:{upload_id}) to https://dendra.science/orgs/tnc/stations/{d_well_id} with {upload_details['record_count']} records")
            
            break   
        elif (r_upload_json['state'] == "error"):
            arcpy.AddMessage(f"Upload encountered an error: {r_upload_json['result_pre']['error']}")
            raise AssertionError

    #Data Gaps Annotations (Crop timeseries by one day on either side, remove outliers, remove non-positives)
    with open(f".\\Utils\\anno_template.json", encoding="utf-8") as anno_template_json: 
        anno_json = json.load(anno_template_json)              
        anno_json = deepcopy(anno_json)
    anno_indices = []

    #Iterate over time series data and find outliers
    anno_indices = np.append(anno_indices, list(well_data_csv.loc[well_data_csv['level_ft'] <= 0].index)).astype('datetime64[D]') #Add non-positive values to exclusion
    
    #Exclude the first and last day of data (survey error)
    date_exclusions = well_data_csv.index[(well_data_csv.index.date == min_dt.date()) | (well_data_csv.index.date == max_dt.date())]
    anno_indices = np.append(anno_indices, date_exclusions)

    arcpy.AddMessage(f"Annotating datapoints on {min_dt.date()} and {max_dt.date()} for exclusion")

    #Create annotation object
    if len(anno_indices) != 0:
        #Create outlier anno timestamp objects
        time_stamps = [{
            'begins_at': str(index).replace(" ","T").replace(".000000000", "") + ".000Z",
            'ends_before': str(index + pd.Timedelta(minutes=1)).replace(" ","T").replace(".000000000", "") + ".000Z"
        } for index in anno_indices]
    else:
        return

    anno_title = f"Survey Exclusion for {d_well_id} on {min_dt.date()} and {max_dt.date()}"
    anno_json['intervals'] = time_stamps
    anno_json['station_ids'] = [d_well_id]
    anno_json['title'] = anno_title

    #Check Annotation with the same title doesn't already exist on Dendra
    r = requests.get(url + "annotations", headers = headers, params = {
        "title": anno_title,
    }) 
    assert r.status_code == 200
    if len(r.json()['data']) >= 1:
        raise AssertionError(f"An annotation may already exist for this data. Check here: https://dendra.science/orgs/tnc/annotations/{r.json()['data'][0]['_id']}")

    anno_r = requests.post(url + "annotations", headers=headers, data = json.dumps(anno_json))
    assert anno_r.status_code == 201

    #If Sensor Depth, update attribute of sensor depth and expand depth to groundwater intervals
    if sensor_depth:
        with open(f".\\Utils\\sensor_depth_anno_template.json", encoding="utf-8") as depth_anno_json: 
            depth_anno_json = json.load(depth_anno_json)              
            depth_anno_json = deepcopy(depth_anno_json)

        depth_anno_json['intervals'] = [
            {
                "begins_at": str(min_dt).replace(" ","T").replace(".000000000", "") + ".000Z",
                "ends_before": str(max_dt).replace(" ","T").replace(".000000000", "") + ".000Z",
            }
        ]
        depth_anno_json['actions'][0]['attrib']['sensor_depth'] = float(sensor_depth)
        depth_anno_json['description'] = f"Sensor Depth @ {d_well_meta['name'].replace('Dangermond ', '')}"
        depth_anno_json['station_ids'] = [d_well_id]
        depth_anno_json['affected_station_ids'] = [d_well_id]

        depth_r = requests.post(url + "annotations", headers=headers, data = json.dumps(depth_anno_json)) 
        assert depth_r.status_code == 201

        arcpy.AddMessage(f"Successfully uploaded data csv with survey annotation (Annotation ID: {anno_r.json()['_id']}) and sensor depth annotation (Annotation ID: {depth_r.json()['_id']})")
        return 
    
    arcpy.AddMessage(f"Successfully uploaded data csv with annotation (Annotation ID: {anno_r.json()['_id']})")
    return

# This is used to execute code if the file was run but not imported
if __name__ == '__main__':
    # Import Libraries
    import arcpy
    import pandas as pd
    import numpy as np
    import os, io, sys
    import json
    import requests
    import subprocess
    import time
    import requests
    from copy import deepcopy

    #Import Dendra Python Helpers
    sys.path.append(f"{os.path.dirname(arcpy.env.workspace)}\\Utils")

    #Import helper functions
    import well_data_dendra_helpers

    # Tool parameter accessed with GetParameter or GetParameterAsText
    well = arcpy.GetParameterAsText(0)
    well_data = arcpy.GetParameterAsText(1)
    out_path = arcpy.GetParameterAsText(2)
    dendra_user = arcpy.GetParameterAsText(3)
    dendra_pass = arcpy.GetParameterAsText(4)
    sensor_depth = arcpy.GetParameterAsText(5) #Optional parameter for updating Depth to Groundwater Datastream

    #Validation
    if arcpy.management.GetCount(well).inputCount != 1:
        raise AssertionError("Please select one (1) well from the Wells Features Layer")
    if not os.path.exists(os.path.dirname(out_path)):
        raise FileNotFoundError("The output directory given is invalid")
    
    UploadWellData(well, well_data, out_path, dendra_user, dendra_pass, sensor_depth)
