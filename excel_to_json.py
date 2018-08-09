# -*- coding: utf-8 -*-
import os
import boto3
import json
import botocore
from botocore.vendored import requests
import xlrd
from collections import OrderedDict




file_url = 'https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.xls'
AWS_BUCKET_NAME = 'steel-eye'
sheet = 'MICs List by CC'
return_val = {
    "statusCode": 200,
    "headers": { "Content-Type": "application/json"}}

debug = False
rows_list = []

def get_xls(event):
    # Snippet to make make excel source and sheet name to be fetched changable

    # file_url_param = event.get('queryStringParameters', {}).get("file_url")
    # url = file_url_param if file_url_param else file_url
    
    # url_params = event.get('queryStringParameters', None)
    # if url_params:
    #     sheet_name = url_params.get("sheet_name", None)
    # else:
    #     sheet_name = sheet
        
    # sheet_name = sheet_name_param if sheet_name_param else 'MICs List by CC'

    # if file_url != url and not sheet_name_param: # if a different file url is present without sheetname, fetch first sheet.
    #     worksheet = workbook.sheet_by_index(0) 
    #     sheet_name = 'first_sheet'

    url = file_url
    sheet_name = sheet

    r = requests.get(url)  # make an HTTP request
    try:
        workbook = xlrd.open_workbook(file_contents=r.content)  # open workbook
    except xlrd.biffh.XLRDError as e:
        return None, None, (str(e) + " There is error with file destination or file format") 
        # Handling error when file source and sheet_name are sent through query params to API.

    
    try:
        worksheet = workbook.sheet_by_name(sheet_name)  #  get sheet by name.
    except xlrd.biffh.XLRDError as e:
        return None, None, (str(e) + " Missing sheet_name value from url or sheet with name <{}> dont exist in Excel file".format(sheet_name))


    headers = worksheet.row_values(0)
    for rowindex in range(1, worksheet.nrows): 
        row_data = worksheet.row_values(rowindex) 
        row_dict = OrderedDict()
        for index in range(len(headers)):
            row_dict[headers[index]] = row_data[index] 
        rows_list.append(row_dict)
    return rows_list, sheet_name, None

def handler(event, context):
    row_lists, sheet_name, err = get_xls(event)
    if err:
        return_val.update({"statusCode": 400, "error": err})
        return return_val

    json_data = json.dumps(row_lists, indent=4).encode('UTF-8')  
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(AWS_BUCKET_NAME)
    path = sheet_name.replace(' ','_').lower() + '.json' # filename for JSON to store in S3
    bucket.put_object(
        ACL='public-read',
        ContentType='application/json',
        Key=path,
        Body=bytes(json_data),
    )
    s3Client = boto3.client('s3')

    # Generation of presigned url in case of private s3 bucket
    # preSignedUrl = s3Client.generate_presigned_url('get_object', Params = {'Bucket': AWS_BUCKET_NAME, 'Key': path}, ExpiresIn = 0)

    # Generating unsigned url since s3 is public 
    bucket_location = boto3.client('s3').get_bucket_location(Bucket=AWS_BUCKET_NAME)
    object_url = "https://s3-{0}.amazonaws.com/{1}/{2}".format(
        bucket_location['LocationConstraint'],
        AWS_BUCKET_NAME,
        path)

    body = {
        "uploaded": "true",
        "object_url": object_url,
    }

    if debug == True:
        body.update({"event": event})

    return_val.update({"body": json.dumps(body)})
    return return_val
