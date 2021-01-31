# -*- coding: utf-8 -*-
#
# Author: Dave Hill
# Test Automation Technical Test Code
# This functionality is designed to be tested by interview candidates
#
# The code will process a csv file that has the following structure:
# ID, Product, Vendor, Transaction Date, Transaction Price, Transaction Status, VAT Number, Customer
#



import json
import urllib
import re
import boto3
import os
from datetime import datetime


s3 = boto3.resource('s3')
output_bucket = os.environ['OUTPUT_BUCKET']

error_msgs = {
    "F1": "File is not .csv",
    "F2": "File is empty",
    "F3": "File can't be read",
    "E1": "Incorrect number of fields in record",
    "E2": "ID field is empty, malformed or over 10 digits long",
    "E3": "Product field is empty or over 256 characters long",
    "E4": "Vendor field is empty or over 256 characters long",
    "E5": "Date field needs to be in this format - YYYY-MM-DDTHH:MM:SS",
    "E6": "Price field is malformed",
    "E7": "Transaction Status needs to be either \'Paid\', \'Unpaid\' or \'Processing\'",
    "E8": "VAT number should be in format IE12345678A or N/A",
    "E9": "Customer field is empty or over 256 characters long"
}

def error_logger(errorCode):
    errorMessage = error_msgs[errorCode]
    if errorMessage:
        return errorMessage
    else:
        return "No dedicated error message for {}".format(errorCode)

def load_file_checks(bucket, fileName, fileSize):
    if not fileName.endswith(".csv"):
        err_msg = error_logger("F1")
        return ("F1", err_msg)
    if fileSize == 0:
        err_msg = error_logger("F2")
        return ("F2", err_msg)
    fileToProcess = s3.Object(bucket, fileName)
    fileContents = fileToProcess.get()['Body'].read().decode('utf-8')
    if fileContents == 0:
        err_msg = error_logger("F3")
        return ("F3", err_msg)
    return ("S", "Success")

def bad_file_log(error):
    filename = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
    filename = "ERROR/" + filename + "-" + error[0]
    s3.Object(output_bucket, filename).put(Body=error[1])

def process_file(bucket, fileName):
    outputJson = {'Records':[]}
    fileToProcess = s3.Object(bucket, fileName)
    fileContents = fileToProcess.get()['Body'].read().decode('utf-8')
    for line in fileContents.splitlines():
        outputJson = record_checker(outputJson, line)
    filename = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
    filename = "PROCESSED/" + filename + "-DONE.json"
    resultsFile = s3.Object(output_bucket, filename)
    resultsFile.put(Body=bytes(json.dumps(outputJson).encode('UTF-8')))


def record_checker(outputJson, lineToProcess):
    if not lineToProcess.count(',') == 7:
        err_msg = error_logger("E1")
        err_dict = build_error_json("E1", err_msg)
        outputJson.get('Records').append({'Error': err_dict})
        return outputJson
    else:
        fields = lineToProcess.split(",")
        date_regex = re.compile("([0-9]*-[0-1][0-9]-[0-3][0-9]*T[0-2][0-9]:[0-5][0-9]:[0-5][0-9])")
        vat_regex = re.compile("([A-Z]*[A-Z0-9]*)")
        if not fields[0].isdigit() or len(fields[0]) > 10:                      # ID field
            err_msg = error_logger("E2")
            err_dict = build_error_json("E2", err_msg)
            outputJson.get('Records').append({'Error': err_dict})
            return outputJson
        elif len(fields[1]) < 2 or len(fields[1]) > 256:                        # Product field
            err_msg = error_logger("E3")
            err_dict = build_error_json("E3", err_msg)
            outputJson.get('Records').append({'Error': err_dict})
            return outputJson
        elif len(fields[2]) < 2 or len(fields[2]) > 256:                        # Vendor field
            err_msg = error_logger("E4")
            err_dict = build_error_json("E4", err_msg)
            outputJson.get('Records').append({'Error': err_dict})
            return outputJson
        elif re.match(date_regex, fields[3]) is None:                           # Transaction Date field
            err_msg = error_logger("E5")
            err_dict = build_error_json("E5", err_msg)
            outputJson.get('Records').append({'Error': err_dict})
            return outputJson
        elif not fields[4].replace(".", "", 1).isdigit():                       # Transaction Price field
            err_msg = error_logger("E6")
            err_dict = build_error_json("E6", err_msg)
            outputJson.get('Records').append({'Error': err_dict})
            return outputJson
        elif not fields[5].upper() in ('PAID', 'UNPAID', 'PROCESSING'):         # Transaction Status field
            err_msg = error_logger("E7")
            err_dict = build_error_json("E7", err_msg)
            outputJson.get('Records').append({'Error': err_dict})
            return outputJson
        elif re.match(vat_regex, fields[6]) is None and not fields[6].upper() in ("N/A", "NA", "NONE"):  # Vat Number field
            err_msg = error_logger("E8")
            err_dict = build_error_json("E8", err_msg)
            outputJson.get('Records').append({'Error': err_dict})
            return outputJson
        elif len(fields[7]) < 2 or len(fields[7]) > 256:                        # Customer field
            err_msg = error_logger("E9")
            err_dict = build_error_json("E9", err_msg)
            outputJson.get('Records').append({'Error': err_dict})
            return outputJson
        else:
            success_dict = build_happy_json(fields)
            outputJson.get('Records').append({'Record': success_dict})
            return outputJson


def build_happy_json(unprocessed):
    happy_dict = {}
    transaction_dict = {}
    transaction_dict['Date'] = unprocessed[3]
    transaction_dict['Price'] = unprocessed[4]
    transaction_dict['Status'] = unprocessed[5]
    transaction_dict['VAT Number'] = unprocessed[6]
    happy_dict['ID'] = unprocessed[0]
    happy_dict['Product'] = unprocessed[1]
    happy_dict['Vendor'] = unprocessed[2]
    happy_dict['Transaction'] = transaction_dict
    happy_dict['Customer'] = unprocessed[7]
    return happy_dict


def build_error_json(errorCode, errorMsg):
    err_dict = {}
    err_dict['ErrorCode'] = errorCode
    err_dict['ErrorMessage'] = errorMsg
    return err_dict

def write_to_json():
    asda = "adas"

def lambda_handler(event, context):
    for record in event.get('Records', []):
        bucket = record['s3']['bucket']['name']
        raw_file_name = record['s3']['object']['key']
        file_name = urllib.parse.unquote_plus(raw_file_name)
        file_size = record['s3']['object']['size']
        load_status = load_file_checks(bucket, file_name, file_size)
        if not load_status[0] == 'S':
            bad_file_log(load_status)
        else:
            process_file(bucket, file_name)



