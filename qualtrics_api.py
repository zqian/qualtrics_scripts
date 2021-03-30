import io
import re
import sys
import zipfile
import pandas as pd
import urllib3
import shutil

import requests
import gspread


def exportSurvey(apiToken, surveyId, dataCenter, fileFormat):

    surveyId = surveyId
    fileFormat = fileFormat
    dataCenter = dataCenter

    # Setting static parameters
    requestCheckProgress = 0.0
    progressStatus = "inProgress"
    baseUrl = "https://{0}.qualtrics.com/API/v3/surveys/{1}/export-responses/".format(
        dataCenter, surveyId)
    headers = {
        "content-type": "application/json",
        "x-api-token": apiToken,
    }

    # Step 1: Creating Data Export
    downloadRequestUrl = baseUrl
    downloadRequestPayload = '{"format":"' + fileFormat + '"}'
    downloadRequestResponse = requests.request(
        "POST", downloadRequestUrl, data=downloadRequestPayload, headers=headers)
    print(downloadRequestResponse.text)
    progressId = downloadRequestResponse.json()["result"]["progressId"]
    print(downloadRequestResponse.text)

    # Step 2: Checking on Data Export Progress and waiting until export is ready
    while progressStatus != "complete" and progressStatus != "failed":
        print("progressStatus=", progressStatus)
        requestCheckUrl = baseUrl + progressId
        requestCheckResponse = requests.request(
            "GET", requestCheckUrl, headers=headers)
        requestCheckProgress = requestCheckResponse.json()[
            "result"]["percentComplete"]
        print("Download is " + str(requestCheckProgress) + " complete")
        progressStatus = requestCheckResponse.json()["result"]["status"]

    # step 2.1: Check for error
    if progressStatus == "failed":
        raise Exception("export failed")

    fileId = requestCheckResponse.json()["result"]["fileId"]

    # Step 3: Downloading file
    requestDownloadUrl = baseUrl + fileId + '/file'
    requestDownload = requests.request(
        "GET", requestDownloadUrl, headers=headers, stream=True)

    # Step 4: Unzipping the file
    zipfile.ZipFile(io.BytesIO(requestDownload.content)
                    ).extractall("MyQualtricsDownload")
    print('Complete')

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '<spreadsheet_id>'


def export_gsheet():

    gc = gspread.service_account(filename='./client_secret.json')
    sh = gc.open_by_key(SPREADSHEET_ID)
    worksheet = sh.get_worksheet(0)

    df = pd.read_csv("MyQualtricsDownload/download.csv")
    df.fillna('', inplace=True)
    print(df.values.tolist())
    print(df.columns.values.tolist())
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())

# https://YOUR_DATA_CENTER_ID.qualtrics.com/Q/File.php?F=FILE_ID


def get_uploaded_files(dataCenter):
    df = pd.read_csv("MyQualtricsDownload/download.csv")
    http = urllib3.PoolManager()
    for i, row in df.iterrows():
        print(str(i))
        print(row['Q13_Id'])
        print(row['Q13_Name'])
        if (i > 1):
            url = f"https://{dataCenter}.qualtrics.com/Q/File.php?F={row['Q13_Id']}"
            print(url)
            fileName = f"AllFiles/{row['Q13_Name']}"

            # download file
            with open(fileName, 'wb') as out:
                r = http.request('GET', url, preload_content=False)
                shutil.copyfileobj(r, out)


def main():

    try:
        apiToken = '<API_TOKEN>'
        dataCenter = 'ca1'
    except KeyError:
        print("set environment variables APIKEY and DATACENTER")
        sys.exit(2)

    try:
        surveyId = '<SURVEY_ID>'
        fileFormat = 'csv'
    except IndexError:
        print("usage: surveyId fileFormat")
        sys.exit(2)

    if fileFormat not in ["csv", "tsv", "spss"]:
        print('fileFormat must be either csv, tsv, or spss')
        sys.exit(2)

    r = re.compile('^SV_.*')
    m = r.match(surveyId)
    if not m:
        print("survey Id must match ^SV_.*")
        sys.exit(2)

    exportSurvey(apiToken, surveyId, dataCenter, fileFormat)
    export_gsheet()
    # get_uploaded_files(dataCenter)


if __name__ == "__main__":
    main()
