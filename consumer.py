import boto3
import json
import sys
import time
import logging

def readFile(requests):
    #get all files from the bucket

    #TODO: dont need to get all objects, just get one, it will be the lowest key value
    #get_object requires a key, in order to know what the lowest key is, i need all the objects in the bucket
    allRequests = requests.objects.all()

    size = sum(1 for _ in allRequests)

    print(size)

    if size == 0:
        return 0

    #get lowest keyed object from the bucket
    lowest = next(x for x in allRequests)
    for obj in allRequests:
        if lowest.key > obj.key:
            lowest = obj
    
    # get information from file into JSON format
    body = lowest.get()['Body'].read()

    #delete objcet from bucket
    key = lowest.key
    lowest.delete()
    logging.warning("Object deleted from: " + bucket)

    if len(body) < 4:
        return -1
    

    return (key,json.loads(body))

def writeToS3(s3, key, data, bucket):
    name = data["owner"]
    name = name.replace(" ", "-")

    path = "widgets/"+name+"/"+key

    #obj = s3.put_object(bucket, key, data)
    obj = s3.Object(bucket,path)
    obj.put(Body=json.dumps(data))
    logging.warning("Object uploaded to  : " + bucket)
    print("writing to bucket")

def writeToDB(key, data):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('widgets')
    
    #dynamodb.put_item(TableName=)
    table.put_item(Item=data)
    print("writing to database")
    logging.warning('item added to database')


# syntax for writing to bucket:
#   {bucket to read from} bucket {bucket to write to}

#syntax for writing to database:
#   {bucket name to read from} db


logging.basicConfig(format='%(asctime)s %(message)s:', filename = "logs.txt")

#use command line arguments
if len(sys.argv) <= 1:
    #if no arguments
    bucket = 'usu-cs5260-hackley-requests'
    whereTo = 'usu-cs5260-hackley-web'
    storage = 1

elif sys.argv[2] == "db":
    bucket = sys.argv[1]
    storage = 0

elif sys.argv[2] == "bucket":
    bucket = sys.argv[1]
    whereTo = sys.argv[3]
    storage = 1

keepGoing = 0
s3 = boto3.resource('s3')
requests = s3.Bucket(bucket)

while keepGoing < 10:
    #read the file and return the json object and key
    info = readFile(requests)

    if info == -1:
        print("File was empty, moving on")

    elif info == 0:
        #try 10 times, after that end program
        keepGoing = keepGoing+1
        print("Out of files to read")
        print("Waiting...")
        time.sleep(.1)

    else:
        keepGoing = 0
        key = info[0]
        data = info[1]
        request = data["type"]

        #determine what kind of request the json is
        if request == "create":
            if storage == 1:
                writeToS3(s3,key,data,whereTo)
            elif storage == 0:
                writeToDB(key,data)

        if request == "delete":
            print("This was a delete request")

        if request == "change":
            print("This was a change request")