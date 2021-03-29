import boto3
import json
import sys
import time
import logging

def readFromBucket(requests):
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

    #delete object from bucket
    key = lowest.key
    lowest.delete()
    logging.warning("Object deleted from: " + requests.name)

    if len(body) < 4:
        return -1
    
    return (key,json.loads(body))

def getWidgetFromSQS(sqs, url):
    # message = queue.receive_messages()
    # print(message['Messages'])
    queue_url = 'https://sqs.us-east-1.amazonaws.com/912483513202/cs5260-requests'
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )

    #if no messages, return 0
    if(len(response) < 2):
        return 0
    
    #get the message and the recipt handler to delete the message
    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    final = json.loads(message['Body'])

    # sqs.delete_message(QueueUrl=queue_url,ReceiptHandle=receipt_handle)
    return (final[0],final[1])

def writeToS3(s3, key, data, bucket):
    name = data["owner"]
    name = name.replace(" ", "-")

    path = "widgets/"+name+"/"+ str(key)

    #obj = s3.put_object(bucket, key, data)
    obj = s3.Object(bucket,path)
    obj.put(Body=json.dumps(data))
    logging.warning("Object uploaded to  : " + bucket)
    print("writing to bucket")

def writeToDB(table, key, data):
    table.put_item(Item=data)
    print("writing to database")
    logging.warning('item added to database')

def deleteFromS3(whereTo, key):
    obj = s3.Object(whereTo,key)
    obj.delete()
    # print(obj)

def delteFromDB():
    print("deleting")

# CL syntax
# {where to read from} {what type of read (sqs/bucket)} {where to write to (bucket/db)} {bucket to write to (if applicable)}

logging.basicConfig(format='%(asctime)s %(message)s:', filename = "logs.txt")

#use command line arguments
s3 = boto3.resource('s3')
if len(sys.argv) <= 1:
    #if no arguments
    # source = 'usu-cs5260-hackley-requests'
    # requests = s3.Bucket(source)
    whereTo = 'usu-cs5260-hackley-web'
    storage = 1
    sourceType = 0
    sqs = boto3.client('sqs')
    sqsUrl = 'https://sqs.us-east-1.amazonaws.com/912483513202/cs5260-requests'

else :
    #get input type
    if sys.argv[2] == 'sqs':
        sqs = boto3.client('sqs')
        sqsUrl = 'https://sqs.us-east-1.amazonaws.com/912483513202/cs5260-requests'
        source = sys.argv[1]
        sourceType = 0

    if sys.argv[2] == 'bucket':
        source = sys.argv[1]
        requests = s3.Bucket(source)
        sourceType = 1

    if sys.argv[3] == "db":
        storage = 0
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('widgets')

    if sys.argv[3] == "bucket":
        whereTo = sys.argv[3]
        storage = 1

keepGoing = 0

while keepGoing < 5:
    #read the file and return the json object and key
    if sourceType == 1:
        info = readFromBucket(requests)
    elif sourceType == 0:
        info = getWidgetFromSQS(sqs, sqsUrl)
    

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
                writeToDB(table,key,data)

        if request == "delete":
            print("This was a delete request")
            print("request " + request)
            if storage == 1:
                deleteFromS3(whereTo, key)
            elif storage == 0:
                delteFromDB()

        if request == "change":
            print("This was a change request")