import json

def handler(event, context):

    print(event)

    response = {
        'statusCode': 200,
        'body': json.dumps("Hello from Lambda")
    }

    return response
