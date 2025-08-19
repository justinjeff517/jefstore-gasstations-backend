import json
def main(event, context):
    message = "Hello World from jefstore-gasstations-namespace"

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": message,

        })
    }
