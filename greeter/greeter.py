#!/usr/bin/env python

from messages_pb2 import SeenCount, GreetRequest, GreetResponse

from statefun import StatefulFunctions
from statefun import RequestReplyHandler
from statefun import kafka_egress_record

import base64

functions = StatefulFunctions()


@functions.bind("example/greeter")
def greet(context, greet_request: GreetRequest):
    state = context.state('seen_count').unpack(SeenCount)
    if not state:
        state = SeenCount()
        state.seen = 1
        state.sum = 0
    else:
        state.seen += 1
        state.sum += greet_request.val
    context.state('seen_count').pack(state)

    response = compute_greeting(greet_request.name, state.seen)
    response.val = greet_request.val

    egress_message = kafka_egress_record(topic="greetings", key="vals".encode('utf-8'), value=state)
    context.pack_and_send_egress("example/greets", egress_message)


def compute_greeting(name, seen):
    """
    Compute a personalized greeting, based on the number of times this @name had been seen before.
    """
    templates = ["", "Welcome %s", "Nice to see you again %s", "Third time is a charm %s"]
    if seen < len(templates):
        greeting = templates[seen] % name
    else:
        greeting = "Nice to see you at the %d-nth time %s!" % (seen, name)

    response = GreetResponse()
    response.name = name
    response.greeting = greeting

    return response


handler = RequestReplyHandler(functions)

def decode_request(request):
    return base64.b64decode(request["body"])

def build_response(response_bytes):
    response_base64 = base64.b64encode(response_bytes).decode('ascii')
    response = {
        "isBase64Encoded": True,
        "statusCode": 200,
        "headers": { "Content-Type": "application/octet-stream"},
        "multiValueHeaders": {},
        "body": response_base64
    }
    return response

def main(args):
    message_bytes = decode_request(args.__ow_body)
    response_bytes = handler(message_bytes)
    return build_response(response_bytes)

#
# Serve the endpoint
#

# from flask import request
# from flask import make_response
# from flask import Flask

# app = Flask(__name__)


# @app.route('/statefun', methods=['POST'])
# def handle():
#     response_data = handler(request.data)
#     response = make_response(response_data)
#     response.headers.set('Content-Type', 'application/octet-stream')
#     return response


# if __name__ == "__main__":
#     app.run()
