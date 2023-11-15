import asyncio
import datetime
import random
import time
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse

import logging

import aiocoap
import aiocoap.resource as resource


SERVER_IP='192.168.1.101'
ROBOT_IP='192.168.1.102'
WEBAPP_PORT=3000
COAP_PORT=5683

logging.basicConfig(level=logging.INFO)
logging.getLogger("coap-server").setLevel(logging.INFO)

coap_ctx: aiocoap.Context = None
ws: WebSocket = None

html = """
<!DOCTYPE html>
<html>
    <head>
        <title>Chat</title>
    </head>
    <body>
        <h1>WebSocket Chat</h1>
        <form action="" onsubmit="sendMessage(event)">
            <textarea rows="20" type="text" id="messageText" style="width:80%"></textarea>
            <button>Send</button>
        </form>
        <div>Distances: <span id="distances"></span></div>
        <div>Speeds: <span id="speeds"></span></div>
        <ul id='messages'>
        </ul>
        <script>
            var ws = new WebSocket("ws://""" + SERVER_IP + ":" + str(WEBAPP_PORT) + """/ws");
            var distances = document.getElementById('distances')
            var speeds = document.getElementById('speeds')
            var messages = document.getElementById('messages')
            ws.onmessage = function(message) {
                const event = JSON.parse(message.data)
                if(event.type === 'command_ack') {
                    var li = document.createElement('li')
                    var content = document.createTextNode(event.payload)
                    li.appendChild(content)
                    messages.appendChild(li)
                } else if(event.type === 'distance') {
                    distances.innerHTML += `${event.angle} -> ${event.distance}, `
                } else if(event.type === 'move') {
                    speeds.innerHTML = `encoderL: ${event.encoderL}, encoderR: ${event.encoderR}, speedL: ${event.speedL}, speedR: ${event.speedR}`
                }
            };
            function sendMessage(event) {
                var input = document.getElementById("messageText")
                ws.send(input.value)
                // input.value = ''
                event.preventDefault()
            }
        </script>
    </body>
</html>
"""

@asynccontextmanager
async def lifespan(app: FastAPI):
    global coap_ctx
    coap_ctx = await start_coap_server()
    # await send_command('{"command":"SWEEP_DISTANCE"}')
    # coap_ctx = await aiocoap.Context.create_client_context()
    yield
    await coap_ctx.shutdown()


app = FastAPI(lifespan=lifespan, debug=True)

@app.get("/")
async def get():
    return HTMLResponse(html)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    global ws
    ws = websocket
    while True:
        message = await websocket.receive_text()
        start_time = time.time()
        resp = await send_command(message)
        end_time = time.time()
        coap_ctx.log.info(f'CoAP request time: {(end_time - start_time) * 1000} ms')
        await websocket.send_json({'type': 'command_ack', 'payload': resp})


class SensorsResource(resource.Resource):
    """Example resource which supports the GET and PUT methods. It sends large
    responses, which trigger blockwise transfer."""

    def __init__(self):
        super().__init__()
        self.set_content(b"This is the resource's default content. It is padded "
                b"with numbers to be large enough to trigger blockwise "
                b"transfer.\n")

    def set_content(self, content):
        self.content = content

    async def render_get(self, request):
        logging.info("GET /sensors received")
        return aiocoap.Message(payload=self.content)

    async def render_put(self, request):
        print('PUT payload: %s' % request.payload)
        self.set_content(request.payload)
        global ws
        print(ws)
        if ws is not None:
            await ws.send_text(self.content.decode('utf8'))
        return aiocoap.Message(code=aiocoap.CHANGED, payload=self.content)

class TimeResource(resource.ObservableResource):
    """Example resource that can be observed. The `notify` method keeps
    scheduling itself, and calles `update_state` to trigger sending
    notifications."""

    def __init__(self):
        super().__init__()

        self.handle = None

    def notify(self):
        self.updated_state()
        self.reschedule()

    def reschedule(self):
        self.handle = asyncio.get_event_loop().call_later(5, self.notify)

    def update_observation_count(self, count):
        if count and self.handle is None:
            print("Starting the clock")
            self.reschedule()
        if count == 0 and self.handle:
            print("Stopping the clock")
            self.handle.cancel()
            self.handle = None

    async def render_get(self, request):
        logging.info("GET /time received")
        payload = datetime.datetime.now().\
                strftime("%Y-%m-%d %H:%M").encode('ascii')
        return aiocoap.Message(payload=payload)

class WhoAmI(resource.Resource):
    async def render_get(self, request):
        logging.info("GET /whoami received")
        text = ["Used protocol: %s." % request.remote.scheme]

        text.append("Request came from %s." % request.remote.hostinfo)
        text.append("The server address used %s." % request.remote.hostinfo_local)

        claims = list(request.remote.authenticated_claims)
        if claims:
            text.append("Authenticated claims of the client: %s." % ", ".join(repr(c) for c in claims))
        else:
            text.append("No claims authenticated.")

        return aiocoap.Message(content_format=0, payload="\n".join(text).encode('utf8'))


async def start_coap_server():
    # Resource tree creation
    root = resource.Site()

    root.add_resource(['.well-known', 'core'], resource.WKCResource(root.get_resources_as_linkheader))
    root.add_resource(['time'], TimeResource())
    root.add_resource(['whoami'], WhoAmI())
    root.add_resource(['sensors'], SensorsResource())
    return await aiocoap.Context.create_server_context(root, bind=("0.0.0.0", COAP_PORT))

async def send_command(message):
    logging.info(f'Sending to ESP32[coap://{ROBOT_IP}:{COAP_PORT}/commands]: {message}')
    req = aiocoap.Message(code=aiocoap.PUT, token=bytes(random.randint(1, 255)), payload=message.encode(encoding='utf-8'),
                          uri=f'coap://{ROBOT_IP}:{COAP_PORT}/commands')

    try:
        response = await coap_ctx.request(req).response
    except Exception as e:
        print('Failed to fetch resource:')
        print(e)
    else:
        print('Code: %r, Token: %d\nResult: %s\n' % (response.code, int(response.token[0]), response.payload.decode('utf8')))
        return response.payload.decode('utf8')

if __name__ == "__main__":
    uvicorn.run("coap:app", port=3000, host="0.0.0.0", reload=True, access_log=False)
