import asyncio
import datetime
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse

import logging

import aiocoap
import aiocoap.resource as resource


SERVER_IP='192.168.1.100'
WEBAPP_PORT=3000
ROBOT_IP='192.168.1.101'

logging.basicConfig(level=logging.INFO)
logging.getLogger("coap-server").setLevel(logging.INFO)

coap_ctx: aiocoap.Context = None
ws: WebSocket = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global coap_ctx
    coap_ctx = await start_coap_server()
    # coap_ctx = await aiocoap.Context.create_client_context()
    yield
    await coap_ctx.shutdown()


app = FastAPI(lifespan=lifespan, debug=True)

@app.get("/api/events")
async def hello():
    return {"message": "Hello World"}

class BlockResource(resource.Resource):
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
        payload = datetime.datetime.now().\
                strftime("%Y-%m-%d %H:%M").encode('ascii')
        return aiocoap.Message(payload=payload)

class WhoAmI(resource.Resource):
    async def render_get(self, request):
        text = ["Used protocol: %s." % request.remote.scheme]

        text.append("Request came from %s." % request.remote.hostinfo)
        text.append("The server address used %s." % request.remote.hostinfo_local)

        claims = list(request.remote.authenticated_claims)
        if claims:
            text.append("Authenticated claims of the client: %s." % ", ".join(repr(c) for c in claims))
        else:
            text.append("No claims authenticated.")

        return aiocoap.Message(content_format=0,
                payload="\n".join(text).encode('utf8'))


async def start_coap_server():
    # Resource tree creation
    root = resource.Site()

    root.add_resource(['.well-known', 'core'], resource.WKCResource(root.get_resources_as_linkheader))
    root.add_resource(['time'], TimeResource())
    root.add_resource(['whoami'], WhoAmI())
    root.add_resource(['sensors'], BlockResource())
    return await aiocoap.Context.create_server_context(root, bind=[SERVER_IP, 5683])



if __name__ == "__main__":
    uvicorn.run("coap:app", port=3000, host="192.168.1.100", reload=True, access_log=False)
