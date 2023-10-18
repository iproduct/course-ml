import time
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
        return aiocoap.Message(payload=self.content)

    async def render_put(self, request):
        print('PUT payload: %s' % request.payload)
        self.set_content(request.payload)
        global ws
        print(ws)
        if ws is not None:
            await ws.send_text(self.content.decode('utf8'))
        return aiocoap.Message(code=aiocoap.CHANGED, payload=self.content)


async def start_coap_server():
    # Resource tree creation
    root = resource.Site()

    root.add_resource(['.well-known', 'core'], resource.WKCResource(root.get_resources_as_linkheader))
    root.add_resource(['sensors'], BlockResource())
    return await aiocoap.Context.create_server_context(root, bind=[SERVER_IP, 5683])



if __name__ == "__main__":
    uvicorn.run("coap:app", port=3000, host="192.168.1.100", reload=True, access_log=False)
