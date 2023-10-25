import uvicorn
from fastapi import FastAPI

app = FastAPI(debug=True)

@app.get("/api/events")
async def hello():
    return {"message": "Hello World"}

if __name__ == "__main__":
    uvicorn.run("main:app", port=3000, host="192.168.1.100", reload=True, access_log=False)