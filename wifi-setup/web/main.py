from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request
import subprocess

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
def show_form(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/", response_class=HTMLResponse)
def submit_wifi(request: Request, ssid: str = Form(...), password: str = Form(...)):
    subprocess.run(["bash", "/home/radxa/wifi-setup/client_connect.sh", ssid, password])
    return templates.TemplateResponse("index.html", {
        "request": request,
        "message": f"{ssid} 연결 시도 중..."
    })
