from fastapi import FastAPI
import time
from fastapi.responses import JSONResponse

app = FastAPI()

@app.get("/")

def visit(
    store_name: str, year: int, month: int, day: int, sensor_id: int | None = None
) -> JSONResponse:
    return JSONResponse(status_code=200, content=f"Hello {store_name}")
