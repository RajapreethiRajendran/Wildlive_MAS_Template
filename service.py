import logging
from typing import List, Dict

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pydantic_settings import BaseSettings

# -----------------------
# Settings
# -----------------------

class Settings(BaseSettings):
    log_level: str = "DEBUG"
    port: int = 8000
    host: str = "0.0.0.0"

settings = Settings()
logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.DEBUG))

# -----------------------
# FastAPI App
# -----------------------

app = FastAPI()


# -----------------------
# Request/Response Models
# -----------------------

class ImageRequest(BaseModel):
    image_url: str

class ProcessedMessageRequest(BaseModel):
    image_url: str
    output: List[Dict]
    image_height: int
    image_width: int

class ErrorMessageRequest(BaseModel):
    image_url: str
    error: str


# -----------------------
# Dummy Detection Logic
# -----------------------

def process_jaquar_detection(image_url: str) -> ProcessedMessageRequest:
    # This should be replaced with real detection logic
    dummy_output = [
        {
            "class": "Jaquar",
            "score": 0.91,
            "boundingBox": {"x": 30, "y": 50, "width": 100, "height": 120}
        }
    ]
    return ProcessedMessageRequest(
        image_url=image_url,
        output=dummy_output,
        image_height=1024,
        image_width=768
    )


# -----------------------
# Endpoint
# -----------------------

@app.post("/run_jaquar_detection", response_model=ProcessedMessageRequest)
async def run_detection(request: ImageRequest):
    try:
        logging.info(f"Processing input image: {request.image_url}")

        if not request.image_url:
            raise HTTPException(status_code=400, detail="No image_url provided.")

        result = process_jaquar_detection(request.image_url)
        return result

    except Exception as e:
        logging.error(f"Error during detection: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing image: {str(e)}")


# -----------------------
# Entrypoint
# -----------------------

if __name__ == "__main__":
    uvicorn.run(app, host=settings.host, port=settings.port)
