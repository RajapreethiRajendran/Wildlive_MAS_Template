import json
import logging
import os
import uuid
from typing import Dict, Any, List, Tuple

import requests
from celery import Celery

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

# Initialize Celery app with broker and backend from environment variables or defaults
celery = Celery(
    "annotation_worker",
    broker=os.environ.get("CELERY_BROKER_URL", "pyamqp://guest@localhost//"),
    backend=os.environ.get("CELERY_RESULT_BACKEND", "rpc://"),
)
celery.conf.task_routes = {
    "process_annotation_job": {"queue": os.environ.get("CELERY_QUEUE", "annotation")}
}

def timestamp_now() -> str:
    from datetime import datetime
    return datetime.utcnow().isoformat() + "Z"

def get_agent() -> Dict[str, Any]:
    return {
        "id": "https://example.org/agent/annotation-service",
        "name": "Annotation Service",
        "type": "SoftwareAgent",
    }

def build_wlmo_fragment_selector(annotation: Dict[str, Any]) -> Dict[str, Any]:
    box = annotation.get("boundingBox", {})
    x = int(box.get("x", 0))
    y = int(box.get("y", 0))
    w = int(box.get("width", 0))
    h = int(box.get("height", 0))

    return {
        "@type": "wlmo:FragmentSelector",
        "wlmo:value": f"xywh={x},{y},{w},{h}",
        "wlmo:conformsTo": "http://www.w3.org/TR/media-frags/"
    }

def run_wildlive_detection(image_uri: str) -> Tuple[List[Dict[str, Any]], int, int]:
    """Call WildLive detection API and return detections and image dimensions."""
    payload = {"image_url": image_uri}
    try:
        response = requests.post(
            "https://wildlive.senckenberg.de/run_jaquar_detection",
            json=payload,
            timeout=15
        )
        response.raise_for_status()
        result = response.json()
        return (
            result.get("output", []),
            result.get("image_height", -1),
            result.get("image_width", -1)
        )
    except Exception as e:
        logging.error(f"Detection failed: {e}")
        raise

def map_result_to_wlmo_annotation(
    digital_object: Dict[str, Any],
    organ_detections: List[Dict[str, Any]],
    image_height: int,
    image_width: int
) -> List[Dict[str, Any]]:
    """Map detection results to WLMO annotations."""
    timestamp = timestamp_now()
    agent = get_agent()
    annotations = []

    for det in organ_detections:
        selector = build_wlmo_fragment_selector(det)

        body = {
            "@type": "wlmo:TextualBody",
            "wlmo:vernacularName": det.get("class"),
            "wlmo:confidenceScore": det.get("score")
        }

        annotation = {
            "@context": {
                "wlmo": "https://w3id.org/wlmo#"
            },
            "@type": "wlmo:Annotation",
            "wlmo:creator": agent,
            "wlmo:created": timestamp,
            "wlmo:motivation": "classifying",
            "wlmo:target": {
                "@type": "wlmo:DigitalObject",
                "wlmo:id": digital_object.get("id"),
                "wlmo:hasSelector": selector
            },
            "wlmo:hasBody": body,
            "wlmo:generator": {
                "@id": "https://wildlive.senckenberg.de/wlmo/current/",
                "@type": "wlmo:Software",
                "wlmo:name": "WildLive Detection Service"
            }
        }

        annotations.append(annotation)

    return annotations

def mark_job_as_running(job_id: str) -> None:
    logging.info(f"Marking job {job_id} as running")

def send_failed_message(job_id: str, message: str) -> None:
    logging.error(f"Job {job_id} failed: {message}")
    # TODO: Add actual failure notification (e.g. messaging queue, API callback)

def publish_annotation_event(event: Dict[str, Any]) -> None:
    logging.info(f"Publishing annotation event:\n{json.dumps(event, indent=2)}")
    # TODO: Replace with actual event publishing (e.g. message queue, database, etc.)

@celery.task(name="process_annotation_job")
def process_annotation_job(job_data: Dict[str, Any]):
    try:
        logging.info(f"Processing job: {json.dumps(job_data)}")
        mark_job_as_running(job_data.get("jobId"))

        digital_object = job_data["object"]
        access_uri = digital_object.get("ac:accessURI")

        detections, height, width = run_wildlive_detection(access_uri)
        annotations = map_result_to_wlmo_annotation(digital_object, detections, height, width)

        event = {"annotations": annotations, "jobId": job_data["jobId"]}
        publish_annotation_event(event)

    except Exception as e:
        logging.exception("Error during annotation processing")
        send_failed_message(job_data.get("jobId", "unknown"), str(e))

if __name__ == "__main__":
    # Example test run
    test_input = {
        "jobId": str(uuid.uuid4()),
        "object": {
            "id": "urn:example:1234",
            "type": "DigitalMediaObject",
            "ac:accessURI": "https://example.org/test-image.jpg"
        }
    }
    process_annotation_job(test_input)
