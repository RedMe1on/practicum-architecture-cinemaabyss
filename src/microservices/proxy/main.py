import datetime
from fastapi import FastAPI, HTTPException, Query, Request, Response
import random
import os
from pydantic import BaseModel
import requests
from conf import Settings

app = FastAPI(
    title="Proxy  API",
    version="1.0.0",
)

conf = Settings.read_or_update()
services = {
    "movies": {
        "v1": conf.MONOLITH_URL,
        "v2": conf.MOVIES_SERVICE_URL,
        "enable": conf.GRADUAL_MIGRATION,
        "traffic_percentage": conf.MOVIES_MIGRATION_PERCENT,
    }
}

@app.get('/health')
def health():
    return {'health': True}

@app.api_route(
    "/api/{service_name}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"]
)
async def proxy_view(request: Request, service_name: str, path: str = "") -> Response:
    target_url = get_target_url(service_name=service_name)
    response = await proxy_request(target_url=target_url, request=request)

    headers = dict(response.headers)

    headers.pop("content-encoding", None)
    headers.pop("transfer-encoding", None)
    return Response(
        content=response.content, status_code=response.status_code, headers=headers
    )

@app.api_route(
    "/api/{service_name}/{path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"]
)
async def proxy_path_view(request: Request, service_name: str, path: str = "") -> Response:
    target_url = get_target_url(service_name=service_name)
    response = await proxy_request(target_url=target_url, request=request)

    headers = dict(response.headers)

    headers.pop("content-encoding", None)
    headers.pop("transfer-encoding", None)
    return Response(
        content=response.content, status_code=response.status_code, headers=headers
    )


async def proxy_request(target_url: str, request: Request) -> requests.Response:
    headers = dict(request.headers)
    headers.pop("host", None)
    body = await request.body()
    url = f"{target_url}{request.url.path}"
    return requests.request(
        method=request.method,
        url=url,
        headers=headers,
        cookies=request.cookies,
        params=request.query_params,
        allow_redirects=False,
        data=body if body else None,
    )


def get_target_url(service_name: str):
    if service_name not in services:
        return conf.MONOLITH_URL

    service = services[service_name]

    if not service["enable"]:
        return service["v1"]

    rand_val = random.uniform(0, 100)

    if rand_val <= service["traffic_percentage"]:
        return service["v2"]
    else:
        return service["v1"]


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ["PORT"])
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
