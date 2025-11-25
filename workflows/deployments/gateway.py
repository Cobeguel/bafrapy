from attrs import define, field
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from ray import serve
from ray.serve.handle import DeploymentHandle

app = FastAPI(title="Workflows API")

class SymbolsRequest(BaseModel):
    provider: str

class DataRequest(BaseModel):
    provider: str
    symbol: str

@serve.deployment(name="api")
@serve.ingress(app)
@define
class ApiDeployment:
    worker_handler: DeploymentHandle = field(kw_only=True)

    @app.post("/symbols")
    async def symbols_run(self, request: SymbolsRequest):
        try:
            self.worker_handler.run_symbols.remote(request.provider)
            
            return JSONResponse(status_code=202, content={"enqueued": True})
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/data")
    async def data_run(self, request: DataRequest):
        try:
            self.worker_handler.run_data.remote(request.provider, request.symbol)
            return JSONResponse(status_code=202, content={"enqueued": True})
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))