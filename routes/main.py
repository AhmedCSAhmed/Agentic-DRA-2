from fastapi import FastAPI
from pydantic import BaseModel

from dra.entry import kickoff_job


app = FastAPI()



class DeployRequest(BaseModel):
    image_name: str
    resource_requirements: dict
    image_id: str    

@app.post(f"/deploy")
async def deploy(request: DeployRequest):
    if request.image_id and request.resource_requirements and request.image_name:
        job = kickoff_job(request.image_id, request.resource_requirements, request.image_name)
        job.kickoff() # this will start the job on the backend 
        
        return {"message": "Deployment initiated"}
    
    return {"message": "Invalid request"}
    