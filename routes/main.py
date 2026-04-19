from fastapi import FastAPI
from .deploy import deploy, router

app = FastAPI()
app.include_router(router)
