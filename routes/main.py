from fastapi import FastAPI

from .deploy import router

app = FastAPI()
app.include_router(router)
