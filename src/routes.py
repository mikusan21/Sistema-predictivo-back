from fastapi import APIRouter

from src.data.router import router as data_router
from src.auth.router import router as auth_router

api_router = APIRouter()

api_router.include_router(data_router, prefix="/data", tags=["data"])
api_router.include_router(auth_router, prefix="/auth", tags=["auth"])
