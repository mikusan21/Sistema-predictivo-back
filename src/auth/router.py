from fastapi import APIRouter
from pathlib import Path

router = APIRouter()

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "auth"


@router.post("/login")
async def login(
    email: str,
    password: str,
    token: str | None = None,
):
    if not email or not password:
        return {
            "status": "error",
            "message": "Email and password are required.",
        }
    
    
    return {
        "status": "success",
    }

