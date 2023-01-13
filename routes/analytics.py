from fastapi import APIRouter
from services.analytics import AnalyticServices



router = APIRouter()

@router.get("/sentiment")
async def sentiment_analytics(
    text:str
):
    result = await AnalyticServices().get_sentiment_analytics(text=text)
    return result
