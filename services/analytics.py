import pandas as pd
from collections import Counter
from services import AppServiceProject
from models.sentiment import get_sentiment


class AnalyticServices(AppServiceProject):
    async def get_sentiment_analytics(self, text):
        try:
            sentiment = await get_sentiment(text)

            data = {
                "data": sentiment
            }

            return self.success_response(data)
        except Exception as e:
            return self.error_response(e)
