import pandas as pd
from collections import Counter
from libs.nlp import preprocess_tweets, preprocess_file
from services import AppServiceProject
from io import BytesIO
from fastapi.responses import StreamingResponse
import io
import sqlite3


class CleansingServices(AppServiceProject):
    async def cleansing(self, type, text):
        try:
            if type == "text":
                preprocess = preprocess_tweets(text)

                data = {
                    "data": preprocess
                }

                return self.success_response(data)
            else:
                preprocess = preprocess_file(text)
                #connect db
                conn = sqlite3.connect("db/tugas.db")
                #create table
                conn.execute(''' CREATE TABLE tugas
                (Tweet varchar(255),
                HS int,
                Abusive int,
                HS_Individual int,
                HS_Group int,
                HS_Religion int,
                HS_Race int,
                HS_Physical int,
                HS_Gender int,
                HS_Other int,
                HS_Weak int,
                HS_Moderate int,
                HS_Strong int);
                ''')
                #convert sql
                preprocess.to_sql('tugas', conn, if_exists='replace', index=False)
                #close connection
                conn.commit()
                conn.close()

                stream = io.StringIO()
                preprocess.to_csv(stream, index=False)

                response = StreamingResponse(iter([stream.getvalue()]),
                                             media_type="text/csv"
                                             )
                response.headers["Content-Disposition"] = "attachment; filename=data_cleansing.csv"

                return response
        except Exception as e:
            return self.error_response(e)
