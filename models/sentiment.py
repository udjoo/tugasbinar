from transformers import AutoModelForSequenceClassification
from transformers import AutoTokenizer
from transformers import pipeline

# Huggingface Model
pretrained = "ayameRushia/bert-base-indonesian-1.5G-sentiment-analysis-smsa"
model = AutoModelForSequenceClassification.from_pretrained(pretrained)
tokenizer = AutoTokenizer.from_pretrained(pretrained)
classifier = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)


async def get_sentiment(input):
    return classifier(input)
