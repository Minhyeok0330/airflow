from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from hdfs import InsecureClient()
import json
import pandas as pd

def analyze_sentiment(comment):
    analyzer = SentimentIntensityAnalyzer()
    result = analyzer.polarity_scores(comment)
    return result

#result = analyze_sentiment('i hate you')
#print(result)

def convert_json_to_csv():
    hdfs_json_path = '/input/yt-data'
    hdfs_csv_path = '/input/yt-data-csv'

    client = InsecureClient('http://llocalhost:9870', user='ubuntu')

    json_files = client.list(hdfs_json_path)

    for json_file in json_files:
        json_file_path = f'{hdfs_json_path}/{json_file}'

        with client.read(json_file_path) as reader:
            data = json.load(reader)

            csv_data = []

            for video_id, comments in data['all_comments'].items():
                for comment in comments:
                    text = comment['text']
                    sentiment = analyze_sentiment(text)
                    csv_data.append({
                        'video_id': video_id,
                        'text': text,
                        'positive': sentiment['pos'],
                        'negative': sentiment['neg'],
                        'neautra': sentiment['neu'],
                        'compound': sentiment['compound']
                        'likeCount': comment['likeCount'],
                        'author': comment['author'],
                    })

            df = pd.DataFrame(csv_data)

            df.to_csv()