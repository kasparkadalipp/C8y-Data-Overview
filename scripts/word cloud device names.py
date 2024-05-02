from wordcloud import WordCloud
from src.utils import readFile

c8y_data = readFile('telia/c8y_data.json')
wordcloudInput = ','.join([device['name'] for device in c8y_data]).replace("_", ' ')

wordcloud = WordCloud(background_color="white", max_words=5000, contour_width=3, contour_color='steelblue', width=1600, height=800)
wordcloud.generate(wordcloudInput)
wordcloud.to_file('../data/telia/wordcloud device names.png')
