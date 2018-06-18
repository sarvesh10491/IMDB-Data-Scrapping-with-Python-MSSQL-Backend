from requests import get
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sqlalchemy import create_engine
import urllib
import time

start_time = time.time()

movie_name = []
movie_year = []
movie_runtime = []
movie_genre = []
movie_rating = []
movie_gross = []

#################################
# Extract Data from imdb website

for i in range(1,6):
    url = 'https://www.imdb.com/search/title?groups=top_250&my_ratings=exclude&sort=user_rating&page='+str(i)+'&ref_=adv_nxt'
    response = get(url)
    # print(response.text[:500])

    imdb_page = BeautifulSoup(response.text, 'html.parser')

    movie_containers = imdb_page.find_all('div', class_ = 'lister-item-content')

    # print(len(movie_containers))

    for i in range(len(movie_containers)):
        cur_movie = movie_containers[i]

        cur_movie_name = cur_movie.h3.a.text
        movie_name.append(cur_movie_name)

        cur_movie_year = cur_movie.h3.find('span', class_ = 'lister-item-year text-muted unbold').text
        movie_year.append(cur_movie_year)

        cur_movie_runtime = cur_movie.find('span', class_ = 'runtime').text
        movie_runtime.append(int(cur_movie_runtime.split(" ")[0]))

        cur_movie_genre = cur_movie.find('span', class_ = 'genre').text
        movie_genre.append(movie_genre)

        cur_movie_rating = cur_movie.strong.text
        movie_rating.append(float(cur_movie_rating))

        cur_movie_stats = cur_movie.find('p', attrs={'class': 'sort-num_votes-visible'}).find_all('span', attrs = {'name':'nv'})
        if len(cur_movie_stats)!=1:
            tmp=(list(cur_movie_stats[1]['data-value']))
            if ',' in tmp:
                tmp.remove(',')
            if ',' in tmp:
                tmp.remove(',')
            if ',' in tmp:
                tmp.remove(',')
            movie_gross.append(int("".join(tmp)))
        else:
            movie_gross.append(0)


#######################################################
# Pandas dataframe for MSSQL server database
imdb_df = pd.DataFrame({'movie': movie_name,
                       'year': movie_year,
                       'runtime': movie_runtime,
                       'genre': movie_genre,
                       'rating': movie_rating,
                       'gross': movie_gross})
print(imdb_df.info())

params = urllib.parse.quote_plus(r'DRIVER={SQL Server};SERVER=SARVESH; DATABASE=testdb; Trusted_Connection=yes')
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
engine = create_engine(conn_str)

# write the DataFrame to a table in the sql database
imdb_df.to_sql("imdb_tab", engine)

print(" %s seconds" % (time.time() - start_time))

####################################################
# Plot

# create data array
x = np.array(movie_runtime)
y = np.array(movie_rating)
z = np.array(movie_gross)


# use the scatter function
plt.scatter(x, y, s=z//1000000, alpha=0.5)
plt.show()