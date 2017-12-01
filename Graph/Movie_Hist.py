
# coding: utf-8

# In[58]:

import pandas as pd
import random
import numpy as np
import matplotlib.pyplot as plt

movie_data = pd.read_csv('/Users/Blair/Downloads/tmdb_5000_movies.txt', header = None)

movie_data['A'],movie_data['B']= movie_data[0].str.split('\t', 1).str


# In[59]:

movie_score=movie_data['A']
movie_score1=movie_score.astype(float)
movie_score1.hist(bins=50)
plt.xlabel('Movie Score')
plt.ylabel('Frequency')
plt.savefig("Movie_Score.png")
plt.show()


# In[60]:

for i in range(len(movie_data[0])):
    try:
        movie_data[0].iloc[i]=movie_data[0].iloc[i].split('4:')[1]
    except:
        movie_data[0].iloc[i]=None


# In[63]:

movie_data['Duration']=movie_data[0]
movie_data['Duration'].dropna()


# In[65]:

movie_duration=movie_data['Duration']
movie_duration1=movie_duration.astype(float)
movie_duration1.hist(bins=50)
plt.xlabel('Movie Duration')
plt.ylabel('Frequency')
plt.savefig("Movie_Duration.png")
plt.show()


# In[ ]:



