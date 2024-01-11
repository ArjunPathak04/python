#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests



# In[2]:


pip install pandasql


# In[ ]:





# In[3]:


import numpy as np
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())


# In[ ]:





# In[4]:


# Provided data
data = {
    'dashboard': ['lens_overall', 'lens_users', 'lens_profiles', 'lens_posts', 'lens_comments', 'lens_mirror',
                  'farecaster_overall', 'farecaster_users', 'farecaster_actions', 'farecaster_revenue',
                  'friendtech_overall', 'friendtech_users', 'friendtech_tvl'],
    'query_id': [3223682, 3223692, 3223700, 3223705, 3224602, 3224610, 3225122, 3225244, 3225293, 3225370,
                 3226756, 3229787, 3229793]
}

# Create DataFrame
df = pd.DataFrame(data)


# In[ ]:





# In[5]:


df


# In[6]:


query_ids = df['query_id']
dashboard = df['dashboard']


# In[7]:


key = 'IGDMjWb6Vp9aQoazWyIb55YQ6Ie6UyNz'


# In[8]:


# for i in range(len(df)):
#     #print(i)
#     res = requests.get(f'https://api.dune.com/api/v1/query/{df["query_id"][i]}/results?api_key={key}')
#     if res.status_code == 200:
#         social_data = res.json()
        
#         temp=pd.DataFrame(social_data['result']['rows'])
#         temp['dashboard_name'] = df["dashboard"][i]
#         temp.reset_index(inplace=True)
#         temp.rename(columns={"index":"uuid"},inplace=True)
#         temp['uuid'] = df["dashboard"][i]+'_'+temp['uuid'].astype(str)
#         dsh_df = pd.concat([dsh_df,temp],ignore_index=True)
        
            
       
       
    
    


# In[9]:


try:
    existing_data = pd.read_csv('social.csv')  # Replace with your file name and path
except FileNotFoundError:
    existing_data = pd.DataFrame()


# In[10]:


d = {}
for i in range(len(df)):
    res = requests.get(f'https://api.dune.com/api/v1/query/{df["query_id"][i]}/results?api_key={key}')
    if res.status_code == 200:
        social_data = res.json()
        
        d[dashboard[i]]=pd.DataFrame(social_data['result']['rows'])
        
            
       
       
    
    


# In[ ]:





# In[11]:


d.keys()


# ## Daily user data of lens, forecaster and friend.tech

# In[12]:


# daily user data of lens protocal

len_daily_user = d['lens_users'][['day','daily_users']]

len_daily_user['project'] = 'lens'
len_daily_user.head()


# In[13]:


# daily user data of lens farecaster


forcaster_daily_user = d['farecaster_users'][['day','daily_users']]
forcaster_daily_user['project'] = 'farecaster'
forcaster_daily_user.head()


# In[14]:


# daily user data of farecaster

frnd_tech_daily_user = d['friendtech_users'][['day','daily_traders']]

frnd_tech_daily_user['project'] = 'friend.tech'

frnd_tech_daily_user.rename(columns={'daily_traders':'daily_users'}, inplace=True)

frnd_tech_daily_user.head()


# #### Final dataframe for Daily user data of lens, forecaster and friend.tech

# In[15]:


daily_user_df = pd.concat([len_daily_user,forcaster_daily_user, frnd_tech_daily_user])


# In[16]:


daily_user_df


# In[17]:


da = daily_user_df.copy()


# In[18]:


da


# In[19]:


da.columns


# In[20]:


da['cumulative_user'] = 'null'


# In[ ]:





# In[21]:


da['day'].dtype


# In[ ]:





# In[22]:


da=da[['day','project','cumulative_user','daily_users']]


# In[23]:


da


# In[ ]:





# In[ ]:





# ## Daily revenue data of lens, forecaster and friend.tech

# In[24]:


lens_daily_rev = d['lens_users'][['day']]
lens_daily_rev['daily_revenue_usd'] = 0
lens_daily_rev['project'] = 'lens'
lens_daily_rev.head()


# In[25]:


# daily revenue data of  farecaster


forcaster_daily_rev = d['farecaster_revenue'][['day','daily_revenue_usd']]
forcaster_daily_rev['project'] = 'farecaster'
forcaster_daily_rev.head()


# In[26]:


# daily revenue data of friend.tech


frndtech_daily_rev = d['friendtech_users'][['day','daily_revenue_USD']]

frndtech_daily_rev.rename(columns={'daily_revenue_USD':'daily_revenue_usd'},inplace=True)

frndtech_daily_rev['project'] = 'friend.tech'
frndtech_daily_rev.head()


# In[ ]:





# ### Final dataframe for Daily Revenue of lens, forecaster and friend.tech

# In[27]:


revenue_df = pd.concat([lens_daily_rev,forcaster_daily_rev, frndtech_daily_rev])


# In[28]:


revenue_df


# In[29]:


qury = """SELECT 
    day,
    project,
    SUM(daily_revenue_usd) OVER(PARTITION BY project ORDER BY day) AS cumulative_revenue_usd,
    daily_revenue_usd
FROM revenue_df
order by day desc
"""


# In[30]:


rev_df =pysqldf(qury)


# In[31]:


rev_df


# In[ ]:





# ## Cumulative & Daily users action data of lens, forecaster and friend.tech 

# In[32]:


# daily users reaction  data of lens protocal


lens_user_action = d['lens_users'][['day','cumulative_actions','daily_actions']]
lens_user_action['project'] = 'lens'


lens_user_action_df= lens_user_action.reindex(columns=['day','project','cumulative_actions','daily_actions'])

lens_user_action_df.tail()


# In[33]:


# daily users action  data of forecaster


forecas_user_act = d['farecaster_users'][['day','cumulative_reactions', 'daily_reactions',]]

forecas_user_act.rename(columns={'daily_reactions':'daily_actions'},inplace=True)
forecas_user_act.rename(columns={'cumulative_reactions':'cumulative_actions'},inplace=True)


forecas_user_act['project'] = 'farecaster'

forecas_user_act_df= forecas_user_act.reindex(columns=['day','project','cumulative_actions','daily_actions'])


forecas_user_act_df.tail()


# In[34]:


# daily users action  data of friend.tech


frndtech_user_act = d['friendtech_users'][['day','daily_trades']]
frndtech_user_act['project'] = 'friend.tech'
frndtech_user_act.rename(columns={'daily_trades':'daily_actions'},inplace=True)
frndtech_user_act


# In[35]:


#sql query to find cumulative reactions
testdf = frndtech_user_act.copy()

qry = """SELECT 
    day,
    project,
    SUM(daily_actions) OVER(ORDER BY day) AS cumulative_actions,
    daily_actions
FROM testdf
order by day desc
"""


# In[36]:


frndtech_user_act_df = pysqldf(qry)


# In[37]:


frndtech_user_act_df


# In[ ]:





# ###  Final dataframe for Daily user actions of lens, forecaster and friend.tech 

# In[38]:


cum_daily_user_reac_df = pd.concat([lens_user_action,forecas_user_act_df, frndtech_user_act_df])


# In[39]:


cum_daily_user_reac_df


# In[ ]:





# In[ ]:





# ## Final Social dashboard dataframe (combining user, revenue & user_actions data) 

# In[40]:


social_dashboard = da.merge(rev_df,on=['day','project']).merge(cum_daily_user_reac_df,on=['day','project'])


# In[41]:


# Append the new data to the existing dataset

# combined_data = pd.concat([existing_data, social_dashboard_df], ignore_index=True)

# # Step 3: Save the combined dataset
# combined_data.to_csv('social.csv', index=False)  # Replace with your file name and path


# In[43]:


social_dashboard.head()


# In[ ]:





# In[44]:


#saving data locally
social_dashboard.to_csv(r'/Users/arjunpathak/Desktop/data_scrap_csv/social_dashboard_data.csv')


# In[ ]:





# In[ ]:




