import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

@st.cache_resource
def create_session():
    connection_parameters = {
        "account": st.secrets["snowflake"]["account"],
        "user": st.secrets["snowflake"]["user"],
        "password": st.secrets["snowflake"]["password"],
        "role": st.secrets["snowflake"]["role"],
        "warehouse": st.secrets["snowflake"]["warehouse"],
        "database": st.secrets["snowflake"]["database"],
        "schema": st.secrets["snowflake"]["schema"]
    }
    return Session.builder.configs(connection_parameters).create()

session = create_session()

st.title(f"🥤Customize Your Smoothie🥤")
st.write(
  """Choose the fruits you want in your customize Smoothie!
  """
)

name_on_order = st.text_input('Name on Smoothie:')
st.write('The name on your Smoothie will be:', name_on_order)


my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
# st.dataframe(data=my_dataframe, use_container_width=True)

ingredient_list = st.multiselect(
    'Choose up to 5 ingredients:',
    my_dataframe,
    max_selections=5
)

if ingredient_list:
    ingredient_string = ''

    for fruit_chosen in ingredient_list:
        ingredient_string += fruit_chosen + ' '

    my_insert_stmt = f"""
    INSERT INTO smoothies.public.orders (INGREDIENTS, NAME_ON_ORDER)
    VALUES ('{ingredient_string}', '{name_on_order}')
"""

   
    # st.write(my_insert_stmt)
    time_to_insert = st.button('Submit Order')

    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        
        st.success(f'Your Smoothie is ordered, {name_on_order}!', icon="✅")
