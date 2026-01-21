import streamlit as st
from snowflake.snowpark.functions import col, when_matched

session = get_active_session()
og_dataset = session.table("smoothies.public.orders")

st.title("🥤Customize Your Smoothie🥤")
st.write("Orders that need to be filled.")

# 1. Load rows that are not yet filled
orders_df = og_dataset.filter(col("ORDER_FILLED") == 0)

pending_count = og_dataset.filter(col("ORDER_FILLED") == 0).count()
if pending_count == 0:
    st.success("There are no more pending orders right now.", icon="👍")
else:
    orders_df = og_dataset.filter(col("ORDER_FILLED") == 0)
    editable_df = st.data_editor(orders_df)

    submitted = st.button("Submit")

    if submitted:
        edited_dataset = session.create_dataframe(editable_df)
        try:
            og_dataset.merge(
                edited_dataset,
                og_dataset["ORDER_UID"] == edited_dataset["ORDER_UID"],
                [
                    when_matched().update(
                        {"ORDER_FILLED": edited_dataset["ORDER_FILLED"]}
                    )
                ],
            )
            st.success("Orders updated.", icon="👍")
        except Exception as e:
            st.write("Something went wrong:", e)

cnx = st.connection("snowflake")
session = cnx.session()
