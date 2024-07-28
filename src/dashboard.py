import pandas as pd
import streamlit as st

from consumer import get_message
from producer import regions, vendors

st.set_page_config(
    page_title="Workshop Streamlit - Real Time Dashboard",
    page_icon="📈",
    layout="wide",
)


@st.cache_data
def get_data():
    df = pd.read_csv("data/fake_orders.csv")
    df["order_date"] = pd.to_datetime(df["order_date"])
    return df


def new_order():
    dict_data = get_message()
    new_order_df = pd.DataFrame([dict_data])
    new_order_df["order_date"] = pd.to_datetime(new_order_df["order_date"])
    return new_order_df


orders_df = get_data()

st.title("Workshop Streamlit - Real Time Dashboard")


dashboard_placeholder = st.empty()

with st.sidebar:
    st.header("Filters")
    vendor_filter = st.multiselect("Vendor", vendors)
    region_filter = st.multiselect("Region", regions)

while True:

    with dashboard_placeholder.container():

        orders_col, itens_col, ticket_col, total_col = st.columns(4)

        with orders_col:
            quantity = len(orders_df["quantity"])
            quantity = f"{quantity:,}"
            st.metric(label="Orders", value=quantity)

        with itens_col:
            itens = orders_df["quantity"].sum()
            itens = f"{itens:,}"
            st.metric(label="Itens", value=itens)

        with ticket_col:
            ticket = orders_df["total_price"].mean()
            ticket = f"R$ {ticket:,.2f}"
            st.metric(label="Ticket", value=ticket)

        with total_col:
            total = orders_df["total_price"].sum()
            total = f"R$ {total:,.2f}"
            st.metric(label="Total", value=total)

        st.header("📊 Charts")
        order_barchat, region_barchart = st.columns(2)
        with order_barchat:
            st.bar_chart(orders_df, x="region", y="total_price")

        with region_barchart:
            st.bar_chart(orders_df, x="vendor", y="total_price")

        st.header("📈 sales By date")
        orders_df = orders_df.sort_values(by="order_date")
        line_df = orders_df.groupby("order_date").sum().reset_index()
        st.line_chart(line_df, x="order_date", y="total_price")

        st.header("Orders Dashboard")
        st.dataframe(orders_df.iloc[::-1])

        new_order_df = new_order()
        orders_df = pd.concat(
            [orders_df, new_order_df],
            ignore_index=True,
        )
