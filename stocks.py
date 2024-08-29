import streamlit as st
import pandas as pd
import base64
import plotly.graph_objects as go
import yfinance as yf
import subprocess
import tempfile

st.title('S&P 500 App')

#st.set_option('deprecation.showPyplotGlobalUse', False)

st.markdown("""
This app retrieves the list of the **S&P 500** (from Wikipedia) and its corresponding **stock closing price** (year-to-date)!
* **Python libraries:** base64, pandas, streamlit, yfinance, plotly
* **Data source:** [Wikipedia](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies).
""")

st.sidebar.header('User Input Features')

@st.cache_data
def load_data():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    html = pd.read_html(url, header=0)
    df = html[0]
    return df

df = load_data()
sector = df.groupby('GICS Sector')

sorted_sector_unique = sorted(df['GICS Sector'].unique())
selected_sector = st.sidebar.multiselect('Sector', sorted_sector_unique, sorted_sector_unique)

df_selected_sector = df[df['GICS Sector'].isin(selected_sector)]

st.header('Display Companies in Selected Sector')
st.write('Data Dimension: ' + str(df_selected_sector.shape[0]) + ' rows and ' + str(df_selected_sector.shape[1]) + ' columns.')
st.dataframe(df_selected_sector)

def filedownload(df):
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="SP500.csv">Download CSV File</a>'
    return href

st.markdown(filedownload(df_selected_sector), unsafe_allow_html=True)

data = yf.download(
    tickers=list(df_selected_sector[:10].Symbol),
    period="ytd",
    interval="1d",
    group_by='ticker',
    auto_adjust=True,
    prepost=True,
    threads=True,
    proxy=None
)

def price_plot(symbol):
    df = pd.DataFrame(data[symbol].Close)
    df['Date'] = df.index
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.Date, y=df.Close, mode='lines+markers', name=symbol))
    fig.update_layout(
        title=symbol,
        xaxis_title='Date',
        yaxis_title='Closing Price',
        xaxis_rangeslider_visible=True
    )
    st.plotly_chart(fig)

num_company = st.sidebar.slider('Number of Companies', 1, 10)

if st.button('Show Plots'):
    st.header('Stock Closing Price')
    for i in list(df_selected_sector.Symbol)[:num_company]:
        price_plot(i)

if st.button('Run MapReduce Job'):
    st.header('MapReduce Results')

    # Save the selected data to a temporary CSV file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
        df_selected_sector.to_csv(temp_file.name, index=False)
        input_csv = temp_file.name
    
    # Debug print for input file
    # st.write(f"Input CSV file path: {input_csv}")
    st.write(pd.read_csv(input_csv).head())

    # Run the MapReduce job
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
        temp_file.close()
        output_csv = temp_file.name
        subprocess.run(['python', 'kpi_mapreduce.py', '--input', input_csv, '--output', output_csv])

        # # Debug print for output file
        # st.write(f"Output CSV file path: {output_csv}")
        # mapreduce_results = pd.read_csv(output_csv)
        # st.write(mapreduce_results)

### FOR THIS MAP REDUCE IS DONE IN kpi_mapreduce.py ###########
