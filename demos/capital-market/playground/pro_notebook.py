
import ipywidgets as widgets
toolbar = widgets.Output()
toolbar



import  prospective as pro
import  pandas as pd, pyarrow, asyncio, threading
import  ipywidgets as widgets, datetime as dt
from    pyodide.http import pyfetch
from    urllib.parse import urlencode


# =============================================
# FETCH HISTORICAL STOCK DATA FROM PROSPECTIVE
# =============================================
table = await pro.open_table("historical_stocks")
df = await (await table.view()).to_dataframe()

# DataFrame Schema:
#   date       : Trading date (datetime)
#   open       : Opening price (float)
#   close      : Closing price (float)
#   high       : Highest price during trading (float)
#   low        : Lowest price during trading (float)
#   volume     : Trading volume (int)
#   symbol     : Stock symbol (str)
#   sector     : Market sector (str)
#   industry   : Stock industry (str)
#   index      : Index name where the stock is included (str)
df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
print("Metadata:")
print(f"  Total Rows: {len(df)}")
print(f"  Unique Sectors: {list(df['sector'].unique())}")
print(f"  Unique Indexes: {list(df['index'].unique())}")
# Metadata:
#   Total Rows: 126505
#   Unique Sectors: ['Information Technology', 'Consumer Discretionary', 'Industrials', 'Energy', 'Communication Services']
#   Unique Indexes: ['NASDAQ 100', 'S&P 500', 'Dow Jones Industrial Average', 'Russell 1000']

# =============================================
# Initialize Propsective Tables and Viewers
# =============================================

# Initialize a Prospective table, indexed by the timestamp
table = await pro.table(df.iloc[:10].to_dict(orient="records"), name="stock_playback", limit=(22 * 10 * 365))
# get viewers
# viewer = await pro.get_viewer("Berlin Weather Playback")

# =============================================
# iPyWidgets Controls: Pin to Top
# =============================================

# widgets::
# playback speed slider (milli-seconds)
pbi_slider = widgets.IntSlider(
    value=150, min=100, max=1000, step=50,
    continuous_update=False,
    layout=widgets.Layout(width='90%'),
    readout=False,
)
pbi_label = widgets.Label(value=f"Play Back Speed: {pbi_slider.value} ms")
# rows per interval slider
rps_slider = widgets.IntSlider(
    value=10, min=1, max=200, step=5,
    continuous_update=False,
    layout=widgets.Layout(width='90%'),
    readout=False,
)
rpi_label = widgets.Label(value=f"Rows Per Interval: {(rps_slider.value * 100):,d}")
# Add ToggleButtons for selecting sectors.
unique_sectors = ['Information Technology', 'Consumer Discretionary', 'Industrials', 'Energy', 'Communication Services']
sectors = widgets.ToggleButtons(
    options=unique_sectors,
    description='Sectors:',
    disabled=False,
    button_style='', # 'success', 'info', 'warning', 'danger' or ''
)

info_label = widgets.Label(value="Nada!")

# event handlers for the widgets

# date range slider event handler
def _on_change_pbi(change):
    pbi_label.value = f"Play Back Speed: {change['new']} ms"

def _on_change_rpi(change):
    rpi_label.value = f"Rows Per Interval: {(change['new'] * 100):,d} rsp"

def _on_change_sectors(change):
    info_label.value = f"Sector selection changed: {change["new"]}"

pbi_slider.observe(_on_change_pbi, names='value')
rps_slider.observe(_on_change_rpi, names='value')
sectors.observe(_on_change_sectors, names='value')


# pin widgets to the top toolbar
with toolbar:
    display(pbi_label, pbi_slider, rpi_label, rps_slider, sectors, info_label)

# =============================================
# Playback Async Loop
# =============================================

# Playback Async Thread
def update_table(table, stop_event, df):
    async def update_loop():
        i, n = 10, len(df)
        while not stop_event.is_set():
            speed_ms = pbi_slider.value
            nrows    = rps_slider.value
            if i < n:
                data = df.iloc[i : i + nrows].to_dict(orient="records")
                await table.update(data)
                i += nrows
            else:
                await table.clear()
                await table.update(df.iloc[:10].to_dict(orient="records"))
                i = 10
            await asyncio.sleep(speed_ms / 1000)
    # Run the async loop in a background thread
    loop = asyncio.new_event_loop()
    loop.run_until_complete(update_loop())

# Kick off the real-time updates
stop = threading.Event()
update_table(table, stop, df)



