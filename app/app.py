import streamlit as st
import geopandas as gpd
from streamlit_folium import st_folium

# Load your county data
gdf = gpd.read_file("/home/jeremymccormick/Downloads/ca_counties/CA_Counties.shp")

st.title("California Counties Explorer")

# Display the map
m = gdf.explore(column="NAME", cmap="viridis")
st_folium(m, width=700)