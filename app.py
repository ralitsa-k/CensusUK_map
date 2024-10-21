import dash
import polars as pl
import plotly.express as px
import dash_leaflet as dl
from dash import html, dcc, Input, Output, State, no_update
import geopandas as gpd
from shapely.ops import unary_union
from shapely.geometry import shape
import json
from glob import glob
from shapely import Polygon, to_geojson
from shapely.ops import unary_union
import plotly.graph_objects as go 
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import matplotlib.colors as mpl_colors
import matplotlib as mpl
from dash_extensions.javascript import arrow_function, assign
# This app gets data from CENSUS UK 2021, and data on England regions
# The aim is to do a simple dash showing the qualifications and economic activity by region clicked

#%% Load data
df_plot2 = pl.read_csv('data/processed/data_for_app.csv')

# City long/lat - location of polygons per region
positions_cities = pl.read_csv('data/raw/Wards_May_2024_Boundaries_UK_BFE_-7664674637076255808.csv')
df_base = positions_cities[['WD24CD','WD24NM','LONG', 'LAT']].unique()

#%% Wrangle the map data - fixing region locations for the map and polygon sizes
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
colors = {'background': '#111111', 'text': '#7FDBFF'}

# get all json files for plotting the map
# the following code gets the polygons for each region and assigns a city name to it, combining two datasets
cities_l = dict()
remove = []
cities = []
wards = []
for city in df_plot2['city'].unique():
    mapped = glob(f'./data/raw/wards_by_lad/{city}.json')
    if mapped:
        cities_l[city] = json.load(open(mapped[0]))
        cities.append(city)
        wards.append(json.load(open(mapped[0]))['features'][0]['properties']['WD13CD'])
    else:
        # add missing cities, to be removed from the econ data
        remove.append(city)


city_wards = dict(zip(cities, wards))
json_object = json.dumps(city_wards, indent=4)  
# writing to sample.json
with open("city_wards.json", "w") as outfile:
    outfile.write(json_object)

city_ward = json.load(open('city_wards.json'))
df2 = df_plot2.with_columns(
    pl.col('city').replace_strict(city_ward, default=None).alias('WD24CD')
).filter(~pl.col('WD24CD').is_null())
df_final = df2.join(df_base, how='inner', on='WD24CD')
df = df_final.select(['WD24CD', 'LONG', 'LAT', 'WD24NM']).unique()

my_list = df_final['city'].to_list()
cities_l = {key: cities_l[key] for key in my_list}

# reduce the polygon of each region, so that there are less subdivisions
def get_super_poly(city_geo):
    sub_district_polys = []
    for sub_district in city_geo["features"]:
        for district_point_set in sub_district["geometry"]["coordinates"]:
            
            match sub_district["geometry"]["type"]:
                case "Polygon":
                    sub_district_polys.append(Polygon([(coord[0], coord[1]) for coord in district_point_set]))
                case "MultiPolygon":
                    for district_point_set in district_point_set:
                        sub_district_polys.append(Polygon([(coord[0], coord[1]) for coord in district_point_set]))
                case _:
                    raise TypeError("Unhandled geometry type: " + sub_district["geometry"]["type"])

    super_poly = unary_union(sub_district_polys)
    return super_poly
super_polys = dict()
for city_key, city_geo in cities_l.items():

    super_poly = json.loads(to_geojson(get_super_poly(city_geo)))

    super_polys[city_key] = {
        "type": "FeatureCollection",
        "crs": { "type": "name", "properties": { "name": city_key } },
        "features": [
            { "type": "Feature", "properties": { }, "geometry": super_poly }
        ]
    }

import numpy as np

def NormalizeData(data):
    data = 1 - data
    return (data - np.min(data)) / ((np.max(data) - np.min(data))) * 0.8

ratio_df = df_plot2.with_columns(pl.lit(NormalizeData(df_plot2['ratio'].to_numpy())).alias('norm_ratio'))

# reduce the polygon sizes so that it's less granular on the map
for city in cities_l.keys():
    cities_l[city]['features'] = super_polys[city]['features']
    ratio_n = ratio_df.filter(pl.col('city')==city).select(pl.first('norm_ratio')).item()
    hex_c = mpl_colors.to_hex(mpl.colormaps["viridis"](ratio_n))
    cities_l[city]['features'][0]['properties']['color'] = hex_c

classes = ratio_df['city'].to_list()
colorscale = []
colorscale = ratio_df['ratio'].map_elements(lambda x: mpl_colors.to_hex(mpl.colormaps["viridis"](x)), return_dtype=str).to_list()

# create GeoJSON elements with dynamic ids (so that the map resets once a new region is clicked)
geoJason = [
    dl.GeoJSON(data=areaJson,
                id={'type': 'city-marker', 'index': cityKey}, n_clicks=0,
                style=assign("""
                    feature => ({
                        color: feature.properties.color
                    })
                """)
                )
    for cityKey, areaJson in cities_l.items()
]

#%% start the dash app and structure the 'website'

# this add the buttons and options to filter the bargraph
controls = html.Div(
    [
        html.Div(dbc.RadioItems(id ='display_figure',
            className='btn-group',
            inputClassName='btn-check',
            labelClassName="btn btn-outline-light",
            labelCheckedClassName="btn btn-light",
            options=[
                {'label': 'Econ inactive, non-Student', 'value': 'Figure1'},
                {'label': 'Econ active, non-Student, Searching', 'value': 'Figure2'},
                {'label': 'Econ active, Student, Searching', 'value': 'Figure3'},
                {'label': 'All three', 'value':'all3'}
            ], value = 'Figure1'
        ))
                 
    ])
# custom colors for each qualification level
colors = {'No qualifications':'#ab5458',
            'Level 1':'#789890',
            'Apprenticeship':'#00646f',
            'Level 3': '#3b9ba6',
            'Level 4': '#69d6e2'}
    
# this is the default plot that will be plotted at the beginning before anything is clicked
full_plot_ec = df_plot2.filter(pl.col('econ')=='Econ inactive, nonStudent')
full_plot = full_plot_ec.filter(pl.col('city') == 'E06000033').group_by(['city', 'econ', 'Qualification',"Qual-code",'Qualifications']).agg(pl.col('observation').mean()).sort(by='observation')
full_plot_round_perc = full_plot.with_columns(((pl.col("observation") / pl.sum("observation")) * 100).round().alias("Percent"))
fig2 = px.bar(full_plot_round_perc, hover_data="Qualification",
                y='Qualifications', x="Percent",color="Qualifications", color_discrete_map = colors, title="Econ inactive, nonStudent")
fig2.update_yaxes(categoryorder='array', categoryarray= ['No qualifications','Level 1', 'Level 2','Apprenticeship', 'Level 3', 'Level 4'])
fig2.update_layout(plot_bgcolor='#222222',paper_bgcolor = '#222222', template = "plotly_dark", showlegend = False)

default_fig = fig2

# navbar is the title bar at the top of the 'website'
navbar = dbc.Navbar(
    dbc.Container(
        [
            html.A(
                dbc.Row(
                    [
                        dbc.Col(html.Img(src='https://hckraainem.be/wp-content/uploads/2018/04/SOPRASTERIA_logo-1920x254.png', height="20px")),
                        dbc.Col(dbc.NavbarBrand("Census 2021 - Qualifications of economically inactive citizens, active students or non-students looking for a job", className="ms-2")),
                    ],
                    align="center",
                    className="g-0",
                ),
                href="https://www.ons.gov.uk/census/maps/choropleth/education/highest-level-of-qualification",
                style={"textDecoration": "none"},
            ),
            dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
            dbc.Collapse(
                id="navbar-collapse",
                is_open=False,
                navbar=True,
            ),
        ]
    ),
    color='#d7d7d7',
    dark=True
)


# create the app layout
app.layout = dbc.Container([
    
    navbar,
    dbc.Col([html.Div([
        dl.Map(
            center=[54, 0],
            zoom=5,
            children=[dl.TileLayer()] + geoJason,
            style={'height': '35vh'},
        ),
        dl.Colorbar(colorscale=colorscale, width=20, height=200, min=0, max=50, position="topright"),
        # add the capital/region ID as text after a city is clicked
        html.Div(
            id="capital", 
            style={
                'font-size': '18pt',
                'color': '#d7d7d7',
                'background-color': '#3f3f3f',
                'width': '30%',
                'border-radius': '0 0 5px 5px',
                'text-align': 'center',
                'position':'absolute'
            })])]),

    dbc.Row([
        # first column: Select economic activity label and controls
            dbc.Col([
                dbc.Row([html.Div('Select economic activity:', 
                         style={'font-size': '16pt', 'color': '#7d7d7d', "margin": "5px 0 0 30px"})]),
                dbc.Row([controls]),
                ]),

        # second column: Bar plot
            dbc.Col([
                dcc.Graph(id='barplot_city', figure=default_fig),dcc.Tooltip(id="graph-tooltip") 
            ])
        ], style = {'position':'absolute','margin':'50px 0 0 0'}),

    dcc.Store(id='last-clicked-city', data=None),
    dcc.Store(id='Filtered-econ', data='Figure1')
])


# This is where the function getting input and providing output on clicks
@app.callback(
    [Output('capital', 'children'), 
     Output('barplot_city', 'figure'),
     Output('last-clicked-city', 'data'),
     Output('Filtered-econ', 'data')],  # update the last-clicked-city store
    
    
    [Input({'type': 'city-marker', 'index': dash.dependencies.ALL}, 'n_clicks'),
     Input('display_figure','value')],
    
    [State('last-clicked-city', 'data'),
     State('Filtered-econ', 'data')]  # get the last clicked city from the store
)
def display_selected_city(n_clicks, selected_figure_from_dropdown, last_clicked_city, last_selection_figure):
    ctx = dash.callback_context
    
    city_clicked = False
    if ctx.triggered:
        triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    # check if the click was on the map
    if 'city-marker' in triggered_id:
        city_clicked = True
        triggered_id = json.loads(triggered_id)
        city = triggered_id['index']  # get the clicked city
        
    if not city_clicked:
        city = last_clicked_city

    # if no city has been clicked so far, return an initial message
    if city is None:
        plotted = 'Click on a marker to select a city.'
        return "Click on a marker to select a city.", dash.no_update, last_clicked_city, selected_figure_from_dropdown
    plotted = df_plot2.filter(pl.col('city')==city)['Auth'].unique().item()
    groupss = ['city', 'econ', "Qual-code",'Qualifications']
    # handle figure selection, change title depending on button clicked, and the filtered data: 
    if selected_figure_from_dropdown == 'Figure1':
        full_plot_ec = df_plot2.filter(pl.col('econ')=='Econ inactive, nonStudent')
        title_selected = "Econ inactive, nonStudent"
        
    elif selected_figure_from_dropdown == 'Figure2':
        full_plot_ec = df_plot2.filter(pl.col('econ')=='Econ active, nonStudent, Searching')
        title_selected = "Econ active, nonStudent, Searching"
       
    elif selected_figure_from_dropdown == 'Figure3':
        full_plot_ec = df_plot2.filter(pl.col('econ')=='Econ active, Student, Searching')
        title_selected = "Econ active, Student, Searching"

    elif selected_figure_from_dropdown == 'all3':
        full_plot_ec = df_plot2.filter(pl.col('econ').is_in(['Econ active, Student, Searching','Econ active, nonStudent, Searching','Econ inactive, nonStudent']))
        title_selected = "All active and inactive"
        groupss = ['city', "Qual-code",'Qualifications']
        
        
    # plot common for all with specific modifications depending on clicks
    full_plot = full_plot_ec.filter(pl.col('city') == city).group_by(groupss).agg(pl.col('observation').mean()).sort(by='observation')
    full_plot_round_perc = full_plot.with_columns(((pl.col("observation") / pl.sum("observation")) * 100).round().alias("Percent"))
    if 'fig' in globals():
        fig.data =[]
    fig = px.bar(full_plot_round_perc,
                    y='Qualifications', x="Percent",color="Qualifications", color_discrete_map = colors)
    fig.update_yaxes(categoryorder='array', categoryarray= ['No qualifications','Level 1', 'Level 2','Apprenticeship', 'Level 3', 'Level 4'])
    fig.update_layout(title = title_selected, plot_bgcolor='#222222',paper_bgcolor = '#222222', template = "plotly_dark", showlegend = False)
    
    return f"{plotted}", fig, city, selected_figure_from_dropdown

if __name__ == '__main__':
    app.run_server(debug=False)
    