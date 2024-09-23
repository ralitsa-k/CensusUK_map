import dash
import polars as pl
import plotly.express as px
import dash_leaflet as dl
from dash import html, dcc, Input, Output, State
import geopandas as gpd
from shapely.ops import unary_union
from shapely.geometry import shape
import json
from glob import glob

# Load and process data from CENSUS
df_econ = pl.read_excel(r'./Highest level of qualification by economic activity status.xls')
df = df_econ.rename({x: y for x, y in zip(df_econ.columns, ['city', 'Auth', 'Qual-code', 'Qualification', 'econ-code', 'econ', 'observation'])})
df_plot = df.filter(pl.col('city').str.starts_with("E"))
df_plot2 = df_plot.group_by(['city', 'Qualification', 'econ']).agg(pl.col('observation').mean()).sort(by='observation', descending=True)

# City long/lat
positions_cities = pl.read_csv('Wards_May_2024_Boundaries_UK_BFE_-7664674637076255808.csv')
df_base = positions_cities[['WD24CD','WD24NM','LONG', 'LAT']].unique()

# recode economic activity
econ_dict = df_plot2.select(pl.col('econ')).unique()
to_ = {'Economically inactive (excluding full-time students)':'Econ inactive, nonStudent',
       'Economically active and a full-time student: In employment':'Econ active, Student, Working',
       'Economically active (excluding full-time students): Unemployed: Seeking work or waiting to start a job already obtained: Available to start working within 2 weeks':'Econ active, nonStudent, Searching',
       'Does not apply':'NA',
       'Economically inactive and a full-time student':'Econ inactive, Student',
       'Economically active (excluding full-time students): In employment':'Econ active, nonStudent, Working',
       'Economically active and a full-time student: Unemployed: Seeking work or waiting to start a job already obtained: Available to start working within 2 weeks': 'Econ active, Student, Searching'}

df_plot2 = df_plot2.with_columns(pl.col('econ').replace(to_))


# recode Qualif activity
qual_dict = df_plot2.select(pl.col('Qualification')).unique()
to_q = {'Other: vocational or work-related qualifications, other qualifications achieved in England or Wales, qualifications achieved outside England or Wales (equivalent not stated or unknown)':'Other',
        'Level 1 and entry level qualifications: 1 to 4 GCSEs grade A* to C, Any GCSEs at other grades, O levels or CSEs (any grades), 1 AS level, NVQ level 1, Foundation GNVQ, Basic or Essential Skills':'Level 1 / Entry',
        'Level 4 qualifications or above: degree (BA, BSc), higher degree (MA, PhD, PGCE), NVQ level 4 to 5, HNC, HND, RSA Higher Diploma, BTEC Higher level, professional qualifications (for example, teaching, nursing, accountancy)':'Level 4, BSc, MA, PhD, nursing, etc',
        'Level 3 qualifications: 2 or more A levels or VCEs, 4 or more AS levels, Higher School Certificate, Progression or Advanced Diploma, Welsh Baccalaureate Advance Diploma, NVQ level 3; Advanced GNVQ, City and Guilds Advanced Craft, ONC, OND, BTEC National, RSA Advanced Diploma': 'Level 3, 2 A levels, Higher School diploma',
        'Level 2 qualifications: 5 or more GCSEs (A* to C or 9 to 4), O levels (passes), CSEs (grade 1), School Certification, 1 A level, 2 to 3 AS levels, VCEs, Intermediate or Higher Diploma, Welsh Baccalaureate Intermediate Diploma, NVQ level 2, Intermediate GNVQ, City and Guilds Craft, BTEC First or General Diploma, RSA Diploma':'Level 2',
        'Does not apply':'NA'}
df_plot2 = df_plot2.with_columns(pl.col('Qualification').replace(to_q))

df_plot2 = df_plot2.filter(pl.col('Qualifications').str.contains(['inactive']))

# start the dash app
app = dash.Dash()
colors = {'background': '#111111', 'text': '#7FDBFF'}

# get all json files for plotting the map
cities_l = dict()
remove = []
cities = []
wards = []
for city in df_plot2['city'].unique():
    mapped = glob(f'./wards_by_lad/{city}.json')
    if mapped:
        cities_l[city] = json.load(open(mapped[0]))
        cities.append(city)
        wards.append(json.load(open(mapped[0]))['features'][0]['properties']['WD13CD'])
    else:
        # add missing cities, to be removed from the econ data
        remove.append(city)


city_wards = dict(zip(cities, wards))
json_object = json.dumps(city_wards, indent=4)  
# Writing to sample.json
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

from shapely import Polygon, to_geojson
from shapely.ops import unary_union
import json

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


# reduce the polygon sizes so that it's less granular on the map
for city in cities_l.keys():
    cities_l[city]['features'] = super_polys[city]['features']

# create GeoJSON elements with dynamic ids
geoJason = [
    dl.GeoJSON(data=areaJson, 
                id={'type': 'city-marker', 'index': cityKey}, n_clicks=0) 
    for cityKey, areaJson in cities_l.items()
]

# create the app layout
app.layout = html.Div([
    dl.Map(
        center=[51, 0],
        zoom=5,
        children=[dl.TileLayer()] + geoJason,
        style={'height': '50vh'},
    ),
    html.Div(id="capital"),
    dcc.Graph(id='barplot_city'),
    dcc.Store(id='last-clicked-city', data=None)  # Store to track the last clicked city
])

@app.callback(
    [Output("capital", "children"), 
     Output('barplot_city', 'figure'),
     Output('last-clicked-city', 'data')],  # Update the last-clicked-city store
    [Input({'type': 'city-marker', 'index': dash.dependencies.ALL}, 'n_clicks')],
    [State('last-clicked-city', 'data')]  # Get the last clicked city from the store
)
def display_selected_city(n_clicks, last_clicked_city):
    ctx = dash.callback_context
    if not ctx.triggered or all(click == 0 for click in n_clicks):
        return "Click on a marker to select a city.", dash.no_update, last_clicked_city
    
    # Get the id of the triggered input
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
    triggered_id = json.loads(triggered_id)
    
    # Get the city index from the triggered id
    city = triggered_id['index']
    
    # Check if the same city was clicked
    if city == last_clicked_city:
        return dash.no_update, dash.no_update, last_clicked_city  # No updates if the city hasn't changed
    
    # Plot Bar chart
    plot_me2 = df_plot2.filter(pl.col('city') == city).group_by(['city', 'econ','Qualification']).agg(pl.col('observation').mean()).sort(by='observation')
    plot_me3 = plot_me2.with_columns(pl.col('observation').round())
    plot_me4 = plot_me3.with_columns(((pl.col("observation") / pl.sum("observation")) * 100).round().alias("Percent"))
    
    # Plot Bar chart
    fig = px.bar(plot_me4, y="econ", x="Percent", color='Qualification', title="Economic activity and qualifications")
    
    # Return the updated city name and figure, and update the last clicked city in the store
    return f"You clicked on {city}", fig, city

if __name__ == '__main__':
    app.run_server(debug=False)