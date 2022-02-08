# reference to : https://towardsdatascience.com/visualizing-networks-in-python-d70f4cbeb259
# visualize the example hyperlink graph
# example index : http://webdatacommons.org/hyperlinkgraph/data/example_index
# example arc : http://webdatacommons.org/hyperlinkgraph/data/example_arcs
# author: hsc
# time : 2022/2/1

import argparse
import visdcc
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State

parser = argparse.ArgumentParser(description='Process the example graph path')
parser.add_argument("--index", type=str, help="the example graph index path", default="/home/hsc/dataset/hyperlink/example_index")
parser.add_argument("--arcs", type=str, help="the example graph arc path", default="/home/hsc/dataset/hyperlink/example_arcs")

args = parser.parse_args()

# create app
app = dash.Dash()

# load the data
index_name = dict()
with open(args.index, "r", encoding='utf-8') as file:
    for line in file:
        name, index = line.split()
        index_name[int(index)] = name

num_nodes = len(index_name.keys())
nodes = [{'id' : index, 'label' : index_name[index], 'shape' : 'dot', 'size' : 7} for index in range(num_nodes)]

edges = []
with open(args.arcs, "r", encoding='utf-8') as file:
    for line in file:
        source, target = line.split()
        edges.append({
            'id' : source + "_" + target,
            'from' : int(source),
            'to' : int(target),
            'width' : 2
        })

# define layout

app.layout = html.Div([
    visdcc.Network(id = 'net', 
                   data = {'nodes' : nodes, 'edges' : edges},
                   options = dict(height = '600px', width='100%')),
    dcc.RadioItems(id = 'color',
                   options = [{'label' : 'Red', 'value' : '#ff0000'},
                              {'label' : 'Green', 'value' : '#0x00ff00'},
                              {'label' : 'Blue', 'value' : '0x0000ff'}],
                   value='Red')
])

# define callback

@app.callback(
    Output('net', 'options'),
    [Input('color', 'value')]
)
def myfun(x):
    return {'nodes' : {'color' : x}}

if __name__ == '__main__':
    app.run_server(debug=True)