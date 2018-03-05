from bokeh.models import ColumnDataSource
from bokeh.models.widgets import DataTable, TableColumn, HTMLTemplateFormatter
from bokeh.io import show

dict1 = {'x':[0]*6,'y':[0,1,0,1,0,1]}
source = ColumnDataSource(data=dict1)

template="""
<div style="background:<%= 
    (function colorfromint(){
        if(value == 1){
            return("blue")}
        else{return("red")}
        }()) %>; 
    color: white"> 
<%= value %></div>
"""

formater =  HTMLTemplateFormatter(template=template)
columns = [
    TableColumn(field="x", title="x"),
    TableColumn(field="y", title="y",formatter=formater)
]

data_table = DataTable(source=source, columns=columns, width=800)
show(data_table)