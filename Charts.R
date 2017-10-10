
#############################################
######### Pie Chart of manufacturer #########

pie <- ggplot(cars, aes(x = "", fill = factor(manufacturer))) + 
  geom_bar(width = 5) +
  theme(axis.line = element_blank(),
        plot.title = element_text(hjust=0.5)) + 
  labs(fill="Manufacturer", 
       x=NULL, 
       y=NULL, 
       title="Pie Chart")

pie + coord_polar(theta = "y", start=0)



######### Google Maps plot Code #########
library(dplyr)
library(ggmap)

States <- map_data("state")

state_sample <- States[sample(1:nrow(States), 234,replace=TRUE),]
cars_data <- cbind(cars,state_sample)

map<-get_map(location='united states', zoom=5, maptype = "terrain",source='google',color='color')
car.plot.basedata<-group_by(cars_data, region)

ggmap(map) +geom_point(data = car.plot.basedata, aes(x = long, y = lat), 
                       color = '#FF6600',size=0.1*car.plot.basedata$group)



##################################
##################################
######### ReporteRs Code #########

require( ggplot2 )
doc <- docx( title = 'My Report' )

doc <- addTitle( doc , 'First 5 lines of iris', level = 1)
doc <- addFlexTable( doc , vanilla.table(iris[1:5, ]) )

doc <- addTitle( doc , 'ggplot2 example', level = 1)
myggplot <- qplot(Sepal.Length, Petal.Length, data = iris, color = Species, size = Petal.Width )
doc <- addPlot( doc = doc , fun = print, x = myggplot )

doc <- addTitle( doc , 'Text example', level = 1)
doc <- addParagraph( doc, 'My tailor is rich.', stylename = 'Normal' )


filename <- tempfile(fileext = ".docx") # the document to produce
writeDoc( doc, "Example Report.docx" )

