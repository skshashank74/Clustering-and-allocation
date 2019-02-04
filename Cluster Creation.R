#install.packages("rJava")
#install.packages("RJDBC")
#install.packages("sqldf")
#install.packages("gsubfn")
#install.packages("proto")
#install.packages("RSQLite")

library(RJDBC)
library(rJava)
library(sqldf)
library(gsubfn)
library(proto)
library(RSQLite)


# download the data from Redshift to R
dir.create('~/.redshiftTools')
download.file('http://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.1.9.1009.jar','~/.redshiftTools/redshift-driver.jar')
driver <- JDBC("com.amazon.redshift.jdbc41.Driver", "~/.redshiftTools/redshift-driver.jar", identifier.quote="`")
url <- sprintf("jdbc:postgresql://genting.cnpzuz3xslqd.us-west-2.redshift.amazonaws.com:")
jconn <- dbConnect(driver, url, 'shashank_kumar', '***')
dbListTables(jconn)


Mal_Mus <- dbGetQuery(jconn, 'Select * from temp.blue_customer_12Dec2018_data_Mal_Mus')
Mal_Mus_data <- subset(Mal_Mus, Mal_Mus$new_cust==0)
Mal_Mus_data_prcnt <- Mal_Mus_data[,c(9:21)]
Mal_Mus_data_int <- Mal_Mus_data[,c(22:30)]

#Capping 
Cap <- function(x){ stopifnot(is.numeric(x))
  quantiles <- quantile( x, c(.001, .97 ) , na.rm = TRUE)
  x[ x < quantiles[1] ] <- quantiles[1]
  x[ x > quantiles[2] ] <- quantiles[2]
  x
}

Mal_Mus_data_int <- as.data.frame(lapply(Mal_Mus_data_int , Cap))
Mal_Mus_data_final <- cbind.data.frame(Mal_Mus_data_prcnt,Mal_Mus_data_int)

#Scaling (Normalization)
normalise <- function(x) {(x - min(x, na.rm=TRUE))/(max(x,na.rm=TRUE) -
                                                      min(x, na.rm=TRUE))}
Mal_Mus_data_final_nor <- as.data.frame(lapply(Mal_Mus_data_final, normalise))


#Elbow Curve
set.seed(1230)
wcsx = vector()
for (i in 1:20) wcsx[i] = sum(kmeans(Mal_Mus_data_final_nor, i, iter.max = 100, nstart = 10)$withinss)
plot(1:20,
     wcsx,
     type = 'b',
     main = paste('The Elbow Method'),
     xlab = 'Number of clusters',
     ylab = 'WCSS')

#KMeans Clustering
Mal_Mus_Cluster <- kmeans(Mal_Mus_data_final_nor, 10, iter.max = 100, nstart=20)

#Scaled Centre Values
write.csv(Mal_Mus_Cluster$centers, '12Dec_Scaled_Mal_Mus_centroid.csv')

#Descaling of the data
d.min <- round(colwise(min)(Mal_Mus_data_final), digits = 2)
d.max <- round(colwise(max)(Mal_Mus_data_final), digits = 2)
d.d <-  rbind.data.frame(d.min,d.max)
m <- as.data.frame(Mal_Mus_Cluster$centers)
rm(abc)
abc <- data.frame()
temp <- data.frame()
for (i in 1:nrow(m))
  for (j in 1:ncol(m))
    temp[i,j] <- (m[i,j]*(d.d[2,j]-d.d[1,j]))+d.d[1,j]
abc <- round(rbind(abc,temp), digits = 2)
colnames(abc) <- colnames(m)
abc

Mal_Mus_Centre_Value <- cbind.data.frame(Mal_Mus_Cluster$size, abc )

datatable(Mal_Mus_Centre_Value )%>%
  formatStyle(names(Mal_Mus_Centre_Value ),
              background = styleColorBar(range(Mal_Mus_Centre_Value), 'lightblue'),
              backgroundRepeat = 'no-repeat',
              backgroundPosition = 'center',
              color = 'black')

#Original Centre Values
write.csv(Mal_Mus_Centre_Value , "12Dec_Malaysian_Muslim_Centres.csv")


