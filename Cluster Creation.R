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


Cust <- dbGetQuery(jconn, 'Select * from temp.customer_12Dec2018_data_')
Cust_data <- subset(Cust, Cust$new_cust==0)
Cust_data_prcnt <- Cust_data[,c(9:21)]
Cust_data_int <- Cust_data[,c(22:30)]

#Capping 
Cap <- function(x){ stopifnot(is.numeric(x))
  quantiles <- quantile( x, c(.001, .97 ) , na.rm = TRUE)
  x[ x < quantiles[1] ] <- quantiles[1]
  x[ x > quantiles[2] ] <- quantiles[2]
  
}

Cust_data_int <- as.data.frame(lapply(Cust_data_int , Cap))
Cust_data_final <- cbind.data.frame(Cust_data_prcnt,Cust_data_int)

#Scaling (Normalization)
normalise <- function(x) {(x - min(x, na.rm=TRUE))/(max(x,na.rm=TRUE) -
                                                      min(x, na.rm=TRUE))}
Cust_data_final_nor <- as.data.frame(lapply(Cust_data_final, normalise))


#Elbow Curve
set.seed(1230)
wcsx = vector()
for (i in 1:20) wcsx[i] = sum(kmeans(Cust_data_final_nor, i, iter.max = 100, nstart = 10)$withinss)
plot(1:20,
     wcsx,
     type = 'b',
     main = paste('The Elbow Method'),
     xlab = 'Number of clusters',
     ylab = 'WCSS')

#KMeans Clustering
Cust_Cluster <- kmeans(Cust_data_final_nor, 10, iter.max = 100, nstart=20)

#Scaled Centre Values
write.csv(Cust_Cluster$centers, '12Dec_Scaled_Cust_centroid.csv')

#Descaling of the data
d.min <- round(colwise(min)(Cust_data_final), digits = 2)
d.max <- round(colwise(max)(Cust_data_final), digits = 2)
d.d <-  rbind.data.frame(d.min,d.max)
m <- as.data.frame(Cust_Cluster$centers)
rm(abc)
abc <- data.frame()
temp <- data.frame()
for (i in 1:nrow(m))
  for (j in 1:ncol(m))
    temp[i,j] <- (m[i,j]*(d.d[2,j]-d.d[1,j]))+d.d[1,j]
abc <- round(rbind(abc,temp), digits = 2)
colnames(abc) <- colnames(m)
abc

Cust_Centre_Value <- cbind.data.frame(Cust_Cluster$size, abc )

datatable(Cust_Centre_Value )%>%
  formatStyle(names(Cust_Centre_Value ),
              background = styleColorBar(range(Cust_Centre_Value), 'lightblue'),
              backgroundRepeat = 'no-repeat',
              backgroundPosition = 'center',
              color = 'black')

#Original Centre Values
write.csv(Cust_Centre_Value , "12Dec_Month.csv")


