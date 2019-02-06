#install.packages("rJava")
#install.packages("RJDBC")
#install.packages("sqldf")
#install.packages("gsubfn")
#install.packages("proto")
#install.packages("RSQLite")
#installed.packages("data.table")

library(RJDBC)
library(rJava)
library(sqldf)
library(gsubfn)
library(proto)
library(RSQLite)
library(data.table)

dir.create('~/.redshiftTools')
download.file('http://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.1.9.1009.jar','~/.redshiftTools/redshift-driver.jar')
driver <- JDBC("com.amazon.redshift.jdbc41.Driver", "~/.redshiftTools/redshift-driver.jar", identifier.quote="`")
url <- sprintf("jdbc:postgresql://.amazonaws.com:")
jconn <- dbConnect(driver, url, 'shashank_kumar', '***')
dbListTables(jconn)

#centre Values of cluster
centre_value_cust <- dbGetQuery(jconn, 'Select * from temp.cluster_centers_12dec order by category, order_sequence')
centre_value_cust <- centre_value_cust[,-1]

colnames(centre_value_cust)


#new data for cluster allocation
cust <- dbGetQuery(jconn, 'Select * from temp.customer_03Jan2019_data_cust')

data <- subset.data.frame(cust[,c(10:12,14:32)],  cust$new_cust ==0) 


#Capping
Cap <- function(x){ stopifnot(is.numeric(x))
  quantiles <- quantile( x, c(.001, .97 ) , na.rm = TRUE)
  x[ x < quantiles[1] ] <- quantiles[1]
  x[ x > quantiles[2] ] <- quantiles[2]
  x
}
data_integer <- as.data.frame(lapply(data_integer , Cap))

#Scaling (Normalization)
normalise <- function(x) {(x - min(x, na.rm=TRUE))/(max(x,na.rm=TRUE) -
                                                      min(x, na.rm=TRUE))}
data_integer <- as.data.frame(lapply(data_integer, normalise))
data_cust <- cbind(data_integer,data_binary)

#Cluster Allocation
set.seed(123)
temp = data.frame()
for (k in 1:nrow(centre_value_cust))
  for (x in 1:nrow(data_cust))  
    temp[x,k] <-  apply(data_cust[x,],1,function(x,cnt1) {(sqrt((x[1] - centre_value_cust[k,1])^2+(x[2]-centre_value_cust[k,2])^2+(x[3]-centre_value_cust[k,3])^2+(x[4]-centre_value_cust[k,4])^2+
                                                             (x[5]-centre_value_cust[k,5])^2+(x[6]-centre_value_cust[k,6])^2+(x[7]-centre_value_cust[k,7])^2+(x[8]-centre_value_cust[k,8])^2
                                                           +(x[9]-centre_value_cust[k,9])^2+(x[10]-centre_value_cust[k,10])^2+(x[11]-centre_value_cust[k,11])^2+(x[12]-centre_value_cust[k,12])^2
                                                           +(x[13]-centre_value_cust[k,13])^2+(x[14]-centre_value_cust[k,14])^2+(x[15]-centre_value_cust[k,15])^2+(x[16]-centre_value_cust[k,16])^2
                                                           +(x[17]-centre_value_cust[k,17])^2+(x[18]-centre_value_cust[k,18])^2+(x[19]-centre_value_cust[k,19])^2+(x[20]-centre_value_cust[k,20])^2
                                                           +(x[21]-centre_value_cust[k,21])^2+(x[22]-centre_value_cust[k,22])^2))},centre_value_cust)

final <-data.frame(apply(temp,1, FUN=which.min))
colnames(final) <- c('Cluster_Number')
final
colnames(data_cust)
colnames(centre_value_cust)

Cluster_Number <-cut(subset(cust$spend_per_trip ,  cust$new_cust ==1), c(-1,200,99999), c('New_Low','New_High'))
New_Customer <- cbind(subset.data.frame(cust[,c(1,2,9)],  cust$new_cust ==1), Cluster_Number)
Old_Customer <- cbind(subset.data.frame(cust[,c(1,2,9)],  cust$new_cust ==0), final)
cust_Alloocation <- rbind.data.frame(Old_Customer, New_Customer )
cust_Alloocation$Variant <- sample(c('A',"B"), size = nrow(cust_Alloocation), replace = TRUE)

# Need to Change the Month Value based on the targeted Month
cust_Alloocation$Variant <-ifelse (month(as.POSIXlt(cust_Alloocation$date_of_birth, format="%Y-%m-%d")) == 3, 'C', cust_Alloocation$Variant)
cust_Alloocation$blue_status <- c('****')

write.csv(cust_Alloocation, "cust_Loocation.csv")
