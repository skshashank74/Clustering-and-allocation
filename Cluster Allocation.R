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
url <- sprintf("jdbc:postgresql://genting.cnpzuz3xslqd.us-west-2.redshift.amazonaws.com:")
jconn <- dbConnect(driver, url, 'shashank_kumar', '***')
dbListTables(jconn)

#centre Values of cluster
centre_value_Mal_Mus <- dbGetQuery(jconn, 'Select * from temp.cluster_centers_12dec order by category, order_sequence')
centre_value_Mal_Mus <- centre_value_Mal_Mus[,-1]
centre_value_Mal_Mus <- centre_value_Mal_Mus[,c("day_trip_prcnt",	"trips",	"avg_trip_len",	"profit_per_trip",	
                                                "wk_end_actvty_percent",	"booked_nights",	"room_rev_per_booked_nght",
                                                "fwh_nght_prcnt",	"non_fwh_nght_prcnt",	"standard_nght_prcnt",
                                                "non_standard_nght_prcnt",	"nongaming_spend_inside_resort_prcnt",	
                                                "show_trnsctn_binary",	"skyway_trnsctn_binay",	"trnsport_binary",	
                                                "themepark_binary",	"no_of_fnb_trans",	"genm_trnsctn_prccnt",	
                                                "fwh_trnsctn_prccnt",	"fnb_rev_per_trnsctn",	"skyway_spend_per_trnsctn",
                                                "themepark_spend_per_trnsctn")]

colnames(centre_value_Mal_Mus)


#new data for cluster allocation
Mal_Mus <- dbGetQuery(jconn, 'Select * from temp.blue_customer_03Jan2019_data_Mal_Mus')

data <- subset.data.frame(Mal_Mus[,c(10:12,14:32)],  Mal_Mus$new_cust ==0) 
data_binary <- data[, c("day_trip_prcnt",	"wk_end_actvty_percent",	"fwh_nght_prcnt",	
                        "non_fwh_nght_prcnt",	"standard_nght_prcnt",	"non_standard_nght_prcnt",	
                        "nongaming_spend_inside_resort_prcnt",	"show_trnsctn_binary",	"skyway_trnsctn_binay",	
                        "trnsport_binary",	"themepark_binary",	"genm_trnsctn_prccnt",	"fwh_trnsctn_prccnt")]

data_integer <- data[, c("trips",	"avg_trip_len",	"profit_per_trip",	"booked_nights",	
                         "room_rev_per_booked_nght",	"no_of_fnb_trans",	"fnb_rev_per_trnsctn",	
                         "skyway_spend_per_trnsctn",	"themepark_spend_per_trnsctn")]


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
data_mal_mus <- cbind(data_integer,data_binary)
data_mal_mus <- data_mal_mus[,c("day_trip_prcnt",	"trips",	"avg_trip_len",	"profit_per_trip",	
                                  "wk_end_actvty_percent",	"booked_nights",	"room_rev_per_booked_nght",
                                  "fwh_nght_prcnt",	"non_fwh_nght_prcnt",	"standard_nght_prcnt",
                                  "non_standard_nght_prcnt",	"nongaming_spend_inside_resort_prcnt",	
                                  "show_trnsctn_binary",	"skyway_trnsctn_binay",	"trnsport_binary",	
                                  "themepark_binary",	"no_of_fnb_trans",	"genm_trnsctn_prccnt",	
                                  "fwh_trnsctn_prccnt",	"fnb_rev_per_trnsctn",	"skyway_spend_per_trnsctn",
                                  "themepark_spend_per_trnsctn")]

#Cluster Allocation
set.seed(123)
temp = data.frame()
for (k in 1:nrow(centre_value_Mal_Mus))
  for (x in 1:nrow(data_mal_mus))  
    temp[x,k] <-  apply(data_mal_mus[x,],1,function(x,cnt1) {(sqrt((x[1] - centre_value_Mal_Mus[k,1])^2+(x[2]-centre_value_Mal_Mus[k,2])^2+(x[3]-centre_value_Mal_Mus[k,3])^2+(x[4]-centre_value_Mal_Mus[k,4])^2+
                                                             (x[5]-centre_value_Mal_Mus[k,5])^2+(x[6]-centre_value_Mal_Mus[k,6])^2+(x[7]-centre_value_Mal_Mus[k,7])^2+(x[8]-centre_value_Mal_Mus[k,8])^2
                                                           +(x[9]-centre_value_Mal_Mus[k,9])^2+(x[10]-centre_value_Mal_Mus[k,10])^2+(x[11]-centre_value_Mal_Mus[k,11])^2+(x[12]-centre_value_Mal_Mus[k,12])^2
                                                           +(x[13]-centre_value_Mal_Mus[k,13])^2+(x[14]-centre_value_Mal_Mus[k,14])^2+(x[15]-centre_value_Mal_Mus[k,15])^2+(x[16]-centre_value_Mal_Mus[k,16])^2
                                                           +(x[17]-centre_value_Mal_Mus[k,17])^2+(x[18]-centre_value_Mal_Mus[k,18])^2+(x[19]-centre_value_Mal_Mus[k,19])^2+(x[20]-centre_value_Mal_Mus[k,20])^2
                                                           +(x[21]-centre_value_Mal_Mus[k,21])^2+(x[22]-centre_value_Mal_Mus[k,22])^2))},centre_value_Mal_Mus)

final <-data.frame(apply(temp,1, FUN=which.min))
colnames(final) <- c('Cluster_Number')
final
colnames(data_mal_mus)
colnames(centre_value_Mal_Mus)

Cluster_Number <-cut(subset(Mal_Mus$spend_per_trip ,  Mal_Mus$new_cust ==1), c(-1,200,99999), c('New_Low','New_High'))
New_Customer <- cbind(subset.data.frame(Mal_Mus[,c(1,2,9)],  Mal_Mus$new_cust ==1), Cluster_Number)
Old_Customer <- cbind(subset.data.frame(Mal_Mus[,c(1,2,9)],  Mal_Mus$new_cust ==0), final)
Mal_Mus_Alloocation <- rbind.data.frame(Old_Customer, New_Customer )
Mal_Mus_Alloocation$Variant <- sample(c('A',"B"), size = nrow(Mal_Mus_Alloocation), replace = TRUE)

# Need to Change the Month Value based on the targeted Month
Mal_Mus_Alloocation$Variant <-ifelse (month(as.POSIXlt(Mal_Mus_Alloocation$date_of_birth, format="%Y-%m-%d")) == 3, 'C', Mal_Mus_Alloocation$Variant)
Mal_Mus_Alloocation$blue_status <- c('MALAYSIAN MUSLIM')

write.csv(Mal_Mus_Alloocation, "Mal_Mus_Loocation.csv")
