install.packages("sqldf")
install.packages("dplyr")
install.packages("reshape")
library(sqldf)
library(dplyr)
library(reshape)
#Load data
# Ok, another place where file Encoding can break data import/export made on Linux/Win/Mac
# BE AWARE, Make standards
basePrice <- read.csv("Data/cena bazowa.csv", header = TRUE, sep = ";", fileEncoding = "UTF-8-BOM")
prediction <- read.csv("Data/predykcja.csv", header = TRUE, sep = ";", fileEncoding = "UTF-8-BOM")

#This table is problematic Relationship(aggregation) is 1..3, why not 1..55? It should be 1..*
sentOrders <- read.csv("Data/wyslane.csv", header = TRUE, sep = ";", fileEncoding = "UTF-8-BOM")
orders <- read.csv("Data/zamowienia.csv", header = TRUE, sep = ";", fileEncoding = "UTF-8-BOM")

#Format data types
prediction$DATA <- as.Date(prediction$DATA,format="%d.%m.%Y")
prediction <- prediction[rowSums(is.na(prediction)) != ncol(prediction),]
prediction$wyprzedaż <- (!is.na(as.integer(prediction$wyprzedaż)) & as.integer(prediction$wyprzedaż) > 0)
orders$order_date <- as.Date(orders$order_date,format="%d.%m.%Y")
orders$couponPercentage <- as.integer(gsub("%","",orders$KUPON))

#Validate Data
#Simple check for data uniqueness. If keys are not unique, whole excursive is pointless.
#If ANY of those queries return ANYTHING or numbers are not equal to each other, data is wrong
sqldf('SELECT  count(order_id), order_id FROM orders GROUP BY order_id HAVING count(order_id) > 1')
sqldf('SELECT count( DISTINCT order_id), count(order_id) FROM orders') 
sqldf('SELECT  count(item_id), item_id FROM basePrice GROUP BY item_id HAVING count(item_id) > 1')
sqldf('SELECT count( DISTINCT item_id), count(item_id) FROM basePrice') 
sqldf('SELECT  count(order_id), order_id FROM sentOrders GROUP BY order_id HAVING count(order_id) > 1')
sqldf('SELECT count( DISTINCT order_id), count(order_id) FROM sentOrders') 

#Generate CLEAN data
ordersClean <- sqldf('SELECT * FROM orders GROUP BY order_id') 

#Questions
print("1)    W jakim dniu roku klienci złożyli najwięcej zamówień?")
sqldf('SELECT count(order_date) as orders_count, order_date FROM ordersClean GROUP BY order_date ORDER BY orders_count DESC LIMIT 1')

ordersClean %>% group_by(order_date) %>% count(order_date) %>% arrange(desc(n)) %>% head(1)


print("2)    Ilu klientów skorzystało z kuponu rabatowego w trakcie zakupów?")
sqldf('SELECT count(client_id) FROM ordersClean WHERE couponPercentage > 0')

ordersClean %>% filter(couponPercentage>0) %>% count()


print("3)    Ilu klientów zrobiło w analizowanym okresie więcej niż jedno zamówienie?")
#This will return false positives because data is "DIRTY"
sqldf('SELECT count(*) FROM (SELECT count(client_id) FROM orders  GROUP BY client_id HAVING count(client_id) > 1)')
#This will accommodate for dirty data
sqldf('SELECT count(*) FROM (SELECT count(client_id), client_id FROM 
      (SELECT DISTINCT ord.order_id, ord.client_id FROM orders as ord) GROUP BY client_id HAVING count(client_id) > 1)') 
#Or you can use this on clean data
sqldf('SELECT count(*) FROM (SELECT count(client_id) FROM ordersClean GROUP BY client_id HAVING count(client_id) > 1)')

ordersClean %>% group_by(client_id) %>% count(client_id) %>% filter(n>1) %>% nrow()

print("4)    Który z produktów cieszył się największym powodzeniem? Ilu klientów kupiło go ze zniżką?")
#UNPIVOT is too complex. Let use UNION ALL. Remember ALL, we need all items in order, not all unique items in order
itemsInOrders <-  sqldf('SELECT order_id, item_id_1 as item_id FROM sentOrders
                         UNION ALL SELECT order_id, item_id_2 as item_id FROM sentOrders WHERE item_id_2 > 0 
                         UNION ALL SELECT order_id, item_id_3 as item_id FROM sentOrders WHERE item_id_3 > 0')
#or use melt from reshape
itemsInOrdersR <- melt(sentOrders, id = c("order_id")) %>% filter(!is.na(value)) %>% arrange(order_id)
names(itemsInOrdersR)[names(itemsInOrdersR) == "value"] <- "item_id"
itemsInOrdersR <- subset(itemsInOrdersR, select = -c(variable))

sqldf('SELECT mostBought.item_id, timesBought, count(couponPercentage) as boughtWithCoupon FROM 
      (SELECT count(item_id) as timesBought, item_id FROM itemsInOrders
      GROUP BY item_id 
      ORDER BY timesBought DESC LIMIT 1) as mostBought,
      itemsInOrders, ordersClean
      WHERE mostBought.item_id == itemsInOrders.item_id AND itemsInOrders.order_id == ordersClean.order_id')

itemsInOrdersR %>% group_by(item_id) %>% count(item_id) %>% arrange(desc(n)) %>% head(1) %>% dplyr::rename(bought = n) %>%
   left_join(itemsInOrdersR, by = c("item_id" = "item_id")) %>% left_join(ordersClean,  by = c("order_id" = "order_id")) %>%
   filter(couponPercentage > 0) %>% ungroup() %>% count(item_id, bought) %>% dplyr::rename(timesBoughtWithCoupon = n)


print("5)    Który produkt był najczęściej kupowany ze zniżką?")
sqldf('SELECT  count(itemsInOrders.item_id) as boughtWithCoupon, itemsInOrders.item_id from itemsInOrders 
      JOIN ordersClean ON itemsInOrders.order_id == ordersClean.order_id
      where ordersClean.couponPercentage >0 
      GROUP BY itemsInOrders.item_id
      ORDER BY boughtWithCoupon DESC LIMIT 1
      ')

ordersClean %>% filter(couponPercentage>0) %>% inner_join(itemsInOrdersR, by = c("order_id" = "order_id")) %>% 
   count(item_id) %>% dplyr::rename(timesBoughtWithCoupon = n) %>% arrange(desc(timesBoughtWithCoupon)) %>% head(1) 

print("6)    Jaka była końcowa wartość wszystkich zamówień w badanym okresie?")
#We DONT KNOW value of "SALE" (wyprzedaż) so this is estimate
sqldf('SELECT  total(basePrice.base_price - (basePrice.base_price * (CAST(IFNULL(ordersClean.couponPercentage, 0) AS float)/100))) 
      as TotalValueOfAllOrders
      FROM itemsInOrders 
      JOIN ordersClean ON itemsInOrders.order_id == ordersClean.order_id
      JOIN basePrice ON itemsInOrders.item_id == basePrice.item_id')

itemsInOrdersR %>% 
   inner_join(ordersClean, by = c("order_id" = "order_id")) %>% 
   inner_join(basePrice, by = c("item_id" = "item_id")) %>% replace(is.na(.), 0) %>%
   mutate(priceWithCoupon = base_price - ((couponPercentage/100)*base_price)) %>% select(priceWithCoupon) %>% sum()

