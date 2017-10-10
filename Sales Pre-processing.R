
########################################################
################# SALES PRE-PROCESSING #################

Sales <- lifepro.filter

library(zoo)
timeframe <- data.frame(SaleMonth <- seq(as.Date("2015-04-01"), 
                                       to = as.Date("2017-09-01"), by = "month"))
timeframe$SaleMonth <- as.yearmon(timeframe$SaleMonth) # yearmon function available in zoo



###Data formatting
Sales$Activated_Dt <- as.Date(Sales$Activated_Dt, "%Y-%m-%d")
Sales$Activate_AsOf_Dt <- as.Date(Sales$Activate_AsOf_Dt, "%Y-%m-%d")

summary(Sales$Activate_AsOf_Dt)

Sales <- Sales[which(Sales$Activate_AsOf_Dt >= "2015-04-01"),]
Sales$SaleMonth <- as.yearmon(Sales$Activate_AsOf_Dt)


### Sales data aggregation per agent per quarter
SalesAggr <- ddply(Sales, .(Writing_Agent, SaleMonth), summarise, 
                  NumSalesperMonth = length(Writing_Agent), 
                  TotalAUMperMonth = round(sum(Contract_Value)),
                  AvgAUMperMonth = round(mean(Contract_Value)))

write.csv(SalesAggr, file = "SalesAggr.csv")

SalesMonth_long <- expand.grid("Writing_Agent" = unique(Sales$Writing_Agent), 
                              "SaleMonth" = timeframe$SaleMonth)
SalesMonth_long1 <- merge(SalesMonth_long, SalesAggr, by.x = c("Writing_Agent", "SaleMonth"), 
                         by.y = c("Writing_Agent", "SaleMonth"), all.x = T)
SalesMonth_long1.1 <-  join(SalesMonth_long, SalesAggr[,c("Writing_Agent", "SaleMonth", 
                                                         "NumSalesperMonth", "TotalAUMperMonth", "AvgAUMperMonth")], 
                           type = "left")

SalesMonth_long1 <- SalesMonth_long1[order(SalesMonth_long1$Writing_Agent, SalesMonth_long1$SaleMonth),]
SalesMonth_long1.1 <- SalesMonth_long1.1[order(SalesMonth_long1.1$Writing_Agent, SalesMonth_long1.1$SaleMonth),]


library(reshape2)
SalesMonth_final <- dcast(SalesMonth_long1[,1:3], Writing_Agent ~ SaleMonth)

names(SalesMonth_final) <- paste("Sales", colnames(SalesMonth_final), sep = "_")



##### TASKS PREPROCESSING #####
tasks.bkp <- tasks.dup 

tasks.prep <- tasks.dup [which(tasks.dup $Subcategory__c == "Lead Gen"),]
tasks.prep <- tasks.prep[which(tasks.prep$RecordTypeName__c == "Investment"),]

tasks.prep <- tasks.prep[order(tasks.prep$SystemModstamp),]
p <- as.data.frame(table(tasks.prep$OwnerId))
p <- p[which(p$Freq !=0),]

tasks.prep <- tasks.prep[,c("Id", "WhoId", "OwnerId" ,"ActivityDate",
                           "Contact_Date__c","CreatedDate", "Date_Complete__c",
                           "LastModifiedDate","SystemModstamp", "Status","Subcategory__c",
                           "Type","RecordTypeName__c")]

write.csv(tasks.prep, "tasks.prep")


library(zoo)
tasks.prep$ConvMonth <- as.yearmon(as.Date(tasks.prep$CreatedDate))

tasks.prep1 <- merge(tasks.prep, WS_Name, by.x = "OwnerId", by.y = "OwnerId", all.x = T)

tasks.aggr1 <- ddply(tasks.prep1, .(WhoId, OwnerId, Contact_Owner_Email_Address__c, ConvMonth), summarise,
                    ConvCount = length(WhoId))


tasks.prep$Status
rm(tasks.aggr)

tasks.prep2 <- tasks.prep1[which(tasks.prep1$Status %in% "Completed"),]

tasks.aggr2 <- ddply(tasks.prep2, .(WhoId, OwnerId, Contact_Owner_Email_Address__c, ConvMonth), summarise,
                    CompletedCount = length(OwnerId))

tasks.prep3 <- tasks.prep2[which(tasks.prep2$RSA %in% "Yes"),]
tasks.aggr3 <- ddply(tasks.prep3, .(WhoId, OwnerId, Contact_Owner_Email_Address__c, ConvMonth), summarise,
                    RSACount = length(OwnerId))

tasks.prep4 <- tasks.prep2[which(tasks.prep2$RSD %in% 'Yes'),]
tasks.aggr4 <- ddply(tasks.prep4, .(WhoId, OwnerId, Contact_Owner_Email_Address__c, ConvMonth), summarise,
                    RSDCount = length(OwnerId))

tasks.aggr4 <- tasks.aggr3
tasks.aggr4$RSDCount <- 0

tasks.prep5 <- tasks.prep2[which(tasks.prep2$Other %in% 'Yes'),]
tasks.aggr5 <- ddply(tasks.prep5, .(WhoId, OwnerId, Contact_Owner_Email_Address__c, ConvMonth), summarise,
                    OtherCount = length(OwnerId))

tasks.aggrfinal <- merge(tasks.aggr1, tasks.aggr2, all.x = T)
tasks.aggrfinal <- merge(tasks.aggrfinal, tasks.aggr3, all.x = T)
tasks.aggrfinal <- merge(tasks.aggrfinal, tasks.aggr4, all.x = T)
tasks.aggrfinal <- merge(tasks.aggrfinal, tasks.aggr5, all.x = T)

tasks.aggrfinal <- tasks.aggrfinal[c("WhoId", "OwnerId", "Contact_Owner_Email_Address__c",
                                    "ConvMonth", "ConvCount", "CompletedCount", "RSACount",
                                    "RSDCount", "OtherCount")]



tasks.aggrfinal1 <- tasks.aggrfinal[,c(1:4,5)]
tasksMonth1 <- dcast(tasks.aggrfinal1, WhoId + OwnerId + Contact_Owner_Email_Address__c ~ ConvMonth, sum)
colnames(tasksMonth1) <- paste("Conv", colnames(tasksMonth1) , sep = "_")

tasks.aggrfinal2 <- tasks.aggrfinal[,c(1:4,6)]
tasksMonth2 <- dcast(tasks.aggrfinal2, WhoId + OwnerId + Contact_Owner_Email_Address__c ~ ConvMonth, sum)
colnames(tasksMonth2) <- paste("Completed", colnames(tasksMonth2) , sep = "_")

tasks.aggrfinal3 <- tasks.aggrfinal[,c(1:4,7)]
tasksMonth3 <- dcast(tasks.aggrfinal3, WhoId + OwnerId + Contact_Owner_Email_Address__c ~ ConvMonth, sum)
colnames(tasksMonth3) <- paste("ByRSA", colnames(tasksMonth3) , sep = "_")

tasks.aggrfinal4 <- tasks.aggrfinal[,c(1:4,8)]
tasksMonth4 <- dcast(tasks.aggrfinal4, WhoId + OwnerId + Contact_Owner_Email_Address__c ~ ConvMonth, sum)
colnames(tasksMonth4) <- paste("ByRSD", colnames(tasksMonth4) , sep = "_")

tasks.aggrfinal5 <- tasks.aggrfinal[,c(1:4,9)]
tasksMonth5 <- dcast(tasks.aggrfinal5, WhoId + OwnerId + Contact_Owner_Email_Address__c ~ ConvMonth, sum)
colnames(tasksMonth5) <- paste("ByOthers", colnames(tasksMonth5) , sep = "_")


tasks.aggrFINAL <- merge(tasksMonth1, tasksMonth2, by.x = c("Conv_WhoId", "Conv_OwnerId", "Conv_Contact_Owner_Email_Address__c"),
                        by.y = c("Completed_WhoId", "Completed_OwnerId", "Completed_Contact_Owner_Email_Address__c"), all.x = T)

tasks.aggrFINAL <- merge(tasks.aggrFINAL, tasksMonth3, by.x = c("Conv_WhoId", "Conv_OwnerId", "Conv_Contact_Owner_Email_Address__c"),
                        by.y = c("ByRSA_WhoId", "ByRSA_OwnerId", "ByRSA_Contact_Owner_Email_Address__c"), all.x = T)

tasks.aggrFINAL <- merge(tasks.aggrFINAL, tasksMonth4, by.x = c("Conv_WhoId", "Conv_OwnerId", "Conv_Contact_Owner_Email_Address__c"),
                        by.y = c("ByRSD_WhoId", "ByRSD_OwnerId", "ByRSD_Contact_Owner_Email_Address__c"))

tasks.aggrFINAL <- merge(tasks.aggrFINAL, tasksMonth5, by.x = c("Conv_WhoId", "Conv_OwnerId", "Conv_Contact_Owner_Email_Address__c"),
                        by.y = c("ByOthers_WhoId", "ByOthers_OwnerId", "ByOthers_Contact_Owner_Email_Address__c"), all.x = T)



######### TASKS PREP FOR DATES AND COMPLETED STATUS ########
tasks.prep <- tasks.dup[which(tasks.dup$Subcategory__c == "Lead Gen"),]
tasks.prep <- tasks.prep[which(tasks.prep$RecordTypeName__c == "Investment"),]

tasks.prep <- tasks.prep[,c("Id", "WhoId", "OwnerId", "ActivityDate","Contact_Date__c","CreatedDate",
                           "Date_Complete__c","LastModifiedDate","SystemModstamp",
                           "Status","Subcategory__c","Type","RecordTypeName__c")]

tasks.prep$Completed <- as.yearmon(tasks.prep$CreatedDate)

tasks.Comp <- tasks.prep[which(tasks.prep$Status %in% 'Completed'),]

