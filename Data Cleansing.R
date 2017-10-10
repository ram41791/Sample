
############################################################
############################################################ 
############### DATA CLEANSING AND PREPERATION ##############


# Import data
library(RODBC)

dbhandle <- odbcDriverConnect('driver={SQL Server}; server=<server name>;
                              database=<database name>; trusted_connection=true')
contacts <- sqlQuery(dbhandle, 'select * FROM [database].[dbo].[table/view]')
tasks <- sqlQuery(dbhandle, 'select * FROM [database].[dbo].[table/view]')
leads <- sqlQuery(dbhandle, 'select * FROM [database].[dbo].[table/view]')
lifepro <- sqlQuery(dbhandle, 'select * FROM [database].[dbo].[table/view]')
ric <- lifepro=sqlQuery(dbhandle, 'select * FROM [database].[dbo].[table/view]')

discovery <- read.csv(file.choose())
crd <- read.csv(file.choose())
fan <- read.csv(file.choose())


dbhandle1 <- odbcDriverConnect('driver={SQL Server};server=<server name>;database=<database name>;trusted_connection=true')
ric <- sqlQuery(dbhandle1, 'select *  FROM [dataase].[dbo].[table/view]')



########### Checking for Nulls and Duplicates ###################

#Removing nulls in RIC
ric.nonull <- ric[!is.na(ric$AgentId),]
ric.nonull <- ric.nonull[!is.na(ric.nonull$RepBrowseKey),]


#Checking duplicates
p <- as.data.frame(table(ric.nonull$AgentId))
p <- p[order(-p$Freq),]
p1 <- p[p$Freq == 1,]

ric.dup <- ric.nonull[which(ric.nonull$AgentId %in% p1$Var1),]


# Checking nulls in Discovery
discovery.nonull <- discovery[!is.na(discovery$RepCRD),]
#No NULLs in discovery

# Checking duplicates in Discovery
p <- as.data.frame(table(discovery.nonull$RepCRD))
discovery.dup <- discovery.nonull
# No Duplicates in RepCRD


# Checking nulls in Contacts
contacts.nonull <- contacts[!is.na(contacts$Rep_Browse_Key__c),]
# Total NULLS 378956

# Checking duplicates in Contacts
p <- as.data.frame(table(contacts.nonull$Rep_Browse_Key__c))
p <- p[which(p$Freq == 1),]
contacts.dup <- contacts.nonull[which(contacts.nonull$Rep_Browse_Key__c %in% p$Var1),]


# Checking nulls in Tasks
tasks.nonull <- tasks[!is.na(tasks$WhoId),]
#No NULLs in Tasks

# Checking duplicates in Tasks
p <- as.data.frame(table(tasks.nonull$WhoId))
tasks.dup <- tasks.nonull
# No Duplicates in WhoId


# Checking nulls in Leads
leads.nonull <- leads[!is.na(leads$FINRA_CRD__c),]

# Checking duplicates in Leads
p <- as.data.frame(table(leads.nonull$FINRA_CRD__c))
p <- p[order(p$Freq),]
p <- p[which(p$Freq == 1),]
leads.dup <- leads.nonull[which(leads.nonull$FINRA_CRD__c %in% p$Var1),]


#Checking nulls in Lifepro
lifepro.bkp <- lifepro # Lifepro original backup
lifepro.nonull <- lifepro[!is.na(lifepro$Writing_Agent),]

# LifePro Filter Products
lifepro.filter <- lifepro.nonull[which(lifepro.nonull$Product_Id %in% c("MGIA-13   ","MHVA-B-16 ","MHVA-C-16 ")),]

p <- as.data.frame(table(lifepro.filter$Writing_Agent))
p <- p[order(-p$Freq),]



# JOINS
disc.leads <- merge(discovery.dup[,c("RepCRD", "AUMSelfReported")], 
                       leads.dup[,c("FINRA_CRD__c", "FirstName", "LastName",
                                        "Email", "ConvertedContactId", "CreatedDate")],
                       by.x = "RepCRD", by.y = "FINRA_CRD__c", all.y = T)

colnames(disc.leads)[which(colnames(disc.leads)=="CreatedDate" )] <- "Lead_Date"

disc.leads <- disc.leads[c("RepCRD", "ConvertedContactId", "FirstName", "LastName", 
                                  "Email", "Lead_Date", "AUMSelfReported")]


disc.leads.contacts <- merge(disc.leads, 
                                contacts.dup[c("Id", "Rep_Browse_Key__c", 
                                                   "Job_Title_Start_Date__c")],
                                by.x = "ConvertedContactId", by.y = "Id", all.x = T)

colnames(disc.leads.contacts)[which(colnames(disc.leads.contacts)=="Job_Title_Start_Date__c" )] <- "Appointed_Date"


# Ordering variables
disc.leads.contacts <- disc.leads.contacts[c("RepCRD", "ConvertedContactId", "Rep_Browse_Key__c", 
                                                    "FirstName", "LastName", "Email", "Lead_Date", 
                                                    "Appointed_Date", "AUMSelfReported" )]

disc.leads.contacts.ric <- merge(disc.leads.contacts, 
                                    ric.dup[c("RepBrowseKey", "AgentId")],
                                    by.x = "Rep_Browse_Key__c", by.y = "RepBrowseKey", 
                                    all.x = T)



disc.leads.contacts.ric <- disc.leads.contacts.ric[c("RepCRD", "ConvertedContactId", "Rep_Browse_Key__c", "AgentId", 
                                                            "FirstName", "LastName", "Email", "Lead_Date", 
                                                            "Appointed_Date", "AUMSelfReported" )]


disc.leads.contacts.ric.SalesMonth <- merge(disc.leads.contacts.ric, SalesMonth_final, 
                                               by.x = "AgentId", by.y = "Sales_Writing_Agent", all.x = T)

disc.leads.contacts.ric.SalesMonth.tasksaggrFINAL <-
  merge(disc.leads.contacts.ric.SalesMonth, tasks.aggrFINAL, 
        by.x = "ConvertedContactId", by.y = "Conv_WhoId", all.x = T)

final.bkp <- final

final <- data.frame(Lead_Date = as.Date(final$Lead_Date),
                    Appointed_Date = as.Date(final$Appointed_Date),
                    ProducerDate = as.Date(final$ProducerDate),
                    Back2AppointedDate = as.Date(final$Back2AppointedDate, "%Y-%m-%d"))

final$Lead_Date <- as.Date(final$Lead_Date)
final$Appointed_Date <- as.Date(final$Appointed_Date)
final$ProducerDate <- as.Date(final$ProducerDate)
final$Back2AppointedDate <- as.Date(final$Back2AppointedDate, "%Y-%m-%d")


final$leadToAppointed <- final1$Appointed_Date - final1$Lead_Date
final$AppointedToProducer <- final1$ProducerDate - final1$Appointed_Date
final$ProducerToBack2Appointed <- final1$Back2AppointedDate - final1$ProducerDate

final[is.na(final$`Sales_Apr 2017`)] <- 0



final$NumSalesfromApr2017 <- (final$`Sales_Apr 2017` + final$`Sales_May 2017` +
                               final$`Sales_Jun 2017` + final$`Sales_Jul 2017` +
                               final$`Sales_Aug 2017` + final$`Sales_Sep 2017`)


final$TotalConv <- (final$`Conv_Apr 2017` + final$`Conv_May 2017` +
                     final$`Conv_Jun 2017` + final$`Conv_Jul 2017`)

final$TotalCompleted <- (final$`Completed_Apr 2017` + final$`Completed_May 2017` +
                          final$`Completed_Jun 2017` + final$`Completed_Jul 2017` +
                          final$`ByRSA_Apr 2017` + final$`ByRSA_May 2017` +
                          final$`ByRSA_Jun 2017` + final$`ByRSA_Jul 2017` +
                          final$`ByRSD_Apr 2017` + final$`ByRSD_May 2017` +
                          final$`ByRSD_Jun 2017` + final$`ByRSD_Jul 2017` +
                          final$`ByOthers_Apr 2017` + final$`ByOthers_May 2017` +
                          final$`ByOthers_Jun 2017` + final$`ByOthers_Jul 2017`)


write.csv(final, "final.csv")


############### SUMMARY STATISTICS ###############

cor(final$NumSalesfromApr2017, final$TotalCompleted)
cor(final$NumSalesfromApr2017, final$TotalConv)
cor(final$TotalConv, final$leadToAppointed)
cor(final$TotalConv, final$AppointedToProducer)
cor(final$TotalConv, final$ProducerToBack2Appointed)
cor(final$TotalCompleted, final$leadToAppointed)
cor(final$TotalCompleted, final$AppointedToProducer)
cor(final$TotalCompleted, final$ProducerToBack2Appointed)


