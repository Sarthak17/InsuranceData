main<-read.csv("C:\\Users\\Sarthak\\Desktop\\Statistical\\Medical\\Sample.csv")
main$FIRST_DATE_OF_SERVICE<-as.character(main$FIRST_DATE_OF_SERVICE)

df<-data.frame("MEMBER_ID"="","SOURCE"="","FIRST_DATE_OF_SERVICE"="","PROCEDURE_CODE"="","BILLED_AMT"="","PAID_AMT"="","PROVIDER_NPI"="")
newdf<-data.frame("MEMBER_ID"="","SOURCE"="","FIRST_DATE_OF_SERVICE"="","PROCEDURE_CODE"="","BILLED_AMT"="","PAID_AMT"="","PROVIDER_NPI"="")
varid<-unique(main$MEMBER_ID)

#Sequencial version
for (i in 1:length(varid)) {
        list<-which(main$MEMBER_ID==varid[i])
        minidf<-main[list,]
        vardate<-unique(minidf$FIRST_DATE_OF_SERVICE)
        for (x in vardate) {
               list1<-which(minidf$FIRST_DATE_OF_SERVICE==vardate[x])
               df$FIRST_DATE_OF_SERVICE=x
               df$SOURCE<-paste(minidf$SOURCE[minidf$FIRST_DATE_OF_SERVICE==x],collapse = ",")
               df$PROCEDURE_CODE<-paste(minidf$PROCEDURE_CODE[minidf$FIRST_DATE_OF_SERVICE==x],collapse = ",")
               df$BILLED_AMT<-paste(minidf$BILLED_AMT[minidf$FIRST_DATE_OF_SERVICE==x],collapse = ",")
               df$PAID_AMT<-paste(minidf$PAID_AMT[minidf$FIRST_DATE_OF_SERVICE==x],collapse = ",")
               df$MEMBER_ID=minidf$MEMBER_ID[1]
               df$PROVIDER_NPI=minidf$PROVIDER_NPI[1]
               newdf<-rbind(df,newdf)
        }
        
}

#Parallel Version
M1<- function(x) {
        len<-varid[x]
        for (i in len){
                list<-which(main$MEMBER_ID==i)
                minidf<-main[list,]
                vardate<-unique(minidf$FIRST_DATE_OF_SERVICE)
                for (x in vardate) {
                        list1<-which(minidf$FIRST_DATE_OF_SERVICE==vardate[x])
                        df$FIRST_DATE_OF_SERVICE=x
                        df$SOURCE<-paste(minidf$SOURCE[minidf$FIRST_DATE_OF_SERVICE==x],collapse = ",")
                        df$PROCEDURE_CODE<-paste(minidf$PROCEDURE_CODE[minidf$FIRST_DATE_OF_SERVICE==x],collapse = ",")
                        df$BILLED_AMT<-paste(minidf$BILLED_AMT[minidf$FIRST_DATE_OF_SERVICE==x],collapse = ",")
                        df$PAID_AMT<-paste(minidf$PAID_AMT[minidf$FIRST_DATE_OF_SERVICE==x],collapse = ",")
                        df$MEMBER_ID=minidf$MEMBER_ID[1]
                        df$PROVIDER_NPI=minidf$PROVIDER_NPI[1]
                        newdf<-rbind(df,newdf)
                }
        }
        return(newdf)
}



cvb<-1:length(varid)
library(snow)
cl <- makeCluster(2)
registerDoParallel(cl)
clusterExport(cl, list=c("cvb","M1", "varid","main","df","newdf"), envir=.GlobalEnv)
r<-parLapply(cl, cvb, function(y) M1(y))

final<-as.data.frame(r)                    

#final<-do.call("rbind", list(df2,df3,df4,df5))
#Remove blank rows
final<-final[!(final$MEMBER_ID==""),]
              

