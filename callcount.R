library(dplyr)
library(stringr)
library(tidyr)

CODE <- read.csv("//knx1fs01/ED Reporting/Lowhorn Big Data/Golden Rule Data/CODE.csv")
#########DILLON HIGHTOWER ADD############
a <- read.csv("//knx1fs01/ED Reporting/Lowhorn Big Data/Golden Rule Data/a.txt", stringsAsFactors=FALSE)
b <- read.csv("//knx1fs01/ED Reporting/Lowhorn Big Data/Golden Rule Data/b.txt", stringsAsFactors=FALSE)
c <- read.csv("//knx1fs01/ED Reporting/Lowhorn Big Data/Golden Rule Data/c.txt", stringsAsFactors=FALSE)
d <- read.csv("//KNX1FS01/ED Reporting/Lowhorn Big Data/Golden Rule Data/d.csv",header=TRUE,stringsAsFactors = FALSE)
e <- read.csv("//KNX1FS01/ED Reporting/Lowhorn Big Data/Golden Rule Data/e.csv",header=TRUE,stringsAsFactors = FALSE)
f <- read.csv("//KNX1FS01/ED Reporting/Lowhorn Big Data/Golden Rule Data/f.csv",header=TRUE,stringsAsFactors = FALSE)
g <- read.csv("//KNX1FS01/ED Reporting/Lowhorn Big Data/Golden Rule Data/g.csv",header=TRUE,stringsAsFactors = FALSE)
h <- read.csv("//KNX1FS01/ED Reporting/Lowhorn Big Data/Golden Rule Data/h.csv",header=TRUE,stringsAsFactors = FALSE)
i <- read.csv("//KNX1FS01/ED Reporting/Lowhorn Big Data/Golden Rule Data/i.csv",header=TRUE,stringsAsFactors = FALSE)
j <- read.csv("//KNX1FS01/ED Reporting/Lowhorn Big Data/Golden Rule Data/j.csv",header=TRUE,stringsAsFactors = FALSE)
k <- read.csv("//KNX1FS01/ED Reporting/Lowhorn Big Data/Golden Rule Data/k.csv",header=TRUE,stringsAsFactors = FALSE)
l <- read.csv("//KNX1FS01/ED Reporting/Lowhorn Big Data/Golden Rule Data/l.csv",header=TRUE,stringsAsFactors = FALSE)
m <- read.csv("//KNX1FS01/ED Reporting/Lowhorn Big Data/Golden Rule Data/m.csv",header=TRUE,stringsAsFactors = FALSE)
n <- read.csv("//KNX1FS01/ED Reporting/Lowhorn Big Data/Golden Rule Data/n.csv",header=TRUE,stringsAsFactors = FALSE)
o <- read.csv("//KNX1FS01/ED Reporting/Lowhorn Big Data/Golden Rule Data/o.csv",header=TRUE,stringsAsFactors = FALSE)

knoxtalx <- read.csv("C:/Users/193344/Desktop/AWG Transactions/knoxtalx.csv", stringsAsFactors=FALSE)
#########################################

#########DILLON HIGHTOWER ADD############
df <- rbind(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o)
rm(a); rm(b); rm(c); rm(d); rm(e); rm(f); rm(g); rm(h); rm(i); rm(j); rm(k); rm(l); rm(m); rm(n); rm(o)
#########################################
#######################################################################################################

DATE <- knoxtalx$Date
DATE <- as.data.frame(DATE)

DATE$DATE <- as.Date(DATE$DATE,"%m/%d/%Y")

accounts <- as.character(knoxtalx$ACCOUNT)

df$TFILE <- as.character(df$TFILE)

df <- df[df$TFILE %in% accounts,]

data <- df[!duplicated(df$TFILE),]


contacts <- c("CM","A3P","AT")
codes <- c("1C","1H","1P","215H","215W","225H","2H","2P","365W","370W","375W","380W","385W","390W","395W","400W","405W")
home <- c("1C","1H","215H","225H","2H","105H","1P","215A","310H","315H","335A","335H",
          "360H","364A","365A","365H","370H","375H","380H","385H","390H","395H","75H",
          "ACRH","ADLH","ADLW","AEHW","AKAA","AKAH","ALTA","ALTAA","ALTH","AP3H","BUSH",
          "CBCH","CBPA","CBPH","CBRH","CBRH","CIDH","CLAA","CLAH","CLIH","DNCH","EMPH","EMPA",
          "FRIH","FRNH","FTHH","HMEA","HMEH","HOH","INVH","MANA","MANH","MOTH","MU","MULTH",
          "N2PH","NB","NBYH","NLEH","OTHA","OTHERA","OTHERH","OTHH","PARH","PHNH","PLCH","PORH",
          "SLFH","SPOH","STDH","TALXH","TLOH","TRKH","TRUH","UNKH","VARH")
poe1 <- c("1P","215W","2P")
poe <- c("1P","215W","2P","365W","370W","375W","380W","385W","390W","395W","400W","405W","335W",
         "TLOW","MANW","POEW","CBPW","SLFW","RELW","ADLW","TRUW","CLAW","PROW","UNKW","310W",
         "CBRW","NBYW","AKAW","CLIW","PLCW","REFW","ALTW","MULTW","EMPW","TRKW")

contact_df <- df[df$CODE_2 %in% contacts,]
contact_df$ACT_DATE <- as.Date(contact_df$ACT_DATE,"%m/%d/%Y")
#contact_df <- contact_df[contact_df$ACT_DATE >= DATE,]

contact_data <- contact_df %>%
  group_by(TFILE) %>%
  summarize(Contacts = n())

##############################################################
df <- df[df$CODE_1 %in% codes,]
df$ACT_DATE <- as.Date(df$ACT_DATE,"%m/%d/%Y")

df$T <- gsub("^.*? ","",df$TIME)
df$DTE <- gsub(" 0:00:00","",df$ACT_DATE)
time <- as.data.frame(str_split_fixed(df$T,":",n=3))
time <- dplyr::rename(time, Hour = V1, Minute = V2, Second = V3)

time$Hour <- as.numeric(as.character(time$Hour))
time$Minute <- as.numeric(as.character(time$Minute))
time$Second <- as.numeric(as.character(time$Second))

df <- cbind(df,time)

df$Day <- weekdays(df$ACT_DATE) 
poedf <- df
poedf$ACT_DATE <- as.Date(poedf$ACT_DATE,"%m/%d/%Y")
knoxtalx <- rename(knoxtalx,TFILE=ACCOUNT)
poedf$TFILE <- as.character(poedf$TFILE)
knoxtalx$TFILE <- as.character(knoxtalx$TFILE)
poedf <- left_join(poedf,knoxtalx,by="TFILE")
poedf$Date <- as.Date(poedf$Date,"%m/%d/%Y")
poedf <- poedf %>%
  mutate(flag = ifelse(ACT_DATE >= Date,1,0))
poedf <- poedf[poedf$flag >= 1,]
poedf <- poedf[poedf$CODE_1 %in% poe,]

##############################################################

mondayafternoon <- df %>%
  mutate(Home = ifelse(CODE_1 %in% home,1,0)) %>%
  filter(Day=="Monday") %>%
  mutate(Afternoon = ifelse(Hour>=12,1,0)) %>%
  filter(Afternoon == 1) %>%
  group_by(TFILE) %>%
  summarize(Monday_Afternoon = sum(Home))

mondaymorning <- df %>%
  mutate(Home = ifelse(CODE_1 %in% home,1,0)) %>%
  filter(Day=="Monday") %>%
  mutate(Afternoon = ifelse(Hour<=12,1,0)) %>%
  filter(Afternoon == 1) %>%
  group_by(TFILE) %>%
  summarize(Monday_Morning = sum(Home))

tuesdayafternoon <- df %>%
  mutate(Home = ifelse(CODE_1 %in% home,1,0)) %>%
  filter(Day=="Tuesday") %>%
  mutate(Afternoon = ifelse(Hour>=12,1,0)) %>%
  filter(Afternoon == 1) %>%
  group_by(TFILE) %>%
  summarize(Tuesday_Afternoon = sum(Home))

tuesdaymorning <- df %>%
  mutate(Home = ifelse(CODE_1 %in% home,1,0)) %>%
  filter(Day=="Tuesday") %>%
  mutate(Afternoon = ifelse(Hour<=12,1,0)) %>%
  filter(Afternoon == 1) %>%
  group_by(TFILE) %>%
  summarize(Tuesday_Morning = sum(Home))

wednesdayafternoon <- df %>%
  mutate(Home = ifelse(CODE_1 %in% home,1,0)) %>%
  filter(Day=="Wednesday") %>%
  mutate(Afternoon = ifelse(Hour>=12,1,0)) %>%
  filter(Afternoon == 1) %>%
  group_by(TFILE) %>%
  summarize(Wednesday_Afternoon = sum(Home))

wednesdaymorning <- df %>%
  mutate(Home = ifelse(CODE_1 %in% home,1,0)) %>%
  filter(Day=="Wednesday") %>%
  mutate(Afternoon = ifelse(Hour<=12,1,0)) %>%
  filter(Afternoon == 1) %>%
  group_by(TFILE) %>%
  summarize(Wednesday_Morning = sum(Home))

thursdayafternoon <- df %>%
  mutate(Home = ifelse(CODE_1 %in% home,1,0)) %>%
  filter(Day=="Thursday") %>%
  mutate(Afternoon = ifelse(Hour>=12,1,0)) %>%
  filter(Afternoon == 1) %>%
  group_by(TFILE) %>%
  summarize(Thursday_Afternoon = sum(Home))

thursdaymorning <- df %>%
  mutate(Home = ifelse(CODE_1 %in% home,1,0)) %>%
  filter(Day=="Thursday") %>%
  mutate(Afternoon = ifelse(Hour<=12,1,0)) %>%
  filter(Afternoon == 1) %>%
  group_by(TFILE) %>%
  summarize(Thursday_Morning = sum(Home))

fridayafternoon <- df %>%
  mutate(Home = ifelse(CODE_1 %in% home,1,0)) %>%
  filter(Day=="Friday") %>%
  mutate(Afternoon = ifelse(Hour>=12,1,0)) %>%
  filter(Afternoon == 1) %>%
  group_by(TFILE) %>%
  summarize(Friday_Afternoon = sum(Home))

fridaymorning <- df %>%
  mutate(Home = ifelse(CODE_1 %in% home,1,0)) %>%
  filter(Day=="Friday") %>%
  mutate(Afternoon = ifelse(Hour<=12,1,0)) %>%
  filter(Afternoon == 1) %>%
  group_by(TFILE) %>%
  summarize(Friday_Morning = sum(Home))

saturdaymorning <- df %>%
  mutate(Home = ifelse(CODE_1 %in% home,1,0)) %>%
  filter(Day=="Saturday") %>%
  mutate(Afternoon = ifelse(Hour<=12,1,0)) %>%
  filter(Afternoon == 1) %>%
  group_by(TFILE) %>%
  summarize(Saturday_Morning = sum(Home))

homedata <- df %>%
  mutate(Home = ifelse(CODE_1 %in% home,1,0)) %>%
  group_by(TFILE) %>%
  summarize(Home_Attempts = sum(Home==1))

poedata <- poedf %>%
  mutate(POE = ifelse(CODE_1 %in% poe1,1,0)) %>%
  group_by(TFILE) %>%
  summarize(POE_Attempts = sum(POE==1))

poemsg <- poedf %>%
   mutate(POE_MSG = ifelse(CODE_3 == "MSG",1,0)) %>%
  group_by(TFILE) %>%
  summarize(POE_Messages = sum(POE_MSG==1))

location1 <- poedf %>%
  filter(CODE_1=="365W") %>%
  group_by(TFILE) %>%
  summarize(Location1 = n())

location2 <- poedf %>%
  filter(CODE_1=="370W") %>%
  group_by(TFILE) %>%
  summarize(Location2 = n())

location3 <- poedf %>%
  filter(CODE_1=="375W") %>%
  group_by(TFILE) %>%
  summarize(Location3 = n())

location4 <- poedf %>%
  filter(CODE_1=="380W") %>%
  group_by(TFILE) %>%
  summarize(Location4 = n())

location5 <- poedf %>%
  filter(CODE_1=="385W") %>%
  group_by(TFILE) %>%
  summarize(Location5 = n())

location6 <- poedf %>%
  filter(CODE_1=="390W") %>%
  group_by(TFILE) %>%
  summarize(Location6 = n())

location7 <- poedf %>%
  filter(CODE_1=="395W") %>%
  group_by(TFILE) %>%
  summarize(Location7 = n())

location8 <- poedf %>%
  filter(CODE_1=="400W") %>%
  group_by(TFILE) %>%
  summarize(Location8 = n())

location9 <- poedf %>%
  filter(CODE_1=="405W") %>%
  group_by(TFILE) %>%
  summarize(Location9 = n())

finaldata <- left_join(homedata,contact_data,by="TFILE")
finaldata <- left_join(finaldata,mondaymorning,by="TFILE")     
finaldata <- left_join(finaldata,mondayafternoon,by="TFILE")  
finaldata <- left_join(finaldata,tuesdaymorning,by="TFILE")  
finaldata <- left_join(finaldata,tuesdayafternoon,by="TFILE")  
finaldata <- left_join(finaldata,wednesdaymorning,by="TFILE") 
finaldata <- left_join(finaldata,wednesdayafternoon,by="TFILE") 
finaldata <- left_join(finaldata,thursdaymorning,by="TFILE") 
finaldata <- left_join(finaldata,thursdayafternoon,by="TFILE") 
finaldata <- left_join(finaldata,fridaymorning,by="TFILE") 
finaldata <- left_join(finaldata,fridayafternoon,by="TFILE") 
finaldata <- left_join(finaldata,saturdaymorning,by="TFILE") 
finaldata$TFILE <- as.character(finaldata$TFILE)
finaldata <- left_join(finaldata,poedata,by="TFILE") 
finaldata <- left_join(finaldata,poemsg,by="TFILE") 
finaldata <- left_join(finaldata,location1,by="TFILE") 
finaldata <- left_join(finaldata,location2,by="TFILE") 
finaldata <- left_join(finaldata,location3,by="TFILE") 
finaldata <- left_join(finaldata,location4,by="TFILE") 
finaldata <- left_join(finaldata,location5,by="TFILE") 
finaldata <- left_join(finaldata,location6,by="TFILE") 
finaldata <- left_join(finaldata,location7,by="TFILE") 
finaldata <- left_join(finaldata,location8,by="TFILE") 
finaldata <- left_join(finaldata,location9,by="TFILE") 

finaldata[is.na(finaldata)] <- 0

Monday <- c("Monday_Afternoon","Monday_Morning")
Tuesday <- c("Tuesday_Afternoon","Tuesday_Morning")
Wednesday <- c("Wednesday_Afternoon","Wednesday_Morning")
Thursday <- c("Thursday_Afternoon","Thursday_Morning")
Friday <- c("Friday_Afternoon","Friday_Morning")
Saturday <- c("Saturday_Afternoon","Saturday_Morning")

finaldata <- finaldata %>%
  mutate(Mon_Morn = ifelse(Monday_Morning >= 1,1,0)) %>%
  mutate(Tues_Morn = ifelse(Tuesday_Morning >= 1,1,0)) %>%
  mutate(Wed_Morn = ifelse(Wednesday_Morning >= 1,1,0)) %>%
  mutate(Thur_Morn = ifelse(Thursday_Morning >= 1,1,0)) %>%
  mutate(Fri_Morn = ifelse(Friday_Morning >= 1,1,0)) %>%
  mutate(Sat_Morn = ifelse(Saturday_Morning >= 1,1,0)) %>%
  mutate(Morning = ifelse((Mon_Morn+Tues_Morn+Wed_Morn+Thur_Morn+Fri_Morn+Sat_Morn) >= 1,1,0)) %>%
  mutate(Mon_Night = ifelse(Monday_Afternoon >= 1,1,0)) %>%
  mutate(Tues_Night = ifelse(Tuesday_Afternoon >= 1,1,0)) %>%
  mutate(Wed_Night = ifelse(Wednesday_Afternoon >= 1,1,0)) %>%
  mutate(Thur_Night = ifelse(Thursday_Afternoon >= 1,1,0)) %>%
  mutate(Fri_Night = ifelse(Friday_Afternoon >= 1,1,0)) %>%
  mutate(Afternoon = ifelse((Mon_Night+Tues_Night+Wed_Night+Thur_Night+Fri_Night) >= 1,1,0)) %>%
  mutate(Time_Of_Day =ifelse((Morning+Afternoon)==2,"Yes","No")) %>%
  mutate(Num_of_Home_Attempts = ifelse(Home_Attempts >= 5,"Yes","No")) %>%
  mutate(Mon = ifelse((Monday_Morning+Monday_Afternoon)>=1,1,0)) %>%
  mutate(Tues = ifelse(sum(Tuesday_Morning,Tuesday_Afternoon)>=1,1,0)) %>%
  mutate(Wed = ifelse((Wednesday_Morning+Wednesday_Afternoon)>=1,1,0)) %>%
  mutate(Thurs = ifelse((Thursday_Morning+Thursday_Afternoon)>=1,1,0)) %>%
  mutate(Fri = ifelse((Friday_Morning+Friday_Afternoon)>=1,1,0)) %>%
  mutate(Sat = ifelse(Saturday_Morning>=1,1,0)) %>%
  mutate(HA_Multiple_Days_of_Week = ifelse((Mon+Tues+Wed+Thurs+Fri+Sat)>=3,"Yes","No")) %>%
  mutate(Location1 = ifelse(Location1 >=1,1,0)) %>%
  mutate(Location2 = ifelse(Location2 >=1,1,0)) %>%
  mutate(Location3 = ifelse(Location3 >=1,1,0)) %>%
  mutate(Location4 = ifelse(Location4 >=1,1,0)) %>%
  mutate(Location5 = ifelse(Location5 >=1,1,0)) %>%
  mutate(Location6 = ifelse(Location6 >=1,1,0)) %>%
  mutate(Location7 = ifelse(Location7 >=1,1,0)) %>%
  mutate(Location8 = ifelse(Location8 >=1,1,0)) %>%
  mutate(Location9 = ifelse(Location9 >=1,1,0)) %>%
  mutate(POE_Location_Attempts = Location1+Location2+Location3+Location4+Location5+Location6+Location7+Location8+Location9) %>%
  select(TFILE,Contacts,Num_of_Home_Attempts,Time_Of_Day,HA_Multiple_Days_of_Week,POE_Attempts,POE_Messages,POE_Location_Attempts)
  
finale <- left_join(knoxtalx, finaldata,by="TFILE")

setwd("C:/Users/193344/Desktop/AWG Transactions")
write.csv(finale,"data.csv")
