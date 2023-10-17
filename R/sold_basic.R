# install.packages("dplyr")
# install.packages("ggplot2")
# install.packages("plotly")

library(dplyr)
library(ggplot2)
library(plotly)
library(ggpie)
library(lubridate)

df <- read.csv("datasets/recentlysold_231014_36m_basic.csv")
df$zipcode <- as.character(df$zipcode)
df$soldYear <- substr(df$dateSold, 1, 4)
df$soldMonth <- substr(df$dateSold, 1, 7)
df$dateSold <- as.Date(df$dateSold)
# df$sell_rent_rate <- df$price / df$rentZestimate


df <- df[df$price < 2000000, ]
df <- df[df$price >= 1000000, ]
df <- df[df$soldYear == "2023", ]
#df <- df[df$price < 3000000, ]

# df<- df[df$bedrooms==4,]
# df <- df[df$propertyType != "TOWNHOUSE",]


p <- ggplot(df, aes(x = dateSold, y = price/1000)) + 
  geom_jitter(width = 0.2, alpha = 0.5, colour = "blue") +
  labs(title = "Jitter Plot of Time Series", x = "Date", y = "Price") +
  theme_minimal()

plotly_object <- ggplotly(p)
plotly_object



df %>%  group_by(zipcode) %>% summarise(n=n(), avg = mean(price/1000)) ->Summary.data
p <- ggplot(data=df, aes(x=zipcode, y=price/1000)) +
  geom_boxplot(width=0.05, fill="green") +
  geom_violin(fill="gold", alpha=0.3) +
  geom_jitter(height = 0, width = 0.1) +
  geom_text(data=Summary.data ,aes(x = zipcode, y = avg, label=n),color="red", fontface =2, size = 5) +
  labs(x="Zip Code", y="Price(K)", title="price group by zipCode for tri-city")

plotly_object <- ggplotly(p)
plotly_object




df %>%  group_by(zipcode) %>% summarise(n=n(), avg = mean(pricePerFt/1000)) ->Summary.data
p <- ggplot(data=df, aes(x=zipcode, y=pricePerFt)) +
  geom_boxplot(width=0.05, fill="green") +
  geom_violin(fill="gold", alpha=0.3) +
  geom_jitter(height = 0, width = 0.1) +
  geom_text(data=Summary.data ,aes(x = zipcode, y = avg, label=n),color="red", fontface =2, size = 5) +
  labs(x="Zip Code", y="PricePerFt", title="pricePerFt group by zipCode for tri-city")

plotly_object <- ggplotly(p)
plotly_object


df %>%  group_by(soldMonth) %>% summarise(n=n(), avg = mean(price/1000)) ->Summary.data
p <- ggplot(data=df, aes(x=soldMonth, y=price/1000)) +
  geom_boxplot(width=0.05, fill="green") +
  geom_violin(fill="gold", alpha=0.3) + 
  geom_jitter(height = 0, width = 0.1) +
  geom_text(data=Summary.data ,aes(x = soldMonth, y = avg, label=n),color="red", fontface =2, size = 5) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  labs(x="month", y="Price(K)", title="Violin Plots of soldMonth price") 

plotly_object <- ggplotly(p)
plotly_object




df %>%  group_by(soldMonth) %>% summarise(n=n(), avg = mean(price/1000), median = median(price/1000)) ->Summary.data
Summary.data$soldMonth <- ym(Summary.data$soldMonth)
write.csv(Summary.data, file = "datasets/summary_group_by_month.csv", row.names = FALSE)


df %>%  group_by(soldYear) %>% summarise(n=n(), avg = mean(price/1000), median = median(price/1000)) ->Summary.data.year


install.packages("lubridate")
library(lubridate)

data <- read.csv("datasets/summary_group_by_month.csv") %>%
  filter(soldMonth != "2020-10-01" & soldMonth != "2023-10-01") 
data$soldMonth <- ymd(data$soldMonth)  

p = ggplot(data, aes(x = soldMonth)) +
  geom_line(aes(y = avg, color = "Average")) +
  geom_line(aes(y = median, color = "Median")) +
  geom_line(aes(y = n, color = "Number")) +
  labs(
    title = "Line Chart of Average and Median",
    x = "Sold Month",
    y = "Value"
  ) +
  scale_x_date(date_labels = "%Y-%m", date_breaks = "1 month") +
  scale_color_manual(values = c("Average" = "blue", "Median" = "red", "Number" = "green")) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) 
plotly_object <- ggplotly(p)
plotly_object


p = ggplot(data, aes(x = soldMonth)) +
  geom_line(aes(y = n, color = "Number")) +
  labs(
    title = "Line Chart of sold number",
    x = "Sold Month",
    y = "Value"
  ) +
  scale_x_date(date_labels = "%Y-%m", date_breaks = "1 month") +
  scale_color_manual(values = c("Number" = "blue")) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) 
plotly_object <- ggplotly(p)
plotly_object



p <- ggpie(df, zipcode)
p




