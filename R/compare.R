library(dplyr)
library(ggpie)
old <- read.csv("datasets/forsale_231001.csv")
new <- read.csv("datasets/detail_231021.csv")


# those houses still in inventory will be shown in merged_df
merged_df <- merge(old, new, by = "zpid", suffixes = c("_old", "_new"))


merged_df <- merged_df[, c('zpid', 'price_old', 'price_new', 'zestimate_old','zestimate_new', 'address_old', 'datePosted', 'homeStatus', 'lotSize', 'bedrooms_new', 'yearBuilt', 'latestEvent','link')]

merged_df %>%  group_by(homeStatus) %>% summarise(n=n(), avg_k = mean(price_new/1000), median_k = median(price_new/1000)) ->Summary.data

p <- ggpie(merged_df, homeStatus)
p
