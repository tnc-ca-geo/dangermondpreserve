library(readr)
library(ggplot2)
library(dplyr)
library(sprintf)

full_well_data <- read_csv("Data/full_well_data.csv")

for (c_well in unique(full_well_data$well)) {
  print(c_well)
  well_data = full_well_data %>%
    filter(well == c_well)
  plot <- ggplot(well_data, aes(x = datetime, y = level_corrected, color = well)) +
    geom_point() +
    labs(x = "Datetime", y = "Well Level", title = sprintf("%s water levels (ft)", c_well))
  print(plot)
  }
  

ggplot(full_well_data, aes(x = datetime, y = level_corrected, color = well)) +
  geom_line() +
  labs(x = "X Axis Label", y = "Y Axis Label", title = "Line Plot with Different Colored Series") +
  ylim(0,100)

tinta5 = full_well_data %>% filter(well == "tinta5")
