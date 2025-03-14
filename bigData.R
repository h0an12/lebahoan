# Cài đặt thư viện cần thiết
library(tidyverse)
library(lubridate)
library(ggplot2)
library(forecast)
library(dplyr)
library(readr)
library(scales)
library(corrplot)
library(sparklyr)

# Kết nối Spark
sc <- spark_connect(master = "local", version = "3.5.4")

# Đọc dữ liệu từ file CSV vào Spark DataFrame
file_path <- "C:/RStudio/revenue_analysis_bigdata.csv"
data_spark <- spark_read_csv(sc, name = "data", path = file_path, infer_schema = TRUE, header = TRUE)

# Chuyển đổi dữ liệu từ Spark DataFrame sang R DataFrame
data <- data_spark %>% collect()

# Kiểm tra dữ liệu ban đầu
str(data)
summary(data)
head(data)

# Chuyển đổi định dạng ngày
data$Date <- as.Date(data$Date, format="%Y-%m-%d")

# Kiểm tra dữ liệu bị thiếu hoặc bất thường
sum(is.na(data))
data <- na.omit(data)  # Loại bỏ dòng có giá trị NA

# Kiểm tra các giá trị bất thường trong doanh thu
data <- data %>% filter(Revenue > 0)

# Tổng doanh thu theo vị trí địa lý
revenue_by_location <- data %>%
  group_by(Location) %>%
  summarise(Total_Revenue = sum(Revenue)) %>%
  arrange(desc(Total_Revenue))

# Tổng doanh thu theo sản phẩm
revenue_by_product <- data %>%
  group_by(Product) %>%
  summarise(Total_Revenue = sum(Revenue)) %>%
  arrange(desc(Total_Revenue))

# Tổng doanh thu theo danh mục sản phẩm
revenue_by_category <- data %>%
  group_by(Category) %>%
  summarise(Total_Revenue = sum(Revenue)) %>%
  arrange(desc(Total_Revenue))

# Vẽ biểu đồ doanh thu theo vị trí địa lý
ggplot(revenue_by_location, aes(x=reorder(Location, -Total_Revenue), y=Total_Revenue)) +
  geom_bar(stat="identity", fill="steelblue") +
  theme_minimal() +
  labs(title="Doanh thu theo vị trí địa lý",
       x="Địa điểm",
       y="Tổng doanh thu") +
  theme(axis.text.x = element_text(angle=45, hjust=1))

# Vẽ biểu đồ doanh thu theo sản phẩm
ggplot(revenue_by_product, aes(x=reorder(Product, -Total_Revenue), y=Total_Revenue)) +
  geom_bar(stat="identity", fill="darkgreen") +
  theme_minimal() +
  labs(title="Doanh thu theo sản phẩm",
       x="Sản phẩm",
       y="Tổng doanh thu") +
  theme(axis.text.x = element_text(angle=45, hjust=1))

# Vẽ biểu đồ doanh thu theo danh mục sản phẩm
ggplot(revenue_by_category, aes(x=reorder(Category, -Total_Revenue), y=Total_Revenue)) +
  geom_bar(stat="identity", fill="purple") +
  theme_minimal() +
  labs(title="Doanh thu theo danh mục sản phẩm",
       x="Danh mục",
       y="Tổng doanh thu") +
  theme(axis.text.x = element_text(angle=45, hjust=1))



# Dự báo doanh thu (Sử dụng mô hình tuyến tính)
revenue_ts <- ts(data$Revenue, start=c(year(min(data$Date)), month(min(data$Date))), frequency=12)
model <- auto.arima(revenue_ts)
forecast_revenue <- forecast(model, h=6)

# Vẽ biểu đồ dự báo
autoplot(forecast_revenue) +
  labs(title="Dự báo doanh thu 6 tháng tiếp theo",
       x="Thời gian",
       y="Doanh thu")

# Ngắt kết nối Spark
spark_disconnect(sc)
