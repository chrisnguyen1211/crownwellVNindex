# Crownwell Investment Platform - Macro Data Dashboard

## Tổng Quan

Trang Macro Data Dashboard cung cấp một bộ công cụ toàn diện để theo dõi và phân tích các chỉ số kinh tế vĩ mô toàn cầu, giúp nhà đầu tư đưa ra quyết định đầu tư thông minh dựa trên dữ liệu kinh tế.

## Tính Năng Chính

### 📊 Key Indicators
- **GDP Growth**: Tăng trưởng GDP theo năm và quý
- **Fed Funds Rate**: Lãi suất Fed hiện tại và dự báo
- **CPI Inflation**: Chỉ số giá tiêu dùng và core CPI
- **Unemployment Rate**: Tỷ lệ thất nghiệp
- **Manufacturing PMI**: Chỉ số quản lý mua hàng sản xuất
- **Consumer Confidence**: Chỉ số niềm tin tiêu dùng
- **Retail Sales**: Doanh số bán lẻ
- **Housing Starts**: Khởi công xây dựng nhà ở

### 💰 Interest Rates & Yield Curve
- **Fed Funds Rate**: Lãi suất Fed hiện tại và dự báo từ CME FedWatch
- **Treasury Yields**: Lãi suất trái phiếu kho bạc các kỳ hạn (3M, 6M, 1Y, 2Y, 5Y, 10Y, 30Y)
- **Yield Curve Analysis**: Phân tích đường cong lãi suất và cảnh báo đảo ngược

### 📈 Inflation & Employment
- **Inflation Indicators**: CPI, Core CPI, PCE, Core PCE
- **Employment Data**: Nonfarm Payrolls, Unemployment Rate, Labor Force Participation
- **Wage Growth**: Tăng trưởng lương trung bình

### 🏭 Economic Activity
- **PMI Data**: Manufacturing, Services, Composite PMI
- **Industrial Production**: Sản xuất công nghiệp và tỷ lệ sử dụng công suất
- **Retail Sales**: Doanh số bán lẻ và xu hướng tiêu dùng
- **Housing Market**: Housing Starts, Building Permits

### 🔍 Analysis & Insights
- **Correlation Analysis**: Phân tích tương quan giữa các chỉ số
- **Market Outlook**: Dự báo xu hướng thị trường (Bullish/Bearish/Neutral)
- **Sector Recommendations**: Khuyến nghị ngành dựa trên dữ liệu macro
- **Risk Assessment**: Đánh giá mức độ rủi ro thị trường
- **Investment Insights**: Gợi ý đầu tư dựa trên phân tích macro

## Nguồn Dữ Liệu

### API Sources
- **FRED (Federal Reserve Economic Data)**: Dữ liệu chính thức từ Fed
- **Alpha Vantage**: Dữ liệu tài chính và kinh tế
- **CME FedWatch**: Dự báo lãi suất Fed

### Web Scraping Fallbacks
- **Treasury.gov**: Lãi suất trái phiếu kho bạc
- **BLS (Bureau of Labor Statistics)**: Dữ liệu việc làm và lạm phát
- **BEA (Bureau of Economic Analysis)**: Dữ liệu GDP
- **ISM (Institute for Supply Management)**: Dữ liệu PMI
- **Census Bureau**: Dữ liệu bán lẻ và nhà ở
- **Conference Board**: Chỉ số niềm tin tiêu dùng

## Cách Sử Dụng

### 1. Truy Cập Dashboard
- Chọn "📈 Macro Data Dashboard" từ navigation menu
- Dashboard sẽ tự động load dữ liệu mới nhất

### 2. Cấu Hình API Keys (Tùy Chọn)
- Vào Settings để cấu hình API keys cho FRED và Alpha Vantage
- Nếu không có API keys, hệ thống sẽ sử dụng mock data

### 3. Theo Dõi Dữ Liệu
- **Auto-refresh**: Bật tự động refresh mỗi 5 phút
- **Manual Refresh**: Nhấn nút "🔄 Refresh All Data"
- **Data Source**: Chọn giữa Live Data và Mock Data

### 4. Phân Tích Insights
- Xem tab "🔍 Analysis & Insights" để hiểu tác động của dữ liệu macro
- Theo dõi Market Outlook và Sector Recommendations
- Đọc Investment Recommendations dựa trên phân tích

## Pipeline Phân Tích Dữ Liệu

### 1. Thu Thập Dữ Liệu
```
API Sources → Web Scraping → Data Validation → Cache Storage
```

### 2. Xử Lý & Làm Sạch
- Chuẩn hóa định dạng dữ liệu
- Xử lý dữ liệu thiếu và outliers
- Tính toán các chỉ số phái sinh (MoM, YoY)

### 3. Phân Tích Tương Quan
- **GDP vs Employment**: Tăng trưởng kinh tế và thị trường lao động
- **Inflation vs Fed Rate**: Lạm phát và chính sách tiền tệ
- **PMI vs GDP**: Chỉ số dẫn dắt và tăng trưởng GDP
- **Consumer Confidence vs Retail Sales**: Niềm tin và chi tiêu

### 4. Ma Trận Ảnh Hưởng
- **GDP mạnh + CPI ổn định**: Tăng phân bổ cổ phiếu cyclical
- **CPI cao + Fed hawkish**: Giảm rủi ro, tăng trái phiếu ngắn hạn
- **PMI yếu + Housing giảm**: Chuyển sang defensive sectors

### 5. Ra Quyết Định Đầu Tư
- **Market Outlook**: Bullish/Bearish/Neutral
- **Sector Recommendations**: Ngành nào nên tăng/giảm phân bổ
- **Risk Level**: High/Medium/Low
- **Key Drivers**: Các yếu tố chính ảnh hưởng thị trường

## Ví Dụ Phân Tích Thực Tế

### Scenario: CPI Cao + PMI Yếu
```
Dữ liệu:
- CPI YoY: 3.5% (>3% target)
- Manufacturing PMI: 48.5 (<50)
- Fed Rate: 5.25%

Phân tích:
- Lạm phát cao → Fed có thể tăng lãi suất
- PMI dưới 50 → Khu vực sản xuất co hẹp
- Kinh tế có dấu hiệu stagflation

Khuyến nghị:
- Giảm phân bổ cổ phiếu cyclical
- Tăng phân bổ trái phiếu ngắn hạn
- Theo dõi Housing Starts và Retail Sales
```

### Scenario: GDP Mạnh + Employment Tốt
```
Dữ liệu:
- GDP Growth YoY: 3.2%
- Unemployment Rate: 3.6%
- Consumer Confidence: 105.2

Phân tích:
- Kinh tế tăng trưởng mạnh
- Thị trường lao động khỏe mạnh
- Niềm tin tiêu dùng cao

Khuyến nghị:
- Tăng phân bổ cổ phiếu cyclical
- Ưu tiên ngành tiêu dùng và công nghiệp
- Theo dõi Fed policy để điều chỉnh
```

## Cấu Hình Nâng Cao

### API Keys Setup
```python
# Trong macro_data_helper.py
fetcher = MacroDataFetcher()
fetcher.fred_api_key = "your_fred_api_key"
fetcher.alpha_vantage_key = "your_alpha_vantage_key"
```

### Custom Alerts
```python
# Thiết lập cảnh báo khi chỉ số vượt ngưỡng
if cpi_yoy > 4.0:
    send_alert("High inflation detected: CPI > 4%")

if unemployment_rate > 6.0:
    send_alert("High unemployment: Rate > 6%")
```

### Data Export
- Export dữ liệu macro ra CSV/Excel
- Tích hợp với portfolio analysis
- Lưu lịch sử dữ liệu để phân tích xu hướng

## Troubleshooting

### Lỗi Thường Gặp
1. **API Rate Limit**: Giảm tần suất refresh hoặc sử dụng mock data
2. **Data Not Loading**: Kiểm tra kết nối internet và API keys
3. **Outdated Data**: Nhấn refresh manual hoặc kiểm tra cache settings

### Performance Tips
- Sử dụng cache để giảm API calls
- Bật auto-refresh chỉ khi cần thiết
- Chọn data source phù hợp (Live vs Mock)

## Roadmap

### Phase 1 (Hiện tại)
- ✅ Basic macro indicators
- ✅ Yield curve analysis
- ✅ Correlation analysis
- ✅ Investment insights

### Phase 2 (Sắp tới)
- 🔄 Real-time data feeds
- 🔄 Advanced charting
- 🔄 Custom alerts
- 🔄 Portfolio integration

### Phase 3 (Tương lai)
- 🔄 Machine learning predictions
- 🔄 Global macro data
- 🔄 Sector rotation analysis
- 🔄 Risk management tools

## Liên Hệ & Hỗ Trợ

Nếu có vấn đề hoặc góp ý về Macro Data Dashboard, vui lòng liên hệ team phát triển.

---
*Crownwell Investment Platform - Empowering Smart Investment Decisions*
