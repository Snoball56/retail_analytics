CREATE TABLE dim_product (
    ProductKey INT PRIMARY KEY,
    StockCode NVARCHAR(50),
    Description NVARCHAR(255)
);

CREATE TABLE dim_customer (
    CustomerKey INT PRIMARY KEY,
    CustomerID NVARCHAR(50),
    Country NVARCHAR(100)
);

CREATE TABLE dim_date (
    DateKey INT PRIMARY KEY,
    Date DATE,
    Year INT,
    Quarter INT,
    MonthNumber INT,
    MonthName NVARCHAR(20),
    YearMonth NVARCHAR(10),
    Day INT,
    Weekday NVARCHAR(20),
    WeekdayNumber INT,
    IsWeekend BIT
);

CREATE TABLE fact_sales (
    InvoiceNo NVARCHAR(20),
    DateKey INT,
    ProductKey INT,
    CustomerKey INT,
    Quantity INT,
    UnitPrice FLOAT,
    Revenue FLOAT
);