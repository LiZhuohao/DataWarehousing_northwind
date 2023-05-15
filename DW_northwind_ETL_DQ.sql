---------------------------------------
--Data Warehousing
--My Northwind's DW ETL+DQ Script File
--Student ID:   	1435523
--Student Name: 	Zhuohao Li
---------------------------------------

print '***************************************************************'
print '****** Section 1: Creating DW Tables'
print '***************************************************************'

print 'Drop all DW tables (except dimTime)'
--Add drop statements below...
--DO NOT DROP dimTime table as you must have used Script provided on the Moodle to create it
DROP TABLE factOrders;
DROP TABLE dimCustomers;
DROP TABLE dimProducts;
DROP TABLE dimSuppliers;
DROP TABLE S_Orders;

print 'Creating all dimension tables required'
--Add statements below... 

CREATE TABLE dimCustomers 
(
	CustomerKey			int				IDENTITY(1,1) PRIMARY KEY,
	CustomerID			nchar(5),
	CompanyName			nvarchar(40),
	ContactName			nvarchar(30),
	ContactTitle		nvarchar(30),
	Address				nvarchar(60),
	City				nvarchar(15),
	Region				nvarchar(15),
	PostalCode			nvarchar(10),
	Country				nvarchar(15),
	Phone				nvarchar(24),
	Fax					nvarchar(24)
);

CREATE TABLE dimProducts 
(
	ProductKey			int				IDENTITY(1,1) PRIMARY KEY,
	ProductID			int,
	ProductName			nvarchar(40),
	QuantityPerUnit		nvarchar(20),
	UnitPrice			money,
	UnitsInStock		smallint,
	UnitsOnOrder		smallint,
	ReorderLevel		smallint,
	Discontinued		bit,
	CategoryName		nvarchar(15),
	Description			ntext,
	Picture				image
);

CREATE TABLE dimSuppliers 
(
	SupplierKey			int				IDENTITY(1,1) PRIMARY KEY,
	SupplierID			int,
	CompanyName			nvarchar(40),
	ContactName			nvarchar(30),
	ContactTitle		nvarchar(30),
	Address				nvarchar(60),
	City				nvarchar(15),
	Region				nvarchar(15),
	PostalCode			nvarchar(10),
	Country				nvarchar(15),
	Phone				nvarchar(24),
	Fax					nvarchar(24),
	HomePage			ntext
);

CREATE TABLE dimTime(
[TimeKey] [int] NOT NULL PRIMARY KEY,
[Date] [datetime] NULL,
[Day] [char](10) NULL,
[DayOfWeek] [smallint] NULL,
[DayOfMonth] [smallint] NULL,
[DayOfYear] [smallint] NULL,
[WeekOfYear] [smallint] NULL,
[Month] [char](10) NULL,
[MonthOfYear] [smallint] NULL,
[QuarterOfYear] [smallint] NULL,
[Year] [int] NULL
);

create table S_Orders(
	OrderID int not null primary key,
	CustomerID nchar(5) null,
	EmployeeID int null,
	OrderDate datetime null,
	RequiredDate datetime null,
	ShippedDate datetime null,
	ShipVia nvarchar(40) null,
	Freight money null,
	ShipName nvarchar(40) null,
	ShipAddress nvarchar(60) null,
	ShipCity nvarchar(15) null,
	ShipRegion nvarchar(15) null,
	ShipPostalCode nvarchar(10) null,
	ShipCountry nvarchar(15) null
);

print 'Creating a fact table required'
--Add statements below... 


CREATE TABLE 		factOrders 
(
	ProductKey			int				FOREIGN KEY REFERENCES dimProducts(ProductKey),
	CustomerKey			int				FOREIGN KEY REFERENCES dimCustomers(CustomerKey),
	SupplierKey			int				FOREIGN KEY REFERENCES dimSuppliers(SupplierKey),
	OrderDateKey		int				FOREIGN KEY REFERENCES dimTime(TimeKey),
	RequiredDateKey		int				FOREIGN KEY REFERENCES dimTime(TimeKey),
	ShippedDateKey		int				FOREIGN KEY REFERENCES dimTime(TimeKey),
	OrderID				int,
	UnitPrice			money,
	Quantity			smallint,
	Discount			real,
	TotalPrice			real,
	ShipperCompany		nvarchar(40) not null, 
	ShipperPhone		nvarchar(24) null,

	PRIMARY KEY (ProductKey, CustomerKey, SupplierKey, OrderDateKey)
);

print '***************************************************************'
print '****** Section 2: Populate DW Dimension Tables (except dimTime)'
print '***************************************************************'

print 'Populating all dimension tables from northwind7 and northwind8'
--Add statements below... 
--IMPORTANT! All Data in dimension tables MUST satisfy all the defined DQ Rules 

CREATE TABLE Numbers_Small (Number INT);
INSERT INTO Numbers_Small VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

CREATE TABLE Numbers_Big (Number_Big BIGINT);
INSERT INTO Numbers_Big ( Number_Big )
SELECT thousands.number * 1000 + hundreds.number * 100 + tens.number * 10 + ones.number as number_big
FROM numbers_small thousands, numbers_small hundreds, numbers_small tens, numbers_small ones;

INSERT INTO dimTime(TimeKey, Date) values(-1,'9999-12-31');
INSERT INTO dimTime (TimeKey, Date)
SELECT number_big, DATEADD(day, number_big,  '1996-01-01') as Date
FROM numbers_big
WHERE DATEADD(day, number_big,  '1996-01-01') BETWEEN '1996-01-01' AND '1998-12-31'
ORDER BY number_big;

UPDATE dimTime
SET Day = DATENAME(DW, Date),
DayOfWeek = DATEPART(WEEKDAY, Date),
DayOfMonth = DAY(Date),
DayOfYear = DATEPART(DY,Date),
WeekOfYear = DATEPART(WK,Date),
Month = DATENAME(MONTH,Date),
MonthOfYear = MONTH(Date),
QuarterOfYear = DATEPART(Q, Date),
Year = YEAR(Date);
drop table Numbers_Small;
drop table Numbers_Big;

---------------------------------------------------------Northwind7----------------------------------------------------------------------
insert into S_Orders(OrderID,CustomerID,EmployeeID,OrderDate,RequiredDate,ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,
ShipPostalCode,ShipCountry)
select OrderID,CustomerID,EmployeeID,OrderDate,RequiredDate,isnull(ShippedDate,'2017-10-27'),ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,
ShipPostalCode,ShipCountry 
from northwind7.dbo.orders o
WHERE (o.%%physloc%% not in (
							SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='Orders' and RuleNo=8 and Action='reject'
							))
	OR (o.%%physloc%% in (
							SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='Orders' and RuleNo=10 and Action='Allow'
							UNION
							SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='Orders' and RuleNo=9 and Action='Fix'
							UNION
							SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='Orders' and RuleNo=11 and Action='Fix'
							));

INSERT INTO dimSuppliers(SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage)
SELECT SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, 'USA', Phone, Fax, HomePage
FROM northwind7.dbo.Suppliers s
WHERE s.%%physloc%% in (
SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='Suppliers' and RuleNo=4 and Action='fix'
	UNION
	SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='Suppliers' and RuleNo=5 and Action='Allow'
	)
	AND s.Country in('US','United States','UNITED STATES');

INSERT INTO dimSuppliers(SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage)
SELECT SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, 'UK', Phone, Fax, HomePage
FROM northwind7.dbo.Suppliers s
WHERE s.%%physloc%% in (
SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='Suppliers' and RuleNo=4 and Action='fix'
	UNION
	SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='Suppliers' and RuleNo=5 and Action='Allow'
	)
	AND s.Country in('United Kingdom', 'UNITED KINGDOM', 'Britain', 'BRITAIN');

INSERT INTO dimSuppliers(SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage)
SELECT SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage
FROM northwind7.dbo.Suppliers s
WHERE s.%%physloc%% not in (SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='Suppliers' and RuleNo=4 and Action='fix');

INSERT INTO dimCustomers(CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax)
SELECT CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, 'USA', Phone, Fax
FROM northwind7.dbo.Customers c
WHERE c.%%physloc%% in (
	SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='Customers' and RuleNo=4 and Action='fix'
	UNION
	SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='Customers' and RuleNo=5 and Action='Allow'
	)
	AND c.Country in('US','United States','UNITED STATES');
	
INSERT INTO dimCustomers(CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax)
SELECT CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, 'UK', Phone, Fax
FROM northwind7.dbo.Customers c
WHERE c.%%physloc%% in (
	SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='Customers' and RuleNo=4 and Action='fix'
	UNION
	SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='Customers' and RuleNo=5 and Action='Allow'
	)
	AND c.Country in('United Kingdom', 'UNITED KINGDOM', 'Britain', 'BRITAIN');

INSERT INTO dimCustomers(CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax)
SELECT CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax
FROM northwind7.dbo.Customers c
WHERE c.%%physloc%% not in (SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='Customers' and RuleNo=4 and Action='fix');





MERGE INTO dimProducts dp
USING
(
	SELECT	ProductID, ProductName, QuantityPerUnit, UnitPrice,	
			UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued,
			CategoryName, Description, Picture
	FROM	northwind7.dbo.Products p, northwind7.dbo.Categories c
	WHERE	p.CategoryID = c.CategoryID 
			and 
			(p.%%physloc%% not in
				(SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='Products' and RuleNo=1 and Action='reject'
				UNION
				SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='Products' and RuleNo=6 and Action='reject'
				)
			)
) pp ON (dp.ProductID = pp.ProductID)
WHEN MATCHED THEN
	UPDATE SET dp.ProductID = pp.ProductID
WHEN NOT MATCHED THEN
	INSERT (ProductID, ProductName, QuantityPerUnit, UnitPrice, 
	UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued,
	CategoryName, Description, Picture)
	VALUES (pp.ProductID, pp.ProductName, pp.QuantityPerUnit, pp.UnitPrice, 
	pp.UnitsInStock, pp.UnitsOnOrder, pp.ReorderLevel, pp.Discontinued,
	pp.CategoryName, pp.Description, pp.Picture);


MERGE INTO dimProducts dp
USING
(
	SELECT	ProductID, ProductName
	FROM	northwind7.dbo.Products p
	WHERE	p.%%physloc%% in
			(	select RowID 
				from DQLog 
				where DBName='northwind7' and TableName='Products' and RuleNo=12 and Action='fix'))pc ON (dp.ProductID = pc.ProductID)
WHEN MATCHED THEN
update set dp.ProductName = upper(pc.ProductName);
--------------------------------------------------------Northwind8-----------------------------------------------------------------------
INSERT INTO S_Orders(OrderID,CustomerID,EmployeeID,OrderDate,RequiredDate,ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion, ShipPostalCode,ShipCountry)
SELECT OrderID,CustomerID,EmployeeID,OrderDate,RequiredDate,ShippedDate,'Unitec',Freight,ShipName,ShipAddress,ShipCity,ShipRegion, ShipPostalCode,ShipCountry
FROM northwind8.dbo.Orders o
WHERE o.%%physloc%% in (SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='orders' and RuleNo=9 and Action='fix');

MERGE INTO S_Orders dp
USING
(
	SELECT	OrderID,CustomerID,EmployeeID,OrderDate,RequiredDate,ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,ShipPostalCode,ShipCountry
	FROM	northwind8.dbo.Orders p
	WHERE  (p.%%physloc%% not in (SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='orders' and RuleNo=8 and Action='reject'))
			or (p.%%physloc%% in (
							SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='orders' and RuleNo=10 and Action='allow'
							UNION
							SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='Orders' and RuleNo=11 and Action='Fix'
							))
			)pc ON (dp.orderID = pc.orderID)
WHEN MATCHED THEN
update set dp.orderid = pc.orderid
when not matched then
insert (OrderID,CustomerID,EmployeeID,OrderDate,RequiredDate,ShippedDate,ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,
ShipPostalCode,ShipCountry)
VALUES (OrderID,CustomerID,EmployeeID,OrderDate,RequiredDate,isnull(ShippedDate,'2017-10-27'),ShipVia,Freight,ShipName,ShipAddress,ShipCity,ShipRegion,
ShipPostalCode,ShipCountry );	

INSERT INTO dimSuppliers(SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage)
SELECT SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, 'USA', Phone, Fax, HomePage
FROM northwind8.dbo.Suppliers s
WHERE s.%%physloc%% in (
	SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='Suppliers' and RuleNo=4 and Action='fix'
	UNION
	SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='Suppliers' and RuleNo=5 and Action='Allow'
	)
	AND s.Country in('US','United States','UNITED STATES');

INSERT INTO dimSuppliers(SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage)
SELECT SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, 'UK', Phone, Fax, HomePage
FROM northwind8.dbo.Suppliers s
WHERE s.%%physloc%% in (
	SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='Suppliers' and RuleNo=4 and Action='fix'
	UNION
	SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='Suppliers' and RuleNo=5 and Action='Allow'
	)
	AND s.Country in('United Kingdom', 'UNITED KINGDOM', 'Britain', 'BRITAIN');

INSERT INTO dimSuppliers(SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage)
SELECT SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage
FROM northwind8.dbo.Suppliers s
WHERE s.%%physloc%% not in (SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='Suppliers' and RuleNo=4 and Action='fix');

INSERT INTO dimCustomers(CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax)
SELECT CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, 'USA', Phone, Fax
FROM northwind8.dbo.Customers c
WHERE c.%%physloc%% in (
	SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='Customers' and RuleNo=4 and Action='fix'
	UNION
	SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='Customers' and RuleNo=5 and Action='Allow'
	)
	AND c.Country in('US','United States','UNITED STATES');
	
INSERT INTO dimCustomers(CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax)
SELECT CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, 'UK', Phone, Fax
FROM northwind8.dbo.Customers c
WHERE c.%%physloc%% in (
	SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='Customers' and RuleNo=4 and Action='fix'
	UNION
	SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='Customers' and RuleNo=5 and Action='Allow'
	)
	AND c.Country in('United Kingdom', 'UNITED KINGDOM', 'Britain', 'BRITAIN');

INSERT INTO dimCustomers(CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax)
SELECT CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax
FROM northwind8.dbo.Customers c
WHERE c.%%physloc%% not in (SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='Customers' and RuleNo=4 and Action='fix');

MERGE INTO dimProducts dp
USING
(
	SELECT	ProductID, ProductName, QuantityPerUnit, UnitPrice,	
			UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued,
			CategoryName, Description, Picture
	FROM	northwind8.dbo.Products p, northwind8.dbo.Categories c
	WHERE	p.CategoryID = c.CategoryID 
			and 
			(p.%%physloc%% not in
				(SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='Products' and RuleNo=1 and Action='reject'
				UNION
				SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='Products' and RuleNo=6 and Action='reject'
				)
			)
) pp ON (dp.ProductID = pp.ProductID)
WHEN MATCHED THEN
	UPDATE SET dp.ProductID = pp.ProductID
WHEN NOT MATCHED THEN
	INSERT (ProductID, ProductName, QuantityPerUnit, UnitPrice, 
	UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued,
	CategoryName, Description, Picture)
	VALUES (pp.ProductID, pp.ProductName, pp.QuantityPerUnit, pp.UnitPrice, 
	pp.UnitsInStock, pp.UnitsOnOrder, pp.ReorderLevel, pp.Discontinued,
	pp.CategoryName, pp.Description, pp.Picture);

MERGE INTO dimProducts dp
USING
(
	SELECT	ProductID, ProductName
	FROM	northwind8.dbo.Products p
	WHERE	p.%%physloc%% in
			(	select RowID 
				from DQLog 
				where DBName='northwind8' and TableName='Products' and RuleNo=12 and Action='fix'))pc ON (dp.ProductID = pc.ProductID)
WHEN MATCHED THEN
update set dp.ProductName = upper(pc.ProductName);


print '***************************************************************'
print '****** Section 3: Populate DW Fact Tables'
print '***************************************************************'

print 'Populating the fact table from northwind7 and northwind8'
--Add statements below... 
--IMPORTANT! All Data in the fact table MUST satisfy all the defined DQ Rules 

MERGE INTO	factOrders fo
USING
(
	SELECT	ProductKey, CustomerKey, SupplierKey, 
			dt1.TimeKey as [OrderDateKey],
			dt2.TimeKey as [RequiredDateKey],
			dt3.TimeKey as [ShippedDateKey],
			od.OrderID, od.UnitPrice, od.Quantity, od.Discount,
			od.Quantity*od.UnitPrice *(1-od.Discount) as [TotalPrice], 
			ns.CompanyName, ns.Phone
	FROM    northwind7.dbo.Orders o,
			northwind7.dbo.[Order Details] od,
			northwind7.dbo.Products p,
			northwind7.dbo.Suppliers s, 
			northwind7.dbo.Shippers ns,
			dimProducts dp, dimCustomers dc, dimSuppliers ds, S_Orders so, 
			dimTime dt1, dimTime dt2, dimTime dt3
	WHERE	
			 od.OrderID = o.OrderID 
			AND o.OrderID = so.OrderID
			AND o.CustomerID = dc.CustomerID
			AND od.ProductID = dp.ProductID
			AND od.ProductID = p.ProductID
			AND p.SupplierID = s.SupplierID
			AND s.SupplierID = ds.SupplierID
			AND o.ShipVia = ns.ShipperID
			AND dt1.Date = o.OrderDate
			AND dt2.Date = o.RequiredDate
			AND dt3.Date = so.ShippedDate
			AND (od.OrderID not in ( 
				SELECT OrderID FROM northwind7.dbo.[Order Details] od WHERE od.%%physloc%% in (
					SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='[Order Details]' and RuleNo=2 and Action='reject'
					UNION 
					SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='[Order Details]' and RuleNo=7 and Action='reject')
				)
				OR od.%%physloc%% in 
				(
				SELECT RowID FROM DQLog WHERE DBName='northwind7' and TableName='[Order Details]' and RuleNo=3 and Action='allow'
				)
				)
) o ON (o.CustomerKey = fo.CustomerKey
		AND o.ProductKey = fo.ProductKey
		AND o.SupplierKey = fo.SupplierKey
		AND o.OrderDateKey = fo.OrderDateKey)
WHEN MATCHED THEN
	UPDATE SET fo.OrderID = o.OrderID
WHEN NOT MATCHED THEN
	INSERT (ProductKey, CustomerKey, SupplierKey, OrderDateKey, RequiredDateKey, ShippedDateKey, OrderID, UnitPrice, Quantity, Discount, TotalPrice, ShipperCompany, ShipperPhone)
	VALUES	(o.ProductKey, o.CustomerKey, o.SupplierKey, o.OrderDateKey, o.RequiredDateKey, o.ShippedDateKey, o.OrderID, o.UnitPrice, o.Quantity, o.Discount, o.TotalPrice, o.CompanyName, o.Phone);

SELECT * FROM S_Orders

MERGE INTO	factOrders fo
USING
(
	SELECT	ProductKey, CustomerKey, SupplierKey, 
			dt1.TimeKey as [OrderDateKey],
			dt2.TimeKey as [RequiredDateKey],
			dt3.TimeKey as [ShippedDateKey],
			od.OrderID, od.UnitPrice, od.Quantity, od.Discount,
			od.Quantity*od.UnitPrice *(1-od.Discount) as [TotalPrice], 
			ns.CompanyName, ns.Phone
	FROM    northwind8.dbo.Orders o,
			northwind8.dbo.[Order Details] od,
			northwind8.dbo.Products p,
			northwind8.dbo.Suppliers s, 
			northwind8.dbo.Shippers ns,
			dimProducts dp, dimCustomers dc, dimSuppliers ds, S_Orders so, 
			dimTime dt1, dimTime dt2, dimTime dt3
	WHERE	
			od.OrderID = o.OrderID 
			AND o.OrderID = so.OrderID
			AND o.CustomerID = dc.CustomerID
			AND od.ProductID = dp.ProductID
			AND od.ProductID = p.ProductID
			AND p.SupplierID = s.SupplierID
			AND s.SupplierID = ds.SupplierID
			AND o.ShipVia = ns.ShipperID
			AND dt1.Date = o.OrderDate
			AND dt2.Date = o.RequiredDate
			AND dt3.Date = so.ShippedDate
			AND (od.OrderID not in ( 
				SELECT OrderID FROM northwind8.dbo.[Order Details] od WHERE od.%%physloc%% in (
					SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='[Order Details]' and RuleNo=2 and Action='reject'
					UNION 
					SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='[Order Details]' and RuleNo=7 and Action='reject')
				)
				OR od.%%physloc%% in 
				(
				SELECT RowID FROM DQLog WHERE DBName='northwind8' and TableName='[Order Details]' and RuleNo=3 and Action='allow'
				))
) o ON (o.CustomerKey = fo.CustomerKey
		AND o.ProductKey = fo.ProductKey
		AND o.SupplierKey = fo.SupplierKey
		AND o.OrderDateKey = fo.OrderDateKey)
WHEN MATCHED THEN
	UPDATE SET fo.OrderID = o.OrderID
WHEN NOT MATCHED THEN
	INSERT (ProductKey, CustomerKey, SupplierKey, OrderDateKey, RequiredDateKey, ShippedDateKey, OrderID, UnitPrice, Quantity, Discount, TotalPrice, ShipperCompany, ShipperPhone)
	VALUES	(o.ProductKey, o.CustomerKey, o.SupplierKey, o.OrderDateKey, o.RequiredDateKey, o.ShippedDateKey, o.OrderID, o.UnitPrice, o.Quantity, o.Discount, o.TotalPrice, o.CompanyName, o.Phone);


print '***************************************************************'
print '****** Section 4: Counting rows of OLTP and DW Tables'
print '***************************************************************'
print 'Checking Number of Rows of each table in the source databases and the DW Database'
-- Write SQL queries to get answers to fill in the information below
-- ****************************************************************************
-- FILL IN THE ##### 
-- ****************************************************************************
-- Source table					Northwind7	Northwind8	Target table 	DW	
-- ****************************************************************************
-- Customers					  13		  78 		dimCustomers	91
-- Products						  77		  77		dimProducts		76
-- Suppliers					  29		  29		dimSuppliers	58
-- Orders						 102		 707		S_Orders		828
-- OrderDetails					 352		 1801		factOrders		4042
-- ****************************************************************************
--Add statements below

SELECT count(*) FROM northwind7.dbo.Customers;
SELECT count(*) FROM northwind8.dbo.Customers;
SELECT * FROM dimCustomers;
SELECT count(*) FROM northwind7.dbo.Products;
SELECT count(*) FROM northwind8.dbo.Products;
SELECT * FROM dimProducts;
SELECT count(*) FROM northwind7.dbo.Suppliers;
SELECT count(*) FROM northwind8.dbo.Suppliers;
SELECT * FROM dimSuppliers;
SELECT count(*) FROM northwind7.dbo.Orders;
SELECT count(*) FROM northwind8.dbo.Orders;
SELECT * FROM S_Orders;
SELECT count(*) FROM northwind7.dbo.[Order Details];
SELECT count(*) FROM northwind8.dbo.[Order Details];
SELECT * FROM factOrders;

print '***************************************************************'
print '****** Section 5: Validating DW Data'
print '***************************************************************'
print 'B: Validating Data in the fact table'
--Add statements below...
print '--------------------------RuleNo 1-------Reject-----------------------------'
SELECT UnitPrice
FROM northwind7.dbo.Products
WHERE (UnitPrice <0 or UnitPrice is null);
SELECT UnitPrice
FROM northwind8.dbo.Products
WHERE (UnitPrice <0 or UnitPrice is null);

SELECT UnitPrice
FROM dimProducts
WHERE (UnitPrice <0 or UnitPrice is null);
print '--------------------------RuleNo 2-------Reject-----------------------------'
SELECT Quantity
FROM northwind7.dbo.[Order Details]
WHERE (quantity <=0 or quantity is null);
SELECT Quantity
FROM northwind8.dbo.[Order Details]
WHERE (quantity <=0 or quantity is null);

SELECT Quantity
FROM factOrders
WHERE (quantity <=0 or quantity is null);
print '--------------------------RuleNo 3-------Allow------------------------------'
SELECT *
FROM northwind7.dbo.[Order Details]
WHERE Discount > 0.5 and UnitPrice  > 300;
SELECT *
FROM northwind8.dbo.[Order Details]
WHERE Discount > 0.5 and UnitPrice  > 300;

SELECT *
FROM factOrders
WHERE Discount > 0.5 and UnitPrice  > 300;
print '--------------------------RuleNo 4-------Fix--------------------------------'
SELECT CustomerID, Country
FROM northwind7.dbo.Customers
WHERE (Country in('US','United States','UNITED STATES', 'United Kingdom', 'UNITED KINGDOM', 'Britain', 'BRITAIN'));
SELECT SupplierID, Country
FROM northwind7.dbo.Suppliers
WHERE (Country in('US','United States','UNITED STATES', 'United Kingdom', 'UNITED KINGDOM', 'Britain', 'BRITAIN'));
SELECT CustomerID, Country
FROM northwind8.dbo.Customers
WHERE (Country in('US','United States','UNITED STATES', 'United Kingdom', 'UNITED KINGDOM', 'Britain', 'BRITAIN'));
SELECT SupplierID, Country
FROM northwind8.dbo.Suppliers
WHERE (Country in('US','United States','UNITED STATES', 'United Kingdom', 'UNITED KINGDOM', 'Britain', 'BRITAIN'));

SELECT CustomerID, Country
FROM dimCustomers
WHERE (Country in('US','United States','UNITED STATES', 'United Kingdom', 'UNITED KINGDOM', 'Britain', 'BRITAIN'));
SELECT SupplierID, Country
FROM dimSuppliers
WHERE (Country in('US','United States','UNITED STATES', 'United Kingdom', 'UNITED KINGDOM', 'Britain', 'BRITAIN'));
SELECT CustomerID, Country
FROM dimCustomers
WHERE (Country in('USA','UK'));
SELECT SupplierID, Country
FROM dimSuppliers
WHERE (Country in('USA','UK'));
print '--------------------------RuleNo 5-------Allow------------------------------'
SELECT * FROM dimCustomers WHERE CustomerID in(
	SELECT CustomerID
	FROM northwind7.dbo.Customers
	WHERE len(PostalCode) <> 5
	UNION
	SELECT CustomerID
	FROM northwind8.dbo.Customers
	WHERE len(PostalCode) <> 5
	);


print '--------------------------RuleNo 6-------Reject-----------------------------'
/***dimProducts***/
SELECT ProductID, CategoryName
FROM dimProducts
WHERE (
	CategoryName in(
		SELECT CategoryName 
		FROM northwind7.dbo.Categories
		WHERE CategoryID is null 
		)
		or CategoryName not in (
			SELECT CategoryName 
			FROM northwind7.dbo.Categories
			)
	);
SELECT CategoryName FROM dimProducts WHERE CategoryName in(
	SELECT CategoryName FROM northwind7.dbo.Categories WHERE CategoryID in (
		SELECT CategoryID FROM northwind7.dbo.Products WHERE CategoryID not in (
			SELECT CategoryID FROM northwind7.dbo.Categories
			)
		)
	);
SELECT ProductID, CategoryName
FROM dimProducts
WHERE (
	CategoryName in(
		SELECT CategoryName 
		FROM northwind8.dbo.Categories
		WHERE CategoryID is null 
		)
		or CategoryName not in (
			SELECT CategoryName 
			FROM northwind8.dbo.Categories
			)
	);
SELECT CategoryName FROM dimProducts WHERE CategoryName in(
	SELECT CategoryName FROM northwind8.dbo.Categories WHERE CategoryID in (
		SELECT CategoryID FROM northwind8.dbo.Products WHERE CategoryID not in (
			SELECT CategoryID FROM northwind8.dbo.Categories
			)
		)
	);
/***dimSuppliers***/
SELECT ProductID, CategoryName
FROM dimProducts
WHERE (
	CategoryName in(
		SELECT CategoryName 
		FROM northwind7.dbo.Categories
		WHERE CategoryID is null 
		)
		or CategoryName not in (
			SELECT CategoryName 
			FROM northwind7.dbo.Categories
			)
	);
SELECT CategoryName FROM dimProducts WHERE CategoryName in(
	SELECT CategoryName FROM northwind7.dbo.Categories WHERE CategoryID in (
		SELECT CategoryID FROM northwind7.dbo.Products WHERE CategoryID not in (
			SELECT CategoryID FROM northwind7.dbo.Categories
			)
		)
	);

SELECT SupplierID, CompanyName
FROM dimSuppliers
WHERE (
	SupplierID is null or SupplierID not in (
		SELECT SupplierID FROM northwind7.dbo.Suppliers
	));

print '--------------------------RuleNo 7-------Reject-----------------------------'
SELECT ProductID
FROM northwind7.dbo.[order details]
WHERE ProductID is null or ProductID not in(SELECT ProductID FROM northwind7.dbo.Products);
SELECT ProductID
FROM northwind8.dbo.[order details]
WHERE ProductID is null or ProductID not in(SELECT ProductID FROM northwind8.dbo.Products);

SELECT OrderID
FROM factOrders f
WHERE f.OrderID in 
				(
				SELECT OrderID FROM northwind7.dbo.[Order Details] 
				WHERE ProductID is null or ProductID not in
					(
					SELECT productID FROM northwind7.dbo.[Order Details])
					UNION
					SELECT OrderID FROM northwind8.dbo.[Order Details] 
					WHERE ProductID is null or ProductID not in
					 (SELECT ProductID FROM northwind8.dbo.[Order Details])
					 );

print '--------------------------RuleNo 8-------Reject--------CustomerID, ShipAdress,ShipCity-------------------'
SELECT * FROM northwind7.dbo.orders 
WHERE CustomerID is null or CustomerID not in
	(SELECT CustomerID FROM northwind7.dbo.Customers) OR (ShipAddress is null AND ShipCity is null);
SELECT * FROM northwind8.dbo.Orders 
WHERE CustomerID is null or CustomerID not in
	(SELECT CustomerID FROM northwind8.dbo.Customers) OR (ShipAddress is null AND ShipCity is null);

SELECT OrderID
FROM factOrders 
WHERE OrderID in
			(
			SELECT OrderID FROM northwind7.dbo.orders 
			WHERE CustomerID is null or CustomerID not in
				(SELECT CustomerID FROM northwind7.dbo.Customers) OR (ShipAddress is null AND ShipCity is null)
			UNION
			SELECT OrderID FROM northwind8.dbo.Orders 
			WHERE CustomerID is null or CustomerID not in
				(SELECT CustomerID FROM northwind8.dbo.Customers) OR (ShipAddress is null AND ShipCity is null)
			);

print '--------------------------RuleNo 9-------Fix-------------------------------'
SELECT OrderID FROM northwind7.dbo.Orders WHERE ShipVia is null;
SELECT OrderID FROM northwind8.dbo.Orders WHERE ShipVia is null;

SELECT OrderID
FROM factOrders 
WHERE OrderID in (
		SELECT OrderID FROM northwind7.dbo.Orders WHERE ShipVia is null
		UNION 
		SELECT OrderID FROM northwind8.dbo.Orders WHERE ShipVia is null);

SELECT OrderID FROM northwind7.dbo.Orders WHERE ShipVia not in (SELECT ShipperID FROM northwind7.dbo.Shippers);
SELECT OrderID FROM northwind8.dbo.Orders WHERE ShipVia not in (SELECT ShipperID FROM northwind8.dbo.Shippers);
print '--------------------------RuleNo 10-------Reject-----------------------------'
SELECT * 
FROM northwind7.dbo.Orders
WHERE (Freight > (			SELECT SUM(UnitPrice*Quantity*(1-Discount)) FROM northwind7.dbo.[Order Details]
							WHERE northwind7.dbo.Orders.OrderID=northwind7.dbo.[Order Details].OrderID
							GROUP BY OrderID));

SELECT * 
FROM northwind8.dbo.Orders
WHERE (Freight > (			SELECT SUM(UnitPrice*Quantity*(1-Discount)) FROM northwind8.dbo.[Order Details]
							WHERE northwind8.dbo.Orders.OrderID=northwind8.dbo.[Order Details].OrderID
							GROUP BY OrderID));

SELECT * FROM factOrders WHERE OrderID in 
(
SELECT OrderID 
FROM northwind7.dbo.Orders
WHERE (Freight > (			SELECT SUM(UnitPrice*Quantity*(1-Discount)) FROM northwind7.dbo.[Order Details]
							WHERE northwind7.dbo.Orders.OrderID=northwind7.dbo.[Order Details].OrderID
							GROUP BY OrderID))
UNION
SELECT OrderID 
FROM northwind8.dbo.Orders
WHERE (Freight > (			SELECT SUM(UnitPrice*Quantity*(1-Discount)) FROM northwind8.dbo.[Order Details]
							WHERE northwind8.dbo.Orders.OrderID=northwind8.dbo.[Order Details].OrderID
							GROUP BY OrderID))
);

print '--------------------------RuleNo 11-------Fix-------------------------------'
SELECT Distinct(OrderID) FROM factOrders WHERE OrderID in(
SELECT Distinct(OrderID) FROM S_Orders WHERE OrderID in 
(
SELECT OrderID
FROM northwind7.dbo.Orders
where ShippedDate is null 
Union
SELECT OrderID
FROM northwind8.dbo.Orders
where ShippedDate is null 
));

print '--------------------------RuleNo 12-------Fix-------------------------------'
SELECT * 
FROM northwind7.dbo.Products
WHERE (ProductName like 'A%' or ProductName like 'F%' or ProductName like 'M%') ;
SELECT * 
FROM northwind8.dbo.Products
WHERE (ProductName like 'A%' or ProductName like 'F%' or ProductName like 'M%') ;

SELECT ProductID, ProductName FROM dimProducts WHERE ProductID in 
(
SELECT ProductID 
FROM northwind7.dbo.Products
WHERE (ProductName like 'A%' or ProductName like 'F%' or ProductName like 'M%') 
UNION
SELECT ProductID
FROM northwind8.dbo.Products
WHERE (ProductName like 'A%' or ProductName like 'F%' or ProductName like 'M%') 
)
print ''
print '***************************************************************'
print 'My Northwind DW creation with data quality assurance is now completed'
print '*************************************************************
**'
