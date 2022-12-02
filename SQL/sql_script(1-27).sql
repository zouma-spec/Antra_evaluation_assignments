Use WideWorldImporters
GO
-- 1.List of Persons’ full name, all their fax and phone numbers, as well as the phone number and fax of the company they are working for (if any).
select a.PersonID, a.FullName, a.PhoneNumber, a.FaxNumber, b.PhoneNumber company_phone, b.FaxNumber company_fax, b.CustomerName company --customer companies
from Application.People a 
join Sales.Customers b
on b.PrimaryContactPersonID = a.PersonID
	or  b.AlternateContactPersonID = a.PersonID
UNION 
select a.PersonID,a.FullName, a.PhoneNumber, a.FaxNumber, b.PhoneNumber company_phone, b.FaxNumber company_fax, b.SupplierName company --supplier companies
from Application.People a 
join Purchasing.Suppliers b
on b.PrimaryContactPersonID = a.PersonID
	or b.AlternateContactPersonID = a.PersonID
UNION
select PersonID,FullName, PhoneNumber, FaxNumber,  PhoneNumber as company_phone , FaxNumber as company_fax,  SUBSTRING(emailaddress, CHARINDEX('@', emailaddress)+1,(CHARINDEX('.', emailaddress)-CHARINDEX('@', emailaddress)-1)) as company  --WWI
from  Application.People
where emailaddress like '%wideworldimporters%'
order by PersonID

-- 2.If the customer's primary contact person has the same phone number as the customer’s phone number, list the customer companies.
select a.CustomerName
from sales.Customers a
join Application.People b
on a.PrimaryContactPersonID=b.PersonID
   and a.PhoneNumber=b.PhoneNumber 

-- 3.List of customers to whom we made a sale prior to 2016 but no sale since 2016-01-01.
select CustomerID
from sales.Orders
group by customerID
having min(OrderDate)<'2016-01-01' and max(OrderDate)<'2016-01-01'
order by CustomerID

-- 4.List of Stock Items and total quantity for each stock item in Purchase Orders in Year 2013
select c.StockItemID, c.StockItemName, sum(c.QuantityPerOuter*b.ReceivedOuters) quantity_purchased
from Purchasing.PurchaseOrders a
join Purchasing.PurchaseOrderLines b
on a.PurchaseOrderID=b.PurchaseOrderID
join Warehouse.StockItems c
on b.StockItemID=c.StockItemID
where year(a.OrderDate)='2013'
group by c.StockItemID, c.StockItemName
order by c.StockItemID

-- 5.List of stock items that have at least 10 characters in description.
select distinct StockItemID
from  Purchasing.PurchaseOrderLines
where len(Description)>=10
order by StockItemID

-- 6.List of stock items that are not sold to the state of Alabama and Georgia in 2014.
select distinct a.StockItemID
from Sales.OrderLines a
join Sales.Orders b
on a.OrderID=b.OrderID
join Sales.Customers c
on b.CustomerID=c.CustomerID
join Application.Cities d
on c.DeliveryCityID=d.CityID
join Application.StateProvinces e
on d.StateProvinceID=e.StateProvinceID
where b.OrderDate between '2014-01-01' and '2014-12-31'
	and e.StateProvinceName not in ('Alabama','Georgia')
order by a.StockItemID      

-- 7.List of States and Avg dates for processing (confirmed delivery date - order date).
select e.StateProvinceID, avg(DATEDIFF(day,b.OrderDate,a.ConfirmedDeliveryTime)) avg_processing_days
from Sales.Invoices a
join Sales.Orders b 
on a.OrderID=b.OrderID
join Sales.Customers c
on b.CustomerID=c.CustomerID
join Application.Cities d
on c.DeliveryCityID=d.CityID
right join Application.StateProvinces e
on d.StateProvinceID=e.StateProvinceID
group by e.StateProvinceID
order by e.StateProvinceID

-- 8. List of States and Avg dates for processing (confirmed delivery date - order date) by month.
select e.StateProvinceID, month(b.OrderDate) month, avg(DATEDIFF(day,b.OrderDate,a.ConfirmedDeliveryTime)) avg_processing_days
from Sales.Invoices a
join Sales.Orders b
on a.OrderID=b.OrderID
join Sales.Customers c
on b.CustomerID=c.CustomerID
join Application.Cities d
on c.DeliveryCityID=d.CityID
right join Application.StateProvinces e
on d.StateProvinceID=e.StateProvinceID
group by e.StateProvinceID,month(b.OrderDate)
order by e.StateProvinceID,month(b.OrderDate)

-- 9.List of StockItems that the company purchased more than sold in the year of 2015
with purchased as(
select c.StockItemID, sum(c.QuantityPerOuter*b.ReceivedOuters) quantity_purchased
from Purchasing.PurchaseOrders a
join Purchasing.PurchaseOrderLines b
on a.PurchaseOrderID=b.PurchaseOrderID
join Warehouse.StockItems c
on b.StockItemID=c.StockItemID
where a.OrderDate between '2015-01-01' and '2015-12-31'
group by c.StockItemID
),
sold as(
select e.StockItemID, sum(e.Quantity) quantity_sold
from Sales.Orders d
join Sales.OrderLines e
on d.OrderID=e.OrderID
where d.OrderDate between '2015-01-01' and '2015-12-31'
group by e.StockItemID
)
select p.StockItemID
from purchased p
join sold s
on p.StockItemID=s.StockItemID
where p.quantity_purchased> s.quantity_sold
order by p.StockItemID

--10. List of Customers and their phone number, together with the primary contact person's name, to whom we did not sell more than 10 mugs (search by name) in the year 2016.
select a.CustomerID, a.PhoneNumber, e.FullName PrimaryContactName
from Sales.Customers a
join Sales.Orders b
on a.CustomerID=b.CustomerID
join Sales.OrderLines c
on b.CustomerID=c.OrderID
join Warehouse.StockItems d                         
on c.StockItemID=d.StockItemID
join Application.People e
on a.PrimaryContactPersonID=e.PersonID
where d.StockItemName like '%mug%'
	and b.OrderDate between '2016-01-01' and '2016-12-31'
group by a.CustomerID, a.PhoneNumber, e.FullName 
having sum(c.Quantity)<=10
order by a.CustomerID

-- 11. List all the cities that were updated after 2015-01-01.
select CityID,CityName
from application.Cities
where ValidFrom > '2015-01-01'

-- 12. List all the Order Detail (Stock Item name, delivery address, delivery state, city, country, customer name, customer contact person name, customer phone, quantity) for the date of 2014-07-01. Info should be relevant to that date.
select d.StockItemName, concat(a.DeliveryAddressLine1, ', ', a.DeliveryAddressLine2,', ', a.DeliveryPostalCode) delivery_address,
       h.CountryName delivery_country, g.StateProvinceName delivery_state, f.CityName delivery_city,
	   a.CustomerName, e.FullName customer_contact_name, a.PhoneNumber customer_phone, c.Quantity
from Sales.Customers a
join Sales.Orders b
on a.CustomerID=b.CustomerID
join Sales.OrderLines c
on b.CustomerID=c.OrderID
join Warehouse.StockItems d
on c.StockItemID=d.StockItemID
join Application.People e
on a.PrimaryContactPersonID=e.PersonID
join Application.Cities f
on a.DeliveryCityID=f.CityID
join Application.StateProvinces_Archive g
on f.StateProvinceID=g.StateProvinceID
join Application.Countries h
on g.CountryID=h.CountryID
where b.OrderDate = '2014-07-01' 

--13.List of stock item groups and total quantity purchased, total quantity sold, and the remaining stock quantity (quantity purchased - quantity sold)
with p as(
select d.StockGroupID, sum(b.QuantityPerOuter*a.ReceivedOuters) purchased_quantity
from Purchasing.PurchaseOrderLines a
join Warehouse.StockItems b
on a.StockItemID=b.StockItemID
join Warehouse.StockItemStockGroups c
on b.StockItemID=c.StockItemID
join Warehouse.StockGroups d
on c.StockGroupID=d.StockGroupID
group by d.StockGroupID
),
s as(
select d.StockGroupID, sum(e.quantity) sold_quantity
from sales.OrderLines e
join Warehouse.StockItems b
on e.StockItemID=b.StockItemID
join Warehouse.StockItemStockGroups c
on b.StockItemID=c.StockItemID
join Warehouse.StockGroups d
on c.StockGroupID=d.StockGroupID
group by d.StockGroupID
)
select p.StockGroupID,  p.purchased_quantity, s.sold_quantity, (p.purchased_quantity-s.sold_quantity) remaining_stock_quantity
from p
join s
on p.StockGroupID=s.StockGroupID
group by p.StockGroupID, p.purchased_quantity, s.sold_quantity
order by p.StockGroupID


--14.List of Cities in the US and the stock item that the city got the most deliveries in 2016. If the city did not purchase any stock items in 2016, print 'No Sales'.
--
with delivery_rank as(
select e.CityID, a.StockitemID, count(*) deliveries, dense_rank() OVER(partition by e.CityID order by count(*) desc) as rank_id
from Warehouse.StockItems a
join Sales.Orderlines b
on a.StockItemID=b.StockItemID
join Sales.Orders c
on c.OrderID=b.OrderID
join Sales.Customers d                                  -- this is not perfect, couldn't be able to print 'No sale'
on d.CustomerID=c.CustomerID
right join Application.Cities e
on d.DeliveryCityID=e.CityID
join Application.StateProvinces f
on e.StateProvinceID=f.StateProvinceID
join Application.Countries g
on g.CountryID=f.CountryID
where c.OrderDate between '2016-01-01' and '2016-12-31'
	and g.CountryName='United States'
group by e.CityID, a.StockitemID 
)
select r.CityID, r.StockitemID, deliveries
from delivery_rank as r
where rank_id =1
order by r.CityID


--15.List any orders that had more than one delivery attempt (located in invoice table).
--#way 1
select OrderID from Sales.Invoices
where lower(ReturnedDeliveryData) like '%deliveryattempt%deliveryattempt%'
--#way 2
with attemp_count as(
select OrderID, ReturnedDeliveryData,
(LEN(lower(ReturnedDeliveryData)) - LEN(REPLACE(lower(ReturnedDeliveryData), 'deliveryattempt',''))) / LEN('deliveryattempt') AS num_attemp
from Sales.Invoices
)
select OrderID from attemp_count
where num_attemp >1

--16.List all stock items that are manufactured in China. (Country of Manufacture)
select StockItemID
from Warehouse.StockItems
where JSON_VALUE(CustomFields,'$.CountryOfManufacture') = 'China'

--17.Total quantity of stock items sold in 2015, group by country of manufacturing. 
with cte as(
select StockItemID, JSON_VALUE(CustomFields,'$.CountryOfManufacture') CountryOfManufacture
from warehouse.stockitems
)
select cte.CountryOfManufacture, sum(a.quantity) stockitems_sold_2015
from cte
join Sales.OrderLines a
on cte.StockItemID=a.StockItemID
join Sales.Orders b
on a.OrderID=b.OrderID
where b.OrderDate between '2015-01-01' and '2015-12-31'
group by cte.CountryOfManufacture

--18. Create a view that shows the total quantity of stock items of each stock group sold (in orders) by year 2013-2017. [Stock Group Name, 2013, 2014, 2015, 2016, 2017]
IF OBJECT_ID('Sales.vStockGroup_Year','view') IS NOT NULL
	DROP VIEW Sales.vStockGroup_Year;
GO
CREATE VIEW Sales.vStockGroup_Year
AS
select *
FROM(
select e.StockGroupName, year(a.OrderDate) year_, sum(b.quantity) quantity_sold
from Sales.Orders a
join sales.OrderLines b
on a.OrderID=b.OrderID
join Warehouse.StockItems c
on c.StockItemID=b.StockItemID
join Warehouse.StockItemStockGroups d
on d.StockItemID=c.StockItemID
join Warehouse.StockGroups e
on e.StockGroupID=d.StockGroupID 
group by e.StockGroupName, year(a.OrderDate)
order by e.StockGroupName, year(a.OrderDate) OFFSET 0 ROWS
)p 
PIVOT (
	sum(quantity_sold)
	for year_ in (
		[2013], [2014],[2015],[2016],[2017]
		) 
)as piv
GO
select * from Sales.vStockGroup_Year


--19. Create a view that shows the total quantity of stock items of each stock group sold (in orders) by year 2013-2017. 
--[Year, Stock Group Name1, Stock Group Name2, Stock Group Name3, … , Stock Group Name10]
IF OBJECT_ID('Sales.vYear_StockGroup','view') IS NOT NULL
	DROP VIEW Sales.vYear_StockGroup;
GO
CREATE VIEW Sales.vYear_StockGroup
AS
select *
from(
select e.StockGroupName, year(a.OrderDate) year_, sum(b.quantity) quantity_sold
from Sales.Orders a
join sales.OrderLines b
on a.OrderID=b.OrderID              --I noticed null values got missing for question 18-19, but don't have time to modify the query, will denifitely look into it later
join Warehouse.StockItems c
on c.StockItemID=b.StockItemID
join Warehouse.StockItemStockGroups d
on d.StockItemID=c.StockItemID
join Warehouse.StockGroups e
on e.StockGroupID=d.StockGroupID 
group by year(a.OrderDate), e.StockGroupName
order by year(a.OrderDate), e.StockGroupName OFFSET 0 ROWS
)p 
PIVOT (
	sum(quantity_sold)
	for StockGroupName in (
		[Clothing], [Computing Novelties],[Furry Footwear],[Mugs],[Novelty Items],[Packaging Materials],[Toys],[T-Shirts],[USB Novelties]
		) 
)as piv
GO
select * from Sales.vYear_StockGroup

--20. Create a function, input: order id; return: total of that order. List invoices and use that function to attach the order total to the other fields of invoices.
IF OBJECT_ID (N'dbo.ufn_OrderTotal', N'FN') IS NOT NULL --table value function
    DROP FUNCTION dbo.ufn_OrderTotal;
GO
CREATE FUNCTION dbo.ufn_OrderTotal(@OrderID int)
RETURNS TABLE
AS
RETURN(
    SELECT OrderID, sum(a.Quantity * a.UnitPrice) OrderTotal                 
    FROM Sales.Orderlines a
	WHERE a.OrderID=@OrderID
	group by a.OrderID
)
GO
select * from dbo.ufn_OrderTotal(5) --check function
GO
--combine tables
select * from Sales.Invoices s
Outer Apply dbo.ufn_OrderTotal (s.OrderID)
GO

--21. Create a new table called ods.Orders. Create a stored procedure, with proper error handling and transactions, that input is a date; 
--when executed, it would find orders of that day, calculate order total, and save the information (order id, order date, order total, customer id) into the new table. 
--If a given date is already existing in the new table, throw an error and roll back. Execute the stored procedure 5 times using different dates.
CREATE SCHEMA ods;
GO
IF OBJECT_ID(N'ods.Orders', N'U') IS NOT NULL  
   DROP TABLE ods.Orders;
GO 
CREATE TABLE ods.Orders(
OrderID int,                                                      
OrderDate date,
OrderTotal decimal(18,2),                     --tried my best with stored prodecures, room to improve if given more time
CustomerID int	
);
GO
select * from ods.Orders --check new table
GO
--create the stored prcedure
IF OBJECT_ID('ods.uspOrderTotalandInsert','P') IS NOT NULL  
   DROP PROCEDURE ods.uspGetOrderTotalandInsert;
GO
CREATE PROCEDURE ods.uspGetOrderTotalandInsert @OrderDate date
AS
BEGIN 
   BEGIN TRAN
   IF @OrderDate NOT IN (SELECT OrderDate FROM ods.Orders)
      Insert into ods.Orders(OrderID, OrderDate, OrderTotal,CustomerID)
      select a.OrderID, a.OrderDate, SUM(b.Quantity * b.UnitPrice) as OrderTotal, a.CustomerID
      from Sales.Orders as a
      join Sales.Orderlines as b
      on a.OrderID=b.OrderID
      where a.OrderDate=@OrderDate
      group by a.OrderID,a.OrderDate, a.CustomerID
   ELSE
      ROLLBACK TRAN
COMMIT TRAN
END

EXECUTE ods.uspGetOrderTotalandInsert '2013-01-01'
GO
EXECUTE ods.uspGetOrderTotalandInsert '2013-02-01'
GO
EXECUTE ods.uspGetOrderTotalandInsert '2013-03-01'
GO
EXECUTE ods.uspGetOrderTotalandInsert '2013-04-01'
GO
EXECUTE ods.uspGetOrderTotalandInsert '2013-05-01'
GO
--check result
select * from ods.Orders

--22. Create a new table called ods.StockItem. It has following columns: [StockItemID], [StockItemName] ,[SupplierID] ,[ColorID] ,[UnitPackageID] ,[OuterPackageID] ,[Brand] ,[Size] ,[LeadTimeDays] ,[QuantityPerOuter] ,[IsChillerStock] ,[Barcode] ,[TaxRate] ,[UnitPrice],[RecommendedRetailPrice] ,[TypicalWeightPerUnit] ,[MarketingComments] ,[InternalComments], [CountryOfManufacture], [Range], [Shelflife]. 
-- Migrate all the data in the original stock item table.
IF OBJECT_ID(N'ods.StockItem', N'U') IS NOT NULL  
   DROP TABLE ods.StockItem;
GO 
CREATE TABLE ods.StockItem(
StockItemID int PRIMARY KEY,
StockItemName nvarchar(100),
SupplierID int,
ColorID int,
UnitPackageID int,
OuterPackageID int,
Brand nvarchar(50),
Size nvarchar(20),
LeadTimeDays int ,
QuantityPerOuter int,
IsChillerStock bit,
Barcode nvarchar(50),
TaxRate decimal(18, 3),
UnitPrice decimal(18, 2),
RecommendedRetailPrice decimal(18, 2),
TypicalWeightPerUnit decimal(18, 3),
MarketingComments nvarchar(MAX),
InternalComments nvarchar(MAX), 
CountryOfManufacture nvarchar(50), 
Range_ nvarchar(50), 
Shelflife nvarchar(50)
);
GO
--check the created table
select * from ods.StockItem
GO
--Migrate data
MERGE INTO ods.StockItem a
USING Warehouse.StockItems b
ON a.StockItemID=b.StockItemID
WHEN NOT MATCHED
THEN INSERT VALUES (b.StockItemID, b.StockItemName, b.SupplierID, b.ColorID, b.UnitPackageID, b.OuterPackageID, b.Brand,
					b.Size, b.LeadTimeDays, b.QuantityPerOuter, b.IsChillerStock, b.Barcode, b.TaxRate, b.UnitPrice, b.RecommendedRetailPrice, b.TypicalWeightPerUnit, b.MarketingComments, b.InternalComments,
					JSON_VALUE(b.CustomFields,'$.CountryOfManufacture'),
					JSON_VALUE(b.CustomFields,'$.Range'),
					JSON_VALUE(b.CustomFields,'$.ShelfLife')
                    );
GO
--check result
select * from ods.StockItem

--23. Rewrite your stored procedure in (21). Now with a given date, it should wipe out all the order data prior to the input date and load the order data that was placed in the next 7 days following the input date.
IF OBJECT_ID('ods.uspOrderTotalWipeandUpdate','P') IS NOT NULL  
   DROP PROCEDURE ods.uspOrderTotalWipeandUpdate;
GO
CREATE PROCEDURE ods.uspOrderTotalWipeandUpdate @OrderDate date
AS
BEGIN                                                                                            
   BEGIN TRAN
      --delete all the order data prior to the input date
      Delete from ods.Orders
      where ods.Orders.OrderDate < @OrderDate
      -- insert new data
      Insert into ods.Orders(OrderID, OrderDate, OrderTotal,CustomerID)
      select a.OrderID, a.OrderDate, SUM(b.Quantity * b.UnitPrice) as OrderTotal, a.CustomerID
      from Sales.Orders as a
      join Sales.Orderlines as b
      on a.OrderID=b.OrderID
      where DATEDIFF(day,@OrderDate, a.OrderDate)<=7 and DATEDIFF(day,@OrderDate, a.OrderDate)>0
      group by a.OrderID,a.OrderDate, a.CustomerID
   COMMIT TRAN
END;

EXECUTE ods.uspOrderTotalWipeandUpdate '2014-05-01'
GO
EXECUTE ods.uspOrderTotalWipeandUpdate '2013-04-01'
GO
EXECUTE ods.uspOrderTotalWipeandUpdate '2013-03-01'
GO
EXECUTE ods.uspOrderTotalWipeandUpdate '2014-04-18'
GO
EXECUTE ods.uspOrderTotalWipeandUpdate '2014-04-20'
GO
--
select * from ods.Orders

--24. Consider the JSON file, Migrate these data into Stock Item, Purchase Order and Purchase Order Lines tables. Of course, save the script.
DECLARE @json NVARCHAR(MAX)
set @json = N'[
{
   "PurchaseOrders":[
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"7",
         "UnitPackageId":"1",
         "OuterPackageId":[
            6,
            7
         ],
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-01",                                      
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"WWI2308"
      },                                                                            
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"5",
         "UnitPackageId":"1",
         "OuterPackageId":"7",
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-025",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"269622390"
      }
   ]
}
]'
select * into 
from OPENJSON(@json, '$.PurchaseOrders')
			 WITH (StockItemName nvarchar(100) '$.StockItemName',
			       SupplierID int '$.Supplier',
				   UnitPackageID int '$.UnitPackageId',
				   OuterPackageID int '$.OuterPackageId',
			       Brand nvarchar(50) '$.Brand',
			       LeadTimeDays int '$.LeadTimeDays',  
			       QuantityPerOuter int '$.QuantityPerOuter',
			       TaxRate decimal(18,3) '$.TaxRate',
			       UnitPrice decimal(18,2) '$.UnitPrice',
			       RecommendedRetailPrice decimal(18,2) '$.RecommendedRetailPrice',
			       TypicalWeightPerUnit decimal(18,3) '$.TypicalWeightPerUnit',
			       CountryOfManufacture nvarchar(50) '$.CountryOfManufacture',
			       Range nvarchar(50) '$.Range')



--25. Revisit your answer in (19). Convert the result in JSON string and save it to the server using TSQL FOR JSON PATH.
select * from Sales.vYear_StockGroup
FOR JSON PATH, ROOT('Year_StockGroup_JSON')

--26. Revisit your answer in (19). Convert the result into an XML string and save it to the server using TSQL FOR XML PATH.
select [Clothing], [Computing Novelties] as Computing_Novelties ,[Furry Footwear] as Furry_Footwear,[Mugs],
        [Novelty Items] as Novelty_Items,[Packaging Materials] as Packaging_Materials,[Toys],[T-Shirts],[USB Novelties] as USB_Novelties
from Sales.vYear_StockGroup
FOR XML PATH, ROOT('Year_StockGroup_XML')

-- 27. Create a new table called ods.ConfirmedDeviveryJson with 3 columns (id, date, value) . Create a stored procedure, input is a date. 
--The logic would load invoice information (all columns) as well as invoice line information (all columns) and forge them into a JSON string and then insert into the new table just created.
-- Then write a query to run the stored procedure for each DATE that customer id 1 got something delivered to him.
IF OBJECT_ID(N'ods.ConfirmedDeviveryJson', N'U') IS NOT NULL  
   DROP TABLE ods.ConfirmedDeviveryJson;
GO 
CREATE TABLE ods.ConfirmedDeviveryJson(
ID int,                                                      
[Date] date,
[value] nvarchar(50)
);
GO
IF OBJECT_ID('ods.uspInvoiceJson','P') IS NOT NULL  
   DROP PROCEDURE ods.uspInvoiceJson;
GO
CREATE PROCEDURE ods.uspInvoiceJson
	@idate date,
	@customerid int
AS
BEGIN 
      INSERT INTO ods.ConfirmedDeviveryJson
	   SELECT 

--need more learning on JSON
