use swiggy_db;
select * from Swiggy_Data;

--Data Validation & cleaning
--Null check is done to see if there are any null values or not but getting the sum if yes or no 
select 
sum( case when state is null then 1 else 0 end) as null_state,
sum( case when City is null then 1 else 0 end) as null_city,
sum( case when Order_date is null then 1 else 0 end) as null_order_date,
sum( case when Restaurant_Name  is null then 1 else 0 end) as null_restaurant,
sum( case when location is null then 1 else 0 end) as null_location,
sum( case when category is null then 1 else 0 end) as null_category,
sum( case when dish_name is null then 1 else 0 end) as null_dish,
sum( case when price_INR is null then 1 else 0 end) as null_price,
sum( case when rating is null then 1 else 0 end) as null_rating,
sum( case when rating_count is null then 1 else 0 end) as null_rating
from Swiggy_Data;

--To check blank strings 

select *
from Swiggy_Data
where State = '' or City = '' or Restaurant_Name = '' or Location = '' or Dish_Name = '' or Category = '' ;

--duplicate detection
select State , Location, City , Order_Date, Restaurant_Name, Category,Dish_Name,Price_INR, Rating,Rating_Count, count(*)
from Swiggy_Data
group by State , Location, City , Order_Date, Restaurant_Name, Category,Dish_Name,Price_INR, Rating,Rating_Count
having count(*) =1;

--we got 29 rows which have occurances more than 1
--delete duplication 
with delete_duplication as (select *,ROW_NUMBER() over(partition by State , Location, City , Order_Date, Restaurant_Name, Category,Dish_Name,Price_INR, Rating,Rating_Count order by (select null)) as rn from Swiggy_Data)
delete from delete_duplication where rn>1;


create table dim_date(
date_id int identity(1,1) primary key ,
full_date date,
year int ,
month int,
Month_Name varchar(20),
Quarter int,
Day int,
Week int
)
select * from dim_date;
create table dim_location(
location_id int identity(1,1) primary key,
State varchar(200),
City varchar(100),
Location varchar(200)
)

create table dim_restaurant(
restaurant_id int identity(1,1) primary key,
Restaurant_name varchar(200)
)


create table dim_category(
category_id int identity(1,1) primary key, 
Category varchar(200)
)

create table dim_dish(
dish_id int identity(1,1) primary key,
dish_name varchar(200)
)
create table fact_swiggy_orders(
order_id int identity(1,1) primary key ,
date_id int ,
price_INR decimal(10,2),
Rating decimal(4,2),
Rating_count int,
location_id int,
restaurant_id int, 
category_id int,
dish_id int,
foreign key(date_id) references dim_date(date_id),
foreign key(location_id) references dim_location(location_id),
foreign key(restaurant_id) references dim_restaurant(restaurant_id),
foreign key(category_id) references dim_category(category_id),
foreign key(dish_id) references dim_dish(dish_id)
)
select * from fact_swiggy_orders;

--Until now what we have done we done normalization that is dividing the large table into smaller tables
--inerting data in tables 
--dim date 

insert into dim_date (full_date ,Year , Month,Month_Name , Quarter, Day , Week)
Select Distinct
Order_Date,
Year(Order_Date),
Month(Order_Date),
DATENAME(Month, Order_Date),
DATEPART(Quarter, Order_Date),
Day(Order_Date),
DATEPART(Week ,Order_Date)
from Swiggy_Data
where Order_Date is not null;

select * from dim_date;


--dim_location
insert into dim_location(state , city , location)
select distinct 
state , city, location from Swiggy_Data ;

--dim restaurant
insert into dim_restaurant(Restaurant_name)
select distinct Restaurant_Name
from Swiggy_Data;
--dim_category
insert into dim_category(Category)
select distinct Category  from Swiggy_Data;


--dim_dish

insert into dim_dish(dish_name)
select DISTINCT dish_name from   Swiggy_Data;


--fact_table 
insert into fact_swiggy_orders
(
date_id,
price_INR,
Rating ,
Rating_count ,
location_id ,
restaurant_id , 
category_id ,
dish_id 
)
select 
 dd.date_id,
 s.price_INR,
 s.Rating,
 s.Rating_count,
 dl.location_id,
 dr.restaurant_id,
 dc.category_id,
 dsh.dish_id
 from swiggy_data s
 join dim_date dd
 on dd.Full_Date = s.Order_Date
 join dim_location dl
 on dl.State = s.State
 and dl.City = s.City
 and dl.Location = s.Location
 join dim_restaurant dr 
 on dr.Restaurant_name = s.Restaurant_Name
 join dim_category dc
 on dc.Category = s.Category
 join dim_dish dsh
 on dsh.dish_name = s.Dish_Name ;


 select * from fact_swiggy_orders;

  select * from fact_swiggy_orders f
  join dim_date d on f.date_id = d.date_id
  join dim_location l on f.location_id = l.location_id
  join dim_restaurant r on f.restaurant_id = r.restaurant_id
  join dim_category c on f.category_id = c.category_id
  join dim_dish di on f.dish_id = di.dish_id;

  --KPi's
  --Total Orders
  select count(*) as total_orders 
  from fact_swiggy_orders ; 

  --total revenue (INR million)
  select round (sum(price_INR)/1000000,2)  as 'revenue'
  from fact_swiggy_orders;

  -- avg dish_price 
    select format(avg(convert(float, price_INR)),'N2') + ' INR'  as 'revenue'
  from fact_swiggy_orders;

  --avg rating 
  select avg(rating) as 'avg_rating'
  from fact_swiggy_orders;

  --deep dive analysis
  --monthly orders trend

  select d.year ,d.month ,d.month_name , count(*) as total_orders 
  from fact_swiggy_orders f
  join dim_date d  on f.date_id = d.date_id
  group by d.year ,d.month ,d.month_name 
  order by count(*) desc;


  --Quaterly Trend 
   select d.year ,d.Quarter , count(*) as total_orders 
  from fact_swiggy_orders f
  join dim_date d  on f.date_id = d.date_id
  group by d.year ,d.Quarter ;


  --yearly trend 
    select d.year , count(*) as total_orders 
  from fact_swiggy_orders f
  join dim_date d  on f.date_id = d.date_id
  group by d.year 
  order by total_orders desc ;

  --orders by day of week (Mon-Sun)
  select 
  DATENAME(weekday , d.full_date) as day_name,
  count(*) as total_orders 
  from fact_swiggy_orders f 
  join dim_date d on f.date_id = d.date_id
  group by DATENAME(weekday,d.full_date), datepart(weekday,d.full_date)
  order by DATEPART(weekday , d.full_date) desc ;

  -- top 10 orders by cities 
  select top 10 
  l.City , count(*) as total_orders
  from fact_swiggy_orders f
  join dim_location l
  on l.location_id = f.location_id
  group by l.City
  order by total_orders desc;
  --revenue contribution by states 
  select l.State ,sum(f.price_INR) as 'total_revenue'
  from fact_swiggy_orders f
  join dim_location l on l.location_id = f.location_id
  group by l.state 
  order by total_revenue desc ;
  -- top 10 restaurant by orders 
  select top 10  r.Restaurant_name , count(f.order_id) as total_orders ,sum(f.price_INR) as total_revenue
  from fact_swiggy_orders f 
  join dim_restaurant r on r.restaurant_id = f.restaurant_id
  group by  r.Restaurant_name
  order by total_revenue  desc ;

  --top category 
  select top 10  c.Category , count(f.order_id) as total_orders 
  from fact_swiggy_orders f 
  join dim_category c on c.category_id = f.category_id
  group by  c.Category
  order by total_orders  desc ;

  --most ordered dishes 
  select d.dish_name , count(*) as total_order
  from fact_swiggy_orders f  
  join dim_dish d on d.dish_id = f.dish_id
  group by  d.dish_name 
  order by total_order desc 

  --cusine performance 
  select top 10  c.Category , count(f.order_id) as total_orders ,AVG(f.rating) as average_rating 
  from fact_swiggy_orders f 
  join dim_category c on c.category_id = f.category_id
  group by  c.Category
  order by total_orders  desc ;
  --customer spending bucket 
  select 
  case
   when price_INR <100 then 'under 100'
   when price_INR  between 100 and 199 then '100 - 199'
   when price_INR  between 200 and 299 then '200 - 299'
   when price_INR  between 300 and 399 then '300 - 399'
   when price_INR  between 400 and 499 then '400 - 499'
   else '500+'
   end as price_range,
   count(*) as total_orders
   from fact_swiggy_orders
   group by 
    case
   when price_INR <100 then 'under 100'
   when price_INR  between 100 and 199 then '100 - 199'
   when price_INR  between 200 and 299 then '200 - 299'
   when price_INR  between 300 and 399 then '300 - 399'
   when price_INR  between 400 and 499 then '400 - 499'
   else '500+'
   end
   order by total_orders desc ;

   --Rating count distribution (1-5)
   select rating,
   count(*) as rating_count
   from fact_swiggy_orders 
   group by Rating
   order by rating_count desc;