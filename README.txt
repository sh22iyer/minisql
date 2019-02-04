README
------

Build/compile instructions

$ flex --noyywrap minisql.l
$ bison minisql.y
$ gcc -o minisql minisql.tab.c

There are 2 tables in the schema :-

saleshistory
{
product_id,
cust_id,
sales,
qrcode
}

customers
{
cust_id,
cust_name,
zipcode,
category
}