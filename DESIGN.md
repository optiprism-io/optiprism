# Main

OptiPrism — platform for tracking user events and building different reports based on the data.

# Architecture

![](/Users/maximbogdanov/optiprism/optiprism/docs/OptiPrism Arch.drawio.svg)

# Terms
## User
User - person who uses the client application/site and being tracked by the system. User produces events on the client application/site.

User has distinct id (i64 in internal format and string in external) and property.
User property, e.g. trait - a key/value pair. 

Example user:

```json
{
  "id": 123,
  "id_str": "some_id",
  "name": "Juan",
  "country": "Spain",
  "interests": [
    "music",
    "art"
  ],
  "age": 23
}
```
Property may be arbitrary type. Property may be an array.

User are stored in LSM-tree based table and may be retrieved by id (in case of OLTP) or via sequential scan (in case of OLAP).

## Event
Event - meaningful action performed by user. Example: "`Search Product`","`View Product`","`Purchase Product`". 

Event has a set of properties. Event properties are similar to user, e.g. it is key-value pairs.

Example event:

```json
{
  "event": "View Product",
  "Product name": "Macbook Pro M2",
  "Category": "Laptops",
  "Brand": "Apple",
  "Price": 3.14
}
```

Events are stored in the LSM-based table

## Platform

Platform is an interface for sending events and retrieving reports and doing all frontend-backend communications. Platform knows about entities like users and events, but database doesn't know.

The Client is registered in the platform and uses it via UI interface to generate reports, manage data, manage users, ...

Platform has protocols:
* HTTP - access via Rest API. Check openapi schema [here](/api/openapi.yaml)


