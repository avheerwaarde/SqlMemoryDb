# SqlMemoryDb
Library that can be used as a memory based replacement for SqlDbConnection for testing purposes. All data is kept in memory, so all is lost when the application is closed. The SQL dialect used is compatible with SQL server.

In many project I work on use SqlDbConnection + Dapper. I tried to find a memory database with a SQL language that is compatible with Microsoft SQL server. But I could not find anything, so I thought why not build it myself (How hard can it be (famous last word ;-) )).

When I get to it, I will start a project wiki. Until this one is up and running, here are some implementation details:
* The SQL is parsed with a parser from Microsoft `Microsoft.SqlServer.Management.SqlParser`. This will ensure that the SQL dialect is fully SQL server compliant. 
* The public classes are all derived from the Db classes in `System.Data` with a Memory prefix. This means `MemoryDbConnection` is inherited from `DbConnection`. As long as it is compatible with the data access layer from `System.Data`, it will probably work (I hope, I hope, I hope).
* The internal data structures for tables, columns and rows can be accessed from the class MemoryDatabase. The singleton instance can be accessed via `var db = MemoryDbConnection.GetMemoryDatabase( )`.

## Supported Framework
* .net standard 2.0

## Limitations
It is still in it's alpha fase and this is a hobby project. So the tests are not production/release worthy and not everything is fully implemented. For instance there is no explicit type conversion, methods, Common Table Expressions (CTE), views, etc. Non supported functionality will throw a NotImplementedException exception. Please feel free to add features yourself if you need anything right now. Please look in the test project to see what is currently supported.
The usage of keywords like: SELECT, UPDATE, INSERT, GROUP BY, HAVING are supported as is an alias.

## Installation
Currently there is no installer or precompiles package. Just copy the sourcecode and compile it as part of your own project.

## Usage
The library is a replacement of the SqlDbConnection package, so replacing SqlDbConnection with MemoryDbConnection should work.

example with Dapper:
```
await using var connection = new MemoryDbConnection( );
await connection.OpenAsync( );
var transaction = await connection.BeginTransactionAsync( );
await connection.ExecuteAsync( "INSERT INTO application ([Name]) VALUES (@Name)", new {Name = "Name string"}, transaction );
await transaction.CommitAsync( );
```

Plain example
```
            const string sqlSelect = @"
SELECT  
	application.Id
	, application.Name
	, application.[User]
	, DefName
	, application_action.Id AS ActionId
	, application_action.Name AS ActionName
	, application_action.Action
	, application_action.fk_application
	, [Order]
FROM  application 
INNER JOIN application_action ON application.Id = application_action.fk_application";

            await using var connection = new MemoryDbConnection( );
            await connection.OpenAsync( );
            var command = connection.CreateCommand( );
            command.CommandText = sqlSelect;
            await command.PrepareAsync( );

            var reader = await command.ExecuteReaderAsync();
            while ( await reader.ReadAsync() )
            {
                // Do something usefull with the data
            }
```

## Licence
MIT licence

## Revisions
Version | Description
--------|---------------
0.10.1| SQL statement IF (NOT) EXISTS, CURRENT_TIMESTAMP, GUID value from string
0.10.2| SQL local variables, sub queries
0.10.3| Stored procedures
0.10.4| SQL case statement, Identity insert on / off, Alter table check constraint, better insert values parsing
0.10.5| SQL in clause
0.10.6| Views
0.10.7| Views + Order By clause
0.10.8| Union, Union All, Except, Intersect
0.10.9| delete statement
0.10.10| select distinct
0.10.11| like + wildards
0.10.12| Insert into + Select
0.10.13| Math Functions: Avg(), Sum(), FLOOR(), CEILING(), ROUND(), Abs(), Sign(), Rand()
0.10.14| Text Functions: ASCII(), CHAR(), CHARINDEX(), CONCAT(), DATALENGTH(), LEFT(), LEN(), LOWER(), LTRIM(), NCHAR(), PATINDEX(), REPLACE(), RIGHT(), RTRIM(), SPACE(), STR(), STUFF(), SUBSTRING(), UPPER()
0.10.15| Date functions: DATEADD(), DATEDIFF(), DATENAME(), DATEPART(), SYSDATETIMEOFFSET(), SYSDATETIME(), GETDATE(), CURRENT_TIMESTAMP SYSUTCDATETIME(), GETUTCDATE(), DAY(), MONTH(), YEAR()
0.10.16| Conversion Functions: Cast(), Try_cast(), Convert(), Try_Convert()
0.10.17| Miscelaneous Functions: @@VERSION, Coalesce(), CURRENT_USER, IsDate(), IsNull(), IsNumeric(), Lag(), Lead(), SESSION_USER, SYSTEM_USER, USER_NAME
0.10.18| Calculations: support for +, -, *, /