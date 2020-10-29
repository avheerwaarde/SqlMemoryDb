# SqlMemoryDb
Welcome to the SqlMemoryDb project.

This library can be used as a memory based replacement for SqlDbConnection for testing purposes. 

In many project I work on use SqlDbConnection + Dapper. I tried to find a memory database with a SQL language that is compatible with Microsoft SQL server. But I could not find anything, so I thought why not build it myself (How hard can it be (famous last word ;-) )).

**Implementation:**
* The SQL is parsed with a parser from Microsoft `Microsoft.SqlServer.Management.SqlParser`. This will ensure that the SQL dialect is fully SQL server compliant. 
* The public classes are all derived from the Db classes in `System.Data` with a Memory prefix. E.g. `MemoryDbConnection` is inherited from `DbConnection`.
* The internal data structures for tables, columns and rows can be accessed from the class MemoryDatabase. The singleton instance can be accessed via `var db = new MemoryDbConnection().GetMemoryDatabase( )`.


## Limitations
It is still in it's alpha fase and this is a hobby project. So the tests are not production/release worthy and not everything is fully implemented. To keep everything simple and manageable there are some things to keep in mind
* There is only a single database (no connection string needed)
* No user/access rights 
* Not all possible SQL permutations are tested
* No implicit type conversions. You get the same data back as you put in.
* Not everything will be fully implemented
* Multi-threading is not supported yet.
 
## Supported Framework
* .net standard 2.0

## Installation
Currently there is no installer or precompiles package. Just copy the source code of the SqlMemoryDb project and compile it as part of your own project.

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
0.10.19| Calculated fields, Common Table Expressions
0.10.20| Temp tables, SELECT INTO
0.10.21| custom datatypes
0.10.22| rowversion
0.10.23| functions as default values for fields
0.10.24| bugfix multiple joins