using Dapper;
using DatabaseTests.Dto;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class TempTableTests
    {
        private const string _SqlSelectLocalTempTable = "SELECT CustomerID, CompanyName, ContactName FROM #BrazilCustomers";
        private const string _SqlSelectGlobalTempTable = "SELECT CustomerID, CompanyName, ContactName FROM ##BrazilCustomers";

        [TestInitialize]
        public void InitializeDb( )
        {
            SqlScripts.InitNorthWindDatabase(  );
        }

        [TestMethod]
        public void TempTable_CreateSelect_Succeeds(  )
        {
            const string sql = @"
CREATE TABLE #BrazilCustomers (
	[CustomerID] nchar (5) NOT NULL ,
	[CompanyName] nvarchar (40) NOT NULL ,
	[ContactName] nvarchar (30) NULL ,
	CONSTRAINT [PK_Customers] PRIMARY KEY  CLUSTERED 
	(
		[CustomerID]
	)
)

INSERT INTO #BrazilCustomers
SELECT CustomerID, CompanyName, ContactName
FROM Customers
WHERE Country = N'Brazil'
";
            using var connection = new MemoryDbConnection( );
            connection.Execute( sql );
            connection.TempTables.Count.Should( ).Be( 1 );
            connection.TempTables[ "dbo.#BrazilCustomers" ].Rows.Should( ).NotBeEmpty( );
            var brazilCustomers = connection.Query<CustomerViewDto>( _SqlSelectLocalTempTable );
            brazilCustomers.Should( ).NotBeEmpty( );

            var secondConnection = new MemoryDbConnection(  );
            secondConnection.TempTables.Count.Should( ).Be( 0 );

        }

        [TestMethod]
        public void TempTableLocal_SelectInto_Succeeds(  )
        {
            const string sql = @"
SELECT CustomerID, CompanyName, ContactName
INTO #BrazilCustomers
FROM Customers
WHERE Country = N'Brazil'
";
            using var connection = new MemoryDbConnection( );
            connection.Execute( sql );
            connection.TempTables.Count.Should( ).Be( 1 );
            connection.TempTables[ "dbo.#BrazilCustomers" ].Rows.Should( ).NotBeEmpty( );
            var brazilCustomers = connection.Query<CustomerViewDto>( _SqlSelectLocalTempTable );
            brazilCustomers.Should( ).NotBeEmpty( );

            var secondConnection = new MemoryDbConnection(  );
            secondConnection.TempTables.Count.Should( ).Be( 0 );
        }

        [TestMethod]
        public void TempTableGlobal_SelectInto_Succeeds(  )
        {
            const string sql = @"
SELECT CustomerID, CompanyName, ContactName
INTO ##BrazilCustomers
FROM Customers
WHERE Country = N'Brazil'
";
            using var connection = new MemoryDbConnection( );
            connection.Execute( sql );
            connection.TempTables.Count.Should( ).Be( 0 );
            var brazilCustomers = connection.Query<CustomerViewDto>( _SqlSelectGlobalTempTable );
            brazilCustomers.Should( ).NotBeEmpty( );

            var secondConnection = new MemoryDbConnection(  );
            secondConnection.TempTables.Count.Should( ).Be( 0 );
        }

        [TestMethod]
        public void GlobalTempTable_CreateSelect_Succeeds(  )
        {
            const string sql = @"
CREATE TABLE ##BrazilCustomers (
	[CustomerID] nchar (5) NOT NULL ,
	[CompanyName] nvarchar (40) NOT NULL ,
	[ContactName] nvarchar (30) NULL ,
	CONSTRAINT [PK_Customers] PRIMARY KEY  CLUSTERED 
	(
		[CustomerID]
	)
)

INSERT INTO ##BrazilCustomers
SELECT CustomerID, CompanyName, ContactName
FROM Customers
WHERE Country = N'Brazil'
";
            using var connection = new MemoryDbConnection( );
            connection.Execute( sql );
            connection.TempTables.Should(  ).BeEmpty(  );
            var db = connection.GetMemoryDatabase( );
            db.Tables.Should( ).ContainKey( "dbo.##BrazilCustomers" );
            db.Tables[ "dbo.##BrazilCustomers" ].Rows.Should( ).NotBeEmpty( );

            var brazilCustomers = connection.Query<CustomerViewDto>( _SqlSelectGlobalTempTable );
            brazilCustomers.Should( ).NotBeEmpty( );
        }

    }

}
