using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dapper;
using DatabaseTests.Dto;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class CalculatedFieldsTests
    {
        private const string InitSalesOrderHeader = @"
/****** Object:  Table [Sales].[SalesOrderHeader]    Script Date: 7/13/2020 4:17:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Sales].[SalesOrderHeader](
	[SalesOrderID] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[RevisionNumber] [tinyint] NOT NULL,
	[OrderDate] [datetime] NOT NULL,
	[DueDate] [datetime] NOT NULL,
	[ShipDate] [datetime] NULL,
	[Status] [tinyint] NOT NULL,
	[OnlineOrderFlag] [bit] NOT NULL,
	[SalesOrderNumber]  AS (isnull(N'SO'+CONVERT([nvarchar](23),[SalesOrderID]),N'*** ERROR ***')),
	[PurchaseOrderNumber] [nvarchar](25) NULL,
	[AccountNumber] [nvarchar](15) NULL,
	[CustomerID] [int] NOT NULL,
	[SalesPersonID] [int] NULL,
	[TerritoryID] [int] NULL,
	[BillToAddressID] [int] NOT NULL,
	[ShipToAddressID] [int] NOT NULL,
	[ShipMethodID] [int] NOT NULL,
	[CreditCardID] [int] NULL,
	[CreditCardApprovalCode] [varchar](15) NULL,
	[CurrencyRateID] [int] NULL,
	[SubTotal] [money] NOT NULL,
	[TaxAmt] [money] NOT NULL,
	[Freight] [money] NOT NULL,
	[TotalDue]  AS (isnull(([SubTotal]+[TaxAmt])+[Freight],(0))),
	[Comment] [nvarchar](128) NULL,
	[rowguid] [uniqueidentifier] ROWGUIDCOL  NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
 CONSTRAINT [PK_SalesOrderHeader_SalesOrderID] PRIMARY KEY CLUSTERED 
(
	[SalesOrderID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET IDENTITY_INSERT [Sales].[SalesOrderHeader] ON 

GO
INSERT [Sales].[SalesOrderHeader] ([SalesOrderID], [RevisionNumber], [OrderDate], [DueDate], [ShipDate], [Status], [OnlineOrderFlag], [PurchaseOrderNumber], [AccountNumber], [CustomerID], [SalesPersonID], [TerritoryID], [BillToAddressID], [ShipToAddressID], [ShipMethodID], [CreditCardID], [CreditCardApprovalCode], [CurrencyRateID], [SubTotal], [TaxAmt], [Freight], [Comment], [rowguid], [ModifiedDate]) VALUES (43659, 8, CAST(N'2011-05-31T00:00:00.000' AS DateTime), CAST(N'2011-06-12T00:00:00.000' AS DateTime), CAST(N'2011-06-07T00:00:00.000' AS DateTime), 5, 0, N'PO522145787', N'10-4020-000676', 29825, 279, 5, 985, 985, 5, 16281, N'105041Vi84182', NULL, 20565.6206, 1971.5149, 616.0984, NULL, N'79b65321-39ca-4115-9cba-8fe0903e12e6', CAST(N'2011-06-07T00:00:00.000' AS DateTime))
GO
INSERT [Sales].[SalesOrderHeader] ([SalesOrderID], [RevisionNumber], [OrderDate], [DueDate], [ShipDate], [Status], [OnlineOrderFlag], [PurchaseOrderNumber], [AccountNumber], [CustomerID], [SalesPersonID], [TerritoryID], [BillToAddressID], [ShipToAddressID], [ShipMethodID], [CreditCardID], [CreditCardApprovalCode], [CurrencyRateID], [SubTotal], [TaxAmt], [Freight], [Comment], [rowguid], [ModifiedDate]) VALUES (43660, 8, CAST(N'2011-05-31T00:00:00.000' AS DateTime), CAST(N'2011-06-12T00:00:00.000' AS DateTime), CAST(N'2011-06-07T00:00:00.000' AS DateTime), 5, 0, N'PO18850127500', N'10-4020-000117', 29672, 279, 5, 921, 921, 5, 5618, N'115213Vi29411', NULL, 1294.2529, 124.2483, 38.8276, NULL, N'738dc42d-d03b-48a1-9822-f95a67ea7389', CAST(N'2011-06-07T00:00:00.000' AS DateTime))
GO
INSERT [Sales].[SalesOrderHeader] ([SalesOrderID], [RevisionNumber], [OrderDate], [DueDate], [ShipDate], [Status], [OnlineOrderFlag], [PurchaseOrderNumber], [AccountNumber], [CustomerID], [SalesPersonID], [TerritoryID], [BillToAddressID], [ShipToAddressID], [ShipMethodID], [CreditCardID], [CreditCardApprovalCode], [CurrencyRateID], [SubTotal], [TaxAmt], [Freight], [Comment], [rowguid], [ModifiedDate]) VALUES (43661, 8, CAST(N'2011-05-31T00:00:00.000' AS DateTime), CAST(N'2011-06-12T00:00:00.000' AS DateTime), CAST(N'2011-06-07T00:00:00.000' AS DateTime), 5, 0, N'PO18473189620', N'10-4020-000442', 29734, 282, 6, 517, 517, 5, 1346, N'85274Vi6854', 4, 32726.4786, 3153.7696, 985.5530, NULL, N'd91b9131-18a4-4a11-bc3a-90b6f53e9d74', CAST(N'2011-06-07T00:00:00.000' AS DateTime))
GO
SET IDENTITY_INSERT [Sales].[SalesOrderHeader] OFF
";

        [TestMethod]
        public void CalculatedField_Simple_Succeeds(  )
        {
            using var connection = new MemoryDbConnection( );
            connection.GetMemoryDatabase(  ).Clear(  );
            connection.Execute( InitSalesOrderHeader );
            var headers = connection.Query<SalesOrderHeaderCalculatedDto>( "SELECT TotalDue, SalesOrderID, SubTotal, TaxAmt, Freight FROM [Sales].[SalesOrderHeader]" ).ToList( );
        }

        [TestMethod]
        public void CalculatedField_IsNullString_Succeeds(  )
        {
         const string sql = @"
/****** Object:  Table [Sales].[SalesOrderHeader]    Script Date: 7/13/2020 4:17:08 PM ******/
CREATE TABLE [Sales].[SalesOrderHeader](
	[Id] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[SalesOrderID] [int] NULL,
	[SalesOrderNumber]  AS (isnull(N'SO'+CONVERT([nvarchar](23),[SalesOrderID]),N'*** ERROR ***')),
 CONSTRAINT [PK_SalesOrderHeader_SalesOrderID] PRIMARY KEY CLUSTERED 
(
	[SalesOrderID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
INSERT [Sales].[SalesOrderHeader] ([SalesOrderID]) VALUES (123)
GO
INSERT [Sales].[SalesOrderHeader] ([SalesOrderID]) VALUES (NULL)
GO
";
            using var connection = new MemoryDbConnection( );
            connection.GetMemoryDatabase(  ).Clear(  );
            connection.Execute( sql );
            var headers = connection.Query<SalesOrderHeaderCalculatedDto>( "SELECT SalesOrderID, SalesOrderNumber FROM [Sales].[SalesOrderHeader]" ).ToList( );
            foreach ( var header in headers )
            {
                if ( header.SalesOrderID.HasValue )
                {
                    header.SalesOrderNumber.Should( ).Be( "SO" + header.SalesOrderID.Value );
                }
                else
                {
                    header.SalesOrderNumber.Should( ).Be( "*** ERROR ***" );
                }
            }
        }

        [TestMethod]
        public void CalculatedField_AsFixedString_Succeeds(  )
        {
            const string sql = @"
/****** Object:  Table [Sales].[SalesOrderHeader]    Script Date: 7/13/2020 4:17:08 PM ******/
CREATE TABLE [Sales].[SalesOrderHeader](
	[Id] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[SalesOrderID] [int] NOT NULL,
	[SalesOrderNumber] AS CONVERT([nchar](23),[SalesOrderID]),
 CONSTRAINT [PK_SalesOrderHeader_SalesOrderID] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
INSERT [Sales].[SalesOrderHeader] ([SalesOrderID]) VALUES (123)
GO
INSERT [Sales].[SalesOrderHeader] ([SalesOrderID]) VALUES (456)
GO
";
            using var connection = new MemoryDbConnection( );
            connection.GetMemoryDatabase(  ).Clear(  );
            connection.Execute( sql );
            var headers = connection.Query<SalesOrderHeaderCalculatedDto>( "SELECT SalesOrderID, SalesOrderNumber FROM [Sales].[SalesOrderHeader]" ).ToList( );
            foreach ( var header in headers )
            {
                header.SalesOrderNumber.Should( ).StartWith( header.SalesOrderID.ToString( ) );
                header.SalesOrderNumber.Length.Should( ).Be( 23 );
            }
        }

        [TestMethod]
        public void CalculatedField_AsYearFromDate_Succeeds(  )
        {
            const string sql = @"
SET DATEFORMAT ymd
GO
/****** Object:  Table [Sales].[SalesOrderHeader]    Script Date: 7/13/2020 4:17:08 PM ******/
CREATE TABLE [Sales].[SalesOrderHeader](
	[Id] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[OrderDate] [datetime] NOT NULL,
	[Year] AS  DATEPART(year, [OrderDate]),
 CONSTRAINT [PK_SalesOrderHeader_SalesOrderID] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
INSERT [Sales].[SalesOrderHeader] ([OrderDate]) VALUES ('2018-01-02')
GO
INSERT [Sales].[SalesOrderHeader] ([OrderDate]) VALUES ('2019-01-02')
GO
INSERT [Sales].[SalesOrderHeader] ([OrderDate]) VALUES ('2020-01-02')
GO
";
            using var connection = new MemoryDbConnection( );
            connection.GetMemoryDatabase(  ).Clear(  );
            connection.Execute( sql );
            var headers = connection.Query<SalesOrderHeaderCalculatedDto>( "SELECT Year, OrderDate FROM [Sales].[SalesOrderHeader]" ).ToList( );
            foreach ( var header in headers )
            {
                header.Year.Should( ).Be( header.OrderDate.Year );
            }
        }

    }
}
