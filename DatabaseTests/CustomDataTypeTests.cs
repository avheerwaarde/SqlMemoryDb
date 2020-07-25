using Dapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlMemoryDb;

namespace DatabaseTests
{
    [TestClass]
    public class CustomDataTypeTests
    {
        private const string _SqlCreateTable = @"
CREATE TYPE [dbo].[Name] FROM [nvarchar](50) NULL
GO
CREATE TYPE [dbo].[NameStyle] FROM [bit] NOT NULL
GO

CREATE TABLE [Person].[Person](
	[BusinessEntityID] [int] NOT NULL,
	[PersonType] [nchar](2) NOT NULL,
	[NameStyle] [dbo].[NameStyle] NOT NULL,
	[Title] [nvarchar](8) NULL,
	[FirstName] [dbo].[Name] NOT NULL,
	[MiddleName] [dbo].[Name] NULL,
	[LastName] [dbo].[Name] NOT NULL,
	[Suffix] [nvarchar](10) NULL,
	[EmailPromotion] [int] NOT NULL,
	[rowguid] [uniqueidentifier] ROWGUIDCOL  NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
 CONSTRAINT [PK_Person_BusinessEntityID] PRIMARY KEY CLUSTERED 
(
	[BusinessEntityID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

INSERT [Person].[Person] ([BusinessEntityID], [PersonType], [NameStyle], [Title], [FirstName], [MiddleName], [LastName], [Suffix], [EmailPromotion], [rowguid], [ModifiedDate]) VALUES (1, N'EM', 0, NULL, N'Ken', N'J', N'Sánchez', NULL, 0, N'92c4279f-1207-48a3-8448-4636514eb7e2', CAST(N'2009-01-07T00:00:00.000' AS DateTime))
GO
INSERT [Person].[Person] ([BusinessEntityID], [PersonType], [NameStyle], [Title], [FirstName], [MiddleName], [LastName], [Suffix], [EmailPromotion], [rowguid], [ModifiedDate]) VALUES (2, N'EM', 0, NULL, N'Terri', N'Lee', N'Duffy', NULL, 1, N'd8763459-8aa8-47cc-aff7-c9079af79033', CAST(N'2008-01-24T00:00:00.000' AS DateTime))
GO
INSERT [Person].[Person] ([BusinessEntityID], [PersonType], [NameStyle], [Title], [FirstName], [MiddleName], [LastName], [Suffix], [EmailPromotion], [rowguid], [ModifiedDate]) VALUES (3, N'EM', 0, NULL, N'Roberto', NULL, N'Tamburello', NULL, 0, N'e1a2555e-0828-434b-a33b-6f38136a37de', CAST(N'2007-11-04T00:00:00.000' AS DateTime))
GO
INSERT [Person].[Person] ([BusinessEntityID], [PersonType], [NameStyle], [Title], [FirstName], [MiddleName], [LastName], [Suffix], [EmailPromotion], [rowguid], [ModifiedDate]) VALUES (4, N'EM', 0, NULL, N'Rob', NULL, N'Walters', NULL, 0, N'f2d7ce06-38b3-4357-805b-f4b6b71c01ff', CAST(N'2007-11-28T00:00:00.000' AS DateTime))
GO
";

        [TestMethod]
        public void Person_CreateTable_Succeeds( )
        {
            using var connection = new MemoryDbConnection( );
			connection.GetMemoryDatabase(  ).Clear(  );
            connection.Execute( _SqlCreateTable );
        }

    }
}
