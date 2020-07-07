using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DatabaseTests
{
    internal class SqlStatements
    {
        private static string _NorthWindCustomSql;

        public const string SqlCreateTableApplication = @"
CREATE TABLE [dbo].[application](
	[Id] int IDENTITY(1,1) NOT NULL,
	[Name] [nvarchar](max) NULL,
	[User] [nvarchar](max) NOT NULL,
	[DefName] [nvarchar](20) NOT NULL DEFAULT(N'Test'),
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]";

        public const string SqlCreateTableApplicationFeature = @"
CREATE TABLE [dbo].[application_feature](
	[Id] int IDENTITY(1,1) NOT NULL,
	[bit] [bit] NULL,
	[char] [char](10) NULL,
	[varchar] [varchar](10) NULL,
	[nchar] [nchar](10) NULL,
	[nvarchar] [nvarchar](10) NULL,
	[nvarchar_max] [nvarchar](max) NULL,
	[byte] [tinyint] NULL,
	[byte_array] [varbinary](50) NULL,
	[numeric] [numeric](10, 4) NULL,
	[float] [float](10, 4) NULL,
	[real] [real](10, 4) NULL,
	[int] [int] NULL,
	[bigint] [bigint] NULL,
	[DateTime] [datetime] NULL,
	[DateTime2] [datetime2] NULL,
	[fk_application] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
CONSTRAINT [FK_application_feature_feature] FOREIGN KEY ([fk_application]) REFERENCES [application]([Id]) ON DELETE CASCADE ON UPDATE CASCADE
) ON [PRIMARY]";

        public const string SqlCreateTableApplicationAction = @"
CREATE TABLE[dbo].[application_action]
(

   [Id] int IDENTITY(1,1) NOT NULL,
   [Name] [nvarchar](max) NULL,
   [Action][nvarchar] (max) NULL,
   [Order][int] NULL,
   [fk_application][int] NULL,
PRIMARY KEY CLUSTERED
(
   [Id] ASC
)WITH(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON[PRIMARY],
CONSTRAINT [FK_application_action_application] FOREIGN KEY ([fk_application]) REFERENCES [application]([Id]) ON DELETE CASCADE ON UPDATE CASCADE
) ON[PRIMARY]
";

        public const string SqlCreateTableTexts = @"
CREATE TABLE[dbo].[TextTable]
(

   [Id] int IDENTITY(1,1) NOT NULL,
   [Text] [nvarchar](max) NULL
) ON[PRIMARY]
";

        public const string SqlCreateTableApplicationAction2 = @"
CREATE TABLE[dbo].[application_action]
(

   [Id] int IDENTITY(1,1) NOT NULL,
   [Name] [nvarchar](max) NULL,
   [Action][nvarchar] (max) NULL,
   [Order][int] NULL,
   [fk_application][int] NULL,
PRIMARY KEY CLUSTERED
(
   [Id] ASC
)WITH(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON[PRIMARY]
) ON[PRIMARY]

ALTER TABLE [dbo].[application_action] WITH CHECK ADD CONSTRAINT [FK_application_action_application] FOREIGN KEY([fk_application])
REFERENCES [dbo].[application] ([Id])
";

        public const string SqlSelectApplicationAction = @"
SELECT Id, Name, Action, [Order], fk_application
FROM application_action
";
        public const string SqlSelectApplication = @"
SELECT Id, Name, [User], [DefName]
FROM application
";

        public const string SqlCreateDbScriptsRun = @"
CREATE TABLE [dbo].[DbScriptsRun]
(
	[Id] INT IDENTITY (1, 1) NOT NULL PRIMARY KEY, 
    [ScriptId] UNIQUEIDENTIFIER NOT NULL, 
    [DateInserted] DATETIME2 NOT NULL
)";

        public const string SqlSelectDbScriptRun = @"
SELECT Id, ScriptId, DateInserted
FROM DbScriptsRun
";
        public static string SqlCreateNorthWindCustom {
            get
            {
                if ( string.IsNullOrWhiteSpace( _NorthWindCustomSql ) )
                {
                    _NorthWindCustomSql = File.ReadAllText( @"Database Scripts\NorthWind Custom.Sql");
                }
                return _NorthWindCustomSql;
            }
        }


        public const string SqlCreateCustomerCustomerDemo = @"
CREATE TABLE [dbo].[CustomerCustomerDemo] 
	([CustomerID] nchar (5) NOT NULL,
	[CustomerTypeID] [nchar] (10) NOT NULL
) ON [PRIMARY] 
GO
ALTER TABLE CustomerCustomerDemo
	ADD CONSTRAINT [PK_CustomerCustomerDemo] PRIMARY KEY  NONCLUSTERED 
	(
		[CustomerID],
		[CustomerTypeID]
	) ON [PRIMARY]
GO";

        public const string SqlLeadExamples = @"
CREATE TABLE departments
( dept_id INT NOT NULL,
  dept_name VARCHAR(50) NOT NULL,
  CONSTRAINT departments_pk PRIMARY KEY (dept_id)
);

CREATE TABLE employees
( employee_number INT NOT NULL,
  last_name VARCHAR(50) NOT NULL,
  first_name VARCHAR(50) NOT NULL,
  salary INT,
  dept_id INT,
  CONSTRAINT employees_pk PRIMARY KEY (employee_number)
);

INSERT INTO departments (dept_id, dept_name) VALUES (30, 'Accounting');
INSERT INTO departments (dept_id, dept_name) VALUES (45, 'Sales');

INSERT INTO employees (employee_number, last_name, first_name, salary, dept_id) VALUES (12009, 'Sutherland', 'Barbara', 54000, 45);
INSERT INTO employees (employee_number, last_name, first_name, salary, dept_id) VALUES (34974, 'Yates', 'Fred', 80000, 45);
INSERT INTO employees (employee_number, last_name, first_name, salary, dept_id) VALUES (34987, 'Erickson', 'Neil', 42000, 45);
INSERT INTO employees (employee_number, last_name, first_name, salary, dept_id) VALUES (45001, 'Parker', 'Salary', 57500, 30);
INSERT INTO employees (employee_number, last_name, first_name, salary, dept_id) VALUES (75623, 'Gates', 'Steve', 65000, 30);
";

    }

}
