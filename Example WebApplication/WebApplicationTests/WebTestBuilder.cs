using Dapper;
using DotNetCoreSqlDb;
using DotNetCoreSqlDb.Data;
using DotNetCoreSqlDb.Models;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SqlMemoryDb;
using System.Net.Http;

namespace WebApplicationTests
{
    class WebTestBuilder
    {
        public TestServer Server;
        public HttpClient Client;
        public MemoryDatabase Database;

        private const string _SqlCreateTable = @"
CREATE TABLE [dbo].[Todo](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Description] [nvarchar](max) NULL,
	[CreatedDate] [datetime2](7) NOT NULL,
	[Version] [rowversion] NOT NULL,
 CONSTRAINT [PK_Todo] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]";

        private const string _SqlInsertTable = @"INSERT into Todo (Description, CreatedDate) VALUES (@Description, @CreatedDate)";

        /// <summary>
        /// Step 1:
        ///     Use a new connection to create the only table the application uses.
        ///     But before we can do this, we need to clear all internal data (table, views, etc..)
        ///     After this we can create the table.
        /// Step 2:
        ///     Store a connection to the internal database structure. This is not a required step.
        ///     But waste not, want not. 
        /// </summary>
        public WebTestBuilder()
        {
            var connection = (MemoryDbConnection)new MemoryDbConnectionFactory(  ).Create(  );
            Database = connection.GetMemoryDatabase( );
            Database.Clear(  );
            connection.Execute( _SqlCreateTable );
        }

        /// <summary>
        /// Fill the database with these entries. We could do this by directly access the table and rows.
        /// But it is better to just add it via SQL. 
        /// </summary>
        /// <param name="entries">The rows to insert into the database</param>
        /// <returns></returns>
        public WebTestBuilder WithTodoEntries( Todo[] entries )
        {
            var connection = new MemoryDbConnectionFactory(  ).Create(  );
            connection.Execute( _SqlInsertTable, entries );
            return this;
        }

        public WebTestBuilder Build()
        {
            var hostBuilder =  Host.CreateDefaultBuilder(  )
                .ConfigureWebHostDefaults( webBuilder =>
                {
                    webBuilder
                        .UseTestServer()
                        .UseStartup<Startup>()
                        .UseEnvironment( "Development" )
                        .ConfigureTestServices( services =>
                         {
                             services.AddTransient<IDbConnectionFactory, MemoryDbConnectionFactory>();
                         } ); ;
                } );

            var host = hostBuilder.Build();
            host.Start();
            Server = host.GetTestServer();

            // Do not use the server.CreateClient() method.
            // We need to store cookies, so we can work with anti forgery tokens.
            Client = new HttpClient( new TestHttpClientHandler( Server.CreateHandler() ) ) { BaseAddress = Server.BaseAddress };
            return this;
        }
    }
}
