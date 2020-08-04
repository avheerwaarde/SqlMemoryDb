using System;
using System.Net;
using System.Threading.Tasks;
using Dapper;
using DotNetCoreSqlDb.Models;
using FluentAssertions;
using Microsoft.AspNetCore.Http;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace WebApplicationTests
{
    [TestClass]
    public class ToDoTests
    {
        [TestMethod]
        public async Task Index_WithEntries_TableIsFilled()
        {
            // Arrange: Build with 3 rows in the database table
            var todoArray = new Todo[]
            {
                new Todo{ Description = "Task 1", CreatedDate = DateTime.Now },
                new Todo{ Description = "Task 2", CreatedDate = DateTime.Now },
                new Todo{ Description = "Task 3", CreatedDate = DateTime.Now },
            };
            var webTest = new WebTestBuilder(  ).WithTodoEntries( todoArray ).Build(  );

            // Act: Get the index page
            var response = await webTest.Client.GetAsync( @"/Todos/Index" );

            // Assert: Check that all went as expected
            response.EnsureSuccessStatusCode();
            var document = await HtmlHelpers.GetDocumentAsync( response );
            var table = document.QuerySelector( "#todoTable tbody" );
            table.Children.Should().HaveCount( todoArray.Length );
            foreach ( var row in table.Children )
            {
                row.Children[ 0 ].InnerHtml.Should( ).Contain( "Task " );
            }
        }

        [TestMethod]
        public async Task Edit_WithEntry_EditFormIsFilled()
        {
            // Arrange: 
            var todoArray = new Todo[]
            {
                new Todo{ Description = "Task 1", CreatedDate = DateTime.Now }
            };
            var webTest = new WebTestBuilder(  ).WithTodoEntries( todoArray ).Build(  );

            var connection = new MemoryDbConnectionFactory(  ).Create(  );
            var todo = await connection.QuerySingleAsync<Todo>( "SELECT ID, Description, CreatedDate, Version FROM Todo WHERE ID=@id", new { id=1 } );

            // Act: Get the index page
            var response = await webTest.Client.GetAsync( $@"/Todos/Edit/{todo.ID}" );

            // Assert: Check that all went as expected
            response.EnsureSuccessStatusCode();
            var document = await HtmlHelpers.GetDocumentAsync( response );
            var form = document.QuerySelector( "#formEdit" );
            form.Should( ).NotBeNull( );
        }

        [TestMethod]
        public async Task Edit_WithoutEntry_NotFoundIsShown()
        {
            var webTest = new WebTestBuilder(  ).Build(  );

            // Act: Get the index page
            var response = await webTest.Client.GetAsync( $@"/Todos/Edit/1" );

            // Assert: Check that all went as expected
            response.StatusCode.Should(  ).Be( StatusCodes.Status404NotFound  );
        }
    }
}
