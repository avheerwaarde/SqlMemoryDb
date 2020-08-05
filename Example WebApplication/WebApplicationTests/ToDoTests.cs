using Dapper;
using DotNetCoreSqlDb.Models;
using FluentAssertions;
using Microsoft.AspNetCore.Http;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

namespace WebApplicationTests
{
    [TestClass]
    public class ToDoTests
    {
        /// <summary>
        /// If we have a rows in the table, make sure they are shown in the table.
        /// Steps:
        /// 1) Build the system under test
        /// 2) Fill the database with 3 rows.
        /// 3) Call the method TodosController.Index
        /// 4) Check the return code = 200
        /// 5) Get the table with the rows from the response document
        /// 6) Make sure we have the same amount of rows as in the database
        /// 7) Make sure the description of the tasks is correct.
        /// </summary>
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

        /// <summary>
        /// Assert that the we start the edit screen with the correct values
        /// Steps:
        /// 1) Build the system under test
        /// 2) Fill the database with 3 rows.
        /// 3) Call the method TodosController.Edit
        /// 4) Check the return code = 200
        /// 5) Get the fields CreatedDate & Description from the edit form
        /// 6) Assert the fields are the same as from the first todo entry
        /// </summary>
        [TestMethod]
        public async Task Edit_WithEntry_EditFormIsFilled()
        {
            var todoArray = new Todo[]
            {
                new Todo{ Description = "Task 1", CreatedDate = DateTime.Now },
                new Todo{ Description = "Task 2", CreatedDate = DateTime.Now },
                new Todo{ Description = "Task 3", CreatedDate = DateTime.Now },
            };
            var webTest = new WebTestBuilder(  ).WithTodoEntries( todoArray ).Build(  );

            var response = await webTest.Client.GetAsync( @"/Todos/Edit/1" );

            response.EnsureSuccessStatusCode();
            var document = await HtmlHelpers.GetDocumentAsync( response );
            var createdDateString = HtmlHelpers.GetInputValue( document, "formEdit", "CreatedDate" );
            var createdDate = DateTime.Parse( createdDateString );
            var description = HtmlHelpers.GetInputValue( document, "formEdit", "Description" );
            description.Should( ).Be( todoArray[ 0 ].Description );
            createdDate.Should( ).BeSameDateAs( todoArray[ 0 ].CreatedDate );
        }

        /// <summary>
        /// If there are no rows in the table with the correct ID, we should see a http 404 error code
        /// Steps"
        /// 1) Build the system under test
        /// 2) Call the method TodosController.Edit
        /// 3) Check the return code = 404
        /// </summary>
        /// <returns></returns>
        [TestMethod]
        public async Task Edit_WithoutEntry_NotFoundError()
        {
            var webTest = new WebTestBuilder(  ).Build(  );

            var response = await webTest.Client.GetAsync( $@"/Todos/Edit/1" );

            response.StatusCode.Should(  ).Be( StatusCodes.Status404NotFound  );
        }

        /// <summary>
        /// Check that we can update a row.
        /// The post of needs a verification token. So we first do a get of the page and get
        /// the relevant fields from the form. Then we post the fields to the url of the form.
        /// 1) Build the system under test
        /// 2) Fill the database with 3 rows.
        /// 3) Call the method TodosController.Edit
        /// 4) Check the return code = 200
        /// 5) Get the fields from the edit form
        /// 6) Post the fields to the url of the form
        /// 7) Get the updated row from the database
        /// 8) Assert that the field is update. 
        /// </summary>
        [TestMethod]
        public async Task Edit_Post_RowIsUpdated()
        {
            var todoArray = new Todo[]
            {
                new Todo{ Description = "Task 1", CreatedDate = DateTime.Now },
                new Todo{ Description = "Task 2", CreatedDate = DateTime.Now },
                new Todo{ Description = "Task 3", CreatedDate = DateTime.Now },
            };
            var webTest = new WebTestBuilder(  ).WithTodoEntries( todoArray ).Build(  );
            var response = await webTest.Client.GetAsync( @"/Todos/Edit/1" );

            var document = await HtmlHelpers.GetDocumentAsync( response );

            var values = new[]
            {
                new KeyValuePair<string, string>("ID", "1" ),
                new KeyValuePair<string, string>("VersionString", HtmlHelpers.GetInputValue( document, "formEdit", "VersionString" ) ),
                new KeyValuePair<string, string>("Description", "Updated Task 1" ), 
                new KeyValuePair<string, string>("CreatedDate", HtmlHelpers.GetInputValue( document, "formEdit", "CreatedDate" ) ), 
                new KeyValuePair<string, string>("__RequestVerificationToken", HtmlHelpers.GetInputValue( document, "formEdit", "__RequestVerificationToken" ) )
            };

            var formDataContent = new FormUrlEncodedContent(values);
            var url = HtmlHelpers.GetFormUrl( document, "formEdit" );
            var responsePost = await webTest.Client.PostAsync(url, formDataContent);
            responsePost.StatusCode.Should(  ).Be( StatusCodes.Status302Found  );

            var connection = new MemoryDbConnectionFactory(  ).Create(  );
            var todo = await connection.QuerySingleAsync<Todo>( "SELECT ID, Description, CreatedDate, Version FROM Todo WHERE ID=@id", new { id=1 } );
            todo.Description.Should( ).Be( "Updated Task 1" );
        }

        /// <summary>
        /// Check that the update of a row fails with an incorrect verification token.
        /// First do a get of the page and get the relevant fields from the form.
        /// Then we post the fields to the url of the form with an incorrect verification token.
        /// 1) Build the system under test
        /// 2) Fill the database with 3 rows.
        /// 3) Call the method TodosController.Edit
        /// 4) Check the return code = 200
        /// 5) Get the fields from the edit form
        /// 6) Post the fields to the url of the form with a wrong verification token.
        /// 7) Make sure it responds with a 400 bad request
        /// 8) Get the row from the database
        /// 9) Assert that the row is not update. 
        /// </summary>
        [TestMethod]
        public async Task Edit_PostInvalidVerificationToken_PostFails()
        {
            var todoArray = new Todo[]
            {
                new Todo{ Description = "Task 1", CreatedDate = DateTime.Now }
            };
            var webTest = new WebTestBuilder(  ).WithTodoEntries( todoArray ).Build(  );
            var response = await webTest.Client.GetAsync( @"/Todos/Edit/1" );

            var document = await HtmlHelpers.GetDocumentAsync( response );

            var values = new[]
            {
                new KeyValuePair<string, string>("ID", "1" ),
                new KeyValuePair<string, string>("VersionString", HtmlHelpers.GetInputValue( document, "formEdit", "VersionString" ) ),
                new KeyValuePair<string, string>("Description", "Updated Task 1" ), 
                new KeyValuePair<string, string>("CreatedDate", HtmlHelpers.GetInputValue( document, "formEdit", "CreatedDate" ) ), 
                new KeyValuePair<string, string>("__RequestVerificationToken", "<invalid verification token>" )
            };

            var formDataContent = new FormUrlEncodedContent(values);
            var url = HtmlHelpers.GetFormUrl( document, "formEdit" );
            var responsePost = await webTest.Client.PostAsync(url, formDataContent);
            responsePost.StatusCode.Should(  ).Be( StatusCodes.Status400BadRequest );

            var connection = new MemoryDbConnectionFactory(  ).Create(  );
            var todo = await connection.QuerySingleAsync<Todo>( "SELECT ID, Description, CreatedDate, Version FROM Todo WHERE ID=@id", new { id=1 } );
            todo.Description.Should( ).Be( "Task 1" );
        }

        /// <summary>
        /// Check that the update of a row fails with an incorrect version code.
        /// First do a get of the page and get the relevant fields from the form.
        /// Then we post the fields to the url of the form with an incorrect verification token.
        /// 1) Build the system under test
        /// 2) Fill the database with 3 rows.
        /// 3) Call the method TodosController.Edit
        /// 4) Check the return code = 200
        /// 5) Get the fields from the edit form
        /// 6) Post the fields to the url of the form with a wrong verification token.
        /// 7) Make sure it responds with a 404 not found
        /// 8) Get the row from the database
        /// 9) Assert that the row is not update. 
        /// </summary>
        [TestMethod]
        public async Task Edit_PostInvalidVersionString_PostFails()
        {
            var todoArray = new Todo[]
            {
                new Todo{ Description = "Task 1", CreatedDate = DateTime.Now }
            };
            var webTest = new WebTestBuilder(  ).WithTodoEntries( todoArray ).Build(  );
            var response = await webTest.Client.GetAsync( @"/Todos/Edit/1" );

            var document = await HtmlHelpers.GetDocumentAsync( response );

            var values = new[]
            {
                new KeyValuePair<string, string>("ID", "1" ),
                new KeyValuePair<string, string>("VersionString", Convert.ToBase64String( new byte []{ 0x1, 0x2, 0x3, 0x4 } ) ),
                new KeyValuePair<string, string>("Description", "Updated Task 1" ), 
                new KeyValuePair<string, string>("CreatedDate", HtmlHelpers.GetInputValue( document, "formEdit", "CreatedDate" ) ), 
                new KeyValuePair<string, string>("__RequestVerificationToken", HtmlHelpers.GetInputValue( document, "formEdit", "__RequestVerificationToken" ) )
            };

            var formDataContent = new FormUrlEncodedContent(values);
            var url = HtmlHelpers.GetFormUrl( document, "formEdit" );
            var responsePost = await webTest.Client.PostAsync(url, formDataContent);
            responsePost.StatusCode.Should(  ).Be( StatusCodes.Status404NotFound );

            var connection = new MemoryDbConnectionFactory(  ).Create(  );
            var todo = await connection.QuerySingleAsync<Todo>( "SELECT ID, Description, CreatedDate, Version FROM Todo WHERE ID=@id", new { id=1 } );
            todo.Description.Should( ).Be( "Task 1" );
        }
    }
}
