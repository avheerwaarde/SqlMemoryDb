using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using AngleSharp;
using AngleSharp.Html.Dom;
using AngleSharp.Io;

namespace WebApplicationTests
{
    public class HtmlHelpers
    {
        public static async Task<IHtmlDocument> GetDocumentAsync( HttpResponseMessage response )
        {
            var content = await response.Content.ReadAsStringAsync();
            var document = await BrowsingContext.New()
                .OpenAsync( ResponseFactory, CancellationToken.None );
            return (IHtmlDocument)document;

            void ResponseFactory( VirtualResponse htmlResponse )
            {
                htmlResponse
                    .Address( response.RequestMessage.RequestUri )
                    .Status( response.StatusCode );

                MapHeaders( response.Headers );
                MapHeaders( response.Content.Headers );

                htmlResponse.Content( content );

                void MapHeaders( HttpHeaders headers )
                {
                    foreach (var header in headers)
                    {
                        foreach (var value in header.Value)
                        {
                            htmlResponse.Header( header.Key, value );
                        }
                    }
                }
            }
        }

        public static async Task<IHtmlDocument> GetDocumentAsync( string content )
        {
            var document = await BrowsingContext.New()
                .OpenAsync(ResponseFactory, CancellationToken.None);
            return (IHtmlDocument)document;

            void ResponseFactory(VirtualResponse htmlResponse)
            {
                htmlResponse.Content(content);
            }
        }

        public static string GetInputValue( IHtmlDocument document, string formId, string inputName )
        {
            var inputElement = document.QuerySelector( $"#{formId} input[name=\"{inputName}\"]" ) as IHtmlInputElement;
            if (inputElement == null)
            {
                throw new ApplicationException( $"An input field with the name '{inputName}' should exist in the form" );
            }

            return inputElement.Value;
        }

        public static string GetFormUrl( IHtmlDocument document, string formId )
        {
            var formElement = document.QuerySelector( $"#{formId}" ) as IHtmlFormElement;
            if (formElement == null)
            {
                throw new ApplicationException( $"The form {formId} could not be found" );
            }

            return formElement.Action;
        }
    }
}