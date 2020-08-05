using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Net.Http.Headers;

namespace WebApplicationTests
{
    public class TestHttpClientHandler : DelegatingHandler
    {
        private readonly CookieContainer _Cookies = new CookieContainer();

        public TestHttpClientHandler( HttpMessageHandler innerHandler)
            : base(innerHandler) { }

        protected override async Task<HttpResponseMessage> SendAsync( HttpRequestMessage request, CancellationToken ct)
        {
            Uri requestUri = request.RequestUri;
            request.Headers.Add(HeaderNames.Cookie, this._Cookies.GetCookieHeader(requestUri));

            HttpResponseMessage response = await base.SendAsync(request, ct);

            if (response.Headers.TryGetValues(HeaderNames.SetCookie, out IEnumerable<string> setCookieHeaders))
            {
                foreach (SetCookieHeaderValue cookieHeader in SetCookieHeaderValue.ParseList(setCookieHeaders.ToList()))
                {
                    Cookie cookie = new Cookie(cookieHeader.Name.Value, cookieHeader.Value.Value, cookieHeader.Path.Value);
                    if (cookieHeader.Expires.HasValue)
                    {
                        cookie.Expires = cookieHeader.Expires.Value.DateTime;
                    }
                    this._Cookies.Add(requestUri, cookie);
                }
            }

            return response;
        }

    }
}
