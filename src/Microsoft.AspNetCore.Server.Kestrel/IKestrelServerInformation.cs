// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using Microsoft.AspNetCore.Server.Kestrel.Filter;

namespace Microsoft.AspNetCore.Server.Kestrel
{
    public interface IKestrelServerInformation
    {
        int ThreadCount { get; set; }

        /// <summary>
        /// The maximum number of bytes that will be buffered in memory when reading a request.
        /// Default value is 1048576 (1 MB).
        /// A custom value can be configured using the "kestrel.maxInputBufferLength" key in
        /// <seealso cref="Microsoft.Extensions.Configuration.IConfiguration"/>.
        /// </summary>
        long MaxInputBufferLength { get; set; }

        /// <summary>
        /// The amount of time after the server begins shutting down before connections will be forcefully closed.
        /// By default, Kestrel will wait 5 seconds for any ongoing requests to complete before terminating
        /// the connection.
        /// A custom timeout can be configured using the "kestrel.shutdownTimeout" key in <seealso cref="Microsoft.Extensions.Configuration.IConfiguration"/>.
        /// The value will be parsed as a float representing the timeout in seconds.
        /// </summary>
        TimeSpan ShutdownTimeout { get; set; }

        bool NoDelay { get; set; }

        /// <summary>
        /// Gets or sets values that instruct <seealso cref="KestrelServer"/> whether it is safe to 
        /// pool the Request and Response <seealso cref="System.IO.Stream"/> objects, Headers etc
        /// for another request after the Response's OnCompleted callback has fired. 
        /// When these values are greater than zero it is not safe to retain references to feature components after this event has fired.
        /// They are zero by default.
        /// </summary>
        KestrelServerPoolingParameters PoolingParameters { get; }

        IConnectionFilter ConnectionFilter { get; set; }
    }
}
