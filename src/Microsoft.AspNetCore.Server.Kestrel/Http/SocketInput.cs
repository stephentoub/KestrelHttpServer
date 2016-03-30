// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Server.Kestrel.Infrastructure;

namespace Microsoft.AspNetCore.Server.Kestrel.Http
{
    public class SocketInput : ICriticalNotifyCompletion, IDisposable
    {
        private static readonly Action _awaitableIsCompleted = () => { };
        private static readonly Action _awaitableIsNotCompleted = () => { };

        private readonly MemoryPool _memory;
        private readonly IThreadPool _threadPool;
        private readonly ManualResetEventSlim _manualResetEvent = new ManualResetEventSlim(false, 0);

        private Action _awaitableState;
        private Exception _awaitableError;

        private MemoryPoolBlock _head;
        private MemoryPoolBlock _tail;
        private MemoryPoolBlock _pinned;

        private int _consumingState;
        private object _sync = new object();

        private readonly BufferLengthConnectionController _bufferLengthConnectionController;

        public SocketInput(MemoryPool memory, IThreadPool threadPool)
        {
            _memory = memory;
            _threadPool = threadPool;
            _awaitableState = _awaitableIsNotCompleted;
        }

        public SocketInput(MemoryPool memory, IThreadPool threadPool, long maxBufferLength, IConnectionControl connectionControl,
            KestrelThread connectionThread) : this(memory, threadPool)
        {
            _bufferLengthConnectionController = new BufferLengthConnectionController(maxBufferLength, connectionControl, connectionThread);
        }

        public bool RemoteIntakeFin { get; set; }

        public bool IsCompleted => ReferenceEquals(_awaitableState, _awaitableIsCompleted);

        public MemoryPoolBlock IncomingStart()
        {
            const int minimumSize = 2048;

            if (_tail != null && minimumSize <= _tail.Data.Offset + _tail.Data.Count - _tail.End)
            {
                _pinned = _tail;
            }
            else
            {
                _pinned = _memory.Lease();
            }

            return _pinned;
        }

        public void IncomingData(byte[] buffer, int offset, int count)
        {
            lock (_sync)
            {
                // Must call Add() before bytes are available to consumer, to ensure that Length is >= 0
                _bufferLengthConnectionController?.Add(count);

                if (count > 0)
                {
                    if (_tail == null)
                    {
                        _tail = _memory.Lease();
                    }

                    var iterator = new MemoryPoolIterator(_tail, _tail.End);
                    iterator.CopyFrom(buffer, offset, count);

                    if (_head == null)
                    {
                        _head = _tail;
                    }

                    _tail = iterator.Block;
                }
                else
                {
                    RemoteIntakeFin = true;
                }

                Complete();
            }
        }

        public void IncomingComplete(int count, Exception error)
        {
            lock (_sync)
            {
                // Must call Add() before bytes are available to consumer, to ensure that Length is >= 0
                _bufferLengthConnectionController?.Add(count);

                if (_pinned != null)
                {
                    _pinned.End += count;

                    if (_head == null)
                    {
                        _head = _tail = _pinned;
                    }
                    else if (_tail == _pinned)
                    {
                        // NO-OP: this was a read into unoccupied tail-space
                    }
                    else
                    {
                        _tail.Next = _pinned;
                        _tail = _pinned;
                    }

                    _pinned = null;
                }

                if (count == 0)
                {
                    RemoteIntakeFin = true;
                }
                if (error != null)
                {
                    _awaitableError = error;
                }

                Complete();
            }
        }

        public void IncomingDeferred()
        {
            Debug.Assert(_pinned != null);

            if (_pinned != null)
            {
                if (_pinned != _tail)
                {
                    _memory.Return(_pinned);
                }

                _pinned = null;
            }
        }

        private void Complete()
        {
            var awaitableState = Interlocked.Exchange(
                ref _awaitableState,
                _awaitableIsCompleted);

            _manualResetEvent.Set();

            if (!ReferenceEquals(awaitableState, _awaitableIsCompleted) &&
                !ReferenceEquals(awaitableState, _awaitableIsNotCompleted))
            {
                _threadPool.Run(awaitableState);
            }
        }

        public MemoryPoolIterator ConsumingStart()
        {
            if (Interlocked.CompareExchange(ref _consumingState, 1, 0) != 0)
            {
                throw new InvalidOperationException("Already consuming input.");
            }

            return new MemoryPoolIterator(_head);
        }

        public void ConsumingComplete(
            MemoryPoolIterator consumed,
            MemoryPoolIterator examined)
        {
            lock (_sync)
            {
                MemoryPoolBlock returnStart = null;
                MemoryPoolBlock returnEnd = null;

                if (!consumed.IsDefault)
                {
                    var lengthConsumed = new MemoryPoolIterator(_head).GetLength(consumed);

                    returnStart = _head;
                    returnEnd = consumed.Block;
                    _head = consumed.Block;
                    _head.Start = consumed.Index;

                    // Must call Subtract() after bytes have been freed, to avoid producer starting too early and growing
                    // buffer beyond max length.
                    _bufferLengthConnectionController?.Subtract(lengthConsumed);
                }

                if (!examined.IsDefault &&
                    examined.IsEnd &&
                    RemoteIntakeFin == false &&
                    _awaitableError == null)
                {
                    _manualResetEvent.Reset();

                    Interlocked.CompareExchange(
                        ref _awaitableState,
                        _awaitableIsNotCompleted,
                        _awaitableIsCompleted);
                }

                while (returnStart != returnEnd)
                {
                    var returnBlock = returnStart;
                    returnStart = returnStart.Next;
                    returnBlock.Pool.Return(returnBlock);
                }

                if (Interlocked.CompareExchange(ref _consumingState, 0, 1) != 1)
                {
                    throw new InvalidOperationException("No ongoing consuming operation to complete.");
                }
            }
        }

        public void CompleteAwaiting()
        {
            Complete();
        }

        public void AbortAwaiting()
        {
            _awaitableError = new TaskCanceledException("The request was aborted");

            Complete();
        }

        public SocketInput GetAwaiter()
        {
            return this;
        }

        public void OnCompleted(Action continuation)
        {
            var awaitableState = Interlocked.CompareExchange(
                ref _awaitableState,
                continuation,
                _awaitableIsNotCompleted);

            if (ReferenceEquals(awaitableState, _awaitableIsNotCompleted))
            {
                return;
            }
            else if (ReferenceEquals(awaitableState, _awaitableIsCompleted))
            {
                _threadPool.Run(continuation);
            }
            else
            {
                _awaitableError = new InvalidOperationException("Concurrent reads are not supported.");

                Interlocked.Exchange(
                    ref _awaitableState,
                    _awaitableIsCompleted);

                _manualResetEvent.Set();

                _threadPool.Run(continuation);
                _threadPool.Run(awaitableState);
            }
        }

        public void UnsafeOnCompleted(Action continuation)
        {
            OnCompleted(continuation);
        }

        public void GetResult()
        {
            if (!IsCompleted)
            {
                _manualResetEvent.Wait();
            }
            var error = _awaitableError;
            if (error != null)
            {
                if (error is TaskCanceledException || error is InvalidOperationException)
                {
                    throw error;
                }
                throw new IOException(error.Message, error);
            }
        }

        public void Dispose()
        {
            AbortAwaiting();

            // Return all blocks
            var block = _head;
            while (block != null)
            {
                var returnBlock = block;
                block = block.Next;

                returnBlock.Pool.Return(returnBlock);
            }

            _head = null;
            _tail = null;
        }

        private class BufferLengthConnectionController
        {
            private readonly long _maxLength;
            private readonly IConnectionControl _connectionControl;
            private readonly KestrelThread _connectionThread;

            private readonly object _lock = new object();

            private long _length;
            private bool _connectionPaused;

            public BufferLengthConnectionController(long maxLength, IConnectionControl connectionControl, KestrelThread connectionThread)
            {
                _maxLength = maxLength;
                _connectionControl = connectionControl;
                _connectionThread = connectionThread;
            }

            public long Length
            {
                get
                {
                    return _length;
                }
                set
                {
                    // Caller should ensure that bytes are never consumed before the producer has called Add()
                    Debug.Assert(value >= 0);

                    _length = value;
                }
            }

            public void Add(int count)
            {
                // Add() should never be called while connection is paused, since ConnectionControl.Pause() runs on a libuv thread
                // and should take effect immediately.
                Debug.Assert(!_connectionPaused);

                lock (_lock)
                {
                    Length += count;
                    if (Length >= _maxLength)
                    {
                        _connectionPaused = true;
                        _connectionControl.Pause();
                    }
                }
            }

            public void Subtract(int count)
            {
                lock (_lock)
                {
                    Length -= count;

                    if (_connectionPaused && Length < _maxLength)
                    {
                        _connectionPaused = false;
                        _connectionThread.Post(
                            (connectionControl) => ((IConnectionControl)connectionControl).Resume(),
                            _connectionControl);
                    }
                }
            }
        }
    }
}
