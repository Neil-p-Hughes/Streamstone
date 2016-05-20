namespace Streamstone.StreamCollection
{
    /// <summary>
    /// Represents the result of a single stream read operation.
    /// </summary>
    /// <typeparam name="T">The type of entity this slice will return</typeparam>
    public sealed class StreamCollectionDictionarySlice<T> where T : class, new()
    {
        /// <summary>
        /// The stream header which has been read
        /// </summary>
        public readonly StreamCollectionDictionary Stream;

        /// <summary>
        /// The events that has been read (page)
        /// </summary>
        public readonly T[] Events;

        /// <summary>
        /// Whether this slice has read any events
        /// </summary>
        public readonly bool HasEvents;

        /// <summary>
        ///  A boolean flag representing whether or not this is the end of the stream.
        /// </summary>
        public readonly bool IsEndOfStream;

        internal StreamCollectionDictionarySlice(StreamCollectionDictionary stream, T[] events, int startVersion, int sliceSize)
        {
            Stream = stream;
            Events = events;

            HasEvents = Events.Length > 0;
            IsEndOfStream = (startVersion + sliceSize - 1) >= 0/*stream.Version*/;
            
        }
    }
}