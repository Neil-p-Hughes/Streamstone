using System;
using System.Linq;

namespace Streamstone.StreamCollection
{
    /// <summary>
    /// Represent the result of stream open attempt.
    /// </summary>
    public sealed class StreamCollectionDictionaryOpenResult
    {
        internal static readonly StreamCollectionDictionaryOpenResult NotFound = new StreamCollectionDictionaryOpenResult(false, null);

        /// <summary>
        ///  A boolean flag representing whether the stream is exists (was found)
        /// </summary>
        public readonly bool Found;

        /// <summary>
        /// The stream header or <c>null</c> if stream has not been found in storage
        /// </summary>
        public readonly StreamCollectionDictionary Stream;

        internal StreamCollectionDictionaryOpenResult(bool found, StreamCollectionDictionary stream)
        {
            Found = found;
            Stream = stream;
        }
    }
}
