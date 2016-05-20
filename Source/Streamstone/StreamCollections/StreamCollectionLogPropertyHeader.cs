using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.StreamCollection
{
    class StreamCollectionLogPropertyHeader 
    {


        public StreamCollectionLogPropertyHeader()
        {
        }

        public StreamCollectionLogPropertyHeader(TimeSpan timespanWindowSize, string PartitionKey, bool consolidateStreams)
        {
            this.TimespanWindowSize = timespanWindowSize;
            this.PartitionKey = PartitionKey;
            this.ConsolidateStreams = consolidateStreams;
        }

        public TimeSpan TimespanWindowSize { get; set; }
        public bool ConsolidateStreams { get; set; }
        public string PartitionKey { get; set; }

        public static StreamCollectionLogPropertyHeader From(StreamCollectionLog streamCollection)
        {
            return new StreamCollectionLogPropertyHeader(streamCollection.TimespanWindowSize, streamCollection.Partition.Key, streamCollection.ConsolidateStreams);
        }
    }
}