using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.StreamCollection
{
    class StreamCollectionDictionaryPropertyHeader 
    {


        public StreamCollectionDictionaryPropertyHeader()
        {
        }

        public StreamCollectionDictionaryPropertyHeader(int PartitionSize, string PartitionKey)
        {
            this.PartitionSize = PartitionSize;
            this.PartitionKey = PartitionKey;
        }

        public int PartitionSize { get; set; }
        public string PartitionKey { get; set; }

        public static StreamCollectionDictionaryPropertyHeader From(StreamCollectionDictionary streamCollection)
        {
            return new StreamCollectionDictionaryPropertyHeader(streamCollection.PartitionSize, streamCollection.Partition.Key);
        }
    }
}