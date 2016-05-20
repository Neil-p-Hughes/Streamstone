using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.StreamCollection
{
    public class StreamCollectionStreamEntity : TableEntity
    {

        public StreamCollectionStreamEntity()
        {
        }

        public StreamCollectionStreamEntity(string partitionKey, string rowKey, string etag, int originalStreamVersion, int currentStreamVersion, string streamKey)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            ETag = etag;
            CurrentStreamVersion = currentStreamVersion;
            OriginalStreamVersion = originalStreamVersion;

            StreamKey = streamKey;
        }

        public int CurrentStreamVersion     { get; set; }
        public int OriginalStreamVersion    { get; set; }
        public string StreamKey             { get; set; }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);
        }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var result = base.WriteEntity(operationContext);
            return result;
        }

        public static StreamCollectionStreamEntity From(DynamicTableEntity entity)
        {
            return new StreamCollectionStreamEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                ETag = entity.ETag,
                Timestamp = entity.Timestamp,
                CurrentStreamVersion = (int)entity["CurrentStreamVersion"].PropertyAsObject,
                OriginalStreamVersion = (int)entity["OriginalStreamVersion"].PropertyAsObject,
                StreamKey = (string)entity["StreamKey"].PropertyAsObject,
            };
        }
    }
}