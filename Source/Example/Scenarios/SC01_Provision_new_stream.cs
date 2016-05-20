using System;
using System.Linq;

using Streamstone;
using Streamstone.StreamCollection;

namespace Example.Scenarios
{
    public class SC01_Provision_new_stream : Scenario
    {
        public override void Run()
        {
            var streamCollection = StreamCollectionDictionary.Provision(Partition);

            Console.WriteLine("Provisioned new empty stream in partition '{0}'", streamCollection.Partition);
            Console.WriteLine("Etag: {0}", streamCollection.ETag);

            var exists = StreamCollectionDictionary.Exists(Partition);
            Console.WriteLine("Checking stream exists in a storage: {0}", exists);
        }
    }
}
