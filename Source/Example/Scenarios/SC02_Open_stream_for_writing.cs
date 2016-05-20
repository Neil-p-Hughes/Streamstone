using System;
using System.Linq;

using Streamstone;
using Streamstone.StreamCollection;

namespace Example.Scenarios
{
    public class SC02_Open_stream_for_writing : Scenario
    {
        public override void Run()
        {
            OpenNonExistingStream();
            OpenExistingStream();
        }

        void OpenNonExistingStream()
        {
            try
            {
                StreamCollectionDictionary.Open(Partition);
            }
            catch (StreamNotFoundException)
            {
                Console.WriteLine("Opening non-existing stream will throw StreamNotFoundException");
            }
        }

        void OpenExistingStream()
        {
            StreamCollectionDictionary.Provision(Partition);

            var streamCollection = StreamCollectionDictionary.Open(Partition);

            Console.WriteLine("Opened existing (empty) stream in partition '{0}'", streamCollection.Partition);
            Console.WriteLine("Etag: {0}", streamCollection.ETag);
        }
    }
}
