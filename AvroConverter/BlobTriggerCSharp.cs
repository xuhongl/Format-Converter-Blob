using System;
using System.IO;
using System.Text;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Hadoop.Avro;
using Microsoft.Hadoop.Avro.Container;
using Newtonsoft.Json;

namespace Company.Function
{
    public static class BlobTriggerCSharp
    {
        [FunctionName("BlobTriggerCSharp")]
        //public static void Run([BlobTrigger("pestore/{name}", Connection = "avroconverter1015_STORAGE")]Stream myBlob, string name, ILogger log)
        public static void Run(
        [BlobTrigger("logsubstance/{name}", Connection = "avroconverter1015_STORAGE")]Stream myBlob, 
        [Blob("logsubstance2/validationResult.json", FileAccess.Write, Connection = "avroconverter1015_STORAGE")] TextWriter validationOutput,
        string name, ILogger log)
        {

            //log.LogInformation($"File type is: {Path.GetExtension(name)}");
            if (Path.GetExtension(name) == ".avro")
            {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
           

            using (var reader = AvroContainer.CreateGenericReader(myBlob))
            {
                while (reader.MoveNext())
                {
                    foreach (dynamic record in reader.Current.Objects)
                    {
                        var sequenceNumber = record.SequenceNumber;
                        var Offset = record.Offset;
                        var EnqueuedTimeUtc = record.EnqueuedTimeUtc;
                        var SystemProperties = record.SystemProperties;
                        var Properties = record.Properties;

                        var bodyText = Encoding.UTF8.GetString(record.Body);
                        Console.WriteLine($@"Before: 
                        ""sequenceNumber"": {sequenceNumber}, 
                        ""Offset"": {Offset}, 
                        ""EnqueuedTimeUtc"": {EnqueuedTimeUtc}, 
                        ""SystemProperties"": {SystemProperties}, 
                        ""Properties"": {Properties}, 
                        ""bodyText"": {bodyText}");

                        // Console.WriteLine($@"After:
                        // [Audit]
                        // ""utcEventTime"": {EnqueuedTimeUtc},
                        // ""SystemProperties"": {SystemProperties},
                        // ""Properties"": {Properties},
                        // ""touchedFields"": {bodyText}");

                        //validationOutput.WriteLine("hello world");
                        validationOutput.WriteLine($@"[Audit]
                        ""utcEventTime"": {EnqueuedTimeUtc},
                        ""SystemProperties"": {SystemProperties},
                        ""Properties"": {Properties},
                        ""touchedFields"": {bodyText}");

                    }
                }
            }

            }

        }
    }
}
