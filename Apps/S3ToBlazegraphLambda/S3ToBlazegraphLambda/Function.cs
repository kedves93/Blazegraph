using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.Rekognition.Model;
using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using VDS.RDF.Storage;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace S3ToBlazegraphLambda
{
    public class Function
    {
        /// <summary>
        /// Blazegraph endpoint URL.
        /// </summary>
        public const string BLAZEGRAPH_ENDPOINT = "http://ec2-18-197-189-202.eu-central-1.compute.amazonaws.com:9999/blazegraph/";

        private readonly IAmazonS3 _s3Client;

        private readonly BlazegraphConnector _blazegraph;

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            _s3Client = new AmazonS3Client();
            _blazegraph = new BlazegraphConnector(BLAZEGRAPH_ENDPOINT);
        }

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used
        /// to respond to S3 notifications.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(S3Event s3Event, ILambdaContext context)
        {
            foreach (var record in s3Event.Records)
            {
                try
                {
                    using (GetObjectResponse response = await _s3Client.GetObjectAsync(new GetObjectRequest()
                    {
                        BucketName = record.S3.Bucket.Name,
                        Key = record.S3.Object.Key
                    }))
                    {
                        using (StreamReader reader = new StreamReader(response.ResponseStream))
                        {
                            var faceDetails = JsonConvert.DeserializeObject<IEnumerable<FaceDetail>>(reader.ReadToEnd());
                            context.Logger.LogLine(reader.ReadToEnd());
                        }
                    }
                }
                catch (AmazonS3Exception ex)
                {
                    context.Logger.LogLine(ex.Message);
                }
            }
        }
    }
}